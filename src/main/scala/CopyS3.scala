import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}
import cats.implicits.toShow
import eu.timepit.refined._
//import eu.timepit.refined.auto._ needed for readFileMultipart
import eu.timepit.refined.collection.NonEmpty
import fs2.aws.s3.S3
import fs2.aws.s3.models.Models.{BucketName, FileKey}
import fs2.data.text.utf8.byteStreamCharLike
import fs2.data.xml.XmlEvent.{EndTag, StartTag}
import fs2.data.xml.xpath.XPathParser
import fs2.data.xml.{XmlEvent, events, xpath}
import fs2.{Collector, Pipe, Pull, Stream}
import io.laserdisc.pure.s3.tagless.{
  S3AsyncClientOp,
  Interpreter => S3Interpreter
}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient

import java.nio.file.{Path, Paths}

object CopyS3 {

  def extractTag[F[_], XmlEvent](
      tag: String
  ): Pipe[F, XmlEvent, List[XmlEvent]] = {

    def go(
        s: Stream[F, XmlEvent],
        state: (Boolean, List[XmlEvent])
    ): Pull[F, List[XmlEvent], Unit] = {
      val s2: fs2.Stream[F, XmlEvent] = s
      // uncons1 to be replaced by uncons to improve performance
      s2.pull.uncons1.flatMap {
        case Some((hd, tl)) =>
          (state._1, hd) match {
            case (false, StartTag(name, _, _)) if name.local == tag =>
              go(tl: Stream[F, XmlEvent], (true, List(hd)))
            case (false, _) => go(tl: Stream[F, XmlEvent], state)
            case (true, StartTag(name, _, _)) if name.local == tag =>
              assert(false, "unexpected tag"); Pull.done
            case (true, EndTag(name)) if name.local == tag =>
              Pull.output1(state._2 ++ List(hd)) >> go(
                tl: Stream[F, XmlEvent],
                (false, List())
              )
            case (true, _) =>
              go(tl: Stream[F, XmlEvent], (true, state._2 ++ List(hd)))
          }

        case None => Pull.done
      }
    }

    (in: Stream[F, XmlEvent]) => go(in, (false, List[XmlEvent]())).stream
  }

  def xmlElementstoString(xmlEvents: List[XmlEvent]) =
    xmlEvents.map(_.show).mkString("").filter(_ != '\n')

  def group[T](n: Int)(in: Stream[IO, T]): Stream[IO, List[T]] = {
    val t = in.take(n).compile.toList.unsafeRunSync()
    t.size match {
      case s if s < n => Stream.emit(t)
      case _          => Stream.emit(t) ++ group(n)(in.drop(n))
    }
  }

  private def s3StreamResource: Resource[IO, S3AsyncClientOp[IO]] =
    for {
      s3 <- S3Interpreter[IO].S3AsyncClientOpResource(
        S3AsyncClient
          .builder()
          .region(Region.US_EAST_1)
      )
    } yield s3

  private val s3Prefix = "s3://"

  def extractBucketNameAndFileKey(
      source: String
  ): Either[String, (BucketName, FileKey)] =
    if (
      source.length < s3Prefix.length || source.substring(
        0,
        s3Prefix.length()
      ) != s3Prefix
    )
      Left(s"Invalid S3 URI, missing prefix: $source")
    else {
      val i = source.indexOf("/", s3Prefix.length())
      val bucketNameStr =
        source.substring(s3Prefix.length(), if (i < 0) source.length() else i)
      val fileKeyStr = if (i < 0) "" else source.substring(i + 1)
      (refineV[NonEmpty](bucketNameStr), refineV[NonEmpty](fileKeyStr)) match {
        case (Right(bn), Right(fk)) => Right((BucketName(bn), FileKey(fk)))
        case (Left(_), _) => Left(s"Invalid S3 URI, empty bucket name: $source")
        case (_, _)       => Left(s"Invalid S3 URI, empty file key: $source")
      }
    }

  def destinationFileKey(source: FileKey, tag: String, n: Long): FileKey = {
    val sourceStr = source.value.value
    val i = sourceStr.lastIndexOf(".")
    val destinationPrefix =
      if (i < 0) sourceStr
      else sourceStr.substring(0, i)
    val name = Paths.get(destinationPrefix).getFileName
    val destinationStr = Path.of(sourceStr, tag, s"$name-$n.txt").toString
    refineV[NonEmpty](destinationStr)
      .map(FileKey.apply)
      .getOrElse(
        // by construction the string should never be empty
        throw new IllegalArgumentException(
          s"Invalid NonEmptyString '$destinationStr'"
        )
      )
  }

  def successFileKey(source: FileKey, tag: String): FileKey = {
    val sourceStr = source.value.value
    val name = Paths.get(sourceStr).getParent.getFileName
    val ret = Path.of(sourceStr, tag, "_SUCCESS").toString
    refineV[NonEmpty](ret)
      .map(FileKey.apply)
      .getOrElse(
        // by construction the string should never be empty
        throw new IllegalArgumentException(
          s"Invalid NonEmptyString '$ret'"
        )
      )
  }

  def lineCollector(): Collector.Aux[XmlEvent, String] =
    new Collector[XmlEvent] {
      type Out = String
      def newBuilder: Collector.Builder[XmlEvent, Out] =
        new LineRenderer()
    }

  def apply(
      source: String,
      destination: String,
      tag: String = "resnet"
  ): Option[String] = {
    val bucketNameAndFileKey = extractBucketNameAndFileKey(source)
    val destinationBucketNameAndFileKey = extractBucketNameAndFileKey(
      destination
    )

    val parsedXPath = XPathParser.either("//" + tag)

    (bucketNameAndFileKey, destinationBucketNameAndFileKey, parsedXPath) match {
      case (
            Right((sourceBucket, sourceKey)),
            Right((destinationBucket, destinationKey)),
            Right(path)
          ) =>
        (IO(println(s"Starting - ${java.time.LocalDateTime.now()}"))
          *>
            s3StreamResource
              .map(S3.create[IO])
              .use { s3 =>
                val csv = s3
                  .readFile(sourceBucket, sourceKey)
                  //                  .readFileMultipart(sourceBucket, sourceKey, partSize = 16)
                  .through(events[IO, Byte]())
                  .through(
                    xpath.filter.collect(
                      path,
                      lineCollector,
                      deterministic = false,
                      maxNest = 0
                    )
                  )
                  .chunkN(10000)
                csv.zipWithIndex
                  // could also use prefetch here
                  // could try parEvalMap
                  .prefetchN(1)
                  .parEvalMapUnordered(1) { case (s, i) =>
                    val oFileKey = destinationFileKey(destinationKey, tag, i)
                    // use cats logging instead
                    (IO(
                      println(
                        s"Store to S3 $oFileKey - ${java.time.LocalDateTime.now()}"
                      )
                    )
                      *>
                        Stream
                          .emits(
                            s.toList.reduce(_ + "\n" + _).getBytes("UTF-8")
                          )
                          .through(
                            s3.uploadFile(destinationBucket, oFileKey)
                          )
                          .compile
                          .drain)
                  }
                  .compile
                  .count

              }
              .flatMap(count =>
                s3StreamResource
                  .map(S3.create[IO])
                  .use { s3 =>
                    Stream
                      .emits(count.toString.getBytes("UTF-8"))
                      .lift[IO]
                      .through(
                        s3.uploadFile(
                          destinationBucket,
                          successFileKey(destinationKey, tag)
                        )
                      )
                      .compile
                      .string

                  }
              )).unsafeRunSync()
        None

      case (Left(error), _, _) =>
        Some(s"Error: $error")

      case (_, Left(error), _) =>
        Some(s"Error: $error")

      case (_, _, Left(error)) =>
        Some(s"Error invalid xpath: $error")
    }
  }
}

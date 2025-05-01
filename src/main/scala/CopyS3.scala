import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}
import cats.implicits.toShow
import eu.timepit.refined._
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import fs2.aws.s3.S3
import fs2.aws.s3.models.Models.{BucketName, FileKey}
import fs2.data.text.utf8.byteStreamCharLike
import fs2.data.xml.XmlEvent
import fs2.data.xml.XmlEvent.{EndTag, StartTag}
import fs2.{Collector, Pipe, Pull, Stream}
import io.laserdisc.pure.s3.tagless.{
  S3AsyncClientOp,
  Interpreter => S3Interpreter
}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient

import scala.concurrent.duration.DurationInt

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

  def destinationFileKey(source: FileKey, n: Long): FileKey = {
    val sourceStr = source.value.value
    val i = sourceStr.lastIndexOf(".")
    val destinationStr =
      if (i < 0) s"$sourceStr-$n.csv"
      else s"${sourceStr.substring(0, i)}-$n.csv"
    refineV[NonEmpty](destinationStr)
      .map(FileKey.apply)
      .getOrElse(
        // by construction the string should never be empty
        throw new IllegalArgumentException(
          "Invalid NonEmptyString for fileKey"
        )
      )
  }

  def lineCollector(): Collector.Aux[XmlEvent, String] =
    new Collector[XmlEvent] {
      type Out = String
      def newBuilder: Collector.Builder[XmlEvent, Out] =
        new LineRenderer()
    }

  def apply(source: String, destination: String): Option[String] = {
    val bucketNameAndFileKey = extractBucketNameAndFileKey(source)
    val destinationBucketNameAndFileKey = extractBucketNameAndFileKey(
      destination
    )
    import fs2.data.xml._
    import fs2.data.xml.xpath.literals._
    val path = xpath"//resnet"

    (bucketNameAndFileKey, destinationBucketNameAndFileKey) match {
      case (
            Right((sourceBucket, sourceKey)),
            Right((destinationBucket, destinationKey))
          ) =>
        (IO(println(s"Starting - ${java.time.LocalDateTime.now()}"))
          *>
            s3StreamResource
              .map(S3.create[IO])
              .use { s3 =>
                val csv = s3
                  .readFileMultipart(sourceBucket, sourceKey, 10)
                  .through(events[IO, Byte]())
                  .through(
                    xpath.filter.collect(
                      path,
                      lineCollector,
                      deterministic = false,
                      maxNest = 0
                    )
                  )
                  .prefetch
                  // .chunkN(1000) not lazy, seems to load the whole file before starting to emit chunks
                  .groupWithin(
                    10000,
                    1.second
                  )
                csv.zipWithIndex
                  // could also use prefetch here
                  // could try parEvalMap
                  .parEvalMap(4) { case (s, i) =>
                    val oFileKey = destinationFileKey(destinationKey, i)
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
                  .drain

              })
          .unsafeRunSync()
        None

      case (Left(error), _) =>
        Some(s"Error: $error")

      case (_, Left(error)) =>
        Some(s"Error: $error")
    }
  }
}

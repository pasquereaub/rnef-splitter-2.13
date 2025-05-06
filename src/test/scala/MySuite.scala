import cats.effect.IO
import cats.effect.unsafe.implicits.global
import eu.timepit.refined._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.boolean._
import eu.timepit.refined.collection._
import fs2._
import fs2.aws.s3.models.Models.{BucketName, FileKey}
import fs2.data.xml.XmlEvent.{EndTag, StartTag, XmlString}
import fs2.data.xml.{Attr, QName, XmlInterpolators}

class MySuite extends munit.FunSuite {
  test("extractTag should extract all sections with the specified tag") {
    val input = xml"""<root>
        <a attr="value">
          <b>text1</b>
        </a>
        <a>text2</a>
      </root>"""

    val expected =
      List(
        List(
          StartTag(
            name = QName(
              prefix = None,
              local = "a"
            ),
            attributes = List(
              Attr(
                name = QName(
                  prefix = None,
                  local = "attr"
                ),
                value = List(
                  XmlString(
                    s = "value",
                    isCDATA = false
                  )
                )
              )
            ),
            isEmpty = false
          ),
          XmlString(
            s = """
          """,
            isCDATA = false
          ),
          StartTag(
            name = QName(
              prefix = None,
              local = "b"
            ),
            attributes = Nil,
            isEmpty = false
          ),
          XmlString(
            s = "text1",
            isCDATA = false
          ),
          EndTag(
            name = QName(
              prefix = None,
              local = "b"
            )
          ),
          XmlString(
            s = """
        """,
            isCDATA = false
          ),
          EndTag(
            name = QName(
              prefix = None,
              local = "a"
            )
          )
        ),
        List(
          StartTag(
            name = QName(
              prefix = None,
              local = "a"
            ),
            attributes = Nil,
            isEmpty = false
          ),
          XmlString(
            s = "text2",
            isCDATA = false
          ),
          EndTag(
            name = QName(
              prefix = None,
              local = "a"
            )
          )
        )
      )

    val result =
      input
        .lift[IO]
        .through(CopyS3.extractTag("a"))
        .compile
        .toList
        .unsafeRunSync()
    assertEquals(result, expected)

  }

  test("groups the elements of a stream") {
    val t = Stream.emits(1 to 10).lift[IO]
    val expected = List(List(1, 2, 3), List(4, 5, 6), List(7, 8, 9), List(10))
    assertEquals(CopyS3.group(3)(t).compile.toList.unsafeRunSync(), expected)
  }

  test("toString formats a list of XmlElements") {
    val l = List(
      StartTag(
        name = QName(
          prefix = None,
          local = "a"
        ),
        attributes = Nil,
        isEmpty = false
      ),
      XmlString(
        s = "text2",
        isCDATA = false
      ),
      EndTag(
        name = QName(
          prefix = None,
          local = "a"
        )
      )
    )

    assertEquals(CopyS3.xmlElementstoString(l), "<a>text2</a>")
  }

  // There is a macro to create a refined type from a litteral (refineMV) but it doesn't seem to be
  // supported in Scala 3 yet. So we use refineV instead. (c.f. https://github.com/fthomas/refined/blob/c78c3e152f61a8eaf183ef1149a135808cc8294c/modules/core/shared/src/main/scala-3.0-/eu/timepit/refined/package.scala#L54
  // and https://github.com/fthomas/refined/blob/c78c3e152f61a8eaf183ef1149a135808cc8294c/modules/core/shared/src/main/scala-3.0%2B/eu/timepit/refined/package.scala#L12)
  def toRefinedName(bucketName: String): String Refined Not[Empty] = {
    val refinedName = refineV[NonEmpty](bucketName).getOrElse(
      throw new IllegalArgumentException(
        "Empty string"
      )
    )
    refinedName
  }

  test("correct bucketName but not key") {
    val uri = "s3://bucket-name"
    val expected = Left("Invalid S3 URI, empty file key: s3://bucket-name")

    assertEquals(CopyS3.extractBucketNameAndFileKey(uri), expected)
  }

  test("correct bucketName with key") {
    val bucketName = "s3://bucket-name/key"
    val expected = Right(
      (BucketName(toRefinedName("bucket-name")), FileKey(toRefinedName("key")))
    )

    assertEquals(CopyS3.extractBucketNameAndFileKey(bucketName), expected)
  }

  test("bucketName with missing prefix") {
    val bucketName = "bucket-name/key"
    val expected = Left("Invalid S3 URI, missing prefix: bucket-name/key")

    assertEquals(CopyS3.extractBucketNameAndFileKey(bucketName), expected)
  }

  test("bucketName with missing prefix - very short key") {
    val bucketName = "b/k"
    val expected = Left("Invalid S3 URI, missing prefix: b/k")

    assertEquals(CopyS3.extractBucketNameAndFileKey(bucketName), expected)
  }

  test("empty bucketName") {
    val bucketName = "s3:///key"
    val expected = Left("Invalid S3 URI, empty bucket name: s3:///key")

    assertEquals(CopyS3.extractBucketNameAndFileKey(bucketName), expected)
  }

  test("destinationFileKey with a short key") {
    val source = FileKey(toRefinedName("key.rnef"))
    val expected = FileKey(toRefinedName("key.rnef/tag/key-1.txt"))

    assertEquals(CopyS3.destinationFileKey(source, "tag", 1), expected)
  }

  test("destinationFileKey with a long key") {
    val source = FileKey(toRefinedName("path/key.rnef"))
    val expected = FileKey(toRefinedName("path/key.rnef/tag/key-1.txt"))

    assertEquals(CopyS3.destinationFileKey(source, "tag", 1), expected)
  }

  test("destinationFileKey without extension") {
    val source = FileKey(toRefinedName("path/key"))
    val expected = FileKey(toRefinedName("path/key/tag/key-1.txt"))

    assertEquals(CopyS3.destinationFileKey(source, "tag", 1), expected)
  }

  test("destinationFileKey with a long key and no index") {
    val source = FileKey(toRefinedName("path/key.rnef"))
    val expected = FileKey(toRefinedName("path/key.rnef/tag/key.txt"))

    assertEquals(CopyS3.destinationFileKey(source, "tag"), expected)
  }

  test("successFileKey happy path") {
    val source = FileKey(toRefinedName("path/key.rnef"))
    val expected = FileKey(toRefinedName("path/key.rnef/tag/_SUCCESS"))

    assertEquals(CopyS3.successFileKey(source, "tag"), expected)
  }

  test("CopyS3 incorrect tag") {
    val uri = "s3://bucket/key"
    val error =
      Some(
        "Error invalid xpath: fs2.data.xml.xpath.XPathSyntaxException: unexpected '+' at index 2, node selector was expected"
      )
    assertEquals(CopyS3.split(uri, uri, "+"), error)
  }
}

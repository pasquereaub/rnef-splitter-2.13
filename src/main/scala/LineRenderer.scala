/*
 * The Renderer shipped with fs2-data-xml returns formatted xml
 * (i.e. on multiple lines), here we need all the xml on one line
 * to process it with spark
 */

import cats.syntax.all._
import fs2.{Chunk, Collector, Pipe, Stream}
import fs2.data.xml.XmlEvent

class LineRenderer() extends Collector.Builder[XmlEvent, String] {

  private val builder = new StringBuilder

  override def +=(chunk: Chunk[XmlEvent]): Unit = {

    chunk.foreach {
      case e @ (XmlEvent.XmlDecl(_, _, _) | XmlEvent.XmlPI(_, _)) =>
        builder ++= e.show

      case XmlEvent.Comment(content) => // Do not include comments
      case XmlEvent.StartTag(name, attributes, isEmpty) =>
        val renderedName = name.show
        builder ++= show"<$renderedName"

        attributes match {
          case a :: as =>
            builder ++= show" $a"
            as.foreach { a =>
              builder += ' '
              builder ++= a.show
            }
          case Nil => // do nothing
        }
        if (isEmpty) {
          builder ++= "/>"
        } else {
          builder += '>'
        }

      case XmlEvent.EndTag(name) =>
        builder ++= show"</$name>"

      case XmlEvent.XmlString(content, true) =>
        builder ++= show"<![CDATA[$content]]>"

      case XmlEvent.StartDocument | XmlEvent.EndDocument =>
      // do nothing

      case e =>
        builder ++= e.show // Do I need to remove newlines to be on the safe side ?
    }
  }

  override def result: String =
    builder.result().filter('\n' != _) /// make sure the string is on one line

}

object LineRenderer {

  def pipe[F[_]](
      pretty: Boolean,
      collapseEmpty: Boolean,
      indent: String,
      attributeThreshold: Int
  ): Pipe[F, XmlEvent, String] =
    in =>
      Stream.suspend(Stream.emit(new LineRenderer())).flatMap { builder =>
        in.mapChunks { chunk =>
          builder += chunk
          Chunk.singleton(builder.result)
        }

      }

}

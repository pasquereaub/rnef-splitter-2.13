object Main {
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("Usage: CopyS3 <sourceURI> <destURI> <tag>")
      System.exit(1)
    }

    CopyS3(args(0), args(1), args(2)) match {
      case Some(error) =>
        println(s"Error: $error")
        System.exit(1)
      case None =>
        System.exit(0)
    }
  }
}

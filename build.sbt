lazy val root = project
  .in(file("."))
  .settings(
    name := "rnef-splitter",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := "2.13.16",

    libraryDependencies ++= Seq(
      "org.gnieh" %% "fs2-data-xml" % "1.11.2",
      "io.laserdisc" %% "fs2-aws-s3" % "6.2.0",
      "org.scalameta" %% "munit" % "1.0.0" % Test
    )

  )

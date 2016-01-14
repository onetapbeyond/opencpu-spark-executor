lazy val root = (project in file(".")).
  settings(
    name := "sentiment-analysis",
    organization := "io.onetapbeyond",
    version := "1.0",
    scalaVersion := "2.10.6",
    libraryDependencies ++= Seq(
      "io.onetapbeyond" % "opencpu-spark-executor_2.10" % "1.0",
      "org.apache.spark" % "spark-core_2.10" % "1.6.0" % "provided"
    ),
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
  )
  
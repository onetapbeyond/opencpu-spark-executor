lazy val root = (project in file(".")).
  settings(
    name := "opencpu-spark-executor",
    organization := "io.onetapbeyond",
    version := "1.0",
    scalaVersion := "2.10.6",
    libraryDependencies ++= Seq(
    	"io.onetapbeyond" % "opencpu-r-executor" % "1.1",
    	"org.apache.spark" % "spark-core_2.10" % "1.5.2" % "provided",
    	"org.scalatest" % "scalatest_2.10" % "2.2.4" % "test"
	)
  )

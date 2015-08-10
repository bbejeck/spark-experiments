name := "spark-experiments"

version := "1.0"

scalaVersion := "2.10.5"

val sparkVersion = "1.4.1"

libraryDependencies ++= Seq(
  //"org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming-kafka" % sparkVersion,
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "com.holdenkarau" %% "spark-testing-base" % "1.2.0_1.1.1_0.0.6" % "test"
)
    
name := "spark-experiments"

version := "1.0"

scalaVersion := "2.10.5"

val sparkVersion = "1.5.1"

libraryDependencies ++= Seq(
  //"org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming-kafka" % sparkVersion,
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "com.holdenkarau" % "spark-testing-base_2.10" % "1.5.0_1.4.0_1.4.1_0.1.2"
)
    
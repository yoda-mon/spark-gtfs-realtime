name := "spark-simple-app"

version := "0.1"

scalaVersion := "2.13.7"

val sparkVersion = "3.2.0"
val hadoopVersion = "3.3.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.hadoop" % "hadoop-aws" % hadoopVersion,
  "io.mobilitydata.transit" % "gtfs-realtime-bindings" % "0.0.5",
)

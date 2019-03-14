name := "kakutaku"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.1"


parallelExecution in ThisBuild := false


libraryDependencies += "org.json4s" %% "json4s-native" % "3.2.11"


libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion //% "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion //% "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion //% "provided"
libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion //% "provided"

// https://mvnrepository.com/artifact/com.redislabs/spark-redis
libraryDependencies += "com.redislabs" % "spark-redis" % "2.3.0"


libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.5" % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"


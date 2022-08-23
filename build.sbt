name := "clickstream-data-pipeline"

version := "1.0.0"
scalaVersion := "2.11.8"
val sparkVersion = "2.3.2"
val jacksonCore = "2.6.7"
val scope = "compile"

libraryDependencies ++= Seq(
//  //dependency for unit testing
//  "org.scalatest" %% "scalatest" % "3.2.13" % "test",
//
  "org.scalamock" %% "scalamock" % "4.4.0" % Test,
  //dependency of reading configuration
  "com.typesafe" % "config" % "1.3.3",
  //spark core libraries, in the production or for spark-submit in local add provided so that dependent jar is not part of assembly jar
  // ex :"org.apache.spark" %% "spark-core" % sparkVersion % provided,
  "org.apache.spark" %% "spark-core" % sparkVersion % scope,
  "org.apache.spark" %% "spark-sql" % sparkVersion % scope,
  "org.apache.spark" %% "spark-hive" % sparkVersion % scope,
  "org.scala-lang" % "scala-library" % scalaVersion.value,
  "com.fasterxml.jackson.core" % "jackson-core" % jacksonCore % scope,
  //logging library
  "org.slf4j" % "slf4j-api" % "1.7.29",
  //for doing testing
  "org.scalatest" %% "scalatest" % "3.1.0" % Test,
  "mysql" % "mysql-connector-java" % "8.0.30"
)


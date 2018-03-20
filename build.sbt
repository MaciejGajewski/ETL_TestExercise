name := "ETL_TestExercise"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= {
  val sparkVer = "2.2.1"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVer withSources(),
    "org.apache.spark" %% "spark-sql" % sparkVer,
    //"com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
    //"ch.qos.logback" % "logback-classic" % "1.1.2",
    "org.elasticsearch" % "elasticsearch-hadoop" % "6.2.2"
    //"com.typesafe.akka" %% "akka-actor" % "2.4.7",
    //"com.typesafe.akka" %% "akka-http" % "10.1.0"
  )
}
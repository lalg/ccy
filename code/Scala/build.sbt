
ThisBuild / version := "0.1.0"
ThisBuild / scalaVersion := "2.13.8"
ThisBuild / organization := "com.lalg"

scalacOptions := Seq("-unchecked", "-deprecation")

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}

/*
 val sparkDir =  settingKey[String]("sparkDir")
 // added using lib directory
Compile / unmanagedJars ++= {
  sparkDir := "/Users/lg/addon/spark-3.3.0-bin-hadoop3-scala2.13/jars"
  
  (sparkDir.value ** "*.jar").classpath
 }
 */

val scalaTest = "org.scalatest" %% "scalatest" % "3.2.7" % "provided"
val sparkCore = "org.apache.spark" %% "spark-core" % "3.3.0" % "provided"
val sparkMLlib = "org.apache.spark" %% "spark-mllib" % "3.3.0" % "provided"
val hadoopfs = "org.apache.hadoop" % "hadoop-hdfs" % "3.3.0" % "provided"
val yahoo = "com.yahoofinance-api" % "YahooFinanceAPI" % "3.17.0"
val scallop = "org.rogach" %% "scallop" % "4.1.0"

lazy val ccy =
  (project in file("."))
    .settings(
      name := "CCY",
      libraryDependencies += sparkCore,
      libraryDependencies += sparkMLlib,
      libraryDependencies += hadoopfs,
      libraryDependencies += scalaTest,
      libraryDependencies += yahoo,
      libraryDependencies += scallop,
      libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.0.9")
 


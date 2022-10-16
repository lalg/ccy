
ThisBuild / version := "0.1.0"
ThisBuild / scalaVersion := "2.13.8"
ThisBuild / organization := "com.lalg"


/*
 val sparkDir =  settingKey[String]("sparkDir")
 // added using lib directory
Compile / unmanagedJars ++= {
  sparkDir := "/Users/lg/addon/spark-3.3.0-bin-hadoop3-scala2.13/jars"
  
  (sparkDir.value ** "*.jar").classpath
 }
 */

val scalaTest = "org.scalatest" %% "scalatest" % "3.2.7" % Test
val sparkCore = "org.apache.spark" %% "spark-core" % "3.3.0"
val sparkMLlib = "org.apache.spark" %% "spark-mllib" % "3.3.0"
val hadoopfs = "org.apache.hadoop" % "hadoop-hdfs" % "3.3.0" % Test

lazy val ccy =
  (project in file("."))
    .settings(
      name := "CCY",
      libraryDependencies += sparkCore,
      libraryDependencies += sparkMLlib,
      libraryDependencies += hadoopfs,
      libraryDependencies += scalaTest)


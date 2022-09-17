ThisBuild / version := "0.1.0"
ThisBuild / scalaVersion := "2.13.8"
ThisBuild/ organization := "com.lalg"

val scalaTest = "org.scalatest" %% "scalatest" % "3.2.7" % Test

lazy val ccy =
  (project in file("."))
    .settings(
      name := "CCY",
      libraryDependencies += scalaTest)


name := "MapReduce for Scala Collections"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.9.2"

scalacOptions += "-deprecation"

libraryDependencies ++= Seq(
  "org.scalaz" %% "scalaz-core" % "6.0.4",
  "org.scalatest" %% "scalatest" % "1.8" % "test"
)

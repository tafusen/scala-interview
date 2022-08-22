ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "scala-hw-melih",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.8",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.8",
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % Test
  )

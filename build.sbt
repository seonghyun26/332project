ThisBuild / organization := "univ.postech.csed-332.team-cyan"
ThisBuild / scalaVersion := "2.12.17"
ThisBuild / scalacOptions += "-release:11"

version := "0.1.0"
name := "DistSort"

lazy val network = (project in file ("network"))
lazy val master = (project in file("master"))
  .settings(
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.7" % Test,
  )
  .dependsOn(network)
lazy val worker = (project in file("worker"))
  .dependsOn(network)
ThisBuild / organization := "univ.postech.csed-332.team-cyan"
ThisBuild / scalaVersion := "2.12.17"
ThisBuild / scalacOptions += "-release:11"

version := "0.1.0"
name := "DistSort"

lazy val scalatest = "org.scalatest" %% "scalatest" % "3.2.7"

lazy val network = (project in file ("network"))
lazy val master = (project in file("master"))
  .dependsOn(network)
  .settings(
    libraryDependencies += scalatest % Test,
    assembly / assemblyJarName := "master.jar",
  )
lazy val worker = (project in file("worker"))
  .dependsOn(network)
  .settings(
    libraryDependencies += scalatest % Test,
    assembly / assemblyJarName := "worker.jar",
  )

lazy val project_root = (project in file ("."))
  .aggregate(master, worker, network)
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    libraryDependencies += scalatest % "it",
    run / aggregate := false,
  )
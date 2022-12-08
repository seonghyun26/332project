ThisBuild / organization := "univ.postech.csed-332.team-cyan"
ThisBuild / scalaVersion := "2.12.17"
// ThisBuild / scalacOptions += "-release:11"

version := "0.1.0"
name := "DistSort"

lazy val scalatest = "org.scalatest" %% "scalatest" % "3.2.7"

ThisBuild / libraryDependencies ++= Seq(
    "io.grpc" % "grpc-netty" % scalapb.compiler.Version.grpcJavaVersion,
    "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion,
    scalatest % Test
  )

lazy val root = (project in file("."))
  .aggregate(master, worker, network, common)
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    libraryDependencies += scalatest % "it,test",
    run / aggregate := false,
  )
  .dependsOn(master, worker, network, common)

lazy val common = (project in file("common"))
lazy val network = (project in file ("network"))
lazy val master = (project in file("master"))
  .dependsOn(network, common)
lazy val worker = (project in file("worker"))
  .dependsOn(network, common)

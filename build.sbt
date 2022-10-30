ThisBuild / organization := "univ.postech.csed-332.team-cyan"
ThisBuild / scalaVersion := "2.12.17"
ThisBuild / scalacOptions += "-release:11"

lazy val master = (project in file("master"))
lazy val worker = (project in file("worker"))

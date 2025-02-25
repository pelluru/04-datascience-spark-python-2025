ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.11"

lazy val root = (project in file("."))
  .settings(
    name := "movie-rating",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.7",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7"

  )

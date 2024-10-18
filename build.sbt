ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "data-cleaner-scala",
    idePackagePrefix := Some("de.datacleaner.spark")
  )


val sparkVersion = "3.5.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scalatest" %% "scalatest" % "3.2.17" % "test",
  "software.amazon.awssdk" %% "s3" % "2.19.31" % sparkVersion
)

name := "test-application"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.0"

lazy val root = (project in file("."))
  .settings(
    name := "test-application",
    libraryDependencies ++= Seq(
      // Spark Dependencies
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
      // For reading Excel files from Spark
      "com.crealytics" %% "spark-excel" % "0.12.5",
      // Tests
      "org.scalatest" %% "scalatest" % "3.1.0" % "test",
    )
  )
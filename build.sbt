ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.16"

lazy val root = (project in file("."))
  .settings(
    name := "SwS-Course",
    idePackagePrefix := Some("com.aciesidle.training")
  )

/** @note
  *   version can be declared in variable of immutable type for common version
  *   maintenance.
  */
val sparkVersion = "3.5.4"

/** @note
  *   %% => means that this will spark-core automatically pick version with
  *   scala-version. for specific version we can declare version like this
  *   "org.apache.spark" %% "spark-sql" % "3.5.2".
  *
  * one can also inject dependencies like libraryDependencies +=
  * "org.apache.spark" %% "spark-core" % "3.5.4" libraryDependencies +=
  * "org.apache.spark" %% "spark-sql" % "3.5.4" % "provided" Or
  * libraryDependencies ++= Seq("dependency1", dependency2)
  */

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scalatest" %% "scalatest" % "3.2.19" % "test"
)

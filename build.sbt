ThisBuild / scalaVersion := "2.12.15"

val sparkVersion = "3.3.0"
val adbcVersion = "0.1.0"

lazy val root = (project in file("."))
  .settings(
    name := "spark-adbc",
    libraryDependencies ++= Seq(
      "org.apache.arrow.adbc" % "adbc-core" % adbcVersion,
      "org.apache.arrow.adbc" % "adbc-driver-manager" % adbcVersion,
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.arrow.adbc" % "adbc-driver-jdbc" % adbcVersion % Test,
      "org.apache.derby" % "derby" % "10.14.2.0" % Test,
      "org.scalatest" %% "scalatest-funsuite" % "3.2.15" % Test
    )
  )
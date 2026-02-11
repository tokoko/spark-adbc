ThisBuild / scalaVersion := "2.13.17"

val sparkVersion = "4.1.1"
val adbcVersion = "0.22.0"
val arrowVersion = "18.3.0"

lazy val root = (project in file("."))
  .settings(
    name := "spark-adbc",
    libraryDependencies ++= Seq(
      "org.apache.arrow.adbc" % "adbc-core" % adbcVersion,
      "org.apache.arrow.adbc" % "adbc-driver-manager" % adbcVersion,
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.arrow.adbc" % "adbc-driver-jni" % adbcVersion % Test,
      "org.apache.arrow" % "arrow-memory-netty" % arrowVersion % Test,
      "org.xerial" % "sqlite-jdbc" % "3.47.2.0" % Test,
      "org.scalatest" %% "scalatest-funsuite" % "3.2.15" % Test
    ),
    Test / fork := true,
    Test / javaOptions ++= Seq(
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
      "--add-opens=jdk.unsupported/sun.misc=ALL-UNNAMED"
    )
  )
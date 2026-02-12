ThisBuild / scalaVersion := "2.13.17"

val sparkVersion = "4.1.1"
val adbcVersion = "0.22.0"
val arrowVersion = "18.3.0"

val javaOpens = Seq(
  "-Xmx8g",
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
      "org.apache.datafusion" %% "comet-spark-spark4.0" % "0.13.0" % Test,
      "org.scalatest" %% "scalatest-funsuite" % "3.2.15" % Test
    ),
    Test / fork := true,
    Test / javaOptions ++= javaOpens
  )

lazy val driverTests = (project in file("driver-tests"))
  .dependsOn(root)
  .settings(
    name := "spark-adbc-driver-tests",
    publish / skip := true,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.arrow.adbc" % "adbc-driver-jni" % adbcVersion % Test,
      "org.apache.arrow" % "arrow-memory-netty" % arrowVersion % Test,
      "org.xerial" % "sqlite-jdbc" % "3.47.2.0" % Test,
      "org.duckdb" % "duckdb_jdbc" % "1.4.4.0" % Test,
      "org.postgresql" % "postgresql" % "42.7.4" % Test,
      "org.testcontainers" % "postgresql" % "1.21.4" % Test,
      "org.testcontainers" % "mssqlserver" % "1.21.4" % Test,
      "com.microsoft.sqlserver" % "mssql-jdbc" % "12.8.1.jre11" % Test,
      "org.scalatest" %% "scalatest-funsuite" % "3.2.15" % Test
    ),
    Test / fork := true,
    Test / javaOptions ++= javaOpens
  )

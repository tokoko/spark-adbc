package com.tokoko.spark.adbc

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class AdbcDerbyWriteReadTest extends AnyFunSuite with BeforeAndAfterAll {

  private var df: DataFrame = _
  private val url = "jdbc:derby:derbyDB;create=true"

  override def beforeAll(): Unit = {
    import java.sql.DriverManager

    Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance
    val conn = DriverManager.getConnection(url)

    val stmt = conn.createStatement()

    // TODO catch exception if non-existent
    stmt.execute("DROP TABLE Employees")

    val query = """CREATE TABLE Employees(Id INT NOT NULL, Name VARCHAR(255), Salary INT NOT NULL)"""

    stmt.execute(query);

    val spark = SparkSession.builder().master("local").getOrCreate()

    val columns = Seq("ID","NAME", "SALARY")
    val data = Seq(
      (1, "Tornike", 2000),
      (2, "Robin", 3000)
    )

    df = spark.createDataFrame(data).toDF(columns:_*)
      .repartition(2)
  }

  test("") {
    df.write
      .format("com.tokoko.spark.adbc")
      .option("driver", "org.apache.arrow.adbc.driver.jdbc.JdbcDriver")
      .option("url", url)
      .option("dbtable", "Employees")
      .mode(SaveMode.Append)
      .save()

    val compare = df.sparkSession.read
      .format("com.tokoko.spark.adbc")
      .option("driver", "org.apache.arrow.adbc.driver.jdbc.JdbcDriver")
      .option("url", url)
      .option("query", "SELECT * FROM Employees")
      .load()

    df.show()
    compare.show()
    assert(df.count() == compare.count())
  }

}

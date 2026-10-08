package com.tokoko.spark.adbc

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowFileWriter
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.util.ArrowUtilsExtended

import java.io.{File, FileOutputStream}

/**
 * DataFusion keeps its catalog per database handle, so tables created in one connection
 * are invisible to the next. The fixtures are Arrow IPC files instead, referenced by path.
 * (Parquet would be the obvious choice, but DataFusion reads parquet strings as Utf8View,
 * which Spark's Arrow reader doesn't support.)
 */
class AdbcDatafusionTest extends AdbcTestBase {

  private val dir = new File("target/datafusion-fixtures").getAbsoluteFile

  override protected def engine: String = "datafusion"

  override protected def adbcParams: Map[String, Object] = Map("jni.driver" -> "datafusion")

  // The fixtures are files, not tables that could be ingested into.
  override protected def supportsWrite: Boolean = false

  override protected def sqlType(t: ColType): Option[String] = Some(s"arrow ${Fixtures.sparkType(t).simpleString}")

  override protected def tableRef(name: String): String = s"'${new File(dir, s"$name.arrow")}'"

  override protected def createTable(spec: TableSpec): Unit = {
    dir.mkdirs()
    val schema = fixtureSchema(spec)
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val root = VectorSchemaRoot.create(ArrowUtilsExtended.toArrowSchema(schema, "UTC"), allocator)
      try {
        val arrowWriter = ArrowWriter.create(root)
        frameOf(spec, schema).queryExecution.toRdd.map(_.copy()).collect().foreach(arrowWriter.write)
        arrowWriter.finish()
        val out = new FileOutputStream(new File(dir, s"${spec.name}.arrow"))
        try {
          val fileWriter = new ArrowFileWriter(root, null, out.getChannel)
          fileWriter.start()
          fileWriter.writeBatch()
          fileWriter.end()
        } finally out.close()
      } finally root.close()
    } finally allocator.close()
  }

}

package org.apache.spark.sql.util

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.ipc.{ReadChannel, WriteChannel}
import org.apache.arrow.vector.ipc.message.MessageSerializer
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.arrow.ArrowConverters
import org.apache.spark.sql.types.StructType

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, IOException}
import java.nio.channels.Channels
import java.util.Collections
import collection.JavaConverters._

object ArrowUtilsExtended {

  // TODO timezone is hardcoded in some methods

  def toArrowSchema(schema: StructType, timeZoneId: String): Schema = {
    ArrowUtils.toArrowSchema(schema, timeZoneId, errorOnDuplicatedFieldNames = true, largeVarTypes = false)
  }

  def fromArrowSchema(schema: Schema): StructType = {
    ArrowUtils.fromArrowSchema(schema)
  }

  def convertToArrowBatchRdd(df: DataFrame): RDD[Array[Byte]] = {
    df.toArrowBatchRdd
  }

  def fromBatchIterator(arrowBatchIter: Iterator[Array[Byte]],
                        schema: StructType,
                        timeZoneId: String,
                        context: TaskContext): Iterator[InternalRow] = {
    ArrowConverters.fromBatchIterator(arrowBatchIter, schema, timeZoneId, errorOnDuplicatedFieldNames = true, context)
  }

  def toBatchIterator(rowIter: Iterator[InternalRow],
                       schema: StructType,
                       maxRecordsPerBatch: Int,
                       timeZoneId: String,
                       context: TaskContext): Iterator[Array[Byte]] = {
    ArrowConverters.toBatchIterator(rowIter, schema, maxRecordsPerBatch, timeZoneId, errorOnDuplicatedFieldNames = true, context)
  }

  def toVectorSchemaRoot(rowIter: Iterator[InternalRow], schema: StructType): VectorSchemaRoot = {
    val it = toBatchIterator(rowIter, schema, 100, "UTC", TaskContext.get())

    val rootAllocator = new RootAllocator(Long.MaxValue)

    val root = VectorSchemaRoot.create(toArrowSchema(schema, "UTC"), rootAllocator)

    // TODO probably only populates vsr with the last RecordBatch
    it.foreach(r => {
      try {
        val arb = MessageSerializer.deserializeRecordBatch(
          new ReadChannel(Channels.newChannel(
            new ByteArrayInputStream(r)
          )), rootAllocator)
        val vectorLoader = new VectorLoader(root)
        vectorLoader.load(arb)
      } catch {
        case e: IOException => e.printStackTrace()
      }
    })

    root
  }

  def fromVectorSchemaRoot(root: VectorSchemaRoot): Iterator[InternalRow] = {
    val sparkSchema = ArrowUtils.fromArrowSchema(root.getSchema)
    val unloader = new VectorUnloader(root)
    val arb = unloader.getRecordBatch
    val out = new ByteArrayOutputStream
    val channel = new WriteChannel(Channels.newChannel(out))
    MessageSerializer.serialize(channel, arb)

    val iter = Collections.singletonList(out.toByteArray)

    fromBatchIterator(
      iter.iterator().asScala,
      sparkSchema,
      "UTC",
      //spark.sqlContext.sessionState.conf.sessionLocalTimeZone,
      TaskContext.get()
    )
  }

}
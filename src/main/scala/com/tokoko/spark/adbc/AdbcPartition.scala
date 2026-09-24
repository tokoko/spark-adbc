package com.tokoko.spark.adbc

import org.apache.spark.sql.connector.read.InputPartition

sealed trait AdbcPartition extends InputPartition

/** Executes a SQL query on the executor. */
case class AdbcQueryPartition(query: String) extends AdbcPartition

/** Reads a driver-produced partition descriptor (from `executePartitioned`) on the executor. */
case class AdbcDescriptorPartition(descriptor: Array[Byte]) extends AdbcPartition

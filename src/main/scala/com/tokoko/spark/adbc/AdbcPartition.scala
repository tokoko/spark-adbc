package com.tokoko.spark.adbc

import org.apache.spark.sql.connector.read.InputPartition

class AdbcPartition(val query: String) extends InputPartition

/*
 * Copyright 2024 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.oceanbase.spark

import com.oceanbase.spark.HBaseRelation.convertToBytes
import com.oceanbase.spark.config.OBKVHbaseConfig
import com.oceanbase.spark.obkv.HTableClientUtils

import org.apache.hadoop.hbase.client.{Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, Filter, InsertableRelation, PrunedFilteredScan}
import org.apache.spark.sql.types.{StructField, StructType}

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}
import java.util

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class HBaseRelation(
    parameters: Map[String, String],
    userSpecifiedSchema: Option[StructType]
)(@transient val sqlContext: SQLContext)
  extends BaseRelation
  with PrunedFilteredScan
  with InsertableRelation
  with Logging {

  import scala.collection.JavaConverters._
  private val config = new OBKVHbaseConfig(parameters.asJava)

  // Validate and extract schema metadata
  require(userSpecifiedSchema.isDefined, "Schema must be specified")
  private val providedSchema = userSpecifiedSchema.get

  // Validate schema structure
  require(providedSchema.fields.nonEmpty, "Schema must have at least one field")

  // First field is rowkey (can be any atomic type)
  private val rowkeyField = providedSchema.fields.head
  require(
    !rowkeyField.dataType.isInstanceOf[StructType],
    s"First field '${rowkeyField.name}' must be an atomic type (rowkey), not STRUCT")

  // Other fields must be STRUCT types (column families)
  private val columnFamilyFields = providedSchema.fields.tail
  columnFamilyFields.foreach {
    field =>
      require(
        field.dataType.isInstanceOf[StructType],
        s"Field '${field.name}' must be a STRUCT type representing a column family")
  }

  override def schema: StructType = providedSchema

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    throw new NotImplementedError("Not supports reading obkv-hbase")
  }

  override def insert(dataFrame: DataFrame, overwrite: Boolean): Unit = {
    dataFrame.foreachPartition(
      (rows: Iterator[Row]) => {
        val hTableClient: Table = HTableClientUtils.getHTableClient(config)
        val buffer = ArrayBuffer[Row]()
        rows.foreach(
          row => {
            buffer += row
            if (buffer.length >= config.getBatchSize) {
              flush(buffer, hTableClient)
            }
          })
        flush(buffer, hTableClient)
        hTableClient.close()
      })
  }

  private def flush(buffer: ArrayBuffer[Row], hTableClient: Table): Unit = {
    // Group puts by column family to handle multiple families correctly
    val familyPutListMap = mutable.HashMap.empty[String, util.ArrayList[Put]]

    buffer.foreach {
      row =>
        // First field is rowkey
        val rowKey: Array[Byte] = convertToBytes(row.get(0))

        // Group columns by family
        val columnsByFamily = new mutable.HashMap[String, ArrayBuffer[(String, Array[Byte])]]()

        // Process each column family (STRUCT fields starting from index 1)
        for (i <- 1 until row.size) {
          val cfFieldIndex = i - 1 // Index in columnFamilyFields array
          val cfField = columnFamilyFields(cfFieldIndex)
          val cfName = cfField.name
          val cfStruct = cfField.dataType.asInstanceOf[StructType]

          // Get the STRUCT value for this column family
          val cfRow = row.getStruct(i)

          if (cfRow != null) {
            // Process each column qualifier in this family
            for (j <- 0 until cfStruct.fields.length) {
              val qualifierField = cfStruct.fields(j)
              val qualifierName = qualifierField.name
              val qualifierValue = cfRow.get(j)

              if (qualifierValue != null) {
                val columnValue = convertToBytes(qualifierValue)
                columnsByFamily.getOrElseUpdate(cfName, ArrayBuffer.empty) +=
                  ((qualifierName, columnValue))
              }
            }
          }
        }

        // Create one Put per family with the same rowKey
        columnsByFamily.foreach {
          case (cfName, columns) =>
            val put: Put = new Put(rowKey)
            val familyName: Array[Byte] = Bytes.toBytes(cfName)
            columns.foreach {
              case (colName, colValue) =>
                put.addColumn(familyName, Bytes.toBytes(colName), colValue)
            }

            familyPutListMap.getOrElseUpdate(cfName, new util.ArrayList[Put]()).add(put)
        }
    }

    // Flush each family's puts separately
    familyPutListMap.values.foreach {
      putList =>
        if (!putList.isEmpty) {
          hTableClient.put(putList)
        }
    }

    buffer.clear()
  }
}

object HBaseRelation {
  def convertToBytes(data: Any): Array[Byte] = data match {
    case null => null
    case _: Boolean => Bytes.toBytes(data.asInstanceOf[Boolean])
    case _: Byte => Bytes.toBytes(data.asInstanceOf[Byte])
    case _: Short => Bytes.toBytes(data.asInstanceOf[Short])
    case _: Integer => Bytes.toBytes(data.asInstanceOf[Integer])
    case _: Long => Bytes.toBytes(data.asInstanceOf[Long])
    case _: Float => Bytes.toBytes(data.asInstanceOf[Float])
    case _: Double => Bytes.toBytes(data.asInstanceOf[Double])
    case _: String => Bytes.toBytes(data.asInstanceOf[String])
    case _: BigDecimal => Bytes.toBytes(data.asInstanceOf[java.math.BigDecimal])
    case _: Date => Bytes.toBytes(data.asInstanceOf[Date].getTime)
    case _: LocalDate => Bytes.toBytes(data.asInstanceOf[LocalDate].toEpochDay)
    case _: Timestamp => Bytes.toBytes(data.asInstanceOf[Timestamp].getTime)
    case _: Instant => Bytes.toBytes(data.asInstanceOf[Instant].getEpochSecond * 1000)
    case _: Array[Byte] => data.asInstanceOf[Array[Byte]]
    case _ =>
      throw new UnsupportedOperationException(s"Unsupported type: ${data.getClass.getSimpleName}")
  }
}

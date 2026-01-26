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

import com.oceanbase.spark.HBaseRelation.{convertToBytes, parseCatalog}
import com.oceanbase.spark.config.OBKVHbaseConfig
import com.oceanbase.spark.obkv.HTableClientUtils

import com.fasterxml.jackson.core.JsonParser.Feature
import org.apache.hadoop.hbase.client.{Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.sources.{BaseRelation, Filter, InsertableRelation, PrunedFilteredScan}
import org.apache.spark.sql.types.{StructField, StructType}
import org.json4s.JsonAST.JObject
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.JsonMethods._

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

  // Parse catalog once and store results as instance variables
  private val (
    userSchema: StructType,
    rowKey: String,
    columnFamilyMap: mutable.Map[String, (String, String)]) =
    parseCatalog(config.getSchema)

  override def schema: StructType = userSpecifiedSchema.getOrElse(userSchema)

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

    buffer.foreach(
      row => {
        // Get field index by col name that defined in catalog (using instance variable now)
        val rowKeyIndex = row.schema.fieldIndex(rowKey)
        val rowKeyBytes: Array[Byte] = convertToBytes(row(rowKeyIndex))

        // Group columns by family
        val columnsByFamily = new mutable.HashMap[String, ArrayBuffer[(String, Array[Byte])]]()

        // Mapping DataFrame's schema to User-defined schema
        for (i <- 0 until (row.size)) {
          if (i != rowKeyIndex) {
            val rowFieldName = row.schema.fieldNames(i)
            // Only write columns defined by the user in the schema (using instance variable now)
            if (columnFamilyMap.contains(rowFieldName)) {
              val userFieldName = columnFamilyMap(rowFieldName)._1
              val cfName = columnFamilyMap(rowFieldName)._2
              val columnValue = convertToBytes(row.get(i))

              columnsByFamily.getOrElseUpdate(cfName, ArrayBuffer.empty) +=
                ((userFieldName, columnValue))
            }
          }
        }

        // Create one Put per family with the same rowKey
        columnsByFamily.foreach {
          case (cfName, columns) =>
            val put: Put = new Put(rowKeyBytes)
            val familyName: Array[Byte] = Bytes.toBytes(cfName)
            columns.foreach {
              case (colName, colValue) =>
                put.addColumn(familyName, Bytes.toBytes(colName), colValue)
            }

            familyPutListMap.getOrElseUpdate(cfName, new util.ArrayList[Put]()).add(put)
        }
      })

    // Flush each family's puts separately
    familyPutListMap.values.foreach(
      putList => {
        if (!putList.isEmpty) {
          hTableClient.put(putList)
        }
      })

    buffer.clear()
  }
}

object HBaseRelation {
  private val CF = "cf"
  private val COLUMN_NAME = "col"
  private val COLUMN_TYPE = "type"

  /**
   * Parse catalog JSON and return schema along with metadata. Returns a tuple of (StructType,
   * rowKey, columnFamilyMap) to ensure proper serialization.
   *
   * @param catalogJson
   *   JSON string defining the catalog schema
   * @return
   *   Tuple of (StructType schema, String rowKey, Map columnFamilyMap)
   */
  def parseCatalog(
      catalogJson: String): (StructType, String, mutable.Map[String, (String, String)]) = {
    JsonMethods.mapper.configure(Feature.ALLOW_SINGLE_QUOTES, true)
    val jObject: JObject = parse(catalogJson).asInstanceOf[JObject]
    val schemaMap = mutable.LinkedHashMap.empty[String, Field]

    // Local variables instead of static ones
    var rowKey = ""
    val columnFamilyMap: mutable.Map[String, (String, String)] =
      mutable.LinkedHashMap.empty[String, (String, String)]

    getColsPreservingOrder(jObject).foreach {
      case (name, column) =>
        if (column(CF).equalsIgnoreCase("rowKey"))
          rowKey = column(COLUMN_NAME)
        else
          columnFamilyMap.put(column(COLUMN_NAME), (name, column(CF)))

        val filed = Field(column(CF), column(COLUMN_NAME), column(COLUMN_TYPE))
        schemaMap.+=((name, filed))
    }

    val fields: Seq[StructField] = schemaMap.map {
      case (name, field) =>
        StructField(name, CatalystSqlParser.parseDataType(field.columnType))
    }.toSeq

    (StructType(fields), rowKey, columnFamilyMap)
  }

  private def getColsPreservingOrder(jObj: JObject): Seq[(String, Map[String, String])] = {
    jObj.obj.map {
      case (name, jValue) =>
        (name, jValue.values.asInstanceOf[Map[String, String]])
    }
  }

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

case class Field(cf: String, columnName: String, columnType: String)

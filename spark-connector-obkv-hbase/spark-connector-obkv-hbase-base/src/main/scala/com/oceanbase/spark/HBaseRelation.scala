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
  private val userSchema: StructType = parseCatalog(config.getSchema)

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
    val putList = new util.ArrayList[Put]()
    buffer.foreach(
      row => {
        // Get field index by col name that defined in catalog
        val rowKeyIndex = row.schema.fieldIndex(HBaseRelation.rowKey)
        val rowKey: Array[Byte] = convertToBytes(row(rowKeyIndex))
        val put: Put = new Put(rowKey)
        // Mapping DataFrame's schema to User-defined schema
        for (i <- 0 until (row.size)) {
          if (i == rowKeyIndex) {
            // do nothing
          } else {
            val rowFieldName = row.schema.fieldNames(i)
            // Only write columns defined by the user in the schema.
            if (HBaseRelation.columnFamilyMap.contains(rowFieldName)) {
              val userFieldName = HBaseRelation.columnFamilyMap(rowFieldName)._1
              val cfName = HBaseRelation.columnFamilyMap(rowFieldName)._2
              val familyName: Array[Byte] = Bytes.toBytes(cfName)
              val columnValue = convertToBytes(row.get(i))
              put.addColumn(familyName, Bytes.toBytes(userFieldName), columnValue)
            }
          }
        }
        putList.add(put)
      })
    hTableClient.put(putList)
    buffer.clear()
  }
}

object HBaseRelation {
  private val CF = "cf"
  private val COLUMN_NAME = "col"
  private val COLUMN_TYPE = "type"

  private var rowKey = ""

  /**
   * Mapping DataFrame's schema to User-defined schema
   *
   * "address": {"cf": "family1","col": "officeAddress","type": "string"} [address, (officeAddress,
   * family1)] [user define obkv-hbase col name, (dataframe schema col name, family name)]
   */
  private val columnFamilyMap: mutable.Map[String, (String, String)] =
    mutable.LinkedHashMap.empty[String, (String, String)]

  def parseCatalog(catalogJson: String): StructType = {
    JsonMethods.mapper.configure(Feature.ALLOW_SINGLE_QUOTES, true)
    val jObject: JObject = parse(catalogJson).asInstanceOf[JObject]
    val schemaMap = mutable.LinkedHashMap.empty[String, Field]
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

    StructType(fields)
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

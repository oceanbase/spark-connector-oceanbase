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

import com.oceanbase.spark.config.OceanBaseConfig
import com.oceanbase.spark.obkv.{OBKVClientUtils, OBKVFilterCompiler}

import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObReadConsistency
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, Filter, InsertableRelation, PrunedFilteredScan}
import org.apache.spark.sql.types.{DataTypes, DecimalType, StructType}

import java.util.{Map => JMap}

import scala.collection.JavaConverters.mapAsJavaMapConverter

/**
 * A Spark V1 BaseRelation for OBKV read/write in non-Catalog mode. Users declare their schema via
 * SchemaRelationProvider.
 */
case class OBKVRelation(
    parameters: Map[String, String],
    userSpecifiedSchema: Option[StructType],
    primaryKeys: Array[String]
)(@transient val sqlContext: SQLContext)
  extends BaseRelation
  with PrunedFilteredScan
  with InsertableRelation
  with Logging {

  private val config = new OceanBaseConfig(parameters.asJava)

  override def schema: StructType = userSpecifiedSchema.getOrElse {
    throw new IllegalArgumentException("Schema must be specified for OBKV non-Catalog mode")
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val readSchema = if (requiredColumns.nonEmpty) {
      StructType(requiredColumns.map(col => schema(col)))
    } else {
      schema
    }

    val sc = sqlContext.sparkContext
    val broadcastConfig = sc.broadcast(config)
    val broadcastSchema = sc.broadcast(readSchema)
    val broadcastPKs = sc.broadcast(primaryKeys)
    val broadcastFilters = sc.broadcast(filters)

    sc.parallelize(Seq(0), 1).flatMap {
      _ =>
        val cfg = broadcastConfig.value
        val sch = broadcastSchema.value
        val pks = broadcastPKs.value
        val fltrs = broadcastFilters.value

        val client = OBKVClientUtils.createClient(cfg)
        try {
          val query = client.query(cfg.getTableName)
          query.select(sch.fieldNames: _*)

          val compiler = new OBKVFilterCompiler(pks)
          val result = compiler.compile(fltrs)
          if (result.serverFilter != null) {
            query.setFilter(result.serverFilter)
          }

          query.setBatchSize(cfg.getObkvBatchSize)
          query.setOperationTimeout(cfg.getObkvOperationTimeout.toLong)

          if ("WEAK".equalsIgnoreCase(cfg.getObkvReadConsistency)) {
            query.setReadConsistency(ObReadConsistency.WEAK)
          }

          val resultSet = query.asyncExecute()
          val rows = new scala.collection.mutable.ArrayBuffer[Row]()

          while (resultSet.next()) {
            val rowMap: JMap[String, Object] = resultSet.getRow
            val values = sch.fields.map {
              field =>
                val value = rowMap.get(field.name)
                if (value == null) null
                else OBKVRelation.convertToRow(value, field.dataType)
            }
            rows += Row.fromSeq(values)
          }
          rows.iterator
        } finally {
          OBKVClientUtils.closeClient(client)
        }
    }
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    if (overwrite) {
      com.oceanbase.spark.utils.OBJdbcUtils.truncateTable(config)
    }
    OBKVRelation.writeDataFrame(data, config)
  }
}

object OBKVRelation extends Logging {

  def convertToRow(value: Object, dataType: org.apache.spark.sql.types.DataType): Any = {
    if (dataType == DataTypes.StringType) {
      value.toString
    } else if (dataType == DataTypes.IntegerType) {
      value match {
        case n: Number => n.intValue()
        case _ => value.toString.toInt
      }
    } else if (dataType == DataTypes.LongType) {
      value match {
        case n: Number => n.longValue()
        case _ => value.toString.toLong
      }
    } else if (dataType == DataTypes.DoubleType) {
      value match {
        case n: Number => n.doubleValue()
        case _ => value.toString.toDouble
      }
    } else if (dataType == DataTypes.FloatType) {
      value match {
        case n: Number => n.floatValue()
        case _ => value.toString.toFloat
      }
    } else if (dataType == DataTypes.BooleanType) {
      value match {
        case b: java.lang.Boolean => b.booleanValue()
        case n: Number => n.intValue() != 0
        case _ => value.toString.toBoolean
      }
    } else if (dataType == DataTypes.ShortType) {
      value match {
        case n: Number => n.shortValue()
        case _ => value.toString.toShort
      }
    } else if (dataType == DataTypes.ByteType) {
      value match {
        case n: Number => n.byteValue()
        case _ => value.toString.toByte
      }
    } else if (dataType.isInstanceOf[DecimalType]) {
      value match {
        case bd: java.math.BigDecimal => bd
        case bi: java.math.BigInteger => new java.math.BigDecimal(bi)
        case _ => new java.math.BigDecimal(value.toString)
      }
    } else if (dataType == DataTypes.DateType) {
      value match {
        case d: java.sql.Date => d
        case _ => java.sql.Date.valueOf(value.toString)
      }
    } else if (dataType == DataTypes.TimestampType) {
      value match {
        case t: java.sql.Timestamp => t
        case _ => java.sql.Timestamp.valueOf(value.toString)
      }
    } else if (dataType == DataTypes.BinaryType) {
      value.asInstanceOf[Array[Byte]]
    } else {
      value.toString
    }
  }

  /**
   * Converts a value to an OBKV-compatible type. The OBKV client does not support BigDecimal, so
   * DecimalType values are converted to Double.
   */
  def convertForObkv(value: Any, dataType: org.apache.spark.sql.types.DataType): Object = {
    if (value == null) return null
    dataType match {
      case _: DecimalType =>
        value match {
          case bd: java.math.BigDecimal => java.lang.Double.valueOf(bd.doubleValue())
          case other => other.asInstanceOf[Object]
        }
      case _ => value.asInstanceOf[Object]
    }
  }

  /** Writes a DataFrame to OceanBase via OBKV batch operations. */
  def writeDataFrame(dataFrame: DataFrame, config: OceanBaseConfig): Unit = {
    val schema = dataFrame.schema
    val primaryKeys = {
      val pk = config.getObkvPrimaryKey
      if (pk != null && pk.nonEmpty) pk.split(",").map(_.trim)
      else Array.empty[String]
    }
    val batchSize = config.getObkvBatchSize
    val dupAction = config.getObkvDupAction
    val tableName = config.getTableName

    dataFrame.foreachPartition {
      (iter: Iterator[Row]) =>
        val client = OBKVClientUtils.createClient(config)
        try {
          val pkSet = primaryKeys.toSet
          val batchOps = client.batch(tableName)
          var count = 0

          iter.foreach {
            row =>
              val rowKeyValues =
                new scala.collection.mutable.ArrayBuffer[Object]()
              val columnNames =
                new scala.collection.mutable.ArrayBuffer[String]()
              val columnValues =
                new scala.collection.mutable.ArrayBuffer[Object]()

              schema.fields.zipWithIndex.foreach {
                case (field, idx) =>
                  val value =
                    if (row.isNullAt(idx)) null
                    else
                      OBKVRelation.convertForObkv(row.get(idx), field.dataType)
                  if (pkSet.contains(field.name)) {
                    rowKeyValues += value
                  } else {
                    columnNames += field.name
                    columnValues += value
                  }
              }

              val rk = rowKeyValues.toArray
              val cols = columnNames.toArray
              val vals = columnValues.toArray

              dupAction match {
                case "INSERT_OR_UPDATE" =>
                  batchOps.insertOrUpdate(rk, cols, vals)
                case "INSERT" => batchOps.insert(rk, cols, vals)
                case "REPLACE" => batchOps.replace(rk, cols, vals)
                case "PUT" => batchOps.put(rk, cols, vals)
                case _ => batchOps.insertOrUpdate(rk, cols, vals)
              }

              count += 1
              if (count >= batchSize) {
                batchOps.execute()
                batchOps.clear()
                count = 0
              }
          }

          if (count > 0) {
            batchOps.execute()
          }
        } finally {
          OBKVClientUtils.closeClient(client)
        }
        ()
    }
  }
}

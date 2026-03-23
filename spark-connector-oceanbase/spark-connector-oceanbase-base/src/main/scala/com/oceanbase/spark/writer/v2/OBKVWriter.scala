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
package com.oceanbase.spark.writer.v2

import com.oceanbase.spark.config.OceanBaseConfig
import com.oceanbase.spark.obkv.{OBKVClientUtils, OBKVTypeConverter}

import com.alipay.oceanbase.rpc.ObTableClient
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ArrayBuffer

class OBKVWriter(
    schema: StructType,
    config: OceanBaseConfig,
    catalogPrimaryKeys: Array[String] = Array.empty)
  extends DataWriter[InternalRow]
  with Logging {

  private val batchSize = config.getObkvBatchSize
  private val buffer: ArrayBuffer[InternalRow] = ArrayBuffer[InternalRow]()
  private lazy val client: ObTableClient = OBKVClientUtils.createClient(config, primaryKeys)
  private val dupAction: String = config.getObkvDupAction
  private val tableName: String = config.getTableName

  private val primaryKeys: Array[String] = {
    if (catalogPrimaryKeys.nonEmpty) {
      catalogPrimaryKeys
    } else {
      val pk = config.getObkvPrimaryKey
      if (pk != null && pk.nonEmpty) {
        pk.split(",").map(_.trim)
      } else {
        Array.empty[String]
      }
    }
  }

  override def write(record: InternalRow): Unit = {
    buffer += record.copy()
    if (buffer.length >= batchSize) flush()
  }

  private def flush(): Unit = {
    if (buffer.isEmpty) return

    try {
      val batchOps = client.batch(tableName)

      buffer.foreach {
        row =>
          val (rowKeys, columns, values) = extractRowData(row)
          dupAction match {
            case "INSERT_OR_UPDATE" =>
              batchOps.insertOrUpdate(rowKeys, columns, values)
            case "INSERT" =>
              batchOps.insert(rowKeys, columns, values)
            case "REPLACE" =>
              batchOps.replace(rowKeys, columns, values)
            case "PUT" =>
              batchOps.put(rowKeys, columns, values)
            case _ =>
              batchOps.insertOrUpdate(rowKeys, columns, values)
          }
      }

      batchOps.execute()
      buffer.clear()
    } catch {
      case ex: Exception =>
        throw new RuntimeException(s"Failed to flush batch to OBKV table: $tableName", ex)
    }
  }

  private def extractRowData(row: InternalRow): (Array[Object], Array[String], Array[Object]) = {
    val pkSet = primaryKeys.toSet
    val allFields = schema.fields
    val rowKeyValues = new ArrayBuffer[Object]()
    val columnNames = new ArrayBuffer[String]()
    val columnValues = new ArrayBuffer[Object]()

    allFields.zipWithIndex.foreach {
      case (field, idx) =>
        val value = OBKVTypeConverter.toObkvValue(row, idx, field.dataType)
        if (pkSet.contains(field.name)) {
          rowKeyValues += value.asInstanceOf[Object]
        } else {
          columnNames += field.name
          columnValues += value.asInstanceOf[Object]
        }
    }

    (rowKeyValues.toArray, columnNames.toArray, columnValues.toArray)
  }

  override def commit(): WriterCommitMessage = {
    flush()
    CommitMessage()
  }

  override def abort(): Unit = {}

  override def close(): Unit = {
    OBKVClientUtils.closeClient(client)
  }
}

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

import java.util.Locale

import scala.collection.mutable.ArrayBuffer

class OBKVWriter(
    schema: StructType,
    config: OceanBaseConfig,
    catalogPrimaryKeys: Array[String] = Array.empty)
  extends DataWriter[InternalRow]
  with Logging {

  private val batchSize = config.getObkvBatchSize
  private val buffer: ArrayBuffer[InternalRow] = ArrayBuffer[InternalRow]()
  private var client: ObTableClient = _
  private val dupAction: String = config.getObkvDupAction.toUpperCase(Locale.ROOT)
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

  private lazy val primaryKeySet: Set[String] = primaryKeys.toSet

  private def ensureClient(): ObTableClient = {
    if (client == null) {
      client = OBKVClientUtils.createClient(config, primaryKeys)
    }
    client
  }

  override def write(record: InternalRow): Unit = {
    buffer += record.copy()
    if (buffer.length >= batchSize) flush()
  }

  private def flush(): Unit = {
    if (buffer.isEmpty) return

    try {
      val batchOps = ensureClient().batch(tableName)
      val appendOperation: (Array[Object], Array[String], Array[Object]) => Any =
        dupAction match {
          case "INSERT" => batchOps.insert
          case "REPLACE" => batchOps.replace
          case "PUT" => batchOps.put
          case _ => batchOps.insertOrUpdate
        }

      buffer.foreach {
        row =>
          val (rowKeys, columns, values) = extractRowData(row)
          appendOperation(rowKeys, columns, values)
      }

      batchOps.execute()
      buffer.clear()
    } catch {
      case ex: Exception =>
        throw new RuntimeException(s"Failed to flush batch to OBKV table: $tableName", ex)
    }
  }

  private def extractRowData(row: InternalRow): (Array[Object], Array[String], Array[Object]) = {
    val allFields = schema.fields
    val rowKeyValues = new ArrayBuffer[Object]()
    val columnNames = new ArrayBuffer[String]()
    val columnValues = new ArrayBuffer[Object]()

    allFields.zipWithIndex.foreach {
      case (field, idx) =>
        val value = OBKVTypeConverter.toObkvValue(row, idx, field.dataType)
        if (primaryKeySet.contains(field.name)) {
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
    if (client != null) {
      OBKVClientUtils.closeClient(client)
    }
  }
}

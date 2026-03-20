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
package com.oceanbase.spark.reader.v2

import com.oceanbase.spark.config.OceanBaseConfig
import com.oceanbase.spark.obkv.{OBKVClientUtils, OBKVFilterCompiler, OBKVPartition, OBKVTypeConverter}

import com.alipay.oceanbase.rpc.ObTableClient
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObReadConsistency
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory, Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

import java.util.{Map => JMap}

case class OBKVScanBuilder(schema: StructType, config: OceanBaseConfig, primaryKeys: Array[String])
  extends ScanBuilder
  with SupportsPushDownFilters
  with SupportsPushDownRequiredColumns
  with Logging {

  private var finalSchema: StructType = schema
  private var pushedFilter: Array[Filter] = Array.empty[Filter]

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val compiler = new OBKVFilterCompiler(primaryKeys)
    val result = compiler.compile(filters)
    val unhandled = result.unhandledFilters
    this.pushedFilter = filters.diff(unhandled)
    unhandled
  }

  override def pushedFilters(): Array[Filter] = pushedFilter

  override def pruneColumns(requiredSchema: StructType): Unit = {
    val requiredCols = requiredSchema.map(_.name)
    this.finalSchema = StructType(finalSchema.filter(field => requiredCols.contains(field.name)))
  }

  override def build(): Scan =
    OBKVBatchScan(finalSchema, config, pushedFilter, primaryKeys)
}

case class OBKVBatchScan(
    schema: StructType,
    config: OceanBaseConfig,
    pushedFilters: Array[Filter],
    primaryKeys: Array[String])
  extends Scan
  with Batch {

  override def readSchema(): StructType = schema

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    // For the first version, use a single full-scan partition.
    // Partition-aware splitting can be added later based on
    // ObTableClient partition information.
    Array(OBKVPartition.fullScan())
  }

  override def createReaderFactory(): PartitionReaderFactory =
    new OBKVReaderFactory(schema, config, pushedFilters, primaryKeys)
}

class OBKVReaderFactory(
    schema: StructType,
    config: OceanBaseConfig,
    pushedFilters: Array[Filter],
    primaryKeys: Array[String])
  extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    new OBKVReader(
      schema,
      config,
      partition.asInstanceOf[OBKVPartition],
      pushedFilters,
      primaryKeys)
}

class OBKVReader(
    schema: StructType,
    config: OceanBaseConfig,
    partition: OBKVPartition,
    pushedFilters: Array[Filter],
    primaryKeys: Array[String])
  extends PartitionReader[InternalRow]
  with Logging {

  private var client: ObTableClient = _
  private var resultSet: com.alipay.oceanbase.rpc.stream.QueryResultSet = _
  private var initialized = false

  private val mutableRow =
    new SpecificInternalRow(schema.fields.map(_.dataType))

  private def initialize(): Unit = {
    if (initialized) return
    client = OBKVClientUtils.createClient(config)

    val tableName = config.getTableName
    val query = client.query(tableName)

    // 1. Select columns
    query.select(schema.fieldNames: _*)

    // 2. Apply scan range from partition
    partition.applyScanRange(query)

    // 3. Apply server-side filter from pushed filters
    val compiler = new OBKVFilterCompiler(primaryKeys)
    val compileResult = compiler.compile(pushedFilters)
    if (compileResult.serverFilter != null) {
      query.setFilter(compileResult.serverFilter)
    }

    // 4. Set batch size for streaming
    query.setBatchSize(config.getObkvBatchSize)

    // 5. Set operation timeout
    query.setOperationTimeout(config.getObkvOperationTimeout.toLong)

    // 6. Set read consistency
    if ("WEAK".equalsIgnoreCase(config.getObkvReadConsistency)) {
      query.setReadConsistency(ObReadConsistency.WEAK)
    }

    resultSet = query.execute()
    initialized = true
  }

  override def next(): Boolean = {
    initialize()
    resultSet.next()
  }

  override def get(): InternalRow = {
    val row: JMap[String, Object] = resultSet.getRow
    schema.fields.zipWithIndex.foreach {
      case (field, i) =>
        val value = row.get(field.name)
        if (value == null) {
          mutableRow.setNullAt(i)
        } else {
          OBKVTypeConverter.setRowValue(mutableRow, i, field.dataType, value)
        }
    }
    mutableRow
  }

  override def close(): Unit = {
    OBKVClientUtils.closeClient(client)
  }
}

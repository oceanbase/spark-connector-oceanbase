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
import com.oceanbase.spark.dialect.OceanBaseDialect

import org.apache.spark.internal.Logging
import org.apache.spark.sql.ExprUtils.compileFilter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory, Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

/**
 * This is for compatibility with Spark 3.1, which does not support the SupportsPushDownAggregates
 * feature.
 */
case class OBJdbcScanBuilder(schema: StructType, config: OceanBaseConfig, dialect: OceanBaseDialect)
  extends ScanBuilder
  with SupportsPushDownFilters
  with SupportsPushDownRequiredColumns
  with Logging {
  private var finalSchema = schema
  private var pushedFilter = Array.empty[Filter]

  /** TODO: support org.apache.spark.sql.connector.read.SupportsPushDownV2Filters */
  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val (pushed, unSupported) = filters.partition(f => compileFilter(f, dialect).isDefined)
    this.pushedFilter = pushed
    unSupported
  }

  override def pushedFilters(): Array[Filter] = pushedFilter

  override def pruneColumns(requiredSchema: StructType): Unit = {
    val requiredCols = requiredSchema.map(_.name)
    this.finalSchema = StructType(finalSchema.filter(field => requiredCols.contains(field.name)))
  }

  override def build(): Scan =
    OBJdbcBatchScan(
      finalSchema: StructType,
      config: OceanBaseConfig,
      pushedFilter: Array[Filter],
      dialect: OceanBaseDialect)
}

case class OBJdbcBatchScan(
    schema: StructType,
    config: OceanBaseConfig,
    pushedFilter: Array[Filter],
    dialect: OceanBaseDialect)
  extends Scan {

  override def readSchema(): StructType = schema

  override def toBatch: Batch =
    new OBJdbcBatch(
      schema: StructType,
      config: OceanBaseConfig,
      pushedFilter: Array[Filter],
      dialect: OceanBaseDialect)
}

class OBJdbcBatch(
    schema: StructType,
    config: OceanBaseConfig,
    pushedFilter: Array[Filter],
    dialect: OceanBaseDialect)
  extends Batch {
  private lazy val inputPartitions: Array[InputPartition] =
    OBMySQLPartition.columnPartition(config)

  override def planInputPartitions(): Array[InputPartition] = inputPartitions

  override def createReaderFactory(): PartitionReaderFactory = new OBJdbcReaderFactory(
    schema: StructType,
    config: OceanBaseConfig,
    pushedFilter: Array[Filter],
    dialect: OceanBaseDialect)
}

class OBJdbcReaderFactory(
    schema: StructType,
    config: OceanBaseConfig,
    pushedFilter: Array[Filter],
    dialect: OceanBaseDialect)
  extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    new OBJdbcReader(
      schema: StructType,
      config: OceanBaseConfig,
      partition: InputPartition,
      pushedFilter: Array[Filter],
      dialect: OceanBaseDialect)
}

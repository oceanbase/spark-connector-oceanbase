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
import com.oceanbase.spark.dialect.{OceanBaseDialect, OceanBaseOracleDialect}
import com.oceanbase.spark.utils.OBJdbcUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.ExprUtils.compileFilter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.expressions.{NamedReference, SortOrder}
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory, Scan, ScanBuilder, SupportsPushDownAggregates, SupportsPushDownFilters, SupportsPushDownLimit, SupportsPushDownRequiredColumns, SupportsPushDownTopN, SupportsRuntimeFiltering}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

import scala.util.control.NonFatal

case class OBJdbcScanBuilder(schema: StructType, config: OceanBaseConfig, dialect: OceanBaseDialect)
  extends ScanBuilder
  with SupportsPushDownFilters
  with SupportsPushDownRequiredColumns
  with SupportsPushDownAggregates
  with SupportsPushDownLimit
  with SupportsPushDownTopN
  with Logging {
  private var finalSchema = schema
  private var pushedFilter = Array.empty[Filter]
  private var pushDownLimit = 0
  private var sortOrders: Array[SortOrder] = Array.empty[SortOrder]

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
  private var pushedAggregateList: Array[String] = Array()

  private var pushedGroupBys: Option[Array[String]] = None

  override def pushAggregation(aggregation: Aggregation): Boolean = {
    if (!config.getPushDownAggregate) return false

    // TODO(oracle-agg-pushdown): Disable aggregate pushdown for Oracle mode.
    // Reasons:
    // 1) NUMBER precision/scale metadata from JDBC can be unstable in some aggregate cases,
    //    leading to value mis-scaling (e.g., 5.3 -> 0.53).
    // 2) MAX/MIN semantics on non-numeric types (e.g., strings) are lexicographical; blindly
    //    casting to numeric changes semantics or throws errors.
    // 3) Needs type-aware rewrite strategy; enable later once fully implemented.
    if (dialect.isInstanceOf[OceanBaseOracleDialect]) {
      logInfo("Disable aggregate pushdown on Oracle mode due to precision/semantic concerns.")
      return false
    }

    val compiledAggs = aggregation.aggregateExpressions.flatMap(dialect.compileExpression(_))
    if (compiledAggs.length != aggregation.aggregateExpressions.length) return false

    val compiledGroupBys = aggregation.groupByExpressions.flatMap(dialect.compileExpression)
    if (compiledGroupBys.length != aggregation.groupByExpressions.length) return false

    // The column names here are already quoted and can be used to build sql string directly.
    // e.g. "DEPT","NAME",MAX("SALARY"),MIN("BONUS") =>
    // SELECT "DEPT","NAME",MAX("SALARY"),MIN("BONUS") FROM "test"."employee"
    //   GROUP BY "DEPT", "NAME"
    val selectList = compiledGroupBys ++ compiledAggs
    val groupByClause = if (compiledGroupBys.isEmpty) {
      ""
    } else {
      "GROUP BY " + compiledGroupBys.mkString(",")
    }

    val aggQuery =
      s"SELECT ${selectList.mkString(",")} FROM ${config.getDbTable} WHERE 1=0 $groupByClause"
    try {
      finalSchema = OBJdbcUtils.getQueryOutputSchema(aggQuery, config, dialect)
      pushedAggregateList = selectList
      pushedGroupBys = Some(compiledGroupBys)
      true
    } catch {
      case NonFatal(e) =>
        logError("Failed to push down aggregation to JDBC", e)
        false
    }
  }

  override def pushLimit(limit: Int): Boolean = {
    if (config.getEnablePushdownLimit) {
      pushDownLimit = limit
      return true
    }
    false
  }

  override def pushTopN(orders: Array[SortOrder], limit: Int): Boolean = {
    if (config.getEnablePushdownLimit && config.getEnablePushdownTopN) {
      pushDownLimit = limit
      sortOrders = orders
      return true
    }
    false
  }

  // Always partially pushdown.
  override def isPartiallyPushed: Boolean = true

  override def build(): Scan = {
    val columnList = if (pushedGroupBys.isEmpty) {
      finalSchema.map(col => dialect.quoteIdentifier(col.name)).toArray
    } else {
      pushedAggregateList
    }
    OBJdbcBatchScan(
      finalSchema: StructType,
      config: OceanBaseConfig,
      pushedFilter: Array[Filter],
      pushDownLimit: Int,
      sortOrders: Array[SortOrder],
      columnList: Array[String],
      pushedGroupBys: Option[Array[String]],
      dialect: OceanBaseDialect
    )
  }
}

case class OBJdbcBatchScan(
    schema: StructType,
    config: OceanBaseConfig,
    pushedFilter: Array[Filter],
    pushDownLimit: Int,
    pushDownTopNSortOrders: Array[SortOrder],
    requiredColumns: Array[String],
    pushedGroupBys: Option[Array[String]],
    dialect: OceanBaseDialect)
  extends Scan
  with SupportsRuntimeFiltering {

  // TODO: support spark runtime filter feat.
  private var runtimeFilters: Array[Filter] = Array.empty

  override def readSchema(): StructType = schema

  override def toBatch: Batch =
    new OBJdbcBatch(
      schema: StructType,
      config: OceanBaseConfig,
      pushedFilter: Array[Filter],
      pushDownLimit: Int,
      pushDownTopNSortOrders: Array[SortOrder],
      requiredColumns: Array[String],
      pushedGroupBys: Option[Array[String]],
      dialect: OceanBaseDialect)

  override def filterAttributes(): Array[NamedReference] = Array.empty

  override def filter(filters: Array[Filter]): Unit = {
    runtimeFilters = filters
  }
}

class OBJdbcBatch(
    schema: StructType,
    config: OceanBaseConfig,
    pushedFilter: Array[Filter],
    pushDownLimit: Int,
    pushDownTopNSortOrders: Array[SortOrder],
    requiredColumns: Array[String],
    pushedGroupBys: Option[Array[String]],
    dialect: OceanBaseDialect)
  extends Batch {
  private lazy val inputPartitions: Array[InputPartition] = {
    OBJdbcUtils.getCompatibleMode(config).map(_.toLowerCase) match {
      case Some("oracle") => OBOraclePartition.columnPartition(config, dialect)
      case _ => OBMySQLPartition.columnPartition(config, dialect)
    }
  }

  override def planInputPartitions(): Array[InputPartition] = inputPartitions

  override def createReaderFactory(): PartitionReaderFactory = new OBJdbcReaderFactory(
    schema: StructType,
    config: OceanBaseConfig,
    pushedFilter: Array[Filter],
    pushDownLimit: Int,
    pushDownTopNSortOrders: Array[SortOrder],
    requiredColumns: Array[String],
    pushedGroupBys: Option[Array[String]],
    dialect: OceanBaseDialect)
}

class OBJdbcReaderFactory(
    schema: StructType,
    config: OceanBaseConfig,
    pushedFilter: Array[Filter],
    pushDownLimit: Int,
    pushDownTopNSortOrders: Array[SortOrder],
    requiredColumns: Array[String],
    pushedGroupBys: Option[Array[String]],
    dialect: OceanBaseDialect)
  extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    new OBJdbcReader(
      schema: StructType,
      config: OceanBaseConfig,
      partition: InputPartition,
      pushedFilter: Array[Filter],
      pushDownLimit: Int,
      pushDownTopNSortOrders: Array[SortOrder],
      requiredColumns: Array[String],
      pushedGroupBys: Option[Array[String]],
      dialect: OceanBaseDialect)
}

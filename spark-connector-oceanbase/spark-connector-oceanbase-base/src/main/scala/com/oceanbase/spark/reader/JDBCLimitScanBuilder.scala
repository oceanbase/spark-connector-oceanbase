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

package com.oceanbase.spark.reader

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.reader.{JDBCLimitRDD, OBJdbcRelation}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

case class JDBCLimitScanBuilder(session: SparkSession, schema: StructType, jdbcOptions: JDBCOptions)
  extends ScanBuilder
  with SupportsPushDownFilters
  with SupportsPushDownRequiredColumns
  with Logging {

  private val isCaseSensitive = session.sessionState.conf.caseSensitiveAnalysis

  private var pushedFilter = Array.empty[Filter]

  private var finalSchema = schema

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    if (jdbcOptions.pushDownPredicate) {
      val dialect = JdbcDialects.get(jdbcOptions.url)
      val (pushed, unSupported) =
        filters.partition(JDBCLimitRDD.compileFilter(_, dialect).isDefined)
      this.pushedFilter = pushed
      unSupported
    } else {
      filters
    }
  }

  override def pushedFilters(): Array[Filter] = pushedFilter

  private val pushedAggregateList: Array[String] = Array()

  private val pushedGroupByCols: Option[Array[String]] = None

  override def pruneColumns(requiredSchema: StructType): Unit = {
    // TODO: Support nested column and nested column pruning.
    val requiredCols =
      requiredSchema.fields.map(PartitioningUtils.getColName(_, isCaseSensitive)).toSet
    val fields = schema.fields.filter {
      field =>
        val colName = PartitioningUtils.getColName(field, isCaseSensitive)
        requiredCols.contains(colName)
    }
    finalSchema = StructType(fields)
  }

  override def build(): Scan = {
    val parts = OBMySQLLimitPartition.columnPartition(jdbcOptions)

    OBJDBCLimitScan(
      OBJdbcRelation(schema, parts, jdbcOptions)(session),
      finalSchema,
      pushedFilter,
      pushedAggregateList,
      pushedGroupByCols)
  }
}

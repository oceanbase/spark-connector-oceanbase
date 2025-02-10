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

package org.apache.spark.sql.reader

import org.apache.spark.Partition
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

object OBJdbcRelation extends Logging {

  /**
   * Takes a (schema, table) specification and returns the table's Catalyst schema. If
   * `customSchema` defined in the JDBC options, replaces the schema's dataType with the custom
   * schema's type.
   *
   * @param resolver
   *   function used to determine if two identifiers are equal
   * @param jdbcOptions
   *   JDBC options that contains url, table and other information.
   * @return
   *   resolved Catalyst schema of a JDBC table
   */
  def getSchema(resolver: Resolver, jdbcOptions: JDBCOptions): StructType = {
    val tableSchema = JDBCLimitRDD.resolveTable(jdbcOptions)
    jdbcOptions.customSchema match {
      case Some(customSchema) => JdbcUtils.getCustomSchema(tableSchema, customSchema, resolver)
      case None => tableSchema
    }
  }

  /** Resolves a Catalyst schema of a JDBC table and returns [[OBJdbcRelation]] with the schema. */
  def apply(parts: Array[Partition], jdbcOptions: JDBCOptions)(
      sparkSession: SparkSession): OBJdbcRelation = {
    val schema = OBJdbcRelation.getSchema(sparkSession.sessionState.conf.resolver, jdbcOptions)
    OBJdbcRelation(schema, parts, jdbcOptions)(sparkSession)
  }
}

case class OBJdbcRelation(
    override val schema: StructType,
    parts: Array[Partition],
    jdbcOptions: JDBCOptions)(@transient val sparkSession: SparkSession)
  extends BaseRelation {

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override val needConversion: Boolean = false

  // Check if JDBLimitCRDD.compileFilter can accept input filters
  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    if (jdbcOptions.pushDownPredicate) {
      filters.filter(JDBCLimitRDD.compileFilter(_, JdbcDialects.get(jdbcOptions.url)).isEmpty)
    } else {
      filters
    }
  }

  def buildScan(
      requiredColumns: Array[String],
      finalSchema: StructType,
      filters: Array[Filter],
      groupByColumns: Option[Array[String]]): RDD[Row] = {
    // Rely on a type erasure hack to pass RDD[InternalRow] back as RDD[Row]
    JDBCLimitRDD
      .scanTable(
        sparkSession.sparkContext,
        schema,
        requiredColumns,
        filters,
        parts,
        jdbcOptions,
        Some(finalSchema),
        groupByColumns)
      .asInstanceOf[RDD[Row]]
  }

  override def toString: String = {
    val partitioningInfo = if (parts.nonEmpty) s" [numPartitions=${parts.length}]" else ""
    // credentials should not be included in the plan output, table information is sufficient.
    s"JDBCRelation(${jdbcOptions.parameters(JDBCOptions.JDBC_TABLE_NAME)})" + partitioningInfo
  }
}

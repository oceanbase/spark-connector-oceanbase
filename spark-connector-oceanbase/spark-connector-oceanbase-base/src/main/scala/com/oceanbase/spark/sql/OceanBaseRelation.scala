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
package com.oceanbase.spark.sql

import com.oceanbase.spark.cfg.{ConnectionOptions, SparkSettings}
import com.oceanbase.spark.jdbc.OBJdbcUtils

import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SQLContext}
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

private[sql] class OceanBaseRelation(
    val sqlContext: SQLContext = null,
    parameters: Map[String, String])
  extends BaseRelation
  with InsertableRelation {

  private lazy val cfg = {
    val conf = new SparkSettings(sqlContext.sparkContext.getConf)
    conf.merge(parameters.asJava)
    conf
  }

  private lazy val lazySchema = {
    val conn = OBJdbcUtils.getConnection(cfg)
    try {
      val statement = conn.prepareStatement(
        s"select * from ${cfg.getProperty(ConnectionOptions.TABLE_NAME)} where 1 = 0")
      try {
        val rs = statement.executeQuery()
        try {
          JdbcUtils.getSchema(rs, dialect)
        } finally {
          rs.close()
        }
      } finally {
        statement.close()
      }
    } finally {
      conn.close()
    }
  }

  private lazy val dialect = JdbcDialects.get("")

  override def schema: StructType = lazySchema

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    // Handle Filters are not currently supported.
    filters
  }

  // Insert Table.
  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    data.write
      .format(OceanBaseSparkSource.SHORT_NAME)
      .options(parameters)
      .mode(if (overwrite) sql.SaveMode.Overwrite else sql.SaveMode.Append)
      .save()
  }
}

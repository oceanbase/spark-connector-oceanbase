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

package org.apache.spark.sql.jdbc

import org.apache.spark.sql.connector.expressions.Expression
import org.apache.spark.sql.types._

import java.util.Locale

/** Spark 4.0 compatible OceanBase Oracle dialect. In Spark 4.0,
  * [[OracleDialect]] changed from a case object to a case class, so we create
  * an instance for delegation.
  */
case object OceanBaseOracleDialect extends JdbcDialect {

  private val delegate = new OracleDialect()

  override def canHandle(url: String): Boolean =
    url.toLowerCase(Locale.ROOT).startsWith("jdbc:oceanbase")

  override def isSupportedFunction(funcName: String): Boolean =
    delegate.isSupportedFunction(funcName)

  override def compileExpression(expr: Expression): Option[String] =
    delegate.compileExpression(expr)

  override def getCatalystType(
      sqlType: Int,
      typeName: String,
      size: Int,
      md: MetadataBuilder
  ): Option[DataType] =
    delegate.getCatalystType(sqlType, typeName, size, md)

  override def getJDBCType(dt: DataType): Option[JdbcType] =
    delegate.getJDBCType(dt)

  override def compileValue(value: Any): Any = delegate.compileValue(value)

  override def isCascadingTruncateTable(): Option[Boolean] =
    delegate.isCascadingTruncateTable()

  /** The SQL query used to truncate a table.
    * @param table
    *   The table to truncate
    * @param cascade
    *   Whether or not to cascade the truncation. Default value is the value of
    *   isCascadingTruncateTable()
    * @return
    *   The SQL query to use for truncating a table
    */
  override def getTruncateQuery(
      table: String,
      cascade: Option[Boolean] = isCascadingTruncateTable()
  ): String =
    delegate.getTruncateQuery(table, cascade)

  override def getAddColumnQuery(
      tableName: String,
      columnName: String,
      dataType: String
  ): String =
    delegate.getAddColumnQuery(tableName, columnName, dataType)

  override def getUpdateColumnTypeQuery(
      tableName: String,
      columnName: String,
      newDataType: String
  ): String =
    delegate.getUpdateColumnTypeQuery(tableName, columnName, newDataType)

  override def getUpdateColumnNullabilityQuery(
      tableName: String,
      columnName: String,
      isNullable: Boolean
  ): String =
    delegate.getUpdateColumnNullabilityQuery(tableName, columnName, isNullable)
}

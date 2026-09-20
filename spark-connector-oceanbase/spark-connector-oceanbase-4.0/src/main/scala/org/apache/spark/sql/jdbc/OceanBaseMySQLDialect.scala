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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.analysis.{
  IndexAlreadyExistsException,
  NoSuchIndexException
}
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.index.TableIndex
import org.apache.spark.sql.connector.expressions.{
  Expression,
  FieldReference,
  NamedReference
}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.types._

import java.sql.{Connection, SQLException, Types}
import java.util
import java.util.Locale

/** Spark 4.0 compatible OceanBase MySQL dialect. In Spark 4.0, [[MySQLDialect]]
  * changed from a case object to a case class, so we create an instance for
  * delegation.
  */
case object OceanBaseMySQLDialect extends JdbcDialect {

  private val delegate = new MySQLDialect()

  override def canHandle(url: String): Boolean =
    url.toLowerCase(Locale.ROOT).startsWith("jdbc:oceanbase") || url
      .toLowerCase(Locale.ROOT)
      .startsWith("jdbc:mysql")

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

  override def quoteIdentifier(colName: String): String =
    delegate.quoteIdentifier(colName)

  override def schemasExists(
      conn: Connection,
      options: JDBCOptions,
      schema: String
  ): Boolean =
    delegate.schemasExists(conn, options, schema)

  override def listSchemas(
      conn: Connection,
      options: JDBCOptions
  ): Array[Array[String]] =
    delegate.listSchemas(conn, options)

  override def getTableExistsQuery(table: String): String =
    delegate.getTableExistsQuery(table)

  override def isCascadingTruncateTable(): Option[Boolean] =
    delegate.isCascadingTruncateTable()

  override def getUpdateColumnTypeQuery(
      tableName: String,
      columnName: String,
      newDataType: String
  ): String =
    delegate.getUpdateColumnTypeQuery(tableName, columnName, newDataType)

  override def getRenameColumnQuery(
      tableName: String,
      columnName: String,
      newName: String,
      dbMajorVersion: Int
  ): String =
    super.getRenameColumnQuery(tableName, columnName, newName, dbMajorVersion)

  override def getUpdateColumnNullabilityQuery(
      tableName: String,
      columnName: String,
      isNullable: Boolean
  ): String =
    delegate.getUpdateColumnNullabilityQuery(tableName, columnName, isNullable)

  override def getTableCommentQuery(table: String, comment: String): String =
    delegate.getTableCommentQuery(table, comment)

  override def getJDBCType(dt: DataType): Option[JdbcType] =
    delegate.getJDBCType(dt)

  override def getSchemaCommentQuery(schema: String, comment: String): String =
    delegate.getSchemaCommentQuery(schema, comment)

  override def removeSchemaCommentQuery(schema: String): String =
    delegate.removeSchemaCommentQuery(schema)

  override def createIndex(
      indexName: String,
      tableIdent: Identifier,
      columns: Array[NamedReference],
      columnsProperties: util.Map[NamedReference, util.Map[String, String]],
      properties: util.Map[String, String]
  ): String =
    delegate.createIndex(
      indexName,
      tableIdent,
      columns,
      columnsProperties,
      properties
    )

  override def indexExists(
      conn: Connection,
      indexName: String,
      tableIdent: Identifier,
      options: JDBCOptions
  ): Boolean =
    delegate.indexExists(conn, indexName, tableIdent, options)

  override def dropIndex(indexName: String, tableIdent: Identifier): String =
    delegate.dropIndex(indexName, tableIdent)

  override def listIndexes(
      conn: Connection,
      tableIdent: Identifier,
      options: JDBCOptions
  ): Array[TableIndex] = delegate.listIndexes(conn, tableIdent, options)

  override def classifyException(
      message: String,
      e: Throwable
  ): AnalysisException =
    delegate.classifyException(message, e)

  override def dropSchema(schema: String, cascade: Boolean): String =
    delegate.dropSchema(schema, cascade)
}

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

package com.oceanbase.spark.dialect

import com.oceanbase.spark.utils.OBJdbcUtils.executeStatement

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.catalog.TableChange
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcOptionsInWrite}
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.schemaString
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types.StructType

import java.sql.Connection

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/** This is for better compatibility with earlier versions of Spark. */
abstract class OceanBaseDialect extends Logging with Serializable {

  /** Returns true if the table already exists in the JDBC database. */
  def tableExists(conn: Connection, options: JdbcOptionsInWrite): Boolean = {
    val dialect = JdbcDialects.get(options.url)

    Try {
      val statement = conn.prepareStatement(dialect.getTableExistsQuery(options.table))
      try {
        statement.setQueryTimeout(options.queryTimeout)
        statement.executeQuery()
      } finally {
        statement.close()
      }
    }.isSuccess
  }

  /** Drops a table from the JDBC database. */
  def dropTable(conn: Connection, table: String, options: JDBCOptions): Unit = {
    executeStatement(conn, options, s"DROP TABLE $table")
  }

  /** Truncates a table from the JDBC database without side effects. */
  def truncateTable(conn: Connection, options: JdbcOptionsInWrite): Unit = {
    val dialect = JdbcDialects.get(options.url)
    val statement = conn.createStatement
    try {
      statement.setQueryTimeout(options.queryTimeout)
      val truncateQuery = if (options.isCascadeTruncate.isDefined) {
        dialect.getTruncateQuery(options.table, options.isCascadeTruncate)
      } else {
        dialect.getTruncateQuery(options.table)
      }
      statement.executeUpdate(truncateQuery)
    } finally {
      statement.close()
    }
  }

  def createTable(
      conn: Connection,
      tableName: String,
      schema: StructType,
      caseSensitive: Boolean,
      options: JdbcOptionsInWrite): Unit = {
    val dialect = JdbcDialects.get(options.url)
    val strSchema = schemaString(schema, caseSensitive, options.url, options.createTableColumnTypes)
    val createTableOptions = options.createTableOptions
    // Create the table if the table does not exist.
    // To allow certain options to append when create a new table, which can be
    // table_options or partition_options.
    // E.g., "CREATE TABLE t (name string) ENGINE=InnoDB DEFAULT CHARSET=utf8"
    val sql = s"CREATE TABLE $tableName ($strSchema) $createTableOptions"
    executeStatement(conn, options, sql)
    if (options.tableComment.nonEmpty) {
      try {
        executeStatement(
          conn,
          options,
          dialect.getTableCommentQuery(tableName, options.tableComment))
      } catch {
        case e: Exception =>
          logWarning("Cannot create JDBC table comment. The table comment will be ignored.")
      }
    }
  }

  /** Rename a table from the JDBC database. */
  def renameTable(
      conn: Connection,
      oldTable: String,
      newTable: String,
      options: JDBCOptions): Unit = {
    val dialect = JdbcDialects.get(options.url)
    executeStatement(conn, options, dialect.renameTable(oldTable, newTable))
  }

  /** Update a table from the JDBC database. */
  def alterTable(
      conn: Connection,
      tableName: String,
      changes: Seq[TableChange],
      options: JDBCOptions): Unit = {
    val dialect = JdbcDialects.get(options.url)
    val metaData = conn.getMetaData
    if (changes.length == 1) {
      executeStatement(
        conn,
        options,
        dialect.alterTable(tableName, changes, metaData.getDatabaseMajorVersion)(0))
    } else {
      conn.setAutoCommit(false)
      val statement = conn.createStatement
      try {
        statement.setQueryTimeout(options.queryTimeout)
        for (sql <- dialect.alterTable(tableName, changes, metaData.getDatabaseMajorVersion)) {
          statement.executeUpdate(sql)
        }
        conn.commit()
      } catch {
        case e: Exception =>
          if (conn != null) conn.rollback()
          throw e
      } finally {
        statement.close()
        conn.setAutoCommit(true)
      }
    }
  }

  /** Creates a schema. */
  def createSchema(conn: Connection, options: JDBCOptions, schema: String, comment: String): Unit

  def schemaExists(conn: Connection, options: JDBCOptions, schema: String): Boolean

  def listSchemas(conn: Connection, options: JDBCOptions): Array[Array[String]]

  /** Drops a schema from the JDBC database. */
  def dropSchema(conn: Connection, options: JDBCOptions, schema: String, cascade: Boolean): Unit

  /**
   * Quotes the identifier. This is used to put quotes around the identifier in case the column name
   * is a reserved keyword, or in case it contains characters that require quotes (e.g. space).
   */
  def quoteIdentifier(colName: String): String = {
    s"""`$colName`"""
  }

  def getPriKeyInfo(
      schemaName: String,
      tableName: String,
      option: JDBCOptions): ArrayBuffer[PriKeyColumnInfo]

  def getInsertIntoStatement(tableName: String, schema: StructType): String

  def getUpsertIntoStatement(
      tableName: String,
      schema: StructType,
      priKeyColumnInfo: ArrayBuffer[PriKeyColumnInfo]): String
}

case class PriKeyColumnInfo(columnName: String, columnType: String, columnKey: String)

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

import com.oceanbase.spark.utils.OBJdbcUtils
import com.oceanbase.spark.utils.OBJdbcUtils.executeStatement

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.types.StructType

import java.sql.Connection

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer;

class OceanBaseMySQLDialect extends OceanBaseDialect {

  /** Creates a schema. */
  override def createSchema(
      conn: Connection,
      options: JDBCOptions,
      schema: String,
      comment: String): Unit = {
    val statement = conn.createStatement
    try {
      statement.setQueryTimeout(options.queryTimeout)
      val schemaCommentQuery = if (comment.nonEmpty) {
        // We generate comment query here so that it can fail earlier without creating the schema.
        getSchemaCommentQuery(schema, comment)
      } else {
        comment
      }
      statement.executeUpdate(s"CREATE SCHEMA ${quoteIdentifier(schema)}")
      if (comment.nonEmpty) {
        statement.executeUpdate(schemaCommentQuery)
      }
    } finally {
      statement.close()
    }
  }

  override def schemaExists(conn: Connection, options: JDBCOptions, schema: String): Boolean = {
    listSchemas(conn, options).exists(_.head == schema)
  }

  override def listSchemas(conn: Connection, options: JDBCOptions): Array[Array[String]] = {
    val schemaBuilder = mutable.ArrayBuilder.make[Array[String]]
    try {
      OBJdbcUtils.executeQuery(conn, options, "SHOW SCHEMAS") {
        rs =>
          while (rs.next()) {
            schemaBuilder += Array(rs.getString("Database"))
          }
      }
    } catch {
      case _: Exception =>
        logWarning("Cannot show schemas.")
    }
    schemaBuilder.result
  }

  override def alterSchemaComment(
      conn: Connection,
      options: JDBCOptions,
      schema: String,
      comment: String): Unit = {
    executeStatement(conn, options, getSchemaCommentQuery(schema, comment))
  }

  override def removeSchemaComment(conn: Connection, options: JDBCOptions, schema: String): Unit = {
    executeStatement(conn, options, removeSchemaCommentQuery(schema))
  }

  /** Drops a schema from OceanBase. */
  override def dropSchema(
      conn: Connection,
      options: JDBCOptions,
      schema: String,
      cascade: Boolean): Unit = {
    executeStatement(
      conn,
      options,
      if (cascade) {
        s"DROP SCHEMA ${quoteIdentifier(schema)} CASCADE"
      } else {
        s"DROP SCHEMA ${quoteIdentifier(schema)}"
      })
  }

  def getPriKeyInfo(
      schemaName: String,
      tableName: String,
      option: JDBCOptions): ArrayBuffer[PriKeyColumnInfo] = {
    val sql =
      s"""
         |select
         |  COLUMN_NAME, COLUMN_TYPE , COLUMN_KEY
         |from
         |  information_schema.columns
         |where
         |      TABLE_SCHEMA = '$schemaName'
         |  and TABLE_NAME = '$tableName';
         |""".stripMargin

    OBJdbcUtils.withConnection(option) {
      val arrayBuffer = ArrayBuffer[PriKeyColumnInfo]()
      conn =>
        OBJdbcUtils.executeQuery(conn, option, sql) {
          rs =>
            {
              while (rs.next()) {
                val columnKey = rs.getString(3)
                if (null != columnKey && columnKey.equals("PRI")) {
                  arrayBuffer += PriKeyColumnInfo(rs.getString(1), rs.getString(2), columnKey)
                }
              }
            }
        }
        arrayBuffer
    }
  }

  def getInsertIntoStatement(tableName: String, schema: StructType): String = {
    val columnClause =
      schema.fieldNames.map(columnName => quoteIdentifier(columnName)).mkString(", ")
    val placeholders = schema.fieldNames.map(_ => "?").mkString(", ")
    s"""
       |INSERT INTO $tableName ($columnClause)
       |VALUES ($placeholders)
       |""".stripMargin
  }

  def getUpsertIntoStatement(
      tableName: String,
      schema: StructType,
      priKeyColumnInfo: ArrayBuffer[PriKeyColumnInfo]): String = {
    val uniqueKeys = priKeyColumnInfo.map(_.columnName).toSet
    val nonUniqueFields = schema.fieldNames.filterNot(uniqueKeys.contains)

    val baseInsert = {
      val columns = schema.fieldNames.map(quoteIdentifier).mkString(", ")
      val placeholders = schema.fieldNames.map(_ => "?").mkString(", ")
      s"INSERT INTO $tableName ($columns) VALUES ($placeholders)"
    }

    if (nonUniqueFields.nonEmpty) {
      // ON DUPLICATE KEY UPDATE
      val updateClause = nonUniqueFields
        .map(f => s"${quoteIdentifier(f)} = VALUES(${quoteIdentifier(f)})")
        .mkString(", ")
      s"$baseInsert ON DUPLICATE KEY UPDATE $updateClause"
    } else {
      // INSERT IGNORE
      baseInsert.replace("INSERT", "INSERT IGNORE")
    }
  }

}

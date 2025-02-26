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

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.ExprUtils
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcOptionsInWrite}
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, CharType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructType, TimestampType, VarcharType}

import java.sql.Connection

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer;

class OceanBaseMySQLDialect extends OceanBaseDialect {

  def createTable(
      conn: Connection,
      tableName: String,
      schema: StructType,
      partitions: Array[Transform],
      options: JdbcOptionsInWrite,
      properties: java.util.Map[String, String]): Unit = {

    def buildCreateTableSQL(
        tableName: String,
        schema: StructType,
        transforms: Array[Transform],
        options: JdbcOptionsInWrite): String = {
      val partitionClause = buildPartitionClause(transforms, options)
      val columnClause = schema.fields
        .map {
          field =>
            val obType = toOceanBaseMySQLType(field.dataType)
            val nullability = if (field.nullable) StringUtils.EMPTY else "NOT NULL"
            val comment = field.getComment() match {
              case Some(v) => s"COMMENT '$v'"
              case _ => StringUtils.EMPTY
            }
            // TODO: support default value
            s"${quoteIdentifier(field.name)} $obType $nullability $comment".trim
        }
        .mkString(",\n  ")
      val tableComment = options.tableComment match {
        case comment if comment.nonEmpty => s"COMMENT '$comment'"
        case _ => StringUtils.EMPTY
      }
      val tableOption = properties.asScala
        .map(tuple => (tuple._1.toLowerCase, tuple._2))
        .flatMap {
          case ("charset", value) => Some(s"DEFAULT CHARSET = $value")
          case ("collate", value) => Some(s"COLLATE = $value")
          case ("primary_zone", value) => Some(s"PRIMARY_ZONE = '$value'")
          case ("replica_num", value) => Some(s"REPLICA_NUM = $value")
          case ("compression", value) => Some(s"COMPRESSION = '$value'")
          case (k, _) =>
            logWarning(s"Ignored unsupported table property: $k")
            None
        }
        .mkString(" ", " ", "")
      s"""
         |CREATE TABLE $tableName (
         |  $columnClause
         |) $tableOption $tableComment
         |$partitionClause;
         |""".stripMargin.trim
    }

    def toOceanBaseMySQLType(dataType: DataType): String = {
      val defaultStringLength = 10240
      dataType match {
        case BooleanType => "BOOLEAN"
        case ByteType => "BYTE"
        case ShortType => "SMALLINT"
        case IntegerType => "INT"
        case LongType => "BIGINT"
        case FloatType => "FLOAT"
        case DoubleType => "DOUBLE"
        case d: DecimalType => s"DECIMAL(${d.precision},${d.scale})"
        case c: CharType => s"CHAR(${c.length})"
        case v: VarcharType => s"VARCHAR(${v.length})"
        case StringType => s"VARCHAR($defaultStringLength)"
        case BinaryType => "BINARY"
        case DateType => "DATE"
        case TimestampType => "DATETIME"
        // TODO: Support array data-type
        case _ => throw new UnsupportedOperationException(s"Unsupported type: $dataType")
      }
    }

    def buildPartitionClause(transforms: Array[Transform], options: JdbcOptionsInWrite): String = {
      transforms match {
        case transforms if transforms.nonEmpty =>
          ExprUtils.toOBMySQLPartition(transforms.head, options)
        case _ => ""
      }
    }

    val sql = buildCreateTableSQL(tableName, schema, partitions, options)
    executeStatement(conn, options, sql)
  }

  /** Creates a schema. */
  override def createSchema(
      conn: Connection,
      options: JDBCOptions,
      schema: String,
      comment: String): Unit = {
    // OceanBase mysql mode does not support schema comments, so we ignore the comment parameter.
    val statement = conn.createStatement
    try {
      statement.setQueryTimeout(options.queryTimeout)
      statement.executeUpdate(s"CREATE SCHEMA ${quoteIdentifier(schema)}")
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

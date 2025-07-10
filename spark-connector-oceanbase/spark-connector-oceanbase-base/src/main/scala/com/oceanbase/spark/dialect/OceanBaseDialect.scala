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

import com.oceanbase.spark.config.OceanBaseConfig
import com.oceanbase.spark.utils.OBJdbcUtils.executeStatement

import org.apache.commons.lang3.StringUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.{DateFormatter, DateTimeUtils, TimestampFormatter}
import org.apache.spark.sql.connector.catalog.TableChange
import org.apache.spark.sql.connector.catalog.TableChange.{AddColumn, DeleteColumn, RenameColumn, UpdateColumnNullability, UpdateColumnType}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MetadataBuilder, ShortType, StringType, StructType, TimestampType}

import java.sql.{Connection, Date, Timestamp}
import java.time.{Instant, LocalDate}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/** This is for better compatibility with earlier versions of Spark. */
abstract class OceanBaseDialect extends Logging with Serializable {

  /** Returns true if the table already exists in the JDBC database. */
  def tableExists(conn: Connection, config: OceanBaseConfig): Boolean = {
    Try {
      val statement = conn.prepareStatement(getTableExistsQuery(config.getDbTable))
      try {
        statement.setQueryTimeout(config.getJdbcQueryTimeout)
        statement.executeQuery()
      } finally {
        statement.close()
      }
    }.isSuccess
  }

  /** Drops a table from the JDBC database. */
  def dropTable(conn: Connection, table: String, config: OceanBaseConfig): Unit = {
    executeStatement(conn, config, s"DROP TABLE $table")
  }

  /** Truncates a table from the JDBC database without side effects. */
  def truncateTable(conn: Connection, config: OceanBaseConfig): Unit = {
    val statement = conn.createStatement
    try {
      statement.setQueryTimeout(config.getJdbcQueryTimeout)
      statement.executeUpdate(getTruncateQuery(config.getDbTable))
    } finally {
      statement.close()
    }
  }

  def createTable(
      conn: Connection,
      tableName: String,
      schema: StructType,
      partitions: Array[Transform],
      config: OceanBaseConfig,
      properties: java.util.Map[String, String]): Unit

  /** Rename a table from the JDBC database. */
  def renameTable(
      conn: Connection,
      oldTable: String,
      newTable: String,
      config: OceanBaseConfig): Unit = {
    executeStatement(conn, config, getRenameTableSql(oldTable, newTable))
  }

  /** Update a table from the JDBC database. */
  def alterTable(
      conn: Connection,
      tableName: String,
      changes: Seq[TableChange],
      config: OceanBaseConfig): Unit = {
    val metaData = conn.getMetaData
    if (changes.length == 1) {
      executeStatement(
        conn,
        config,
        alterTable(tableName, changes, metaData.getDatabaseMajorVersion)(0))
    } else {
      conn.setAutoCommit(false)
      val statement = conn.createStatement
      try {
        statement.setQueryTimeout(config.getJdbcQueryTimeout)
        for (sql <- alterTable(tableName, changes, metaData.getDatabaseMajorVersion)) {
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

  def getTableExistsQuery(table: String): String = {
    s"SELECT * FROM $table WHERE 1=0"
  }

  def getRenameTableSql(oldTable: String, newTable: String): String = {
    s"ALTER TABLE $oldTable RENAME TO $newTable"
  }

  def getTruncateQuery(table: String, cascade: Option[Boolean] = Some(false)): String = {
    s"TRUNCATE TABLE $table"
  }

  def getDeleteWhereSql(tableName: String, whereClause: String): String = {
    val sql = s"DELETE FROM $tableName $whereClause"
    logInfo(s"The generated OceanBase delete sql statement: $sql")
    sql
  }

  /** returns the LIMIT clause for the SELECT statement */
  def getLimitClause(limit: Integer): String = {
    if (limit > 0) s"LIMIT $limit" else ""
  }

  def getJDBCType(dt: DataType): Option[JdbcType] = None

  /** Creates a schema. */
  def createSchema(conn: Connection, config: OceanBaseConfig, schema: String, comment: String): Unit

  def schemaExists(conn: Connection, config: OceanBaseConfig, schema: String): Boolean

  def listSchemas(conn: Connection, config: OceanBaseConfig): Array[Array[String]]

  /** Drops a schema from the JDBC database. */
  def dropSchema(conn: Connection, config: OceanBaseConfig, schema: String, cascade: Boolean): Unit

  /**
   * Quotes the identifier. This is used to put quotes around the identifier in case the column name
   * is a reserved keyword, or in case it contains characters that require quotes (e.g. space).
   */
  def quoteIdentifier(colName: String): String = {
    s"""`$colName`"""
  }

  def unQuoteIdentifier(colName: String): String = {
    colName.replace("`", "")
  }

  def getPriKeyInfo(
      connection: Connection,
      schemaName: String,
      tableName: String,
      config: OceanBaseConfig): ArrayBuffer[PriKeyColumnInfo]

  def getUniqueKeyInfo(
      connection: Connection,
      schemaName: String,
      tableName: String,
      config: OceanBaseConfig): ArrayBuffer[PriKeyColumnInfo]

  def getInsertIntoStatement(tableName: String, schema: StructType): String

  def getUpsertIntoStatement(
      tableName: String,
      schema: StructType,
      priKeyColumnInfo: ArrayBuffer[PriKeyColumnInfo]): String

  /**
   * The SQL query that should be used to discover the schema of a table. It only needs to ensure
   * that the result set has the same schema as the table, such as by calling "SELECT * ...".
   * Dialects can override this method to return a query that works best in a particular database.
   * @param table
   *   The name of the table.
   * @return
   *   The SQL query to use for discovering the schema.
   */
  def getSchemaQuery(table: String): String = {
    s"SELECT * FROM $table WHERE 1=0"
  }

  def getJdbcType(dt: DataType): JdbcType = {
    getJDBCType(dt)
      .orElse(getCommonJDBCType(dt))
      .getOrElse(throw new RuntimeException(s"cannotGetJdbcTypeError $dt"))
  }

  /**
   * Get the custom datatype mapping for the given jdbc meta information.
   * @param sqlType
   *   The sql type (see java.sql.Types)
   * @param typeName
   *   The sql type name (e.g. "BIGINT UNSIGNED")
   * @param size
   *   The size of the type.
   * @param md
   *   Result metadata associated with this type.
   * @return
   *   The actual DataType (subclasses of [[org.apache.spark.sql.types.DataType]]) or null if the
   *   default type mapping should be used.
   */
  def getCatalystType(
      sqlType: Int,
      typeName: String,
      size: Int,
      md: MetadataBuilder): Option[DataType] = None

  /**
   * Retrieve standard jdbc types.
   *
   * @param dt
   *   The datatype (e.g. [[org.apache.spark.sql.types.StringType]])
   * @return
   *   The default JdbcType for this DataType
   */
  def getCommonJDBCType(dt: DataType): Option[JdbcType] = {
    dt match {
      case IntegerType => Option(JdbcType("INTEGER", java.sql.Types.INTEGER))
      case LongType => Option(JdbcType("BIGINT", java.sql.Types.BIGINT))
      case DoubleType => Option(JdbcType("DOUBLE PRECISION", java.sql.Types.DOUBLE))
      case FloatType => Option(JdbcType("REAL", java.sql.Types.FLOAT))
      case ShortType => Option(JdbcType("INTEGER", java.sql.Types.SMALLINT))
      case ByteType => Option(JdbcType("BYTE", java.sql.Types.TINYINT))
      case BooleanType => Option(JdbcType("BIT(1)", java.sql.Types.BIT))
      case StringType => Option(JdbcType("TEXT", java.sql.Types.CLOB))
      case BinaryType => Option(JdbcType("BLOB", java.sql.Types.BLOB))
      case TimestampType => Option(JdbcType("TIMESTAMP", java.sql.Types.TIMESTAMP))
      case DateType => Option(JdbcType("DATE", java.sql.Types.DATE))
      case t: DecimalType =>
        Option(JdbcType(s"DECIMAL(${t.precision},${t.scale})", java.sql.Types.DECIMAL))
      case _ => None
    }
  }

  /**
   * Escape special characters in SQL string literals.
   * @param value
   *   The string to be escaped.
   * @return
   *   Escaped string.
   */
  def escapeSql(value: String): String =
    if (value == null) null else StringUtils.replace(value, "'", "''")

  /**
   * Converts value to SQL expression.
   * @param value
   *   The value to be converted.
   * @return
   *   Converted value.
   */
  def compileValue(value: Any): Any = value match {
    case stringValue: String => s"'${escapeSql(stringValue)}'"
    case timestampValue: Timestamp => "'" + timestampValue + "'"
    case timestampValue: Instant =>
      val timestampFormatter = TimestampFormatter.getFractionFormatter(
        DateTimeUtils.getZoneId(SQLConf.get.sessionLocalTimeZone))
      s"'${timestampFormatter.format(timestampValue)}'"
    case dateValue: Date => "'" + dateValue + "'"
    case dateValue: LocalDate => s"'${DateFormatter().format(dateValue)}'"
    case arrayValue: Array[Any] => arrayValue.map(compileValue).mkString(", ")
    case _ => value
  }

  /**
   * Alter an existing table.
   *
   * @param tableName
   *   The name of the table to be altered.
   * @param changes
   *   Changes to apply to the table.
   * @return
   *   The SQL statements to use for altering the table.
   */
  def alterTable(
      tableName: String,
      changes: Seq[TableChange],
      dbMajorVersion: Int): Array[String] = {
    val updateClause = mutable.ArrayBuilder.make[String]
    for (change <- changes) {
      change match {
        case add: AddColumn if add.fieldNames.length == 1 =>
          val dataType = getJdbcType(add.dataType()).databaseTypeDefinition
          val name = add.fieldNames
          updateClause += getAddColumnQuery(tableName, name(0), dataType)
        case rename: RenameColumn if rename.fieldNames.length == 1 =>
          val name = rename.fieldNames
          updateClause += getRenameColumnQuery(tableName, name(0), rename.newName, dbMajorVersion)
        case delete: DeleteColumn if delete.fieldNames.length == 1 =>
          val name = delete.fieldNames
          updateClause += getDeleteColumnQuery(tableName, name(0))
        case updateColumnType: UpdateColumnType if updateColumnType.fieldNames.length == 1 =>
          val name = updateColumnType.fieldNames
          val dataType = getJdbcType(updateColumnType.newDataType()).databaseTypeDefinition
          updateClause += getUpdateColumnTypeQuery(tableName, name(0), dataType)
        case updateNull: UpdateColumnNullability if updateNull.fieldNames.length == 1 =>
          val name = updateNull.fieldNames
          updateClause += getUpdateColumnNullabilityQuery(tableName, name(0), updateNull.nullable())
        case _ =>
          throw new RuntimeException(
            s"unsupportedTableChangeInJDBCCatalogError: ${change.toString}")
      }
    }
    updateClause.result()
  }

  def getAddColumnQuery(tableName: String, columnName: String, dataType: String): String =
    s"ALTER TABLE $tableName ADD COLUMN ${quoteIdentifier(columnName)} $dataType"

  def getRenameColumnQuery(
      tableName: String,
      columnName: String,
      newName: String,
      dbMajorVersion: Int): String =
    s"ALTER TABLE $tableName RENAME COLUMN ${quoteIdentifier(columnName)} TO" +
      s" ${quoteIdentifier(newName)}"

  def getDeleteColumnQuery(tableName: String, columnName: String): String =
    s"ALTER TABLE $tableName DROP COLUMN ${quoteIdentifier(columnName)}"

  def getUpdateColumnTypeQuery(tableName: String, columnName: String, newDataType: String): String =
    s"ALTER TABLE $tableName ALTER COLUMN ${quoteIdentifier(columnName)} $newDataType"

  def getUpdateColumnNullabilityQuery(
      tableName: String,
      columnName: String,
      isNullable: Boolean): String = {
    val nullable = if (isNullable) "NULL" else "NOT NULL"
    s"ALTER TABLE $tableName ALTER COLUMN ${quoteIdentifier(columnName)} SET $nullable"
  }
}

case class PriKeyColumnInfo(
    columnName: String,
    columnType: String,
    columnKey: String,
    dataType: String,
    extra: String)

case class JdbcType(databaseTypeDefinition: String, jdbcNullType: Int)

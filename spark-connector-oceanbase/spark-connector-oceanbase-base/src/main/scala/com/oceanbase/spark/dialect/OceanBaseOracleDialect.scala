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
import com.oceanbase.spark.utils.OBJdbcUtils
import com.oceanbase.spark.utils.OBJdbcUtils.executeStatement

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.ExprUtils
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.expressions.{Expression, Transform}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MetadataBuilder, ShortType, StringType, StructType, TimestampType, VarcharType}

import java.sql.{Connection, Date, Timestamp, Types}
import java.util
import java.util.TimeZone

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

class OceanBaseOracleDialect extends OceanBaseDialect {
  override def quoteIdentifier(colName: String): String = {
    s""""$colName""""
  }

  override def unQuoteIdentifier(colName: String): String = {
    colName.replace("\"", "")
  }

  override def createTable(
      conn: Connection,
      tableName: String,
      schema: StructType,
      partitions: Array[Transform],
      config: OceanBaseConfig,
      properties: util.Map[String, String]): Unit = {

    // Collect column and table comments (use COMMENT ON statements in Oracle mode)
    val columnComments: mutable.ArrayBuffer[(String, String)] = mutable.ArrayBuffer.empty
    val tableCommentOpt: Option[String] = Option(config.getTableComment)

    def buildCreateTableSQL(
        tableName: String,
        schema: StructType,
        transforms: Array[Transform],
        config: OceanBaseConfig): String = {
      val partitionClause = buildPartitionClause(transforms, config)
      val columnClause = schema.fields
        .map {
          field =>
            val obType = toOceanBaseOracleType(field.dataType, config)
            val nullability = if (field.nullable) StringUtils.EMPTY else "NOT NULL"
            field.getComment().foreach(c => columnComments += ((field.name, c)))
            s"${quoteIdentifier(field.name)} $obType $nullability".trim
        }
        .mkString(",\n  ")
      var primaryKey = ""
      val tableOption = scala.collection.JavaConverters
        .mapAsScalaMapConverter(properties)
        .asScala
        .map(tuple => (tuple._1.toLowerCase, tuple._2))
        .flatMap {
          case ("tablespace", value) => Some(s"TABLESPACE $value")
          case ("compression", value) =>
            logWarning(s"Ignored unsupported table property on Oracle mode: compression=$value");
            None
          case ("replica_num", value) =>
            logWarning(s"Ignored unsupported table property on Oracle mode: replica_num=$value");
            None
          case ("primary_key", value) =>
            primaryKey = s", CONSTRAINT pk_${config.getTableName} PRIMARY KEY($value)"
            None
          case (k, _) =>
            logWarning(s"Ignored unsupported table property: $k")
            None
        }
        .mkString(" ", " ", "")
      s"""
         |CREATE TABLE $tableName (
         |  $columnClause
         |  $primaryKey
         |) $tableOption
         |$partitionClause;
         |""".stripMargin.trim
    }

    def toOceanBaseOracleType(dataType: DataType, config: OceanBaseConfig): String = {
      var stringConvertType = s"VARCHAR2(${config.getLengthString2Varchar})"
      if (config.getEnableString2Text) stringConvertType = "CLOB"
      dataType match {
        case BooleanType => "NUMBER(1)"
        case ByteType => "NUMBER(3)"
        case ShortType => "NUMBER(5)"
        case IntegerType => "NUMBER(10)"
        case LongType => "NUMBER(19)"
        case FloatType => "BINARY_FLOAT"
        case DoubleType => "BINARY_DOUBLE"
        case d: DecimalType => s"NUMBER(${d.precision},${d.scale})"
        case StringType => stringConvertType
        case BinaryType => "RAW(2000)"
        case DateType => "DATE"
        case TimestampType => "TIMESTAMP"
        case v: VarcharType => s"VARCHAR2(${v.length})"
        case _ => throw new UnsupportedOperationException(s"Unsupported type: $dataType")
      }
    }

    def buildPartitionClause(transforms: Array[Transform], config: OceanBaseConfig): String = {
      transforms match {
        case transforms if transforms.nonEmpty =>
          ExprUtils.toOBOraclePartition(transforms.head, config)
        case _ => ""
      }
    }

    val sql = buildCreateTableSQL(tableName, schema, partitions, config)
    executeStatement(conn, config, sql)

    // Apply column comments
    columnComments.foreach {
      case (col, comment) =>
        val colName = quoteIdentifier(col)
        val commentSql = s"COMMENT ON COLUMN $tableName.$colName IS '${escapeSql(comment)}'"
        executeStatement(conn, config, commentSql)
    }

    // Apply table comment
    tableCommentOpt.foreach {
      comment =>
        val tblCommentSql = s"COMMENT ON TABLE $tableName IS '${escapeSql(comment)}'"
        executeStatement(conn, config, tblCommentSql)
    }

    // In Oracle mode, complex table properties (compression/replica count) are not supported; ignored
  }

  /** Creates a schema. */
  override def createSchema(
      conn: Connection,
      config: OceanBaseConfig,
      schema: String,
      comment: String): Unit = {
    // In Oracle mode, schema equals user; create a user
    val statement = conn.createStatement
    try {
      statement.setQueryTimeout(config.getJdbcQueryTimeout)
      // Note: a real password is required; using a default here
      // In Oracle mode, password should not use single quotes
      statement.executeUpdate(s"CREATE USER ${quoteIdentifier(schema)} IDENTIFIED BY password")
      // Grant basic privileges
      statement.executeUpdate(s"GRANT CONNECT, RESOURCE TO ${quoteIdentifier(schema)}")
    } finally {
      statement.close()
    }
  }

  override def schemaExists(conn: Connection, config: OceanBaseConfig, schema: String): Boolean = {
    listSchemas(conn, config).exists(_.head == schema)
  }

  override def listSchemas(conn: Connection, config: OceanBaseConfig): Array[Array[String]] = {
    val schemaBuilder = mutable.ArrayBuilder.make[Array[String]]
    try {
      OBJdbcUtils.executeQuery(conn, config, "SELECT USERNAME FROM ALL_USERS ORDER BY USERNAME") {
        rs =>
          while (rs.next()) {
            schemaBuilder += Array(rs.getString("USERNAME"))
          }
      }
    } catch {
      case _: Exception =>
        logWarning("Cannot list schemas.")
    }
    schemaBuilder.result
  }

  /** Drops a schema from OceanBase. */
  override def dropSchema(
      conn: Connection,
      config: OceanBaseConfig,
      schema: String,
      cascade: Boolean): Unit = {
    // In OceanBase Oracle mode, DROP USER without CASCADE is not supported
    // Always use CASCADE regardless of the cascade parameter
    executeStatement(conn, config, s"DROP USER ${quoteIdentifier(schema)} CASCADE")
  }

  override def getPriKeyInfo(
      connection: Connection,
      schemaName: String,
      tableName: String,
      config: OceanBaseConfig): ArrayBuffer[PriKeyColumnInfo] = {
    val sql =
      s"""
         |SELECT
         |  cc.COLUMN_NAME,
         |  tc.DATA_TYPE,
         |  cc.CONSTRAINT_NAME,
         |  tc.DATA_TYPE,
         |  cc.CONSTRAINT_NAME
         |FROM
         |  ALL_CONSTRAINTS c
         |  JOIN ALL_CONS_COLUMNS cc ON c.CONSTRAINT_NAME = cc.CONSTRAINT_NAME
         |    AND c.OWNER = cc.OWNER
         |  JOIN ALL_TAB_COLUMNS tc ON cc.TABLE_NAME = tc.TABLE_NAME
         |    AND cc.COLUMN_NAME = tc.COLUMN_NAME
         |    AND cc.OWNER = tc.OWNER
         |WHERE
         |  c.OWNER = '$schemaName'
         |  AND c.TABLE_NAME = '$tableName'
         |  AND c.CONSTRAINT_TYPE = 'P'
         |ORDER BY cc.POSITION
         |""".stripMargin

    val arrayBuffer = ArrayBuffer[PriKeyColumnInfo]()
    OBJdbcUtils.executeQuery(connection, config, sql) {
      rs =>
        {
          while (rs.next()) {
            arrayBuffer += PriKeyColumnInfo(
              quoteIdentifier(rs.getString(1)),
              rs.getString(2),
              "PRI",
              rs.getString(4),
              rs.getString(5))
          }
          arrayBuffer
        }
    }
  }

  override def getUniqueKeyInfo(
      connection: Connection,
      schemaName: String,
      tableName: String,
      config: OceanBaseConfig): ArrayBuffer[PriKeyColumnInfo] = {
    val sql =
      s"""
         |SELECT
         |  cc.COLUMN_NAME,
         |  tc.DATA_TYPE,
         |  cc.CONSTRAINT_NAME,
         |  tc.DATA_TYPE,
         |  cc.CONSTRAINT_NAME
         |FROM
         |  ALL_CONSTRAINTS c
         |  JOIN ALL_CONS_COLUMNS cc ON c.CONSTRAINT_NAME = cc.CONSTRAINT_NAME
         |    AND c.OWNER = cc.OWNER
         |  JOIN ALL_TAB_COLUMNS tc ON cc.TABLE_NAME = tc.TABLE_NAME
         |    AND cc.COLUMN_NAME = tc.COLUMN_NAME
         |    AND cc.OWNER = tc.OWNER
         |WHERE
         |  c.OWNER = '$schemaName'
         |  AND c.TABLE_NAME = '$tableName'
         |  AND c.CONSTRAINT_TYPE = 'U'
         |ORDER BY cc.POSITION
         |""".stripMargin

    val arrayBuffer = ArrayBuffer[PriKeyColumnInfo]()
    OBJdbcUtils.executeQuery(connection, config, sql) {
      rs =>
        {
          while (rs.next()) {
            arrayBuffer += PriKeyColumnInfo(
              quoteIdentifier(rs.getString(1)),
              rs.getString(2),
              "UNI",
              rs.getString(4),
              rs.getString(5))
          }
          arrayBuffer
        }
    }
  }

  override def getInsertIntoStatement(
      tableName: String,
      schema: StructType,
      config: OceanBaseConfig): String = {
    val columnClause =
      schema.fieldNames.map(columnName => quoteIdentifier(columnName)).mkString(", ")
    val placeholders = schema.fieldNames.map(_ => "?").mkString(", ")

    val hints = config.getJdbcWriteHintsPushdown match {
      case hint if hint.trim.nonEmpty => s"/*+ $hint */"
      case _ => OceanBaseConfig.EMPTY_STRING
    }

    s"""
       |INSERT $hints INTO $tableName ($columnClause)
       |VALUES ($placeholders)
       |""".stripMargin
  }

  override def getUpsertIntoStatement(
      tableName: String,
      schema: StructType,
      priKeyColumnInfo: ArrayBuffer[PriKeyColumnInfo],
      config: OceanBaseConfig): String = {
    val uniqueKeys = priKeyColumnInfo.map(_.columnName).toSet
    val nonUniqueFields =
      schema.fieldNames.filterNot(fieldName => uniqueKeys.contains(quoteIdentifier(fieldName)))

    val columns = schema.fieldNames.map(quoteIdentifier).mkString(", ")
    val keyColumns = priKeyColumnInfo.map(_.columnName).mkString(", ")

    // Build SELECT ? AS col1, ? AS col2, ... for USING clause
    val selectClause = schema.fieldNames.map(f => s"? AS ${quoteIdentifier(f)}").mkString(", ")

    // For VALUES clause, reference the subquery columns
    val valuesClause = schema.fieldNames.map(f => s"s.${quoteIdentifier(f)}").mkString(", ")

    val whenMatchedClause = if (nonUniqueFields.nonEmpty) {
      val updateClause = nonUniqueFields
        .map(f => s"t.${quoteIdentifier(f)} = s.${quoteIdentifier(f)}")
        .mkString(", ")
      s"WHEN MATCHED THEN UPDATE SET $updateClause"
    } else {
      // In Oracle mode, columns referenced in the ON clause must not be updated (even self-assignment).
      // If there are no updatable non-key columns, omit the WHEN MATCHED THEN UPDATE clause.
      ""
    }

    s"""
       |MERGE INTO $tableName t
       |USING (SELECT $selectClause FROM DUAL) s
       |ON (${keyColumns.split(", ").map(col => s"t.$col = s.$col").mkString(" AND ")})
       |$whenMatchedClause
       |WHEN NOT MATCHED THEN INSERT ($columns) VALUES ($valuesClause)
       |""".stripMargin
  }

  /**
   * returns the LIMIT clause for the SELECT statement
   *
   * Oracle mode not supported
   */
  override def getLimitClause(limit: Integer): String = {
    ""
  }

  override def compileValue(value: Any): Any = value match {
    // The JDBC drivers support date literals in SQL statements written in the
    // format: {d 'yyyy-mm-dd'} and timestamp literals in SQL statements written
    // in the format: {ts 'yyyy-mm-dd hh:mm:ss.f...'}. For details, see
    // 'Oracle Database JDBC Developer’s Guide and Reference, 11g Release 1 (11.1)'
    // Appendix A Reference Information.
    case stringValue: String => s"'${escapeSql(stringValue)}'"
    case timestampValue: Timestamp => "{ts '" + timestampValue + "'}"
    case dateValue: Date => "{d '" + dateValue + "'}"
    case arrayValue: Array[Any] => arrayValue.map(compileValue).mkString(", ")
    case _ => value
  }

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    // For more details, please see
    // https://docs.oracle.com/cd/E19501-01/819-3659/gcmaz/
    case BooleanType => Some(JdbcType("NUMBER(1)", java.sql.Types.BOOLEAN))
    case IntegerType => Some(JdbcType("NUMBER(10)", java.sql.Types.INTEGER))
    case LongType => Some(JdbcType("NUMBER(19)", java.sql.Types.BIGINT))
    case FloatType => Some(JdbcType("NUMBER(19, 4)", java.sql.Types.FLOAT))
    case DoubleType => Some(JdbcType("NUMBER(19, 4)", java.sql.Types.DOUBLE))
    case ByteType => Some(JdbcType("NUMBER(3)", java.sql.Types.SMALLINT))
    case ShortType => Some(JdbcType("NUMBER(5)", java.sql.Types.SMALLINT))
    case StringType => Some(JdbcType("VARCHAR2(255)", java.sql.Types.VARCHAR))
    case _ => None
  }

  override def getCatalystType(
      sqlType: Int,
      typeName: String,
      size: Int,
      md: MetadataBuilder): Option[DataType] = {
    sqlType match {
      case Types.NUMERIC =>
        val scale = if (null != md) md.build().getLong("scale") else 0L
        size match {
          // Handle NUMBER fields that have no precision/scale in special way
          // because JDBC ResultSetMetaData converts this to 0 precision and -127 scale
          // For more details, please see
          // https://github.com/apache/spark/pull/8780#issuecomment-145598968
          // and
          // https://github.com/apache/spark/pull/8780#issuecomment-144541760
          case 0 => Option(DecimalType(DecimalType.MAX_PRECISION, 10))
          // Handle FLOAT fields in a special way because JDBC ResultSetMetaData converts
          // this to NUMERIC with -127 scale
          // Not sure if there is a more robust way to identify the field as a float (or other
          // numeric types that do not specify a scale.
          case _ if scale == -127L => Option(DecimalType(DecimalType.MAX_PRECISION, 10))
          case _ => None
        }
      case TIMESTAMPTZ if supportTimeZoneTypes =>
        Some(TimestampType) // Value for Timestamp with Time Zone in Oracle
      case BINARY_FLOAT => Some(FloatType) // Value for OracleTypes.BINARY_FLOAT
      case BINARY_DOUBLE => Some(DoubleType) // Value for OracleTypes.BINARY_DOUBLE
      case _ => None
    }
  }

  private val BINARY_FLOAT = 100
  private val BINARY_DOUBLE = 101
  private val TIMESTAMPTZ = -101

  private def supportTimeZoneTypes: Boolean = {
    val timeZone = DateTimeUtils.getTimeZone(SQLConf.get.sessionLocalTimeZone)
    // TODO: support timezone types when users are not using the JVM timezone, which
    // is the default value of SESSION_LOCAL_TIMEZONE
    timeZone == TimeZone.getDefault
  }

  private val distinctUnsupportedAggregateFunctions =
    Set(
      "VAR_POP",
      "VAR_SAMP",
      "STDDEV_POP",
      "STDDEV_SAMP",
      "COVAR_POP",
      "COVAR_SAMP",
      "CORR",
      "REGR_INTERCEPT",
      "REGR_R2",
      "REGR_SLOPE",
      "REGR_SXY")

  private val supportedAggregateFunctions =
    Set("MAX", "MIN", "SUM", "COUNT", "AVG") ++ distinctUnsupportedAggregateFunctions
  private val supportedFunctions = supportedAggregateFunctions

  override def isSupportedFunction(funcName: String): Boolean =
    supportedFunctions.contains(funcName)

  class OracleSQLBuilder extends JDBCSQLBuilder {
    override def visitAggregateFunction(
        funcName: String,
        isDistinct: Boolean,
        inputs: Array[String]): String =
      if (isDistinct && distinctUnsupportedAggregateFunctions.contains(funcName)) {
        throw new UnsupportedOperationException(
          s"${this.getClass.getSimpleName} does not " +
            s"support aggregate function: $funcName with DISTINCT");
      } else {
        super.visitAggregateFunction(funcName, isDistinct, inputs)
      }
  }

  override def compileExpression(expr: Expression): Option[String] = {
    val oracleSQLBuilder = new OracleSQLBuilder()
    try {
      Some(oracleSQLBuilder.build(expr))
    } catch {
      case NonFatal(e) =>
        logWarning("Error occurs while compiling V2 expression", e)
        None
    }
  }

  /**
   * Get actual column type names from ALL_TAB_COLUMNS for Oracle mode. Returns DATA_TYPE which
   * includes type definition
   */
  override def getActualColumnTypes(
      connection: Connection,
      tableName: String,
      schemaName: String,
      config: OceanBaseConfig): Map[String, String] = {
    val sql =
      s"""
         |SELECT COLUMN_NAME, DATA_TYPE
         |FROM ALL_TAB_COLUMNS
         |WHERE OWNER = ? AND TABLE_NAME = ?
         |""".stripMargin

    val stmt = connection.prepareStatement(sql)
    try {
      stmt.setString(1, schemaName.toUpperCase)
      stmt.setString(2, tableName.toUpperCase)
      val rs = stmt.executeQuery()
      val typeMap = scala.collection.mutable.Map[String, String]()
      while (rs.next()) {
        val columnName = rs.getString("COLUMN_NAME")
        val dataType = rs.getString("DATA_TYPE").toUpperCase
        typeMap(columnName.toUpperCase) = dataType
      }
      rs.close()
      typeMap.toMap
    } catch {
      case e: Exception =>
        // Log warning but don't fail - fall back to JDBC metadata
        logWarning(s"Failed to get actual column types from ALL_TAB_COLUMNS: ${e.getMessage}")
        Map.empty[String, String]
    } finally {
      stmt.close()
    }
  }
}

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

import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.expressions.{Expression, Transform}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MetadataBuilder, ShortType, StringType, StructType, TimestampType}

import java.sql.{Connection, Date, Timestamp, Types}
import java.util
import java.util.TimeZone

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
      properties: util.Map[String, String]): Unit =
    throw new UnsupportedOperationException("Not currently supported in oracle mode")

  /** Creates a schema. */
  override def createSchema(
      conn: Connection,
      config: OceanBaseConfig,
      schema: String,
      comment: String): Unit =
    throw new UnsupportedOperationException("Not currently supported in oracle mode")

  override def schemaExists(conn: Connection, config: OceanBaseConfig, schema: String): Boolean =
    throw new UnsupportedOperationException("Not currently supported in oracle mode")

  override def listSchemas(conn: Connection, config: OceanBaseConfig): Array[Array[String]] =
    throw new UnsupportedOperationException("Not currently supported in oracle mode")

  /** Drops a schema from OceanBase. */
  override def dropSchema(
      conn: Connection,
      config: OceanBaseConfig,
      schema: String,
      cascade: Boolean): Unit = throw new UnsupportedOperationException(
    "Not currently supported in oracle mode")

  override def getPriKeyInfo(
      connection: Connection,
      schemaName: String,
      tableName: String,
      config: OceanBaseConfig): ArrayBuffer[PriKeyColumnInfo] = {
    throw new UnsupportedOperationException("Not currently supported in oracle mode")
  }

  override def getUniqueKeyInfo(
      connection: Connection,
      schemaName: String,
      tableName: String,
      config: OceanBaseConfig): ArrayBuffer[PriKeyColumnInfo] = {
    throw new UnsupportedOperationException("Not currently supported in oracle mode")
  }

  override def getInsertIntoStatement(tableName: String, schema: StructType): String = {
    throw new UnsupportedOperationException("Not currently supported in oracle mode")
  }

  override def getUpsertIntoStatement(
      tableName: String,
      schema: StructType,
      priKeyColumnInfo: ArrayBuffer[PriKeyColumnInfo],
      config: OceanBaseConfig): String = {
    throw new UnsupportedOperationException("Not currently supported in oracle mode")
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
}

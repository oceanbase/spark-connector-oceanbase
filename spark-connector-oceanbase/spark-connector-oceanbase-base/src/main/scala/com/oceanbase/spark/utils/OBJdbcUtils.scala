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

package com.oceanbase.spark.utils

import com.oceanbase.spark.catalog.OceanBaseCatalogException
import com.oceanbase.spark.config.OceanBaseConfig
import com.oceanbase.spark.dialect.OceanBaseDialect

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, CharType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, MetadataBuilder, ShortType, StringType, StructField, StructType, TimestampType, VarcharType}
import org.apache.spark.sql.types.DecimalType.{MAX_PRECISION, MAX_SCALE}

import java.sql.{Connection, PreparedStatement, ResultSet, ResultSetMetaData, SQLException}

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.math.min

object OBJdbcUtils {

  private val COMPATIBLE_MODE_STATEMENT = "SHOW VARIABLES LIKE 'ob_compatibility_mode'"

  def getDbTable(oceanBaseConfig: OceanBaseConfig): String = {
    getCompatibleMode(oceanBaseConfig).map(_.toLowerCase) match {
      case Some("mysql") => s"`${oceanBaseConfig.getSchemaName}`.`${oceanBaseConfig.getTableName}`"
      case Some("oracle") =>
        s""""${oceanBaseConfig.getSchemaName}"."${oceanBaseConfig.getTableName}""""
      case _ => throw new RuntimeException("Failed to get OceanBase's compatible mode")
    }
  }

  def truncateTable(oceanBaseConfig: OceanBaseConfig): Unit = {
    val conn = getConnection(oceanBaseConfig)
    val statement = conn.createStatement
    try {
      statement.executeUpdate(
        s"truncate table ${oceanBaseConfig.getSchemaName}.${oceanBaseConfig.getTableName}")
    } finally {
      statement.close()
      conn.close()
    }
  }

  def getCompatibleMode(config: OceanBaseConfig): Option[String] = {
    withConnection(config) {
      conn =>
        {
          var compatibleMode: Option[String] = None
          executeQuery(conn, config, COMPATIBLE_MODE_STATEMENT) {
            rs =>
              if (rs.next()) {
                compatibleMode = Option(rs.getString("VALUE"))
              } else throw new RuntimeException("Failed to obtain compatible mode of OceanBase.")
          }
          compatibleMode
        }
    }
  }

  def getConnection(option: JDBCOptions): Connection = {
    val config = new OceanBaseConfig(option.parameters.asJava)
    val connection = OceanBaseConnectionProvider.getConnection(oceanBaseConfig = config)
    require(
      connection != null,
      s"The driver could not open a JDBC connection. Check the URL: ${config.getURL}")
    connection
  }

  def getConnection(config: OceanBaseConfig): Connection = {
    val connection = OceanBaseConnectionProvider.getConnection(oceanBaseConfig = config)
    require(
      connection != null,
      s"The driver could not open a JDBC connection. Check the URL: ${config.getURL}")
    connection
  }

  def withConnection[T](config: OceanBaseConfig)(f: Connection => T): T = {
    val conn = getConnection(config)
    try {
      f(conn)
    } finally {
      conn.close()
    }
  }

  def withConnection[T](options: JDBCOptions)(f: Connection => T): T = {
    val conn = getConnection(options)
    try {
      f(conn)
    } finally {
      conn.close()
    }
  }

  def executeStatement(conn: Connection, config: OceanBaseConfig, sql: String): Unit = {
    val statement = conn.createStatement
    try {
      statement.setQueryTimeout(config.getJdbcQueryTimeout)
      statement.executeUpdate(sql)
    } catch {
      case exception: Exception =>
        throw new RuntimeException(s"Failed to execute sql: $sql", exception)
    } finally {
      statement.close()
    }
  }

  def executeQuery[T](conn: Connection, config: OceanBaseConfig, sql: String)(
      f: ResultSet => T): T = {
    val statement = conn.createStatement
    try {
      statement.setQueryTimeout(config.getJdbcQueryTimeout)
      val rs = statement.executeQuery(sql)
      try {
        f(rs)
      } finally {
        rs.close()
      }
    } catch {
      case exception: Exception =>
        throw new RuntimeException(s"Failed to execute sql: $sql", exception)
    } finally {
      statement.close()
    }
  }

  def unifiedCatalogException[T](message: String)(f: => T): T = {
    try {
      f
    } catch {
      case e: Throwable => throw OceanBaseCatalogException(message, e)
    }
  }

  type OBValueSetter = (PreparedStatement, InternalRow, Int) => Unit

  /** Convert ArrayData to OceanBase ARRAY string format. For example: [1, 2, 3] for INT array */
  private def convertArrayToString(
      array: org.apache.spark.sql.catalyst.util.ArrayData,
      elementType: DataType): String = {
    val elements = (0 until array.numElements()).map {
      i =>
        if (array.isNullAt(i)) {
          "NULL"
        } else {
          elementType match {
            case IntegerType => array.getInt(i).toString
            case LongType => array.getLong(i).toString
            case ShortType => array.getShort(i).toString
            case ByteType => array.getByte(i).toString
            case FloatType => array.getFloat(i).toString
            case DoubleType => array.getDouble(i).toString
            case BooleanType => array.getBoolean(i).toString
            case StringType => s"'${array.getUTF8String(i).toString}'"
            case _ => array.get(i, elementType).toString
          }
        }
    }
    "[" + elements.mkString(", ") + "]"
  }

  /** Convert MapData to OceanBase MAP string format. For example: {1:10, 2:20} for MAP(INT, INT) */
  private def convertMapToString(
      map: org.apache.spark.sql.catalyst.util.MapData,
      keyType: DataType,
      valueType: DataType): String = {
    val keys = map.keyArray()
    val values = map.valueArray()
    val pairs = (0 until keys.numElements()).map {
      i =>
        val key = keyType match {
          case IntegerType => keys.getInt(i).toString
          case LongType => keys.getLong(i).toString
          case StringType => keys.getUTF8String(i).toString
          case _ => keys.get(i, keyType).toString
        }
        val value = if (values.isNullAt(i)) {
          "NULL"
        } else {
          valueType match {
            case IntegerType => values.getInt(i).toString
            case LongType => values.getLong(i).toString
            case StringType => s"'${values.getUTF8String(i).toString}'"
            case _ => values.get(i, valueType).toString
          }
        }
        s"$key:$value"
    }
    "{" + pairs.mkString(", ") + "}"
  }

  def makeSetter(dataType: DataType): OBValueSetter = dataType match {
    case IntegerType =>
      (stmt: PreparedStatement, row: InternalRow, pos: Int) => stmt.setInt(pos + 1, row.getInt(pos))

    case LongType =>
      (stmt: PreparedStatement, row: InternalRow, pos: Int) =>
        stmt.setLong(pos + 1, row.getLong(pos))

    case DoubleType =>
      (stmt: PreparedStatement, row: InternalRow, pos: Int) =>
        stmt.setDouble(pos + 1, row.getDouble(pos))

    case FloatType =>
      (stmt: PreparedStatement, row: InternalRow, pos: Int) =>
        stmt.setFloat(pos + 1, row.getFloat(pos))

    case ShortType =>
      (stmt: PreparedStatement, row: InternalRow, pos: Int) =>
        stmt.setInt(pos + 1, row.getShort(pos))

    case ByteType =>
      (stmt: PreparedStatement, row: InternalRow, pos: Int) =>
        stmt.setInt(pos + 1, row.getByte(pos))

    case BooleanType =>
      (stmt: PreparedStatement, row: InternalRow, pos: Int) =>
        stmt.setBoolean(pos + 1, row.getBoolean(pos))

    case StringType =>
      (stmt: PreparedStatement, row: InternalRow, pos: Int) =>
        stmt.setString(pos + 1, row.getUTF8String(pos).toString)

    case t: VarcharType =>
      (stmt: PreparedStatement, row: InternalRow, pos: Int) =>
        stmt.setString(pos + 1, row.getUTF8String(pos).toString)

    case t: CharType =>
      (stmt: PreparedStatement, row: InternalRow, pos: Int) =>
        stmt.setString(pos + 1, row.getUTF8String(pos).toString)

    case BinaryType =>
      (stmt: PreparedStatement, row: InternalRow, pos: Int) =>
        stmt.setBytes(pos + 1, row.getBinary(pos))

    case TimestampType =>
      (stmt: PreparedStatement, row: InternalRow, pos: Int) =>
        stmt.setTimestamp(pos + 1, DateTimeUtils.toJavaTimestamp(row.getLong(pos)))

    case DateType =>
      (stmt: PreparedStatement, row: InternalRow, pos: Int) =>
        stmt.setDate(pos + 1, DateTimeUtils.toJavaDate(row.getInt(pos)))

    case t: DecimalType =>
      (stmt: PreparedStatement, row: InternalRow, pos: Int) =>
        stmt.setBigDecimal(pos + 1, row.getDecimal(pos, t.precision, t.scale).toJavaBigDecimal)

    case ArrayType(et, _) =>
      // Convert Array to string format for OceanBase ARRAY type
      (stmt: PreparedStatement, row: InternalRow, pos: Int) => {
        val array = row.getArray(pos)
        val arrayString = convertArrayToString(array, et)
        stmt.setString(pos + 1, arrayString)
      }

    case MapType(kt, vt, _) =>
      // Convert Map to string format for OceanBase MAP type
      (stmt: PreparedStatement, row: InternalRow, pos: Int) => {
        val map = row.getMap(pos)
        val mapString = convertMapToString(map, kt, vt)
        stmt.setString(pos + 1, mapString)
      }

    case _ =>
      (_: PreparedStatement, _: InternalRow, pos: Int) =>
        throw new IllegalArgumentException(s"Can't translate non-null value for field $pos")
  }

  /**
   * Get actual column type names from information_schema.columns Returns COLUMN_TYPE which includes
   * full type definition like "ARRAY<INT>", "VECTOR<FLOAT>"
   */
  private def getActualColumnTypes(
      connection: Connection,
      tableName: String,
      schemaName: String): Map[String, String] = {
    val sql =
      s"""
         |SELECT COLUMN_NAME, COLUMN_TYPE
         |FROM information_schema.columns
         |WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
         |""".stripMargin

    val stmt = connection.prepareStatement(sql)
    try {
      stmt.setString(1, schemaName)
      stmt.setString(2, tableName)
      val rs = stmt.executeQuery()
      val typeMap = scala.collection.mutable.Map[String, String]()
      while (rs.next()) {
        val columnName = rs.getString("COLUMN_NAME")
        val columnType = rs.getString("COLUMN_TYPE").toUpperCase
        typeMap(columnName.toUpperCase) = columnType
      }
      rs.close()
      typeMap.toMap
    } finally {
      stmt.close()
    }
  }

  /**
   * Copy from
   * [[org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.getSchema(ResultSet, JdbcDialect, Boolean)]]
   * to solve compatibility issues with lower Spark versions.
   */
  def getSchema(
      resultSet: ResultSet,
      dialect: OceanBaseDialect,
      alwaysNullable: Boolean = false,
      config: OceanBaseConfig): StructType = {
    val rsmd = resultSet.getMetaData
    val ncols = rsmd.getColumnCount
    val fields = new Array[StructField](ncols)

    // Try to get actual column types from information_schema
    val actualTypeMap: Map[String, String] =
      try {
        val tableName = rsmd.getTableName(1)
        val connection = resultSet.getStatement.getConnection
        if (tableName != null && !tableName.isEmpty && connection != null) {
          getActualColumnTypes(connection, tableName, config.getSchemaName)
        } else {
          Map.empty[String, String]
        }
      } catch {
        case _: Exception => Map.empty[String, String]
      }

    var i = 0
    while (i < ncols) {
      val columnName = rsmd.getColumnLabel(i + 1)
      val dataType = rsmd.getColumnType(i + 1)
      var typeName = rsmd.getColumnTypeName(i + 1)

      // Override typeName with actual type from information_schema if available
      val actualType = actualTypeMap.get(columnName.toUpperCase)
      if (actualType.isDefined) {
        typeName = actualType.get
      }

      val fieldSize = rsmd.getPrecision(i + 1)
      val fieldScale = rsmd.getScale(i + 1)
      val isSigned =
        try rsmd.isSigned(i + 1)
        catch {
          // Workaround for HIVE-14684:
          case e: SQLException
              if e.getMessage == "Method not supported" &&
                rsmd.getClass.getName == "org.apache.hive.jdbc.HiveResultSetMetaData" =>
            true
        }
      val nullable =
        if (alwaysNullable) true else rsmd.isNullable(i + 1) != ResultSetMetaData.columnNoNulls
      val metadata = new MetadataBuilder()
      metadata.putLong("scale", fieldScale)

      dataType match {
        case java.sql.Types.TIME =>
          // SPARK-33888
          // - include TIME type metadata
          // - always build the metadata
          metadata.putBoolean("logical_time_type", true)
        case java.sql.Types.ROWID =>
          metadata.putBoolean("rowid", true)
        case _ =>
      }

      val columnType =
        dialect
          .getCatalystType(dataType, typeName, fieldSize, metadata)
          .getOrElse(OBJdbcUtils.getCatalystType(dataType, fieldSize, fieldScale, isSigned, config))
      fields(i) = StructField(columnName, columnType, nullable, metadata.build())
      i = i + 1
    }
    new StructType(fields)
  }

  /**
   * Maps a JDBC type to a Catalyst type. This function is called only when the JdbcDialect class
   * corresponding to your database driver returns null.
   *
   * @param sqlType
   *   \- A field of java.sql.Types
   * @return
   *   The Catalyst type corresponding to sqlType.
   */
  def getCatalystType(
      sqlType: Int,
      precision: Int,
      scale: Int,
      signed: Boolean,
      config: OceanBaseConfig): DataType = {
    val answer = sqlType match {
      // scalastyle:off
      case java.sql.Types.ARRAY => null
      case java.sql.Types.BIGINT =>
        if (signed) { LongType }
        else { DecimalType(20, 0) }
      case java.sql.Types.BINARY => BinaryType
      case java.sql.Types.BIT => BooleanType // @see JdbcDialect for quirks
      case java.sql.Types.BLOB => BinaryType
      case java.sql.Types.BOOLEAN => BooleanType
      case java.sql.Types.CHAR if precision != 0 =>
        if (config.getEnableSparkVarcharDataType) CharType(precision) else StringType
      case java.sql.Types.CHAR => StringType
      case java.sql.Types.CLOB => StringType
      case java.sql.Types.DATALINK => null
      case java.sql.Types.DATE => DateType
      case java.sql.Types.DECIMAL if precision != 0 || scale != 0 =>
        // Optimize DECIMAL(P, 0) to BIGINT to avoid precision loss when comparing with string literals
        // Spark converts String + DECIMAL to DOUBLE (losing precision), but String + BIGINT stays as BIGINT
        // Only convert when optimization is enabled AND scale == 0 AND precision <= 19 (BIGINT range: -9223372036854775808 to 9223372036854775807)
        if (config.getOptimizeDecimalStringComparison && scale == 0 && precision <= 19) {
          LongType
        } else {
          DecimalType(min(precision, MAX_PRECISION), min(scale, MAX_SCALE))
        }
      case java.sql.Types.DECIMAL => DecimalType.SYSTEM_DEFAULT
      case java.sql.Types.DISTINCT => null
      case java.sql.Types.DOUBLE => DoubleType
      case java.sql.Types.FLOAT => FloatType
      case java.sql.Types.INTEGER =>
        if (signed) { IntegerType }
        else { LongType }
      case java.sql.Types.JAVA_OBJECT => null
      case java.sql.Types.LONGNVARCHAR => StringType
      case java.sql.Types.LONGVARBINARY => BinaryType
      case java.sql.Types.LONGVARCHAR => StringType
      case java.sql.Types.NCHAR => StringType
      case java.sql.Types.NCLOB => StringType
      case java.sql.Types.NULL => null
      case java.sql.Types.NUMERIC if precision != 0 || scale != 0 =>
        // Optimize NUMERIC(P, 0) to BIGINT to avoid precision loss
        // Only convert when optimization is enabled AND scale == 0 AND precision <= 19 (BIGINT range: -9223372036854775808 to 9223372036854775807)
        if (config.getOptimizeDecimalStringComparison && scale == 0 && precision <= 19) {
          LongType
        } else {
          DecimalType(min(precision, MAX_PRECISION), min(scale, MAX_SCALE))
        }
      case java.sql.Types.NUMERIC => DecimalType.SYSTEM_DEFAULT
      case java.sql.Types.NVARCHAR => StringType
      case java.sql.Types.OTHER => null
      case java.sql.Types.REAL => DoubleType
      case java.sql.Types.REF => StringType
      case java.sql.Types.REF_CURSOR => null
      case java.sql.Types.ROWID => StringType
      case java.sql.Types.SMALLINT => IntegerType
      case java.sql.Types.SQLXML => StringType
      case java.sql.Types.STRUCT => StringType
      case java.sql.Types.TIME => TimestampType
      case java.sql.Types.TIME_WITH_TIMEZONE => null
      case java.sql.Types.TIMESTAMP => TimestampType
      case java.sql.Types.TIMESTAMP_WITH_TIMEZONE => null
      case java.sql.Types.TINYINT => IntegerType
      case java.sql.Types.VARBINARY => BinaryType
      case java.sql.Types.VARCHAR if precision != 0 =>
        if (config.getEnableSparkVarcharDataType) VarcharType(precision) else StringType
      case java.sql.Types.VARCHAR => StringType
      case _ =>
        throw new RuntimeException(s"Unsupported type: $sqlType ")
      // scalastyle:on
    }

    if (answer == null) {
      throw new RuntimeException(s"Unsupported type: $sqlType ")
    }
    answer
  }

  def getQueryOutputSchema(
      query: String,
      config: OceanBaseConfig,
      dialect: OceanBaseDialect): StructType = {

    OBJdbcUtils.withConnection(config) {
      conn =>
        {
          val statement = conn.prepareStatement(query)
          try {
            statement.setQueryTimeout(config.getJdbcQueryTimeout)
            val rs = statement.executeQuery()
            try {
              OBJdbcUtils.getSchema(rs, dialect, alwaysNullable = true, config)
            } finally {
              rs.close()
            }
          } finally {
            statement.close()
          }
        }
    }
  }
}

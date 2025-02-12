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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources.jdbc.{DriverRegistry, JDBCOptions}
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampType}

import java.sql.{Connection, Driver, DriverManager, PreparedStatement, ResultSet}

object OBJdbcUtils {

  private val COMPATIBLE_MODE_STATEMENT = "SHOW VARIABLES LIKE 'ob_compatibility_mode'"

  def getConnection(oceanBaseConfig: OceanBaseConfig): Connection = {
    val connection = DriverManager.getConnection(
      oceanBaseConfig.getURL,
      oceanBaseConfig.getUsername,
      oceanBaseConfig.getPassword
    )
    connection
  }

  def getCompatibleMode(oceanBaseConfig: OceanBaseConfig): String = {
    val conn = getConnection(oceanBaseConfig)
    val statement = conn.createStatement
    try {
      val rs = statement.executeQuery("SHOW VARIABLES LIKE 'ob_compatibility_mode'")
      if (rs.next) rs.getString("VALUE")
      else throw new RuntimeException("Failed to obtain compatible mode of OceanBase.")
    } finally {
      statement.close()
      conn.close()
    }
  }

  def getDbTable(oceanBaseConfig: OceanBaseConfig): String = {
    if ("MySQL".equalsIgnoreCase(getCompatibleMode(oceanBaseConfig))) {
      s"`${oceanBaseConfig.getSchemaName}`.`${oceanBaseConfig.getTableName}`"
    } else {
      s""""${oceanBaseConfig.getSchemaName}"."${oceanBaseConfig.getTableName}""""
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

  def getCompatibleMode(option: JDBCOptions): Option[String] = {
    withConnection(option) {
      conn =>
        {
          var compatibleMode: Option[String] = None
          executeQuery(conn, option, COMPATIBLE_MODE_STATEMENT) {
            rs =>
              if (rs.next()) {
                compatibleMode = Option(rs.getString("VALUE"))
              } else throw new RuntimeException("Failed to obtain compatible mode of OceanBase.")
          }
          compatibleMode
        }
    }
  }

  def getConnection(options: JDBCOptions): Connection = {
    val driverClass: String = options.driverClass
    DriverRegistry.register(driverClass)
    val driver: Driver = DriverRegistry.get(driverClass)
    val connection = OceanBaseConnectionProvider.getConnection(driver, options.parameters)
    require(
      connection != null,
      s"The driver could not open a JDBC connection. Check the URL: ${options.url}")
    connection
  }

  def withConnection[T](options: JDBCOptions)(f: Connection => T): T = {
    val conn = getConnection(options)
    try {
      f(conn)
    } finally {
      conn.close()
    }
  }

  def executeStatement(conn: Connection, options: JDBCOptions, sql: String): Unit = {
    val statement = conn.createStatement
    try {
      statement.setQueryTimeout(options.queryTimeout)
      statement.executeUpdate(sql)
    } finally {
      statement.close()
    }
  }

  def executeQuery(conn: Connection, options: JDBCOptions, sql: String)(
      f: ResultSet => Unit): Unit = {
    val statement = conn.createStatement
    try {
      statement.setQueryTimeout(options.queryTimeout)
      val rs = statement.executeQuery(sql)
      try {
        f(rs)
      } finally {
        rs.close()
      }
    } catch {
      case exception: Exception =>
        throw new RuntimeException(s"Failed to execute sql: $sql", exception.getCause)
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
      // remove type length parameters from end of type name
      throw new UnsupportedOperationException(s"Not support Array data-type now")

    case _ =>
      (_: PreparedStatement, _: InternalRow, pos: Int) =>
        throw new IllegalArgumentException(s"Can't translate non-null value for field $pos")
  }
}

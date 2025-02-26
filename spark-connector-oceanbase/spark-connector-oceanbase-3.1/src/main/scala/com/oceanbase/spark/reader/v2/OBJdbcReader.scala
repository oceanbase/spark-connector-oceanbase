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

package com.oceanbase.spark.reader.v2

import com.oceanbase.spark.dialect.OceanBaseDialect
import com.oceanbase.spark.reader.v2.OBJdbcReader.{makeGetters, OBValueGetter}
import com.oceanbase.spark.utils.OBJdbcUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.ExprUtils.compileFilter
import org.apache.spark.sql.catalyst.{InternalRow, SQLConfHelper}
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, GenericArrayData}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.sql.{PreparedStatement, ResultSet}
import java.util.Objects
import java.util.concurrent.TimeUnit

class OBJdbcReader(
    schema: StructType,
    options: JDBCOptions,
    partition: InputPartition,
    pushedFilter: Array[Filter],
    dialect: OceanBaseDialect)
  extends PartitionReader[InternalRow]
  with SQLConfHelper
  with Logging {

  private val getters: Array[OBValueGetter] = makeGetters(schema)
  private val mutableRow = new SpecificInternalRow(schema.fields.map(x => x.dataType))
  private lazy val conn = OBJdbcUtils.getConnection(options)
  private lazy val stmt: PreparedStatement =
    conn.prepareStatement(buildQuerySql(), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
  private lazy val rs: ResultSet = {
    stmt.setFetchSize(options.fetchSize)
    stmt.setQueryTimeout(options.queryTimeout)
    stmt.executeQuery()
  }

  private var currentRecord: InternalRow = _

  override def next(): Boolean = {
    val hasNext = rs.next()
    if (hasNext) currentRecord = {
      var i = 0
      while (i < getters.length) {
        getters(i)(rs, mutableRow, i)
        if (rs.wasNull) mutableRow.setNullAt(i)
        i = i + 1
      }
      mutableRow
    }
    hasNext
  }

  override def get(): InternalRow = currentRecord

  override def close(): Unit = {
    if (Objects.nonNull(rs)) {
      rs.close()
    }
    if (Objects.nonNull(stmt)) {
      stmt.close()
    }
    if (Objects.nonNull(conn)) {
      conn.close()
    }
  }

  private def buildQuerySql(): String = {
    val columns = schema.map(col => dialect.quoteIdentifier(col.name)).toArray
    val columnStr: String = if (columns.isEmpty) "1" else columns.mkString(",")

    val filterWhereClause: String =
      pushedFilter
        .flatMap(compileFilter(_, dialect))
        .map(p => s"($p)")
        .mkString(" AND ")

    val whereClause: String = {
      if (filterWhereClause.nonEmpty) {
        "WHERE " + filterWhereClause
      } else {
        ""
      }
    }
    val part: OBMySQLPartition = partition.asInstanceOf[OBMySQLPartition]
    s"""
       |SELECT $columnStr FROM ${options.tableOrQuery} ${part.partitionClause}
       |$whereClause ${part.limitOffsetClause}
       |""".stripMargin
  }
}

object OBJdbcReader extends SQLConfHelper {

  // A `JDBCValueGetter` is responsible for getting a value from `ResultSet` into a field
  // for `MutableRow`. The last argument `Int` means the index for the value to be set in
  // the row and also used for the value in `ResultSet`.
  type OBValueGetter = (ResultSet, InternalRow, Int) => Unit

  /**
   * Creates `JDBCValueGetter`s according to [[StructType]], which can set each value from
   * `ResultSet` to each field of [[InternalRow]] correctly.
   */
  def makeGetters(schema: StructType): Array[OBValueGetter] =
    schema.fields.map(sf => makeGetter(sf.dataType, sf.metadata))

  private def makeGetter(dt: DataType, metadata: Metadata): OBValueGetter = dt match {
    case BooleanType =>
      (rs: ResultSet, row: InternalRow, pos: Int) => row.setBoolean(pos, rs.getBoolean(pos + 1))

    case DateType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        // DateTimeUtils.fromJavaDate does not handle null value, so we need to check it.
        val dateVal = rs.getDate(pos + 1)
        if (dateVal != null) {
          row.setInt(pos, DateTimeUtils.fromJavaDate(dateVal))
        } else {
          row.update(pos, null)
        }

      // When connecting with Oracle DB through JDBC, the precision and scale of BigDecimal
      // object returned by ResultSet.getBigDecimal is not correctly matched to the table
      // schema reported by ResultSetMetaData.getPrecision and ResultSetMetaData.getScale.
      // If inserting values like 19999 into a column with NUMBER(12, 2) type, you get through
      // a BigDecimal object with scale as 0. But the dataframe schema has correct type as
      // DecimalType(12, 2). Thus, after saving the dataframe into parquet file and then
      // retrieve it, you will get wrong result 199.99.
    // So it is needed to set precision and scale for Decimal based on JDBC metadata.
    case _: DecimalType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        val decimal =
          nullSafeConvert[java.math.BigDecimal](
            rs.getBigDecimal(pos + 1),
            d => Decimal(d, d.precision(), d.scale()))
        row.update(pos, decimal)

    case DoubleType =>
      (rs: ResultSet, row: InternalRow, pos: Int) => row.setDouble(pos, rs.getDouble(pos + 1))

    case FloatType =>
      (rs: ResultSet, row: InternalRow, pos: Int) => row.setFloat(pos, rs.getFloat(pos + 1))

    case IntegerType =>
      (rs: ResultSet, row: InternalRow, pos: Int) => row.setInt(pos, rs.getInt(pos + 1))

    case LongType if metadata.contains("binarylong") =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        val bytes = rs.getBytes(pos + 1)
        var ans = 0L
        var j = 0
        while (j < bytes.length) {
          ans = 256 * ans + (255 & bytes(j))
          j = j + 1
        }
        row.setLong(pos, ans)

    case LongType =>
      (rs: ResultSet, row: InternalRow, pos: Int) => row.setLong(pos, rs.getLong(pos + 1))

    case ShortType =>
      (rs: ResultSet, row: InternalRow, pos: Int) => row.setShort(pos, rs.getShort(pos + 1))

    case ByteType =>
      (rs: ResultSet, row: InternalRow, pos: Int) => row.setByte(pos, rs.getByte(pos + 1))

    case StringType if metadata.contains("rowid") =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.update(pos, UTF8String.fromString(rs.getRowId(pos + 1).toString))

    case StringType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        // TODO(davies): use getBytes for better performance, if the encoding is UTF-8
        row.update(pos, UTF8String.fromString(rs.getString(pos + 1)))

      // SPARK-34357 - sql TIME type represents as zero epoch timestamp.
      // It is mapped as Spark TimestampType but fixed at 1970-01-01 for day,
      // time portion is time of day, with no reference to a particular calendar,
      // time zone or date, with a precision till microseconds.
    // It stores the number of milliseconds after midnight, 00:00:00.000000
    case TimestampType if metadata.contains("logical_time_type") =>
      (rs: ResultSet, row: InternalRow, pos: Int) => {
        val rawTime = rs.getTime(pos + 1)
        if (rawTime != null) {
          val localTimeMicro = TimeUnit.NANOSECONDS.toMicros(rawTime.toLocalTime.toNanoOfDay)
          val utcTimeMicro = DateTimeUtils.toUTCTime(localTimeMicro, conf.sessionLocalTimeZone)
          row.setLong(pos, utcTimeMicro)
        } else {
          row.update(pos, null)
        }
      }

    case TimestampType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        val t = rs.getTimestamp(pos + 1)
        if (t != null) {
          row.setLong(pos, DateTimeUtils.fromJavaTimestamp(t))
        } else {
          row.update(pos, null)
        }

    case BinaryType =>
      (rs: ResultSet, row: InternalRow, pos: Int) => row.update(pos, rs.getBytes(pos + 1))

    case ArrayType(et, _) =>
      val elementConversion = et match {
        case TimestampType =>
          (array: Object) =>
            array.asInstanceOf[Array[java.sql.Timestamp]].map {
              timestamp => nullSafeConvert(timestamp, DateTimeUtils.fromJavaTimestamp)
            }

        case StringType =>
          (array: Object) =>
            // some underling types are not String such as uuid, inet, cidr, etc.
            array
              .asInstanceOf[Array[java.lang.Object]]
              .map(obj => if (obj == null) null else UTF8String.fromString(obj.toString))

        case DateType =>
          (array: Object) =>
            array.asInstanceOf[Array[java.sql.Date]].map {
              date => nullSafeConvert(date, DateTimeUtils.fromJavaDate)
            }

        case dt: DecimalType =>
          (array: Object) =>
            array.asInstanceOf[Array[java.math.BigDecimal]].map {
              decimal =>
                nullSafeConvert[java.math.BigDecimal](
                  decimal,
                  d => Decimal(d, dt.precision, dt.scale))
            }

        case LongType if metadata.contains("binarylong") =>
          throw new UnsupportedOperationException(
            s"unsupportedArrayElementTypeBasedOnBinaryError ${dt.catalogString}")

        case ArrayType(_, _) =>
          throw new UnsupportedOperationException(s"Not support Array data-type now")

        case _ => (array: Object) => array.asInstanceOf[Array[Any]]
      }

      (rs: ResultSet, row: InternalRow, pos: Int) =>
        val array = nullSafeConvert[java.sql.Array](
          input = rs.getArray(pos + 1),
          array => new GenericArrayData(elementConversion.apply(array.getArray)))
        row.update(pos, array)

    case _ =>
      throw new UnsupportedOperationException(s"unsupportedJdbcTypeError ${dt.catalogString}")
  }

  private def nullSafeConvert[T](input: T, f: T => Any): Any = {
    if (input == null) {
      null
    } else {
      f(input)
    }
  }
}

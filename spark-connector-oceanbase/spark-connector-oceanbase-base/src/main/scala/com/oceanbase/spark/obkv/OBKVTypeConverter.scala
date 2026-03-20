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

package com.oceanbase.spark.obkv

import java.math.{BigDecimal => JBigDecimal, BigInteger => JBigInteger}
import java.sql.{Date, Timestamp}
import java.time.{LocalDate, LocalDateTime}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/** Converts between OBKV Java objects and Spark InternalRow values. */
object OBKVTypeConverter {

  /** Sets a value from an OBKV result into a Spark InternalRow. */
  def setRowValue(row: SpecificInternalRow, pos: Int, dataType: DataType, value: Any): Unit = {
    dataType match {
      case BooleanType =>
        value match {
          case b: java.lang.Boolean => row.setBoolean(pos, b)
          case _ => row.setBoolean(pos, toNumber(value).intValue() != 0)
        }
      case ByteType => row.setByte(pos, toNumber(value).byteValue())
      case ShortType => row.setShort(pos, toNumber(value).shortValue())
      case IntegerType => row.setInt(pos, toNumber(value).intValue())
      case LongType => row.setLong(pos, toNumber(value).longValue())
      case FloatType => row.setFloat(pos, toNumber(value).floatValue())
      case DoubleType => row.setDouble(pos, toNumber(value).doubleValue())
      case StringType =>
        row.update(pos, UTF8String.fromString(value.toString))
      case dt: DecimalType =>
        val bd = value match {
          case v: JBigDecimal => v
          case v: JBigInteger => new JBigDecimal(v)
          case _ => new JBigDecimal(value.toString)
        }
        row.update(pos, Decimal.apply(bd, dt.precision, dt.scale))
      case DateType =>
        value match {
          case v: Date =>
            row.setInt(pos, DateTimeUtils.fromJavaDate(v))
          case v: LocalDate =>
            row.setInt(pos, DateTimeUtils.fromJavaDate(Date.valueOf(v)))
          case _ =>
            row.setInt(pos, DateTimeUtils.fromJavaDate(Date.valueOf(value.toString)))
        }
      case TimestampType =>
        value match {
          case v: Timestamp =>
            row.setLong(pos, DateTimeUtils.fromJavaTimestamp(v))
          case v: LocalDateTime =>
            row.setLong(pos, DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf(v)))
          case _ =>
            row.setLong(pos, DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf(value.toString)))
        }
      case BinaryType =>
        row.update(pos, value.asInstanceOf[Array[Byte]])
      case _ =>
        row.update(pos, UTF8String.fromString(value.toString))
    }
  }

  /** Converts a Spark InternalRow value to a Java object suitable for OBKV operations. */
  def toObkvValue(row: InternalRow, pos: Int, dataType: DataType): Any = {
    if (row.isNullAt(pos)) {
      return null
    }
    dataType match {
      case BooleanType => row.getBoolean(pos)
      case ByteType => row.getByte(pos)
      case ShortType => row.getShort(pos)
      case IntegerType => row.getInt(pos)
      case LongType => row.getLong(pos)
      case FloatType => row.getFloat(pos)
      case DoubleType => row.getDouble(pos)
      case StringType => row.getUTF8String(pos).toString
      case dt: DecimalType =>
        row.getDecimal(pos, dt.precision, dt.scale).toJavaBigDecimal
      case DateType => DateTimeUtils.toJavaDate(row.getInt(pos))
      case TimestampType => DateTimeUtils.toJavaTimestamp(row.getLong(pos))
      case BinaryType => row.getBinary(pos)
      case _ => row.getUTF8String(pos).toString
    }
  }

  private def toNumber(value: Any): Number = {
    value match {
      case n: Number => n
      case _ => java.lang.Long.parseLong(value.toString)
    }
  }
}

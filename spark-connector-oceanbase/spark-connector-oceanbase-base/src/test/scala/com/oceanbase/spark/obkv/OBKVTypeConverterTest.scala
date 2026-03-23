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

import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import java.math.{BigDecimal => JBigDecimal}
import java.sql.{Date, Timestamp}
import java.time.{LocalDate, LocalDateTime}

class OBKVTypeConverterTest {

  // ========== setRowValue tests ==========

  @Test
  def testSetBoolean(): Unit = {
    val row = new SpecificInternalRow(Array[DataType](BooleanType))
    OBKVTypeConverter.setRowValue(row, 0, BooleanType, java.lang.Boolean.TRUE)
    assertTrue(row.getBoolean(0))
  }

  @Test
  def testSetBooleanFromNumber(): Unit = {
    val row = new SpecificInternalRow(Array[DataType](BooleanType))
    OBKVTypeConverter.setRowValue(row, 0, BooleanType, java.lang.Integer.valueOf(1))
    assertTrue(row.getBoolean(0))

    OBKVTypeConverter.setRowValue(row, 0, BooleanType, java.lang.Integer.valueOf(0))
    assertFalse(row.getBoolean(0))
  }

  @Test
  def testSetByte(): Unit = {
    val row = new SpecificInternalRow(Array[DataType](ByteType))
    OBKVTypeConverter.setRowValue(row, 0, ByteType, java.lang.Byte.valueOf(42.toByte))
    assertEquals(42.toByte, row.getByte(0))
  }

  @Test
  def testSetShort(): Unit = {
    val row = new SpecificInternalRow(Array[DataType](ShortType))
    OBKVTypeConverter.setRowValue(row, 0, ShortType, java.lang.Short.valueOf(1000.toShort))
    assertEquals(1000.toShort, row.getShort(0))
  }

  @Test
  def testSetInt(): Unit = {
    val row = new SpecificInternalRow(Array[DataType](IntegerType))
    OBKVTypeConverter.setRowValue(row, 0, IntegerType, java.lang.Integer.valueOf(12345))
    assertEquals(12345, row.getInt(0))
  }

  @Test
  def testSetLong(): Unit = {
    val row = new SpecificInternalRow(Array[DataType](LongType))
    OBKVTypeConverter.setRowValue(row, 0, LongType, java.lang.Long.valueOf(999999999L))
    assertEquals(999999999L, row.getLong(0))
  }

  @Test
  def testSetFloat(): Unit = {
    val row = new SpecificInternalRow(Array[DataType](FloatType))
    OBKVTypeConverter.setRowValue(row, 0, FloatType, java.lang.Float.valueOf(3.14f))
    assertEquals(3.14f, row.getFloat(0), 0.001f)
  }

  @Test
  def testSetDouble(): Unit = {
    val row = new SpecificInternalRow(Array[DataType](DoubleType))
    OBKVTypeConverter.setRowValue(row, 0, DoubleType, java.lang.Double.valueOf(2.718))
    assertEquals(2.718, row.getDouble(0), 0.001)
  }

  @Test
  def testSetString(): Unit = {
    val row = new SpecificInternalRow(Array[DataType](StringType))
    OBKVTypeConverter.setRowValue(row, 0, StringType, "hello")
    assertEquals("hello", row.getUTF8String(0).toString)
  }

  @Test
  def testSetDecimal(): Unit = {
    val dt = DecimalType(10, 2)
    val row = new SpecificInternalRow(Array[DataType](dt))
    OBKVTypeConverter.setRowValue(row, 0, dt, new JBigDecimal("123.45"))
    val result = row.getDecimal(0, dt.precision, dt.scale)
    assertEquals(new JBigDecimal("123.45"), result.toJavaBigDecimal)
  }

  @Test
  def testSetDate(): Unit = {
    val row = new SpecificInternalRow(Array[DataType](DateType))
    val date = Date.valueOf("2024-01-15")
    OBKVTypeConverter.setRowValue(row, 0, DateType, date)
    val result = DateTimeUtils.toJavaDate(row.getInt(0))
    assertEquals(date.toString, result.toString)
  }

  @Test
  def testSetDateFromLocalDate(): Unit = {
    val row = new SpecificInternalRow(Array[DataType](DateType))
    val localDate = LocalDate.of(2024, 6, 15)
    OBKVTypeConverter.setRowValue(row, 0, DateType, localDate)
    val result = DateTimeUtils.toJavaDate(row.getInt(0))
    assertEquals("2024-06-15", result.toString)
  }

  @Test
  def testSetTimestamp(): Unit = {
    val row = new SpecificInternalRow(Array[DataType](TimestampType))
    val ts = Timestamp.valueOf("2024-01-15 10:30:00")
    OBKVTypeConverter.setRowValue(row, 0, TimestampType, ts)
    val result = DateTimeUtils.toJavaTimestamp(row.getLong(0))
    assertEquals(ts, result)
  }

  @Test
  def testSetTimestampFromLocalDateTime(): Unit = {
    val row = new SpecificInternalRow(Array[DataType](TimestampType))
    val ldt = LocalDateTime.of(2024, 6, 15, 14, 30, 0)
    OBKVTypeConverter.setRowValue(row, 0, TimestampType, ldt)
    val result = DateTimeUtils.toJavaTimestamp(row.getLong(0))
    assertEquals(Timestamp.valueOf(ldt), result)
  }

  @Test
  def testSetBinary(): Unit = {
    val row = new SpecificInternalRow(Array[DataType](BinaryType))
    val bytes = Array[Byte](1, 2, 3, 4)
    OBKVTypeConverter.setRowValue(row, 0, BinaryType, bytes)
    assertArrayEquals(bytes, row.getBinary(0))
  }

  // ========== toObkvValue tests ==========

  @Test
  def testToNull(): Unit = {
    val row = new SpecificInternalRow(Array[DataType](IntegerType))
    row.setNullAt(0)
    assertNull(OBKVTypeConverter.toObkvValue(row, 0, IntegerType))
  }

  @Test
  def testToBoolean(): Unit = {
    val row = new SpecificInternalRow(Array[DataType](BooleanType))
    row.setBoolean(0, true)
    assertEquals(true, OBKVTypeConverter.toObkvValue(row, 0, BooleanType))
  }

  @Test
  def testToInt(): Unit = {
    val row = new SpecificInternalRow(Array[DataType](IntegerType))
    row.setInt(0, 123)
    assertEquals(123, OBKVTypeConverter.toObkvValue(row, 0, IntegerType))
  }

  @Test
  def testToLong(): Unit = {
    val row = new SpecificInternalRow(Array[DataType](LongType))
    row.setLong(0, 999L)
    assertEquals(999L, OBKVTypeConverter.toObkvValue(row, 0, LongType))
  }

  @Test
  def testToFloat(): Unit = {
    val row = new SpecificInternalRow(Array[DataType](FloatType))
    row.setFloat(0, 3.14f)
    assertEquals(
      3.14f,
      OBKVTypeConverter.toObkvValue(row, 0, FloatType).asInstanceOf[Float],
      0.001f)
  }

  @Test
  def testToDouble(): Unit = {
    val row = new SpecificInternalRow(Array[DataType](DoubleType))
    row.setDouble(0, 2.718)
    assertEquals(
      2.718,
      OBKVTypeConverter.toObkvValue(row, 0, DoubleType).asInstanceOf[Double],
      0.001)
  }

  @Test
  def testToString(): Unit = {
    val row = new SpecificInternalRow(Array[DataType](StringType))
    import org.apache.spark.unsafe.types.UTF8String
    row.update(0, UTF8String.fromString("hello"))
    assertEquals("hello", OBKVTypeConverter.toObkvValue(row, 0, StringType))
  }

  @Test
  def testToDecimal(): Unit = {
    val dt = DecimalType(10, 2)
    val row = new SpecificInternalRow(Array[DataType](dt))
    row.update(0, Decimal.apply(new JBigDecimal("123.45"), dt.precision, dt.scale))
    val result = OBKVTypeConverter.toObkvValue(row, 0, dt)
    assertEquals("123.45", result)
  }

  @Test
  def testToDate(): Unit = {
    val row = new SpecificInternalRow(Array[DataType](DateType))
    val date = Date.valueOf("2024-01-15")
    row.setInt(0, DateTimeUtils.fromJavaDate(date))
    val result = OBKVTypeConverter.toObkvValue(row, 0, DateType)
    assertTrue(result.isInstanceOf[Date])
    assertEquals(date.toString, result.toString)
  }

  @Test
  def testToTimestamp(): Unit = {
    val row = new SpecificInternalRow(Array[DataType](TimestampType))
    val ts = Timestamp.valueOf("2024-01-15 10:30:00")
    row.setLong(0, DateTimeUtils.fromJavaTimestamp(ts))
    val result = OBKVTypeConverter.toObkvValue(row, 0, TimestampType)
    assertTrue(result.isInstanceOf[Timestamp])
    assertEquals(ts, result)
  }

  @Test
  def testToBinary(): Unit = {
    val row = new SpecificInternalRow(Array[DataType](BinaryType))
    val bytes = Array[Byte](10, 20, 30)
    row.update(0, bytes)
    val result = OBKVTypeConverter.toObkvValue(row, 0, BinaryType)
    assertArrayEquals(bytes, result.asInstanceOf[Array[Byte]])
  }
}

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

package com.oceanbase.spark.writer.v2

import com.oceanbase.spark.config.OceanBaseConfig
import com.oceanbase.spark.directload.{DirectLoader, DirectLoadUtils}
import com.oceanbase.spark.utils.RetryUtils
import com.oceanbase.spark.writer.v2.DirectLoadWriteV2.convertFieldToJava

import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadBucket
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, DateTimeUtils}
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, DataType, DateType, Decimal, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructType, TimestampType}

import scala.collection.JavaConverters.asJavaIterableConverter
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

class DirectLoadWriteV2(schema: StructType, oceanBaseConfig: OceanBaseConfig)
  extends DataWriter[InternalRow]
  with Logging {

  private val DEFAULT_BUFFER_SIZE = oceanBaseConfig.getDirectLoadBatchSize
  val buffer: ArrayBuffer[InternalRow] = ArrayBuffer[InternalRow]()
  val directLoader: DirectLoader =
    DirectLoadUtils.buildDirectLoaderFromSetting(oceanBaseConfig)
  directLoader.begin()

  override def write(record: InternalRow): Unit = {
    // Added copy() method to fix data duplication issue caused by CTAS syntax
    buffer += record.copy()
    if (buffer.length >= DEFAULT_BUFFER_SIZE) flush()
  }

  private def flush(): Unit = {
    RetryUtils.retry() {
      Try {
        doFlush()
      } match {
        case Success(_) => buffer.clear()
        case Failure(exception) => throw exception
      }
    }
  }

  private def doFlush(): Unit = {
    if (buffer.isEmpty) return
    try {
      val bucket = new ObDirectLoadBucket()
      buffer.foreach(
        row => {
          val array = new Array[ObObj](row.numFields)
          for (i <- 0 until (row.numFields)) {
            array(i) = DirectLoader.createObObj(convertFieldToJava(row, i, schema(i).dataType))
          }
          bucket.addRow(array)
        })

      directLoader.write(bucket)
    } catch {
      case ex: Exception =>
        throw new RuntimeException(s"The direct-load failed to write to ob", ex)
    }
  }

  override def commit(): WriterCommitMessage = {
    flush()
    CommitMessage()
  }

  override def abort(): Unit = {}

  override def close(): Unit = {}
}

object DirectLoadWriteV2 {

  private def convertFieldToJava(row: InternalRow, index: Int, dataType: DataType): AnyRef = {
    if (row.isNullAt(index)) return null

    dataType match {
      case BooleanType => Boolean.box(row.getBoolean(index))
      case ByteType => Byte.box(row.getByte(index))
      case ShortType => Short.box(row.getShort(index))
      case IntegerType => Int.box(row.getInt(index))
      case LongType => Long.box(row.getLong(index))
      case FloatType => Float.box(row.getFloat(index))
      case DoubleType => Double.box(row.getDouble(index))

      case StringType => row.getUTF8String(index).toString
      case BinaryType => row.getBinary(index)

      case DateType => DateTimeUtils.toJavaDate(row.getInt(index))
      case TimestampType => DateTimeUtils.toJavaTimestamp(row.getLong(index))

      case d: DecimalType => row.getDecimal(index, d.precision, d.scale).toJavaBigDecimal

      case ArrayType(elementType, _) =>
        row.getArray(index).toObjectArray(elementType).map {
          elem => convertElementToJava(elem, elementType)
        }

      case struct: StructType =>
        row
          .getStruct(index, struct.fields.length)
          .toSeq(struct)
          .zip(struct.fields)
          .map { case (value, field) => convertElementToJava(value, field.dataType) }
          .asJava

      case _ =>
        throw new UnsupportedOperationException(
          s"Unsupported data type: ${dataType.catalogString}"
        )
    }
  }

  private def convertElementToJava(element: Any, dataType: DataType): AnyRef = {
    element match {
      case null => null
      case arr: ArrayData =>
        arr.toObjectArray(dataType).map(convertElementToJava(_, dataType))
      case struct: InternalRow =>
        val structType = dataType.asInstanceOf[StructType]
        struct
          .toSeq(structType)
          .zip(structType.fields)
          .map { case (v, f) => convertElementToJava(v, f.dataType) }
          .asJava
      case _ =>
        dataType match {
          case BooleanType => Boolean.box(element.asInstanceOf[Boolean])
          case ByteType => Byte.box(element.asInstanceOf[Byte])
          case ShortType => Short.box(element.asInstanceOf[Short])
          case IntegerType => Int.box(element.asInstanceOf[Int])
          case LongType => Long.box(element.asInstanceOf[Long])
          case FloatType => Float.box(element.asInstanceOf[Float])
          case DoubleType => Double.box(element.asInstanceOf[Double])
          case StringType => element.toString
          case _: DecimalType => element.asInstanceOf[Decimal].toJavaBigDecimal
          case _ =>
            throw new IllegalArgumentException(
              s"Unsupported element type: ${dataType.catalogString}"
            )
        }
    }
  }
}

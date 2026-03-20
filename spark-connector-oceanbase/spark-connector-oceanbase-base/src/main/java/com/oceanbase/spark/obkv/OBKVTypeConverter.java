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

package com.oceanbase.spark.obkv;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;


import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.unsafe.types.UTF8String;

/** Converts between OBKV Java objects and Spark InternalRow values. */
public class OBKVTypeConverter {

    /**
     * Sets a value from an OBKV result into a Spark InternalRow.
     *
     * @param row the mutable row to set the value on
     * @param pos the column position
     * @param dataType the Spark data type
     * @param value the OBKV value (non-null)
     */
    public static void setRowValue(
            SpecificInternalRow row, int pos, DataType dataType, Object value) {
        if (dataType == DataTypes.BooleanType) {
            if (value instanceof Boolean) {
                row.setBoolean(pos, (Boolean) value);
            } else {
                row.setBoolean(pos, toNumber(value).intValue() != 0);
            }
        } else if (dataType == DataTypes.ByteType) {
            row.setByte(pos, toNumber(value).byteValue());
        } else if (dataType == DataTypes.ShortType) {
            row.setShort(pos, toNumber(value).shortValue());
        } else if (dataType == DataTypes.IntegerType) {
            row.setInt(pos, toNumber(value).intValue());
        } else if (dataType == DataTypes.LongType) {
            row.setLong(pos, toNumber(value).longValue());
        } else if (dataType == DataTypes.FloatType) {
            row.setFloat(pos, toNumber(value).floatValue());
        } else if (dataType == DataTypes.DoubleType) {
            row.setDouble(pos, toNumber(value).doubleValue());
        } else if (dataType == DataTypes.StringType) {
            row.update(pos, UTF8String.fromString(value.toString()));
        } else if (dataType instanceof DecimalType) {
            DecimalType dt = (DecimalType) dataType;
            BigDecimal bd;
            if (value instanceof BigDecimal) {
                bd = (BigDecimal) value;
            } else if (value instanceof BigInteger) {
                bd = new BigDecimal((BigInteger) value);
            } else {
                bd = new BigDecimal(value.toString());
            }
            row.update(pos, Decimal.apply(bd, dt.precision(), dt.scale()));
        } else if (dataType == DataTypes.DateType) {
            if (value instanceof Date) {
                row.setInt(pos, DateTimeUtils.fromJavaDate((Date) value));
            } else if (value instanceof LocalDate) {
                row.setInt(pos, DateTimeUtils.fromJavaDate(Date.valueOf((LocalDate) value)));
            } else {
                row.setInt(pos, DateTimeUtils.fromJavaDate(Date.valueOf(value.toString())));
            }
        } else if (dataType == DataTypes.TimestampType) {
            if (value instanceof Timestamp) {
                row.setLong(pos, DateTimeUtils.fromJavaTimestamp((Timestamp) value));
            } else if (value instanceof LocalDateTime) {
                row.setLong(
                        pos,
                        DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf((LocalDateTime) value)));
            } else {
                row.setLong(
                        pos, DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf(value.toString())));
            }
        } else if (dataType == DataTypes.BinaryType) {
            row.update(pos, (byte[]) value);
        } else {
            row.update(pos, UTF8String.fromString(value.toString()));
        }
    }

    /**
     * Converts a Spark InternalRow value to a Java object suitable for OBKV operations.
     *
     * @param row the Spark internal row
     * @param pos the column position
     * @param dataType the Spark data type
     * @return a Java object for OBKV
     */
    public static Object toObkvValue(InternalRow row, int pos, DataType dataType) {
        if (row.isNullAt(pos)) {
            return null;
        }
        if (dataType == DataTypes.BooleanType) {
            return row.getBoolean(pos);
        } else if (dataType == DataTypes.ByteType) {
            return row.getByte(pos);
        } else if (dataType == DataTypes.ShortType) {
            return row.getShort(pos);
        } else if (dataType == DataTypes.IntegerType) {
            return row.getInt(pos);
        } else if (dataType == DataTypes.LongType) {
            return row.getLong(pos);
        } else if (dataType == DataTypes.FloatType) {
            return row.getFloat(pos);
        } else if (dataType == DataTypes.DoubleType) {
            return row.getDouble(pos);
        } else if (dataType == DataTypes.StringType) {
            return row.getUTF8String(pos).toString();
        } else if (dataType instanceof DecimalType) {
            DecimalType dt = (DecimalType) dataType;
            return row.getDecimal(pos, dt.precision(), dt.scale()).toJavaBigDecimal();
        } else if (dataType == DataTypes.DateType) {
            return DateTimeUtils.toJavaDate(row.getInt(pos));
        } else if (dataType == DataTypes.TimestampType) {
            return DateTimeUtils.toJavaTimestamp(row.getLong(pos));
        } else if (dataType == DataTypes.BinaryType) {
            return row.getBinary(pos);
        } else {
            return row.getUTF8String(pos).toString();
        }
    }

    private static Number toNumber(Object value) {
        if (value instanceof Number) {
            return (Number) value;
        }
        return Long.parseLong(value.toString());
    }
}

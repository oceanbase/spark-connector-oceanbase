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

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.types.StructType

import java.sql.Connection

import scala.collection.mutable.ArrayBuffer

class OceanBaseOracleDialect extends OceanBaseDialect {
  override def quoteIdentifier(colName: String): String = {
    s""""$colName""""
  }

  /** Creates a schema. */
  override def createSchema(
      conn: Connection,
      options: JDBCOptions,
      schema: String,
      comment: String): Unit =
    throw new UnsupportedOperationException("Not currently supported in oracle mode")

  override def schemaExists(conn: Connection, options: JDBCOptions, schema: String): Boolean =
    throw new UnsupportedOperationException("Not currently supported in oracle mode")

  override def listSchemas(conn: Connection, options: JDBCOptions): Array[Array[String]] =
    throw new UnsupportedOperationException("Not currently supported in oracle mode")

  /** Drops a schema from OceanBase. */
  override def dropSchema(
      conn: Connection,
      options: JDBCOptions,
      schema: String,
      cascade: Boolean): Unit = throw new UnsupportedOperationException(
    "Not currently supported in oracle mode")

  override def getPriKeyInfo(
      schemaName: String,
      tableName: String,
      option: JDBCOptions): ArrayBuffer[PriKeyColumnInfo] = {
    throw new UnsupportedOperationException("Not currently supported in oracle mode")
  }

  override def getInsertIntoStatement(tableName: String, schema: StructType): String = {
    throw new UnsupportedOperationException("Not currently supported in oracle mode")
  }

  override def getUpsertIntoStatement(
      tableName: String,
      schema: StructType,
      priKeyColumnInfo: ArrayBuffer[PriKeyColumnInfo]): String = {
    throw new UnsupportedOperationException("Not currently supported in oracle mode")
  }
}

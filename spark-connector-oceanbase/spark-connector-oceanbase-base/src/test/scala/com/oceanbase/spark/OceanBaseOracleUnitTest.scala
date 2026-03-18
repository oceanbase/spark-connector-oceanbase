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
package com.oceanbase.spark

import com.oceanbase.spark.config.OceanBaseConfig
import com.oceanbase.spark.dialect.{OceanBaseOracleDialect, PriKeyColumnInfo}

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

import scala.collection.mutable.ArrayBuffer

/**
 * Unit tests for OceanBase Oracle dialect. Each test covers a specific behavior (e.g. write-hints
 * pushdown in upsert/MERGE); more cases can be added as needed.
 */
class OceanBaseOracleUnitTest extends OceanBaseOracleTestBase {

  @Test
  def testWriteHintsInUpsertStatement(): Unit = {
    val dialect = new OceanBaseOracleDialect
    val schema = StructType(
      Array(
        StructField("ID", IntegerType),
        StructField("NAME", StringType)
      ))
    val priKeyColumnInfo = ArrayBuffer(
      PriKeyColumnInfo("\"ID\"", "NUMBER(10)", "PRI", "NUMBER", "")
    )
    val options = new java.util.HashMap[String, String](getOptions)
    options.put("table-name", "PRODUCTS")
    options.put("jdbc.pushdown-write-hints", "MONITOR PARALLEL(2)")
    val config = new OceanBaseConfig(options)
    val tableName = s"${config.getSchemaName}.PRODUCTS"
    val sql = dialect.getUpsertIntoStatement(tableName, schema, priKeyColumnInfo, config)
    Assertions.assertTrue(
      sql.contains("/*+ MONITOR PARALLEL(2) */"),
      s"Generated upsert SQL should contain write hint: $sql")
    Assertions.assertTrue(sql.contains("MERGE"), s"Generated SQL should be MERGE: $sql")
  }
}

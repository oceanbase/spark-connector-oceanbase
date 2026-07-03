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

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class OceanBaseDialectTest {

  private val mysqlDialect = new OceanBaseMySQLDialect
  private val oracleDialect = new OceanBaseOracleDialect

  @Test
  def testMySQLQuoteIdentifierForReservedWords(): Unit = {
    Seq("usage", "order", "group", "key", "rank", "index").foreach {
      colName => assertEquals(s"`$colName`", mysqlDialect.quoteIdentifier(colName))
    }
  }

  @Test
  def testMySQLQuoteIdentifierEscapesEmbeddedBackticks(): Unit = {
    assertEquals("`a``b`", mysqlDialect.quoteIdentifier("a`b"))
  }

  @Test
  def testOracleQuoteIdentifierForReservedWords(): Unit = {
    assertEquals("\"usage\"", oracleDialect.quoteIdentifier("usage"))
  }

  @Test
  def testOracleQuoteIdentifierEscapesEmbeddedDoubleQuotes(): Unit = {
    assertEquals("\"a\"\"b\"", oracleDialect.quoteIdentifier("a\"b"))
  }
}

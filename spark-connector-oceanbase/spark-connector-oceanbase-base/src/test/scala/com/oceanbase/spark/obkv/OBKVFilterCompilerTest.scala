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

import com.alipay.oceanbase.rpc.filter.{ObTableFilterList, ObTableValueFilter}
import org.apache.spark.sql.sources._
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class OBKVFilterCompilerTest {

  private val primaryKeys = Array("id", "tenant_id")
  private val compiler = new OBKVFilterCompiler(primaryKeys)

  @Test
  def testCompileEqualTo(): Unit = {
    val result = compiler.compile(Array(EqualTo("name", "test")))
    assertNotNull(result.serverFilter)
    assertTrue(result.serverFilter.isInstanceOf[ObTableValueFilter])
    assertEquals(0, result.unhandledFilters.length)
  }

  @Test
  def testCompileGreaterThan(): Unit = {
    val result = compiler.compile(Array(GreaterThan("age", 18)))
    assertNotNull(result.serverFilter)
    assertTrue(result.serverFilter.isInstanceOf[ObTableValueFilter])
    assertEquals(0, result.unhandledFilters.length)
  }

  @Test
  def testCompileGreaterThanOrEqual(): Unit = {
    val result = compiler.compile(Array(GreaterThanOrEqual("age", 18)))
    assertNotNull(result.serverFilter)
    assertTrue(result.serverFilter.isInstanceOf[ObTableValueFilter])
    assertEquals(0, result.unhandledFilters.length)
  }

  @Test
  def testCompileLessThan(): Unit = {
    val result = compiler.compile(Array(LessThan("price", 100.0)))
    assertNotNull(result.serverFilter)
    assertTrue(result.serverFilter.isInstanceOf[ObTableValueFilter])
    assertEquals(0, result.unhandledFilters.length)
  }

  @Test
  def testCompileLessThanOrEqual(): Unit = {
    val result = compiler.compile(Array(LessThanOrEqual("price", 100.0)))
    assertNotNull(result.serverFilter)
    assertTrue(result.serverFilter.isInstanceOf[ObTableValueFilter])
    assertEquals(0, result.unhandledFilters.length)
  }

  @Test
  def testCompileIsNull(): Unit = {
    val result = compiler.compile(Array(IsNull("col")))
    assertNotNull(result.serverFilter)
    assertTrue(result.serverFilter.isInstanceOf[ObTableValueFilter])
    assertEquals(0, result.unhandledFilters.length)
  }

  @Test
  def testCompileIsNotNull(): Unit = {
    val result = compiler.compile(Array(IsNotNull("col")))
    assertNotNull(result.serverFilter)
    assertTrue(result.serverFilter.isInstanceOf[ObTableValueFilter])
    assertEquals(0, result.unhandledFilters.length)
  }

  @Test
  def testCompileAnd(): Unit = {
    val filter = And(EqualTo("name", "test"), GreaterThan("age", 18))
    val result = compiler.compile(Array(filter))
    assertNotNull(result.serverFilter)
    assertTrue(result.serverFilter.isInstanceOf[ObTableFilterList])
    assertEquals(0, result.unhandledFilters.length)
  }

  @Test
  def testCompileOr(): Unit = {
    val filter = Or(EqualTo("name", "a"), EqualTo("name", "b"))
    val result = compiler.compile(Array(filter))
    assertNotNull(result.serverFilter)
    assertTrue(result.serverFilter.isInstanceOf[ObTableFilterList])
    assertEquals(0, result.unhandledFilters.length)
  }

  @Test
  def testCompileNot(): Unit = {
    val filter = Not(EqualTo("name", "test"))
    val result = compiler.compile(Array(filter))
    assertNull(result.serverFilter)
    assertEquals(1, result.unhandledFilters.length)
    assertTrue(result.unhandledFilters(0).isInstanceOf[Not])
  }

  @Test
  def testCompileUnsupported(): Unit = {
    val filter = In("a", Array(1, 2, 3))
    val result = compiler.compile(Array(filter))
    assertNull(result.serverFilter)
    assertEquals(1, result.unhandledFilters.length)
  }

  @Test
  def testCompileMultipleMixed(): Unit = {
    val filters = Array[Filter](
      EqualTo("name", "test"),
      GreaterThan("age", 18),
      In("status", Array("A", "B"))
    )
    val result = compiler.compile(filters)
    assertNotNull(result.serverFilter)
    assertTrue(result.serverFilter.isInstanceOf[ObTableFilterList])
    assertEquals(1, result.unhandledFilters.length)
    assertTrue(result.unhandledFilters(0).isInstanceOf[In])
  }

  @Test
  def testCompileEmpty(): Unit = {
    val result = compiler.compile(Array.empty[Filter])
    assertNull(result.serverFilter)
    assertEquals(0, result.unhandledFilters.length)
  }

  @Test
  def testCompileAndWithOneUnsupported(): Unit = {
    val filter = And(EqualTo("name", "test"), Not(EqualTo("age", 18)))
    val result = compiler.compile(Array(filter))
    assertNull(result.serverFilter)
    assertEquals(1, result.unhandledFilters.length)
  }

  @Test
  def testIsPrimaryKeyFilter(): Unit = {
    assertTrue(compiler.isPrimaryKeyFilter(EqualTo("id", 1)))
    assertTrue(compiler.isPrimaryKeyFilter(GreaterThan("tenant_id", 10)))
    assertTrue(compiler.isPrimaryKeyFilter(GreaterThanOrEqual("id", 5)))
    assertTrue(compiler.isPrimaryKeyFilter(LessThan("id", 100)))
    assertTrue(compiler.isPrimaryKeyFilter(LessThanOrEqual("tenant_id", 50)))
    assertFalse(compiler.isPrimaryKeyFilter(EqualTo("name", "test")))
    assertFalse(compiler.isPrimaryKeyFilter(GreaterThan("price", 10.0)))
  }

  @Test
  def testIsPrimaryKeyFilterUnsupported(): Unit = {
    assertFalse(compiler.isPrimaryKeyFilter(IsNull("id")))
    assertFalse(compiler.isPrimaryKeyFilter(IsNotNull("id")))
    assertFalse(compiler.isPrimaryKeyFilter(Not(EqualTo("id", 1))))
    assertFalse(compiler.isPrimaryKeyFilter(In("id", Array(1, 2))))
  }
}

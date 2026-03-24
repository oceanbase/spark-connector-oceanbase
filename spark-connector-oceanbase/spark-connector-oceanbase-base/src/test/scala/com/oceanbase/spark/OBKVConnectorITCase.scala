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

import com.oceanbase.spark.OceanBaseMySQLTestBase.{constructConfigUrlForODP, createSysUser, getConfigServerAddress}
import com.oceanbase.spark.OceanBaseTestBase.assertEqualsInAnyOrder

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.junit.jupiter.api.{AfterAll, AfterEach, Assertions, BeforeAll, BeforeEach, Tag, Test}

import java.util

@Tag("obkv-it")
class OBKVConnectorITCase extends OceanBaseMySQLTestBase {

  @BeforeEach
  def before(): Unit = {
    initialize("sql/mysql/obkv_test.sql")
  }

  @AfterEach
  def after(): Unit = {
    dropTables(
      "obkv_products",
      "obkv_composite_pk",
      "obkv_all_types",
      "obkv_partitioned",
      "obkv_type_write_test",
      "obkv_boundary_test",
      "obkv_null_test"
    )
  }

  val OB_CATALOG_CLASS = "com.oceanbase.spark.catalog.OceanBaseCatalog"

  private def createObkvCatalogSession(): SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .config("spark.sql.catalog.ob", OB_CATALOG_CLASS)
      .config("spark.sql.catalog.ob.url", getJdbcUrl)
      .config("spark.sql.catalog.ob.username", getUsername)
      .config("spark.sql.catalog.ob.password", getPassword)
      .config("spark.sql.catalog.ob.schema-name", getSchemaName)
      .config("spark.sql.catalog.ob.obkv.enabled", "true")
      .config("spark.sql.catalog.ob.obkv.odp-mode", "true")
      .config("spark.sql.catalog.ob.obkv.odp-addr", OceanBaseMySQLTestBase.ODP.getHost)
      .config("spark.sql.catalog.ob.obkv.odp-port", OceanBaseMySQLTestBase.ODP.getRpcPort.toString)
      .config("spark.sql.catalog.ob.obkv.full-user-name", s"$getUsername#$getClusterName")
      .config("spark.sql.catalog.ob.obkv.password", getPassword)
      .getOrCreate()
  }

  @Test
  def testCatalogObkvWriteAndRead(): Unit = {
    val session = createObkvCatalogSession()
    session.sql("use ob;")

    session.sql(s"""
                   |INSERT INTO $getSchemaName.obkv_products VALUES
                   |(101, 'scooter', 'Small 2-wheel scooter', 3.14),
                   |(102, 'car battery', '12V car battery', 8.1);
                   |""".stripMargin)

    import scala.collection.JavaConverters._
    val expected = util.Arrays.asList(
      "101,scooter,Small 2-wheel scooter,3.14",
      "102,car battery,12V car battery,8.1"
    )
    val actual = session
      .sql(s"SELECT * FROM $getSchemaName.obkv_products")
      .collect()
      .map(_.toString().drop(1).dropRight(1))
      .toList
      .asJava
    assertEqualsInAnyOrder(expected, actual)

    session.stop()
  }

  @Test
  def testCatalogObkvWriteWithJdbcVerify(): Unit = {
    val session = createObkvCatalogSession()
    session.sql("use ob;")

    session.sql(s"""
                   |INSERT INTO $getSchemaName.obkv_products VALUES
                   |(101, 'scooter', 'Small 2-wheel scooter', 3.14),
                   |(102, 'car battery', '12V car battery', 8.1),
                   |(103, 'hammer', '12oz hammer', 0.75);
                   |""".stripMargin)

    session.stop()

    waitingAndAssertTableCount("obkv_products", 3)
    import scala.collection.JavaConverters._
    val expected = util.Arrays.asList(
      "101,scooter,Small 2-wheel scooter,3.14",
      "102,car battery,12V car battery,8.1",
      "103,hammer,12oz hammer,0.75"
    )
    val actual = queryTable("obkv_products")
    assertEqualsInAnyOrder(expected, actual)
  }

  @Test
  def testCatalogObkvReadAfterJdbcInsert(): Unit = {
    val conn = getJdbcConnection()
    try {
      val stmt = conn.createStatement()
      try {
        stmt.executeUpdate(
          s"INSERT INTO $getSchemaName.obkv_products VALUES (201, 'widget', 'A small widget', 1.5)")
        stmt.executeUpdate(
          s"INSERT INTO $getSchemaName.obkv_products VALUES (202, 'gadget', 'A fancy gadget', 2.5)")
      } finally {
        stmt.close()
      }
    } finally {
      conn.close()
    }

    val session = createObkvCatalogSession()
    session.sql("use ob;")

    import scala.collection.JavaConverters._
    val expected = util.Arrays.asList(
      "201,widget,A small widget,1.5",
      "202,gadget,A fancy gadget,2.5"
    )
    val actual = session
      .sql(s"SELECT * FROM $getSchemaName.obkv_products")
      .collect()
      .map(_.toString().drop(1).dropRight(1))
      .toList
      .asJava
    assertEqualsInAnyOrder(expected, actual)

    session.stop()
  }

  @Test
  def testNonCatalogDataFrameWriteAndRead(): Unit = {
    val session = SparkSession.builder().master("local[*]").getOrCreate()

    val df = session
      .createDataFrame(
        Seq(
          (101, "scooter", "Small 2-wheel scooter", 3.14),
          (102, "car battery", "12V car battery", 8.1)
        ))
      .toDF("id", "name", "description", "weight")

    df.write
      .format("oceanbase")
      .mode(SaveMode.Append)
      .option("url", getJdbcUrl)
      .option("username", getUsername)
      .option("password", getPassword)
      .option("table-name", "obkv_products")
      .option("schema-name", getSchemaName)
      .option("obkv.enabled", "true")
      .option("obkv.odp-mode", "true")
      .option("obkv.odp-addr", OceanBaseMySQLTestBase.ODP.getHost)
      .option("obkv.odp-port", OceanBaseMySQLTestBase.ODP.getRpcPort)
      .option("obkv.full-user-name", s"$getUsername#$getClusterName")
      .option("obkv.password", getPassword)
      .option("obkv.primary-key", "id")
      .save()

    // Verify via JDBC
    waitingAndAssertTableCount("obkv_products", 2)
    import scala.collection.JavaConverters._
    val expected = util.Arrays.asList(
      "101,scooter,Small 2-wheel scooter,3.14",
      "102,car battery,12V car battery,8.1"
    )
    val actual = queryTable("obkv_products")
    assertEqualsInAnyOrder(expected, actual)

    // Read back via DataFrame
    val readDf = session.read
      .format("oceanbase")
      .option("url", getJdbcUrl)
      .option("username", getUsername)
      .option("password", getPassword)
      .option("table-name", "obkv_products")
      .option("schema-name", getSchemaName)
      .option("obkv.enabled", "true")
      .option("obkv.odp-mode", "true")
      .option("obkv.odp-addr", OceanBaseMySQLTestBase.ODP.getHost)
      .option("obkv.odp-port", OceanBaseMySQLTestBase.ODP.getRpcPort)
      .option("obkv.full-user-name", s"$getUsername#$getClusterName")
      .option("obkv.password", getPassword)
      .option("obkv.primary-key", "id")
      .load()

    val readActual = readDf
      .collect()
      .map(_.toString().drop(1).dropRight(1))
      .toList
      .asJava
    assertEqualsInAnyOrder(expected, readActual)

    session.stop()
  }

  @Test
  def testPredicatePushdown(): Unit = {
    val session = createObkvCatalogSession()
    session.sql("use ob;")

    session.sql(s"""
                   |INSERT INTO $getSchemaName.obkv_products VALUES
                   |(101, 'scooter', 'Small 2-wheel scooter', 3.14),
                   |(102, 'car battery', '12V car battery', 8.1),
                   |(103, 'hammer', '12oz hammer', 0.75),
                   |(104, 'rocks', 'box of assorted rocks', null),
                   |(105, 'jacket', 'wind breaker', 0.1);
                   |""".stripMargin)

    import scala.collection.JavaConverters._

    // Test EqualTo pushdown
    val eqResult = session
      .sql(s"SELECT * FROM $getSchemaName.obkv_products WHERE id = 101")
      .collect()
    Assertions.assertEquals(1, eqResult.length)
    Assertions.assertTrue(eqResult(0).toString().contains("scooter"))

    // Test GreaterThan pushdown
    val gtResult = session
      .sql(s"SELECT * FROM $getSchemaName.obkv_products WHERE id > 103")
      .collect()
    Assertions.assertEquals(2, gtResult.length)

    // Test IsNotNull pushdown
    val notNullResult = session
      .sql(s"SELECT * FROM $getSchemaName.obkv_products WHERE weight IS NOT NULL")
      .collect()
    Assertions.assertEquals(4, notNullResult.length)

    // Test IsNull pushdown
    val nullResult = session
      .sql(s"SELECT * FROM $getSchemaName.obkv_products WHERE weight IS NULL")
      .collect()
    Assertions.assertEquals(1, nullResult.length)
    Assertions.assertTrue(nullResult(0).toString().contains("rocks"))

    session.stop()
  }

  @Test
  def testColumnPruning(): Unit = {
    val session = createObkvCatalogSession()
    session.sql("use ob;")

    session.sql(s"""
                   |INSERT INTO $getSchemaName.obkv_products VALUES
                   |(101, 'scooter', 'Small 2-wheel scooter', 3.14),
                   |(102, 'car battery', '12V car battery', 8.1);
                   |""".stripMargin)

    val result = session
      .sql(s"SELECT id, name FROM $getSchemaName.obkv_products")
      .collect()
    Assertions.assertEquals(2, result.length)
    // Each row should only have 2 columns
    Assertions.assertEquals(2, result(0).length)

    session.stop()
  }

  @Test
  def testCompositePrimaryKey(): Unit = {
    val session = createObkvCatalogSession()
    session.sql("use ob;")

    session.sql(s"""
                   |INSERT INTO $getSchemaName.obkv_composite_pk VALUES
                   |(1, 101, 'product_a', 10.5),
                   |(1, 102, 'product_b', 20.0),
                   |(2, 101, 'product_c', 15.0);
                   |""".stripMargin)

    import scala.collection.JavaConverters._
    val expected = util.Arrays.asList(
      "1,101,product_a,10.5",
      "1,102,product_b,20.0",
      "2,101,product_c,15.0"
    )
    val actual = session
      .sql(s"SELECT * FROM $getSchemaName.obkv_composite_pk")
      .collect()
      .map(_.toString().drop(1).dropRight(1))
      .toList
      .asJava
    assertEqualsInAnyOrder(expected, actual)

    // Test query with condition on composite primary key
    val filtered = session
      .sql(s"SELECT * FROM $getSchemaName.obkv_composite_pk WHERE tenant_id = 1")
      .collect()
    Assertions.assertEquals(2, filtered.length)

    session.stop()
  }

  @Test
  def testBatchFlush(): Unit = {
    val session = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.sql.catalog.ob", OB_CATALOG_CLASS)
      .config("spark.sql.catalog.ob.url", getJdbcUrl)
      .config("spark.sql.catalog.ob.username", getUsername)
      .config("spark.sql.catalog.ob.password", getPassword)
      .config("spark.sql.catalog.ob.schema-name", getSchemaName)
      .config("spark.sql.catalog.ob.obkv.enabled", "true")
      .config("spark.sql.catalog.ob.obkv.odp-mode", "true")
      .config("spark.sql.catalog.ob.obkv.odp-addr", OceanBaseMySQLTestBase.ODP.getHost)
      .config("spark.sql.catalog.ob.obkv.odp-port", OceanBaseMySQLTestBase.ODP.getRpcPort.toString)
      .config("spark.sql.catalog.ob.obkv.full-user-name", s"$getUsername#$getClusterName")
      .config("spark.sql.catalog.ob.obkv.password", getPassword)
      .config("spark.sql.catalog.ob.obkv.batch-size", "5")
      .getOrCreate()

    session.sql("use ob;")

    val values = (1 to 12).map(i => s"($i, 'item_$i', 'desc_$i', ${i * 1.1})").mkString(",\n")
    session.sql(s"""
                   |INSERT INTO $getSchemaName.obkv_products VALUES
                   |$values;
                   |""".stripMargin)

    session.stop()

    // Verify all 12 rows were written despite batch-size=5
    waitingAndAssertTableCount("obkv_products", 12)
  }

  @Test
  def testInsertOrUpdate(): Unit = {
    val session = createObkvCatalogSession()
    session.sql("use ob;")

    // First insert
    session.sql(s"""
                   |INSERT INTO $getSchemaName.obkv_products VALUES
                   |(101, 'scooter', 'Small 2-wheel scooter', 3.14);
                   |""".stripMargin)

    // Insert with same primary key (should update via INSERT_OR_UPDATE)
    session.sql(s"""
                   |INSERT INTO $getSchemaName.obkv_products VALUES
                   |(101, 'scooter_v2', 'Updated scooter', 5.0);
                   |""".stripMargin)

    import scala.collection.JavaConverters._
    val expected = util.Arrays.asList("101,scooter_v2,Updated scooter,5.0")
    val actual = session
      .sql(s"SELECT * FROM $getSchemaName.obkv_products")
      .collect()
      .map(_.toString().drop(1).dropRight(1))
      .toList
      .asJava
    assertEqualsInAnyOrder(expected, actual)

    session.stop()
  }

  @Test
  def testNullValues(): Unit = {
    val session = createObkvCatalogSession()
    session.sql("use ob;")

    session.sql(s"""
                   |INSERT INTO $getSchemaName.obkv_products VALUES
                   |(101, 'scooter', null, null);
                   |""".stripMargin)

    val result = session
      .sql(s"SELECT * FROM $getSchemaName.obkv_products WHERE id = 101")
      .collect()
    Assertions.assertEquals(1, result.length)
    Assertions.assertTrue(result(0).isNullAt(2)) // description is null
    Assertions.assertTrue(result(0).isNullAt(3)) // weight is null

    session.stop()
  }

  @Test
  def testAllDataTypes(): Unit = {
    // Insert via JDBC to test OBKV read for multiple types
    val conn = getJdbcConnection()
    try {
      val stmt = conn.createStatement()
      try {
        stmt.executeUpdate(s"""INSERT INTO $getSchemaName.obkv_all_types VALUES
                              |(1, true, 127, 32000, 100000, 9999999999, 3.14, 2.718281828,
                              | 12345.67890, 'hello world', '2024-01-15', '2024-01-15 10:30:00')
                              |""".stripMargin)
      } finally {
        stmt.close()
      }
    } finally {
      conn.close()
    }

    val session = createObkvCatalogSession()
    session.sql("use ob;")

    val result = session
      .sql(s"SELECT * FROM $getSchemaName.obkv_all_types")
      .collect()
    Assertions.assertEquals(1, result.length)

    val row = result(0)
    Assertions.assertEquals(1, row.getInt(0)) // pk_id
    Assertions.assertEquals("hello world", row.getString(9)) // col_varchar

    session.stop()
  }

  @Test
  def testPartitionedTableReadWrite(): Unit = {
    val session = createObkvCatalogSession()
    session.sql("use ob;")

    // Insert data across multiple partitions
    // p0: id < 1000
    // p1: id >= 1000 and id < 2000
    // p2: id >= 2000 and id < 3000
    // p3: id >= 3000
    session.sql(s"""
                   |INSERT INTO $getSchemaName.obkv_partitioned VALUES
                   |(100, 'item_p0_a', 1.0),
                   |(500, 'item_p0_b', 2.0),
                   |(1000, 'item_p1_a', 3.0),
                   |(1500, 'item_p1_b', 4.0),
                   |(2000, 'item_p2_a', 5.0),
                   |(2500, 'item_p2_b', 6.0),
                   |(3000, 'item_p3_a', 7.0),
                   |(4000, 'item_p3_b', 8.0);
                   |""".stripMargin)

    import scala.collection.JavaConverters._
    val expected = util.Arrays.asList(
      "100,item_p0_a,1.0",
      "500,item_p0_b,2.0",
      "1000,item_p1_a,3.0",
      "1500,item_p1_b,4.0",
      "2000,item_p2_a,5.0",
      "2500,item_p2_b,6.0",
      "3000,item_p3_a,7.0",
      "4000,item_p3_b,8.0"
    )
    val actual = session
      .sql(s"SELECT * FROM $getSchemaName.obkv_partitioned ORDER BY id")
      .collect()
      .map(_.toString().drop(1).dropRight(1))
      .toList
      .asJava
    assertEqualsInAnyOrder(expected, actual)

    // Test predicate pushdown on partitioned table
    val filtered = session
      .sql(s"SELECT * FROM $getSchemaName.obkv_partitioned WHERE id >= 1000 AND id < 2000")
      .collect()
    Assertions.assertEquals(2, filtered.length)

    session.stop()
  }

  @Test
  def testPartitionedTableWithJdbcVerify(): Unit = {
    val session = createObkvCatalogSession()
    session.sql("use ob;")

    // Insert data across multiple partitions
    session.sql(s"""
                   |INSERT INTO $getSchemaName.obkv_partitioned VALUES
                   |(100, 'item_p0', 1.0),
                   |(1500, 'item_p1', 2.0),
                   |(2500, 'item_p2', 3.0),
                   |(3500, 'item_p3', 4.0);
                   |""".stripMargin)

    session.stop()

    // Verify via JDBC
    waitingAndAssertTableCount("obkv_partitioned", 4)
    import scala.collection.JavaConverters._
    val expected = util.Arrays.asList(
      "100,item_p0,1.0",
      "1500,item_p1,2.0",
      "2500,item_p2,3.0",
      "3500,item_p3,4.0"
    )
    val actual = queryTable("obkv_partitioned")
    assertEqualsInAnyOrder(expected, actual)
  }

  // ==================== Comprehensive Type Tests ====================

  @Test
  def testAllTypesRead(): Unit = {
    // Insert all types via JDBC to test OBKV read
    val conn = getJdbcConnection()
    try {
      val stmt = conn.createStatement()
      try {
        stmt.executeUpdate(s"""
                              |INSERT INTO $getSchemaName.obkv_all_types VALUES (
                              |  1,                          -- pk_id
                              |  true,                       -- col_bool
                              |  127,                        -- col_tinyint (max)
                              |  32767,                      -- col_small (max)
                              |  2147483647,                 -- col_int (max)
                              |  9223372036854775807,        -- col_bigint (max)
                              |  3.14159,                    -- col_float
                              |  2.718281828459045,          -- col_double
                              |  123456789.12345,            -- col_decimal
                              |  'hello world',              -- col_varchar
                              |  'fixed char',               -- col_char
                              |  'this is a text field',     -- col_text
                              |  '2024-03-24',               -- col_date
                              |  '2024-03-24 10:30:45.123',  -- col_ts
                              |  UNHEX('DEADBEEF')           -- col_binary
                              |)
                              |""".stripMargin)
        stmt.executeUpdate(s"""
                              |INSERT INTO $getSchemaName.obkv_all_types VALUES (
                              |  2,                          -- pk_id
                              |  false,                      -- col_bool
                              |  -128,                       -- col_tinyint (min)
                              |  -32768,                     -- col_small (min)
                              |  -2147483648,                -- col_int (min)
                              |  -9223372036854775808,       -- col_bigint (min)
                              |  -3.14159,                   -- col_float
                              |  -2.718281828459045,         -- col_double
                              |  -99999.99999,               -- col_decimal
                              |  'negative values',          -- col_varchar
                              |  'negative',                 -- col_char
                              |  'testing negatives',        -- col_text
                              |  '2020-01-01',               -- col_date
                              |  '2020-01-01 00:00:00',      -- col_ts
                              |  NULL                        -- col_binary
                              |)
                              |""".stripMargin)
        // Insert row with null values
        stmt.executeUpdate(s"""
                              |INSERT INTO $getSchemaName.obkv_all_types VALUES (
                              |  3,           -- pk_id
                              |  NULL,        -- col_bool
                              |  NULL,        -- col_tinyint
                              |  NULL,        -- col_small
                              |  NULL,        -- col_int
                              |  NULL,        -- col_bigint
                              |  NULL,        -- col_float
                              |  NULL,        -- col_double
                              |  NULL,        -- col_decimal
                              |  NULL,        -- col_varchar
                              |  NULL,        -- col_char
                              |  NULL,        -- col_text
                              |  NULL,        -- col_date
                              |  NULL,        -- col_ts
                              |  NULL         -- col_binary
                              |)
                              |""".stripMargin)
      } finally {
        stmt.close()
      }
    } finally {
      conn.close()
    }

    val session = createObkvCatalogSession()
    session.sql("use ob;")

    // Test reading all rows
    val result =
      session.sql(s"SELECT * FROM $getSchemaName.obkv_all_types ORDER BY pk_id").collect()
    Assertions.assertEquals(3, result.length)

    // Verify row 1 (max values)
    val row1 = result(0)
    Assertions.assertEquals(1, row1.getInt(0)) // pk_id
    Assertions.assertEquals(true, row1.getBoolean(1)) // col_bool
    Assertions.assertEquals(127, row1.getByte(2)) // col_tinyint
    Assertions.assertEquals(32767, row1.getShort(3)) // col_small
    Assertions.assertEquals(2147483647, row1.getInt(4)) // col_int
    Assertions.assertEquals(9223372036854775807L, row1.getLong(5)) // col_bigint
    Assertions.assertEquals(3.14159f, row1.getFloat(6), 0.001f) // col_float
    Assertions.assertEquals(2.718281828459045, row1.getDouble(7), 0.0001) // col_double
    Assertions.assertEquals("hello world", row1.getString(9)) // col_varchar
    Assertions.assertEquals("2024-03-24", row1.getDate(12).toString) // col_date

    // Verify row 2 (min/negative values)
    val row2 = result(1)
    Assertions.assertEquals(2, row2.getInt(0))
    Assertions.assertEquals(false, row2.getBoolean(1))
    Assertions.assertEquals(-128, row2.getByte(2))
    Assertions.assertEquals(-32768, row2.getShort(3))
    Assertions.assertEquals(-2147483648, row2.getInt(4))

    // Verify row 3 (null values)
    val row3 = result(2)
    Assertions.assertEquals(3, row3.getInt(0))
    Assertions.assertTrue(row3.isNullAt(1)) // col_bool
    Assertions.assertTrue(row3.isNullAt(2)) // col_tinyint
    Assertions.assertTrue(row3.isNullAt(3)) // col_small
    Assertions.assertTrue(row3.isNullAt(4)) // col_int
    Assertions.assertTrue(row3.isNullAt(5)) // col_bigint
    Assertions.assertTrue(row3.isNullAt(6)) // col_float
    Assertions.assertTrue(row3.isNullAt(7)) // col_double
    Assertions.assertTrue(row3.isNullAt(8)) // col_decimal
    Assertions.assertTrue(row3.isNullAt(9)) // col_varchar
    Assertions.assertTrue(row3.isNullAt(12)) // col_date
    Assertions.assertTrue(row3.isNullAt(13)) // col_ts

    session.stop()
  }

  @Test
  def testAllTypesWrite(): Unit = {
    val session = createObkvCatalogSession()
    session.sql("use ob;")

    // Write all types via OBKV
    session.sql(
      s"""
         |INSERT INTO $getSchemaName.obkv_type_write_test VALUES
         |(1, true, 100, 10000, 1000000, 10000000000, 1.5, 2.5, 123.4567, 'test varchar', 'test char', '2024-03-24', '2024-03-24 10:30:00', UNHEX('010203')),
         |(2, false, -50, -5000, -500000, -5000000000, -1.5, -2.5, -999.9999, 'negative', 'neg', '2020-01-01', '2020-01-01 00:00:00', NULL),
         |(3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
         |""".stripMargin)

    session.stop()

    // Verify via JDBC
    waitingAndAssertTableCount("obkv_type_write_test", 3)

    val conn = getJdbcConnection()
    try {
      val stmt = conn.createStatement()
      try {
        val rs = stmt.executeQuery(s"SELECT * FROM $getSchemaName.obkv_type_write_test ORDER BY id")
        Assertions.assertTrue(rs.next())

        // Verify row 1
        Assertions.assertEquals(1, rs.getInt("id"))
        Assertions.assertEquals(true, rs.getBoolean("col_bool"))
        Assertions.assertEquals(100, rs.getByte("col_tinyint"))
        Assertions.assertEquals(10000, rs.getShort("col_smallint"))
        Assertions.assertEquals(1000000, rs.getInt("col_int"))
        Assertions.assertEquals(10000000000L, rs.getLong("col_bigint"))
        Assertions.assertEquals(1.5f, rs.getFloat("col_float"), 0.001f)
        Assertions.assertEquals(2.5, rs.getDouble("col_double"), 0.001)
        Assertions.assertEquals("test varchar", rs.getString("col_varchar"))
        Assertions.assertEquals("2024-03-24", rs.getDate("col_date").toString)

        Assertions.assertTrue(rs.next())
        // Verify row 2
        Assertions.assertEquals(2, rs.getInt("id"))
        Assertions.assertEquals(false, rs.getBoolean("col_bool"))
        Assertions.assertEquals(-50, rs.getByte("col_tinyint"))
        Assertions.assertEquals(-5000, rs.getShort("col_smallint"))

        Assertions.assertTrue(rs.next())
        // Verify row 3 (nulls)
        Assertions.assertEquals(3, rs.getInt("id"))
        Assertions.assertTrue(rs.wasNull() || rs.getObject("col_bool") == null)
      } finally {
        stmt.close()
      }
    } finally {
      conn.close()
    }
  }

  @Test
  def testBoundaryValues(): Unit = {
    // Note: OBKV has strict type checking. Spark SQL parses numbers as INT by default,
    // which causes type mismatch for TINYINT and SMALLINT columns.
    // So we only test INT and BIGINT boundary values here.
    val session = createObkvCatalogSession()
    session.sql("use ob;")

    // Write boundary values for INT and BIGINT only
    session.sql(
      s"""
         |INSERT INTO $getSchemaName.obkv_boundary_test (id, min_int, max_int, min_bigint, max_bigint) VALUES
         |(1, -2147483648, 2147483647, -9223372036854775808, 9223372036854775807),
         |(2, 0, 0, 0, 0);
         |""".stripMargin)

    session.stop()

    // Verify via JDBC
    waitingAndAssertTableCount("obkv_boundary_test", 2)

    val conn = getJdbcConnection()
    try {
      val stmt = conn.createStatement()
      try {
        val rs = stmt.executeQuery(s"SELECT * FROM $getSchemaName.obkv_boundary_test ORDER BY id")
        Assertions.assertTrue(rs.next())

        // Verify boundary values for INT and BIGINT
        Assertions.assertEquals(-2147483648, rs.getInt("min_int"))
        Assertions.assertEquals(2147483647, rs.getInt("max_int"))
        Assertions.assertEquals(Long.MinValue, rs.getLong("min_bigint"))
        Assertions.assertEquals(Long.MaxValue, rs.getLong("max_bigint"))
      } finally {
        stmt.close()
      }
    } finally {
      conn.close()
    }
  }

  @Test
  def testNullHandlingWriteAndRead(): Unit = {
    val session = createObkvCatalogSession()
    session.sql("use ob;")

    // Write with null values
    session.sql(s"""
                   |INSERT INTO $getSchemaName.obkv_null_test VALUES
                   |(1, 'has value', 10.5, '2024-03-24 10:00:00'),
                   |(2, NULL, NULL, NULL),
                   |(3, 'only name', NULL, NULL),
                   |(4, NULL, 20.5, NULL);
                   |""".stripMargin)

    // Read back and verify nulls
    val result = session.sql(s"SELECT * FROM $getSchemaName.obkv_null_test ORDER BY id").collect()
    Assertions.assertEquals(4, result.length)

    // Row 1: all values present
    Assertions.assertEquals(1, result(0).getInt(0))
    Assertions.assertEquals("has value", result(0).getString(1))
    Assertions.assertEquals(10.5, result(0).getDouble(2), 0.001)
    Assertions.assertFalse(result(0).isNullAt(3))

    // Row 2: all nulls
    Assertions.assertEquals(2, result(1).getInt(0))
    Assertions.assertTrue(result(1).isNullAt(1))
    Assertions.assertTrue(result(1).isNullAt(2))
    Assertions.assertTrue(result(1).isNullAt(3))

    // Row 3: partial nulls
    Assertions.assertEquals(3, result(2).getInt(0))
    Assertions.assertEquals("only name", result(2).getString(1))
    Assertions.assertTrue(result(2).isNullAt(2))
    Assertions.assertTrue(result(2).isNullAt(3))

    // Row 4: partial nulls
    Assertions.assertEquals(4, result(3).getInt(0))
    Assertions.assertTrue(result(3).isNullAt(1))
    Assertions.assertEquals(20.5, result(3).getDouble(2), 0.001)
    Assertions.assertTrue(result(3).isNullAt(3))

    session.stop()
  }

  @Test
  def testDecimalPrecision(): Unit = {
    val conn = getJdbcConnection()
    try {
      val stmt = conn.createStatement()
      try {
        // Insert various decimal values
        stmt.executeUpdate(s"""
                              |INSERT INTO $getSchemaName.obkv_all_types (pk_id, col_decimal) VALUES
                              |(10, 0.0),
                              |(11, 1.0),
                              |(12, 12345.67890),
                              |(13, 99999999999.99999),
                              |(14, -99999999999.99999),
                              |(15, 0.00001);
                              |""".stripMargin)
      } finally {
        stmt.close()
      }
    } finally {
      conn.close()
    }

    val session = createObkvCatalogSession()
    session.sql("use ob;")

    val result = session
      .sql(
        s"SELECT pk_id, col_decimal FROM $getSchemaName.obkv_all_types WHERE pk_id >= 10 ORDER BY pk_id")
      .collect()

    Assertions.assertEquals(6, result.length)

    // Note: Due to OBKV limitation, decimal is converted to double
    // So we check approximate values
    Assertions.assertEquals(0.0, result(0).getDouble(1), 0.001)
    Assertions.assertEquals(1.0, result(1).getDouble(1), 0.001)
    Assertions.assertEquals(12345.67890, result(2).getDouble(1), 0.001)
    Assertions.assertEquals(99999999999.99999, result(3).getDouble(1), 1.0) // larger tolerance
    Assertions.assertEquals(-99999999999.99999, result(4).getDouble(1), 1.0)
    Assertions.assertEquals(0.00001, result(5).getDouble(1), 0.000001)

    session.stop()
  }

  @Test
  def testLargeBatchWrite(): Unit = {
    val session = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.sql.catalog.ob", OB_CATALOG_CLASS)
      .config("spark.sql.catalog.ob.url", getJdbcUrl)
      .config("spark.sql.catalog.ob.username", getUsername)
      .config("spark.sql.catalog.ob.password", getPassword)
      .config("spark.sql.catalog.ob.schema-name", getSchemaName)
      .config("spark.sql.catalog.ob.obkv.enabled", "true")
      .config("spark.sql.catalog.ob.obkv.odp-mode", "true")
      .config("spark.sql.catalog.ob.obkv.odp-addr", OceanBaseMySQLTestBase.ODP.getHost)
      .config("spark.sql.catalog.ob.obkv.odp-port", OceanBaseMySQLTestBase.ODP.getRpcPort.toString)
      .config("spark.sql.catalog.ob.obkv.full-user-name", s"$getUsername#$getClusterName")
      .config("spark.sql.catalog.ob.obkv.password", getPassword)
      .config("spark.sql.catalog.ob.obkv.batch-size", "10")
      .getOrCreate()

    session.sql("use ob;")

    // Write 100 rows with batch size 10 to test multiple flushes
    val values = (1 to 100).map(i => s"($i, 'item_$i', 'desc_$i', ${i * 1.1})").mkString(",\n")
    session.sql(s"""
                   |INSERT INTO $getSchemaName.obkv_products VALUES
                   |$values;
                   |""".stripMargin)

    session.stop()

    // Verify all 100 rows were written
    waitingAndAssertTableCount("obkv_products", 100)
  }

  @Test
  def testReadWriteRoundTrip(): Unit = {
    val session = createObkvCatalogSession()
    session.sql("use ob;")

    // Write data via OBKV
    session.sql(s"""
                   |INSERT INTO $getSchemaName.obkv_products VALUES
                   |(1001, 'product_a', 'desc_a', 10.5),
                   |(1002, 'product_b', 'desc_b', 20.5),
                   |(1003, 'product_c', 'desc_c', 30.5);
                   |""".stripMargin)

    // Read back via OBKV
    val readResult = session
      .sql(s"SELECT * FROM $getSchemaName.obkv_products ORDER BY id")
      .collect()

    Assertions.assertEquals(3, readResult.length)
    Assertions.assertEquals(1001, readResult(0).getInt(0))
    Assertions.assertEquals("product_a", readResult(0).getString(1))
    Assertions.assertEquals(10.5, readResult(0).getDouble(3), 0.001)

    // Update via insertOrUpdate (same primary key)
    session.sql(s"""
                   |INSERT INTO $getSchemaName.obkv_products VALUES
                   |(1001, 'product_a_v2', 'updated', 99.9);
                   |""".stripMargin)

    // Verify update
    val updatedResult = session
      .sql(s"SELECT * FROM $getSchemaName.obkv_products WHERE id = 1001")
      .collect()

    Assertions.assertEquals(1, updatedResult.length)
    Assertions.assertEquals("product_a_v2", updatedResult(0).getString(1))
    Assertions.assertEquals(99.9, updatedResult(0).getDouble(3), 0.001)

    session.stop()
  }

  @Test
  def testFilterPushdownWithAllTypes(): Unit = {
    val conn = getJdbcConnection()
    try {
      val stmt = conn.createStatement()
      try {
        stmt.executeUpdate(
          s"""
             |INSERT INTO $getSchemaName.obkv_all_types (pk_id, col_int, col_double, col_varchar) VALUES
             |(100, 10, 10.5, 'alpha'),
             |(101, 20, 20.5, 'beta'),
             |(102, 30, 30.5, 'gamma'),
             |(103, 40, 40.5, 'delta');
             |""".stripMargin)
      } finally {
        stmt.close()
      }
    } finally {
      conn.close()
    }

    val session = createObkvCatalogSession()
    session.sql("use ob;")

    // Test various filter pushdowns
    // EqualTo
    val eqResult =
      session.sql(s"SELECT * FROM $getSchemaName.obkv_all_types WHERE pk_id = 101").collect()
    Assertions.assertEquals(1, eqResult.length)
    Assertions.assertEquals(101, eqResult(0).getInt(0))

    // GreaterThan
    val gtResult =
      session.sql(s"SELECT * FROM $getSchemaName.obkv_all_types WHERE col_int > 20").collect()
    Assertions.assertEquals(2, gtResult.length)

    // LessThan
    val ltResult =
      session.sql(s"SELECT * FROM $getSchemaName.obkv_all_types WHERE col_int < 30").collect()
    Assertions.assertEquals(2, ltResult.length)

    // GreaterThanOrEqual
    val gteResult =
      session.sql(s"SELECT * FROM $getSchemaName.obkv_all_types WHERE col_int >= 30").collect()
    Assertions.assertEquals(2, gteResult.length)

    // LessThanOrEqual
    val lteResult =
      session.sql(s"SELECT * FROM $getSchemaName.obkv_all_types WHERE col_int <= 30").collect()
    Assertions.assertEquals(3, lteResult.length)

    // And
    val andResult = session
      .sql(s"SELECT * FROM $getSchemaName.obkv_all_types WHERE col_int >= 20 AND col_int < 40")
      .collect()
    Assertions.assertEquals(2, andResult)

    // Or
    val orResult = session
      .sql(s"SELECT * FROM $getSchemaName.obkv_all_types WHERE pk_id = 100 OR pk_id = 103")
      .collect()
    Assertions.assertEquals(2, orResult.length)

    session.stop()
  }

}

object OBKVConnectorITCase {
  @BeforeAll
  def setup(): Unit = {
    OceanBaseMySQLTestBase.CONFIG_SERVER.start()
    val configServerAddress = getConfigServerAddress(OceanBaseMySQLTestBase.CONFIG_SERVER)
    val configUrlForODP = constructConfigUrlForODP(configServerAddress)
    OceanBaseMySQLTestBase.CONTAINER.withEnv("OB_CONFIGSERVER_ADDRESS", configServerAddress).start()
    val password = "test"
    createSysUser("proxyro", password)
    OceanBaseMySQLTestBase.ODP.withPassword(password).withConfigUrl(configUrlForODP).start()
  }

  @AfterAll
  def tearDown(): Unit = {
    List(
      OceanBaseMySQLTestBase.CONFIG_SERVER,
      OceanBaseMySQLTestBase.CONTAINER,
      OceanBaseMySQLTestBase.ODP)
      .foreach(_.stop())
  }
}

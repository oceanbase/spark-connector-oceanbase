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
    dropTables("obkv_products", "obkv_composite_pk", "obkv_all_types", "obkv_partitioned")
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

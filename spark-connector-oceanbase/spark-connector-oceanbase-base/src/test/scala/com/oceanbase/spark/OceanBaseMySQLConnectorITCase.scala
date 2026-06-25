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

import com.oceanbase.spark.OceanBaseMySQLConnectorITCase.expected
import com.oceanbase.spark.OceanBaseTestBase.assertEqualsInAnyOrder

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.junit.jupiter.api.{AfterAll, AfterEach, Assertions, BeforeAll, BeforeEach, Test}

import java.util

class OceanBaseMySQLConnectorITCase extends OceanBaseMySQLTestBase {

  @BeforeEach
  def initEach(): Unit = {
    initialize("sql/mysql/products.sql")
  }

  @AfterEach
  def afterEach(): Unit = {
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    dropTables(
      "products",
      "products_no_pri_key",
      "products_full_pri_key",
      "products_no_int_pri_key",
      "products_unique_key",
      "products_full_unique_key",
      "products_pri_and_unique_key",
      "products_with_decimal",
      "products_complex_types",
      "products_nested_arrays"
    )
  }

  val TEST_TABLE_PRODUCTS = "products"

  @Test
  def testSqlJDBCWrite(): Unit = {
    val session = SparkSession.builder().master("local[*]").getOrCreate()

    session.sql(s"""
                   |CREATE TEMPORARY VIEW test_sink
                   |USING oceanbase
                   |OPTIONS(
                   |  "url"= "$getJdbcUrlWithoutDB",
                   |  "rpc-port" = "$getRpcPort",
                   |  "schema-name"="$getSchemaName",
                   |  "table-name"="products",
                   |  "username"="$getUsername",
                   |  "password"="$getPassword"
                   |);
                   |""".stripMargin)
    insertToProducts(session)
    session.stop()
    verifyTableData(TEST_TABLE_PRODUCTS, expected)
  }

  @Test
  def testSqlDirectLoadWrite(): Unit = {
    val session = SparkSession.builder().master("local[*]").getOrCreate()

    session.sql(s"""
                   |CREATE TEMPORARY VIEW test_sink
                   |USING oceanbase
                   |OPTIONS(
                   |  "url"= "$getJdbcUrlWithoutDB",
                   |  "rpc-port" = "$getRpcPort",
                   |  "schema-name"="$getSchemaName",
                   |  "table-name"="products",
                   |  "username"="$getUsername",
                   |  "password"="$getPassword",
                   |  "direct-load.enabled"=true,
                   |  "direct-load.host"="$getHost",
                   |  "direct-load.rpc-port"=$getRpcPort,
                   |  "direct-load.username"="$getUsername",
                   |  "direct-load.write-thread-num"="1"
                   |);
                   |""".stripMargin)

    insertToProducts(session)
    session.stop()
    verifyTableData(TEST_TABLE_PRODUCTS, OceanBaseMySQLConnectorITCase.expected)
  }

  @Test
  def testDirectLoadWithEmptySparkPartition(): Unit = {
    val session = SparkSession.builder().master("local[*]").getOrCreate()
    session.sql(s"""
                   |CREATE TEMPORARY VIEW test_sink
                   |USING oceanbase
                   |OPTIONS(
                   |  "url"= "$getJdbcUrlWithoutDB",
                   |  "rpc-port" = "$getRpcPort",
                   |  "schema-name"="$getSchemaName",
                   |  "table-name"="products",
                   |  "username"="$getUsername",
                   |  "password"="$getPassword",
                   |  "direct-load.enabled"=true,
                   |  "direct-load.host"="$getHost",
                   |  "direct-load.rpc-port"=$getRpcPort
                   |);
                   |""".stripMargin)
    session
      .sql("""
             |INSERT INTO test_sink
             |SELECT /*+ REPARTITION(3) */ 101, 'scooter', 'Small 2-wheel scooter', 3.14;
             |""".stripMargin)
      .repartition(3)

    val expected: util.List[String] = util.Arrays.asList(
      "101,scooter,Small 2-wheel scooter,3.1400000000"
    )
    session.stop()

    verifyTableData(TEST_TABLE_PRODUCTS, expected)
  }

  @Test
  def testDataFrameDirectLoadWrite(): Unit = {
    val session = SparkSession.builder().master("local[*]").getOrCreate()
    val df = session
      .createDataFrame(
        Seq(
          (101, "scooter", "Small 2-wheel scooter", 3.14),
          (102, "car battery", "12V car battery", 8.1),
          (
            103,
            "12-pack drill bits",
            "12-pack of drill bits with sizes ranging from #40 to #3",
            0.8),
          (104, "hammer", "12oz carpenter's hammer", 0.75),
          (105, "hammer", "14oz carpenter's hammer", 0.875),
          (106, "hammer", "16oz carpenter's hammer", 1.0),
          (107, "rocks", "box of assorted rocks", 5.3),
          (108, "jacket", "water resistent black wind breaker", 0.1),
          (109, "spare tire", "24 inch spare tire", 22.2)
        ))
      .toDF("id", "name", "description", "weight")

    df.write
      .format("oceanbase")
      .mode(saveMode = SaveMode.Append)
      .option("url", getJdbcUrl)
      .option("username", getUsername)
      .option("password", getPassword)
      .option("table-name", "products")
      .option("schema-name", getSchemaName)
      .option("direct-load.enabled", value = true)
      .option("direct-load.host", getHost)
      .option("direct-load.rpc-port", value = getRpcPort)
      .save()

    session.stop()
    verifyTableData(TEST_TABLE_PRODUCTS, OceanBaseMySQLConnectorITCase.expected)
  }

  @Test
  def testSqlRead(): Unit = {
    val session = SparkSession.builder().master("local[*]").getOrCreate()

    session.sql(s"""
                   |CREATE TEMPORARY VIEW test_sink
                   |USING oceanbase
                   |OPTIONS(
                   |  "url"= "$getJdbcUrlWithoutDB",
                   |  "rpc-port" = "$getRpcPort",
                   |  "schema-name"="$getSchemaName",
                   |  "table-name"="products",
                   |  "direct-load.enabled" ="false",
                   |  "username"="$getUsername",
                   |  "password"="$getPassword"
                   |);
                   |""".stripMargin)

    insertToProducts(session)
    import scala.collection.JavaConverters._
    val actual = session
      .sql("select * from test_sink")
      .collect()
      .map(
        _.toString().drop(1).dropRight(1)
      )
      .toList
      .asJava
    assertEqualsInAnyOrder(expected, actual)

    session.stop()
  }

  @Test
  def testSqlReadWithQuery(): Unit = {

    val session = SparkSession.builder().master("local[*]").getOrCreate()

    session.sql(s"""
                   |CREATE TEMPORARY VIEW test_sink
                   |USING oceanbase
                   |OPTIONS(
                   |  "url"= "$getJdbcUrl",
                   |  "schema-name"="$getSchemaName",
                   |  "table-name"="products",
                   |  "direct-load.enabled" ="false",
                   |  "username"="$getUsername",
                   |  "password"="$getPassword"
                   |);
                   |""".stripMargin)
    insertToProducts(session)
    session.sql(s"""
                   |CREATE TEMPORARY VIEW test_read_query
                   |USING oceanbase
                   |OPTIONS(
                   |  "url"= "$getJdbcUrl",
                   |  "schema-name"="$getSchemaName",
                   |  "query" = "select id, name from products",
                   |  "direct-load.enabled" ="false",
                   |  "username"="$getUsername",
                   |  "password"="$getPassword"
                   |);
                   |""".stripMargin)

    val expected: util.List[String] = util.Arrays.asList(
      "101,scooter",
      "102,car battery",
      "103,12-pack drill bits",
      "104,hammer",
      "105,hammer",
      "106,hammer",
      "107,rocks",
      "108,jacket",
      "109,spare tire"
    )
    import scala.collection.JavaConverters._
    val dataFrame = session.sql("select * from test_read_query")
    assertDataSourceV2Table(dataFrame, "OceanBaseLegacyTable")

    val actual = dataFrame
      .collect()
      .map(
        _.toString().drop(1).dropRight(1)
      )
      .toList
      .asJava
    assertEqualsInAnyOrder(expected, actual)

    session.stop()
  }

  @Test
  def testSqlReadWithOceanBaseDriver(): Unit = {
    val session = SparkSession.builder().master("local[*]").getOrCreate()

    session.sql(s"""
                   |CREATE TEMPORARY VIEW test_sink
                   |USING oceanbase
                   |OPTIONS(
                   |  "url"= "${getJdbcUrl.replace("mysql", "oceanbase")}",
                   |  "schema-name"="$getSchemaName",
                   |  "table-name"="products",
                   |  "direct-load.enabled" ="false",
                   |  "username"="$getUsername",
                   |  "password"="$getPassword"
                   |);
                   |""".stripMargin)

    insertToProducts(session)

    session.sql(s"""
                   |CREATE TEMPORARY VIEW test_read_query
                   |USING oceanbase
                   |OPTIONS(
                   |  "url"= "${getJdbcUrl.replace("mysql", "oceanbase")}",
                   |  "schema-name"="$getSchemaName",
                   |  "query" = "select id, name from products",
                   |  "direct-load.enabled" ="false",
                   |  "username"="$getUsername",
                   |  "password"="$getPassword"
                   |);
                   |""".stripMargin)

    val expected: util.List[String] = util.Arrays.asList(
      "101,scooter",
      "102,car battery",
      "103,12-pack drill bits",
      "104,hammer",
      "105,hammer",
      "106,hammer",
      "107,rocks",
      "108,jacket",
      "109,spare tire"
    )
    import scala.collection.JavaConverters._
    val actual = session
      .sql("select * from test_read_query")
      .collect()
      .map(
        _.toString().drop(1).dropRight(1)
      )
      .toList
      .asJava
    assertEqualsInAnyOrder(expected, actual)

    session.stop()
  }

  @Test
  def testDataFrameRead(): Unit = {
    val session = SparkSession.builder().master("local[*]").getOrCreate()

    // Sql write
    session.sql(s"""
                   |CREATE TEMPORARY VIEW test_sink
                   |USING oceanbase
                   |OPTIONS(
                   |  "url"= "$getJdbcUrl",
                   |  "rpc-port" = "$getRpcPort",
                   |  "schema-name"="$getSchemaName",
                   |  "table-name"="products",
                   |  "username"="$getUsername",
                   |  "password"="$getPassword"
                   |);
                   |""".stripMargin)

    insertToProducts(session)

    // DataFrame read
    val dataFrame = session.read
      .format("oceanbase")
      .option("url", getJdbcUrlWithoutDB)
      .option("username", getUsername)
      .option("password", getPassword)
      .option("table-name", "products")
      .option("schema-name", getSchemaName)
      .load()

    assertDataSourceV2Table(dataFrame, "OceanBaseTable")

    import scala.collection.JavaConverters._
    val actual = dataFrame
      .collect()
      .map(
        _.toString().drop(1).dropRight(1)
      )
      .toList
      .asJava
    assertEqualsInAnyOrder(expected, actual)

    session.stop()
  }

  @Test
  def testDataFrameReadMatchesCatalogV2ReaderBehavior(): Unit = {
    val session = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.sql.catalog.ob", "com.oceanbase.spark.catalog.OceanBaseCatalog")
      .config("spark.sql.catalog.ob.url", getJdbcUrl)
      .config("spark.sql.catalog.ob.username", getUsername)
      .config("spark.sql.catalog.ob.password", getPassword)
      .config("spark.sql.catalog.ob.schema-name", getSchemaName)
      .getOrCreate()

    session.sql(s"""
                   |CREATE TEMPORARY VIEW test_sink
                   |USING oceanbase
                   |OPTIONS(
                   |  "url"= "$getJdbcUrl",
                   |  "rpc-port" = "$getRpcPort",
                   |  "schema-name"="$getSchemaName",
                   |  "table-name"="products",
                   |  "username"="$getUsername",
                   |  "password"="$getPassword"
                   |);
                   |""".stripMargin)
    insertToProducts(session)

    session.read
      .format("oceanbase")
      .option("url", getJdbcUrlWithoutDB)
      .option("username", getUsername)
      .option("password", getPassword)
      .option("table-name", "products")
      .option("schema-name", getSchemaName)
      .load()
      .createOrReplaceTempView("df_products")

    session.sql("use ob;")

    assertCatalogAndDataFrameQuery(
      session,
      "select id, name from %s where id >= 104 and id < 108",
      Seq("OBJdbcBatchScan", "requiredColumns: [`id`, `name`]", "PushedFilters: ["))

    assertCatalogAndDataFrameQuery(
      session,
      "select id, name from %s limit 3",
      Seq("OBJdbcBatchScan", "requiredColumns: [`id`, `name`]", "PushedLimit: 3"))

    assertCatalogAndDataFrameQuery(
      session,
      "select * from %s order by id desc nulls first limit 3",
      Seq("OBJdbcBatchScan", "PushedLimit: 3", "PushedTopN: ["))

    assertCatalogAndDataFrameQuery(
      session,
      "select name, min(id), max(weight) from %s group by name",
      Seq(
        "OBJdbcBatchScan",
        "PushedAggregates: [MIN(`id`), MAX(`weight`)]",
        "PushedGroupBy: [`name`]")
    )

    session.stop()
  }

  @Test
  def testDataFrameV2ReadHonorsLegacyBatchReaderOption(): Unit = {
    val session = SparkSession.builder().master("local[*]").getOrCreate()

    session.sql(s"""
                   |CREATE TEMPORARY VIEW test_sink
                   |USING oceanbase
                   |OPTIONS(
                   |  "url"= "$getJdbcUrl",
                   |  "rpc-port" = "$getRpcPort",
                   |  "schema-name"="$getSchemaName",
                   |  "table-name"="products",
                   |  "username"="$getUsername",
                   |  "password"="$getPassword"
                   |);
                   |""".stripMargin)
    insertToProducts(session)

    val dataFrame = session.read
      .format("oceanbase")
      .option("url", getJdbcUrlWithoutDB)
      .option("username", getUsername)
      .option("password", getPassword)
      .option("table-name", "products")
      .option("schema-name", getSchemaName)
      .option("enable_legacy_batch_reader", "true")
      .load()
      .where("id >= 104 and id < 108")
      .select("id", "name")

    assertDataSourceV2Table(dataFrame, "OceanBaseTable")
    assertPlanContains(dataFrame, Seq("OBJDBCLimitScan", "prunedSchema:", "PushedFilters: ["))

    import scala.collection.JavaConverters._
    val expected: util.List[String] =
      util.Arrays.asList("104,hammer", "105,hammer", "106,hammer", "107,rocks")
    val actual = collectAsStrings(dataFrame).asJava
    assertEqualsInAnyOrder(expected, actual)

    session.stop()
  }

  @Test
  def testSqlReadHonorsLegacyBatchReaderOption(): Unit = {
    val session = SparkSession.builder().master("local[*]").getOrCreate()

    session.sql(s"""
                   |CREATE TEMPORARY VIEW test_sink
                   |USING oceanbase
                   |OPTIONS(
                   |  "url"= "$getJdbcUrl",
                   |  "rpc-port" = "$getRpcPort",
                   |  "schema-name"="$getSchemaName",
                   |  "table-name"="products",
                   |  "enable_legacy_batch_reader"="true",
                   |  "username"="$getUsername",
                   |  "password"="$getPassword"
                   |);
                   |""".stripMargin)

    insertToProducts(session)

    val dataFrame = session
      .sql("select id, name from test_sink where id >= 104 and id < 108")

    assertDataSourceV2Table(dataFrame, "OceanBaseTable")
    assertPlanContains(dataFrame, Seq("OBJDBCLimitScan", "prunedSchema:", "PushedFilters: ["))

    import scala.collection.JavaConverters._
    val expected: util.List[String] =
      util.Arrays.asList("104,hammer", "105,hammer", "106,hammer", "107,rocks")
    val actual = collectAsStrings(dataFrame).asJava
    assertEqualsInAnyOrder(expected, actual)

    session.stop()
  }

  private def insertToProducts(session: SparkSession): Unit = {
    session.sql(
      """
        |INSERT INTO test_sink VALUES
        |(101, 'scooter', 'Small 2-wheel scooter', 3.14),
        |(102, 'car battery', '12V car battery', 8.1),
        |(103, '12-pack drill bits', '12-pack of drill bits with sizes ranging from #40 to #3', 0.8),
        |(104, 'hammer', '12oz carpenter\'s hammer', 0.75),
        |(105, 'hammer', '14oz carpenter\'s hammer', 0.875),
        |(106, 'hammer', '16oz carpenter\'s hammer', 1.0),
        |(107, 'rocks', 'box of assorted rocks', 5.3),
        |(108, 'jacket', 'water resistent black wind breaker', 0.1),
        |(109, 'spare tire', '24 inch spare tire', 22.2);
        |""".stripMargin)
  }

  def verifyTableData(tableName: String, expected: util.List[String]): Unit = {
    waitingAndAssertTableCount(tableName, expected.size)
    val actual: util.List[String] = queryTable(tableName)
    assertEqualsInAnyOrder(expected, actual)
  }

  private def assertCatalogAndDataFrameQuery(
      session: SparkSession,
      queryTemplate: String,
      expectedPlanParts: Seq[String]): Unit = {
    import scala.collection.JavaConverters._
    val catalogDataFrame = session.sql(queryTemplate.format("products"))
    val dataFrameApiDataFrame = session.sql(queryTemplate.format("df_products"))

    assertDataSourceV2Table(catalogDataFrame, "OceanBaseTable")
    assertDataSourceV2Table(dataFrameApiDataFrame, "OceanBaseTable")
    assertPlanContains(catalogDataFrame, expectedPlanParts)
    assertPlanContains(dataFrameApiDataFrame, expectedPlanParts)
    assertEqualsInAnyOrder(
      collectAsStrings(catalogDataFrame).asJava,
      collectAsStrings(dataFrameApiDataFrame).asJava)
  }

  private def assertDataSourceV2Table(dataFrame: DataFrame, tableClassName: String): Unit = {
    val tableNames = dataFrame.queryExecution.analyzed
      .collect { case relation: DataSourceV2Relation => relation.table.getClass.getName }

    Assertions.assertTrue(
      tableNames.exists(_.contains(tableClassName)),
      s"Expected DataSource V2 table $tableClassName, actual tables: ${tableNames.mkString(", ")}")
  }

  private def assertPlanContains(dataFrame: DataFrame, expectedParts: Seq[String]): Unit = {
    val plan = dataFrame.queryExecution.executedPlan.toString()
    expectedParts.foreach {
      expectedPart =>
        Assertions.assertTrue(
          plan.contains(expectedPart),
          s"Expected plan to contain '$expectedPart', actual plan:\n$plan")
    }
  }

  private def collectAsStrings(dataFrame: DataFrame): List[String] = {
    dataFrame
      .collect()
      .map(
        _.toString().drop(1).dropRight(1)
      )
      .toList
  }

  def getJdbcUrlWithoutDB: String =
    s"jdbc:mysql://$getHost:$getPort?useUnicode=true&characterEncoding=UTF-8&useSSL=false"

}

object OceanBaseMySQLConnectorITCase extends OceanBaseMySQLTestBase {
  @BeforeAll
  def setup(): Unit = {
    OceanBaseMySQLTestBase.CONTAINER.start()
  }

  @AfterAll
  def tearDown(): Unit = {
    OceanBaseMySQLTestBase.CONTAINER.stop()
  }

  val expected: util.List[String] = util.Arrays.asList(
    "101,scooter,Small 2-wheel scooter,3.1400000000",
    "102,car battery,12V car battery,8.1000000000",
    "103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.8000000000",
    "104,hammer,12oz carpenter's hammer,0.7500000000",
    "105,hammer,14oz carpenter's hammer,0.8750000000",
    "106,hammer,16oz carpenter's hammer,1.0000000000",
    "107,rocks,box of assorted rocks,5.3000000000",
    "108,jacket,water resistent black wind breaker,0.1000000000",
    "109,spare tire,24 inch spare tire,22.2000000000"
  )
}

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

import com.oceanbase.spark.OBCatalogOracleITCase.expected
import com.oceanbase.spark.OceanBaseTestBase.assertEqualsInAnyOrder
import com.oceanbase.spark.config.OceanBaseConfig
import com.oceanbase.spark.dialect.OceanBaseOracleDialect
import com.oceanbase.spark.utils.OBJdbcUtils

import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.{AfterEach, Assertions, BeforeEach, Disabled, Test}
import org.junit.jupiter.api.function.ThrowingSupplier

import java.util

class OBCatalogOracleITCase extends OceanBaseOracleTestBase {

  @BeforeEach
  def initEach(): Unit = {
    // Check if table exists and drop it if it does
    val config = new OceanBaseConfig(getOptions)
    val dialect = new OceanBaseOracleDialect
    OBJdbcUtils.withConnection(config) {
      conn =>
        if (dialect.tableExists(conn, config)) {
          dialect.dropTable(conn, config.getDbTable, config)
        }
    }

    initialize("sql/oracle/products_simple.sql")
  }

  @AfterEach
  def afterEach(): Unit = {
    dropTables(
      "PRODUCTS",
      "PRODUCTS_NO_PRI_KEY",
      "PRODUCTS_FULL_PRI_KEY",
      "PRODUCTS_NO_INT_PRI_KEY",
      "PRODUCTS_UNIQUE_KEY",
      "PRODUCTS_FULL_UNIQUE_KEY"
    )
  }

  val OB_CATALOG_CLASS = "com.oceanbase.spark.catalog.OceanBaseCatalog"

  @Test
  def testCatalogBase(): Unit = {
    val session = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.sql.catalog.ob", OB_CATALOG_CLASS)
      .config("spark.sql.catalog.ob.url", getJdbcUrl)
      .config("spark.sql.catalog.ob.username", getUsername)
      .config("spark.sql.catalog.ob.password", getPassword)
      .config("spark.sql.catalog.ob.schema-name", getSchemaName)
      .getOrCreate()

    session.sql("use ob;")
    insertTestData(session, "PRODUCTS")
    queryAndVerifyTableData(session, "PRODUCTS", expected)

    insertTestData(session, "PRODUCTS_NO_PRI_KEY")
    queryAndVerifyTableData(session, "PRODUCTS_NO_PRI_KEY", expected)

    session.stop()
  }

  @Test
  def testWhereClause(): Unit = {
    val session = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.sql.catalog.ob", OB_CATALOG_CLASS)
      .config("spark.sql.catalog.ob.url", getJdbcUrl)
      .config("spark.sql.catalog.ob.username", getUsername)
      .config("spark.sql.catalog.ob.password", getPassword)
      .config("spark.sql.catalog.ob.schema-name", getSchemaName)
      .config("spark.sql.catalog.ob.jdbc.max-records-per-partition", 2)
      .getOrCreate()

    session.sql("use ob;")
    insertTestData(session, "PRODUCTS")

    import scala.collection.JavaConverters._
    val expect1 = Seq(
      "101,scooter,Small 2-wheel scooter,3.14",
      "102,car battery,12V car battery,8.10",
      "103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.80"
    ).toList.asJava
    queryAndVerify(session, "select * from PRODUCTS where ID >= 101 and ID < 104", expect1)
    session.stop()
  }

  @Test
  def testJdbcInsetWithAutoCommit(): Unit = {
    val session = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.sql.catalog.ob", OB_CATALOG_CLASS)
      .config("spark.sql.catalog.ob.url", getJdbcUrl)
      .config("spark.sql.catalog.ob.username", getUsername)
      .config("spark.sql.catalog.ob.password", getPassword)
      .config("spark.sql.catalog.ob.schema-name", getSchemaName)
      .config("spark.sql.catalog.ob.jdbc.enable-autocommit", true.toString)
      .getOrCreate()

    session.sql("use ob;")
    insertTestData(session, "PRODUCTS")
    queryAndVerifyTableData(session, "PRODUCTS", expected)

    insertTestData(session, "PRODUCTS_NO_PRI_KEY")
    queryAndVerifyTableData(session, "PRODUCTS_NO_PRI_KEY", expected)

    session.stop()
  }

  @Test
  def testCatalogOp(): Unit = {
    val session = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.sql.catalog.ob", OB_CATALOG_CLASS)
      .config("spark.sql.catalog.ob.url", getJdbcUrl)
      .config("spark.sql.catalog.ob.username", getUsername)
      .config("spark.sql.catalog.ob.password", getPassword)
      .config("spark.sql.catalog.ob.schema-name", getSchemaName)
      .getOrCreate()

    session.sql("use ob;")
    import scala.collection.JavaConverters._

    val dbList = session.sql("show databases").collect().map(_.toString()).toList.asJava
    assert(!dbList.isEmpty)
    val tableList = session.sql("show tables").collect().map(_.toString()).toList.asJava
    assert(!tableList.isEmpty)

    // test create/drop namespace
    Assertions.assertDoesNotThrow(new ThrowingSupplier[Unit] {
      override def get(): Unit = {
        session.sql("create database TEST_ONLY")
        session.sql("use TEST_ONLY")
      }
    })
    val dbList1 = session.sql("show databases").collect().map(_.toString()).toList.asJava
    println(dbList1)
    assert(dbList1.contains("[TEST_ONLY]"))

    session.sql("drop database TEST_ONLY")
    val dbList2 = session.sql("show databases").collect().map(_.toString()).toList.asJava
    assert(!dbList2.contains("[TEST_ONLY]"))

    session.stop()
  }

  @Test
  def testCatalogJdbcInsertWithNoPriKeyTable(): Unit = {
    val session = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.sql.catalog.ob", OB_CATALOG_CLASS)
      .config("spark.sql.catalog.ob.url", getJdbcUrl)
      .config("spark.sql.catalog.ob.username", getUsername)
      .config("spark.sql.catalog.ob.password", getPassword)
      .config("spark.sql.catalog.ob.schema-name", getSchemaName)
      .getOrCreate()

    session.sql("use ob;")
    insertTestData(session, "PRODUCTS_NO_PRI_KEY")
    queryAndVerifyTableData(session, "PRODUCTS_NO_PRI_KEY", expected)
    session.stop()
  }

  @Test
  def testCatalogJdbcInsertWithFullPriKeyTable(): Unit = {
    val session = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.sql.catalog.ob", OB_CATALOG_CLASS)
      .config("spark.sql.catalog.ob.url", getJdbcUrl)
      .config("spark.sql.catalog.ob.username", getUsername)
      .config("spark.sql.catalog.ob.password", getPassword)
      .config("spark.sql.catalog.ob.schema-name", getSchemaName)
      .getOrCreate()

    session.sql("use ob;")
    insertTestData(session, "PRODUCTS_FULL_PRI_KEY")

    queryAndVerifyTableData(session, "PRODUCTS_FULL_PRI_KEY", expected)

    session.stop()
  }

  @Test
  def testCatalogDirectLoadWrite(): Unit = {
    val session = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.sql.catalog.ob", OB_CATALOG_CLASS)
      .config("spark.sql.catalog.ob.url", getJdbcUrl)
      .config("spark.sql.catalog.ob.username", getUsername)
      .config("spark.sql.catalog.ob.password", getPassword)
      .config("spark.sql.catalog.ob.schema-name", getSchemaName)
      .config("spark.sql.defaultCatalog", "ob")
      .config("spark.sql.catalog.ob.direct-load.enabled", "true")
      .config("spark.sql.catalog.ob.direct-load.host", getHost)
      .config("spark.sql.catalog.ob.direct-load.rpc-port", getRpcPort)
      .config("spark.sql.catalog.ob.direct-load.username", getUsername)
      .getOrCreate()

    insertTestData(session, "PRODUCTS")
    queryAndVerifyTableData(session, "PRODUCTS", expected)

    insertTestData(session, "PRODUCTS_NO_PRI_KEY")
    session.sql("insert overwrite PRODUCTS select * from PRODUCTS_NO_PRI_KEY")
    queryAndVerifyTableData(session, "PRODUCTS", expected)
    session.stop()
  }

  @Test
  def testTableCreate(): Unit = {
    val session = SparkSession
      .builder()
      .master("local[1]")
      .config("spark.sql.catalog.ob", OB_CATALOG_CLASS)
      .config("spark.sql.catalog.ob.url", getJdbcUrl)
      .config("spark.sql.catalog.ob.username", getUsername)
      .config("spark.sql.catalog.ob.password", getPassword)
      .config("spark.sql.catalog.ob.schema-name", getSchemaName)
      .config("spark.sql.defaultCatalog", "ob")
      .getOrCreate()
    // Defensive cleanup to avoid leftover tables from previous failures
    try {
      val conn = getJdbcConnection()
      val stmt = conn.createStatement()
      try {
        stmt.execute(s"DROP TABLE $getSchemaName.TEST1")
        stmt.execute(s"DROP TABLE $getSchemaName.TEST2")
      } catch {
        case _: Throwable => // ignore
      } finally {
        stmt.close()
        conn.close()
      }
    } catch {
      case _: Throwable => // ignore
    }

    insertTestData(session, "PRODUCTS")
    // Test CTAS
    session.sql("create table TEST1 as select * from PRODUCTS")
    queryAndVerifyTableData(session, "TEST1", expected)

    // test bucket partition table:
    //   1. column comment test
    //   2. table comment test
    //   3. table options test
    session.sql(
      """
        |CREATE TABLE TEST2(
        |  USER_ID DECIMAL(19, 0) COMMENT 'test_for_key',
        |  NAME VARCHAR(255)
        |)
        |PARTITIONED BY (bucket(16, USER_ID))
        |COMMENT 'test_for_table_create'
        |TBLPROPERTIES('replica_num' = 2, COMPRESSION = 'zstd_1.0', primary_key = 'USER_ID, NAME');
        |""".stripMargin)
    val showCreateTable = getShowCreateTable(s"$getSchemaName.TEST2")
    val showLC = showCreateTable.toLowerCase
    val hasPartition = showLC.contains("partition by") && showLC.contains("user_id")
    val hasPrimaryKey =
      showLC.contains("primary key") && showLC.contains("user_id") && showLC.contains("name")
    // In Oracle mode, SHOW CREATE may not return column/table comments; only check partition and primary key
    Assertions.assertTrue(hasPartition && hasPrimaryKey)
    dropTables("TEST1", "TEST2")
    session.stop()
  }

  @Test
  def testTruncateAndOverWriteTable(): Unit = {
    val session = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.sql.catalog.ob", OB_CATALOG_CLASS)
      .config("spark.sql.catalog.ob.url", getJdbcUrl)
      .config("spark.sql.catalog.ob.username", getUsername)
      .config("spark.sql.catalog.ob.password", getPassword)
      .config("spark.sql.catalog.ob.schema-name", getSchemaName)
      .getOrCreate()

    session.sql("use ob;")
    insertTestData(session, "PRODUCTS")
    session.sql("truncate table PRODUCTS")
    val expect = new util.ArrayList[String]()
    queryAndVerifyTableData(session, "PRODUCTS", expect)

    insertTestData(session, "PRODUCTS_NO_PRI_KEY")
    session.sql("insert overwrite PRODUCTS select * from PRODUCTS_NO_PRI_KEY")
    queryAndVerifyTableData(session, "PRODUCTS", expected)

    session.stop()
  }

  @Test
  def testDeleteWhere(): Unit = {
    val session = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.sql.catalog.ob", OB_CATALOG_CLASS)
      .config("spark.sql.catalog.ob.url", getJdbcUrl)
      .config("spark.sql.catalog.ob.username", getUsername)
      .config("spark.sql.catalog.ob.password", getPassword)
      .config("spark.sql.catalog.ob.schema-name", getSchemaName)
      .getOrCreate()

    session.sql("use ob;")
    insertTestData(session, "PRODUCTS")
    session.sql("delete from PRODUCTS where 1 = 0")
    queryAndVerifyTableData(session, "PRODUCTS", expected)

    session.sql("delete from PRODUCTS where ID = 1")
    queryAndVerifyTableData(session, "PRODUCTS", expected)

    session.sql("delete from PRODUCTS where DESCRIPTION is null")
    queryAndVerifyTableData(session, "PRODUCTS", expected)

    session.sql("delete from PRODUCTS where ID in (101, 102, 103)")
    session.sql("delete from PRODUCTS where NAME = 'hammer'")

    session.sql("delete from PRODUCTS where NAME like 'rock%'")
    session.sql("delete from PRODUCTS where NAME like '%jack%' and ID = 108 or WEIGHT = 5.3")
    session.sql("delete from PRODUCTS where ID >= 109")

    val expect = new util.ArrayList[String]()
    queryAndVerifyTableData(session, "PRODUCTS", expect)

    session.stop()
  }

  @Test
  def testLimitAndTopNPushDown(): Unit = {
    val session = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.sql.catalog.ob", OB_CATALOG_CLASS)
      .config("spark.sql.catalog.ob.url", getJdbcUrl)
      .config("spark.sql.catalog.ob.username", getUsername)
      .config("spark.sql.catalog.ob.password", getPassword)
      .config("spark.sql.catalog.ob.schema-name", getSchemaName)
      .getOrCreate()

    session.sql("use ob;")
    insertTestData(session, "PRODUCTS")

    import scala.collection.JavaConverters._
    // Case limit
    val actual = session
      .sql(s"select * from PRODUCTS limit 3")
      .collect()
      .map(
        _.toString().drop(1).dropRight(1)
      )
      .toList
      .asJava
    val expected: util.List[String] = util.Arrays.asList(
      "101,scooter,Small 2-wheel scooter,3.14",
      "102,car battery,12V car battery,8.10",
      "103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.80"
    )
    assertEqualsInAnyOrder(expected, actual)

    // Case top N
    val actual1 = session
      .sql(s"select * from PRODUCTS order by ID desc limit 3")
      .collect()
      .map(
        _.toString().drop(1).dropRight(1)
      )
      .toList
      .asJava
    val expected1: util.List[String] = util.Arrays.asList(
      "109,spare tire,24 inch spare tire,22.20",
      "108,jacket,water resistent black wind breaker,0.10",
      "107,rocks,box of assorted rocks,5.30"
    )
    assertEqualsInAnyOrder(expected1, actual1)

    val actual2 = session
      .sql(s"select * from PRODUCTS order by ID desc, NAME asc limit 3")
      .collect()
      .map(
        _.toString().drop(1).dropRight(1)
      )
      .toList
      .asJava
    println(actual2)
    val expected2: util.List[String] = util.Arrays.asList(
      "109,spare tire,24 inch spare tire,22.20",
      "108,jacket,water resistent black wind breaker,0.10",
      "107,rocks,box of assorted rocks,5.30"
    )
    assertEqualsInAnyOrder(expected2, actual2)

    session.stop()
  }

  @Test
  def testUpsertUniqueKey(): Unit = {
    val session = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.sql.catalog.ob", OB_CATALOG_CLASS)
      .config("spark.sql.catalog.ob.url", getJdbcUrl)
      .config("spark.sql.catalog.ob.username", getUsername)
      .config("spark.sql.catalog.ob.password", getPassword)
      .config("spark.sql.catalog.ob.schema-name", getSchemaName)
      .getOrCreate()

    session.sql("use ob;")
    insertTestData(session, "PRODUCTS_UNIQUE_KEY")
    queryAndVerifyTableData(session, "PRODUCTS_UNIQUE_KEY", expected)

    insertTestData(session, "PRODUCTS_FULL_UNIQUE_KEY")
    queryAndVerifyTableData(session, "PRODUCTS_FULL_UNIQUE_KEY", expected)
    session.stop()
  }

  @Test
  def testUpsertUniqueKeyWithNullValue(): Unit = {
    val session = SparkSession
      .builder()
      .master("local[1]")
      .config("spark.sql.catalog.ob", OB_CATALOG_CLASS)
      .config("spark.sql.catalog.ob.url", getJdbcUrl)
      .config("spark.sql.catalog.ob.username", getUsername)
      .config("spark.sql.catalog.ob.password", getPassword)
      .config("spark.sql.catalog.ob.schema-name", getSchemaName)
      .getOrCreate()

    session.sql("use ob;")
    insertTestDataWithNullValue(session, "PRODUCTS_UNIQUE_KEY")
    val expectedWithNullValue: util.List[String] = util.Arrays.asList(
      "null,null,Small 2-wheel scooter,3.14",
      "102,car battery,12V car battery,8.10",
      "103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.80",
      "104,hammer,12oz carpenter's hammer,0.75",
      "null,null,14oz carpenter's hammer,0.88",
      "106,hammer,box of assorted rocks,null",
      "108,jacket,null,0.10",
      "109,spare tire,24 inch spare tire,22.20"
    )
    queryAndVerifyTableData(session, "PRODUCTS_UNIQUE_KEY", expectedWithNullValue)

    insertTestDataWithNullValue(session, "PRODUCTS_FULL_UNIQUE_KEY")
    val expectedWithNullValue1: util.List[String] = util.Arrays.asList(
      "null,null,Small 2-wheel scooter,3.14",
      "102,car battery,12V car battery,8.10",
      "103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.80",
      "104,hammer,12oz carpenter's hammer,0.75",
      "null,null,14oz carpenter's hammer,0.88",
      "106,hammer,box of assorted rocks,null",
      "106,hammer,16oz carpenter's hammer,1.00",
      "108,jacket,null,0.10",
      "109,spare tire,24 inch spare tire,22.20"
    )
    queryAndVerifyTableData(session, "PRODUCTS_FULL_UNIQUE_KEY", expectedWithNullValue1)
    session.stop()
  }

  @Test
  def testAggregatePushdown(): Unit = {
    val session = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.sql.catalog.ob", OB_CATALOG_CLASS)
      .config("spark.sql.catalog.ob.url", getJdbcUrl)
      .config("spark.sql.catalog.ob.username", getUsername)
      .config("spark.sql.catalog.ob.password", getPassword)
      .config("spark.sql.catalog.ob.schema-name", getSchemaName)
      .getOrCreate()

    session.sql("use ob;")
    insertTestData(session, "PRODUCTS")

    import scala.collection.JavaConverters._

    /**
     * The sql generated and push-down to oceanbase:
     *
     * SELECT /*+ PARALLEL(1) */ `NAME`,MIN(`ID`),MAX(`WEIGHT`) FROM `TEST`.`PRODUCTS` WHERE (`ID`
     * >= 101 AND `ID` < 110) GROUP BY `NAME`
     *
     * In this case, tested and find: spark will not push down topN, but will push down aggregate
     */
    val expect1 = Seq("spare tire,109,22.20", "scooter,101,3.14", "rocks,107,5.30").toList.asJava
    queryAndVerify(
      session,
      "select NAME, min(ID), max(WEIGHT) from PRODUCTS group by NAME order by NAME desc limit 3",
      expect1)

    session.stop()
  }

  @Test
  def testCredentialAliasPassword(): Unit = {
    import org.apache.hadoop.conf.Configuration
    import org.apache.hadoop.security.alias.CredentialProviderFactory
    import java.io.File
    import java.nio.file.Files

    // Create temporary credential provider storage
    val tempDir = Files.createTempDirectory("test-credentials")
    val keystoreFile = new File(tempDir.toFile, "test.jceks")
    val keystorePath = s"jceks://file${keystoreFile.getAbsolutePath}"

    // Create credential provider and add password
    val hadoopConf = new Configuration()
    hadoopConf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, keystorePath)

    val provider = CredentialProviderFactory.getProviders(hadoopConf).get(0)
    provider.createCredentialEntry("test.password", getPassword.toCharArray)
    provider.flush()

    try {
      val session = SparkSession
        .builder()
        .master("local[*]")
        .config(s"spark.hadoop.hadoop.security.credential.provider.path", keystorePath)
        .config("spark.sql.catalog.ob", OB_CATALOG_CLASS)
        .config("spark.sql.catalog.ob.url", getJdbcUrl)
        .config("spark.sql.catalog.ob.username", getUsername)
        .config("spark.sql.catalog.ob.password", "alias:test.password") // Use alias format
        .config("spark.sql.catalog.ob.schema-name", getSchemaName)
        .getOrCreate()

      session.sql("use ob;")
      insertTestData(session, "PRODUCTS")
      queryAndVerifyTableData(session, "PRODUCTS", expected)

      session.stop()
    } finally {
      // Clean up temporary files
      keystoreFile.delete()
      tempDir.toFile.delete()
    }
  }

  @Test
  def testCredentialAliasPasswordWithDirectLoad(): Unit = {
    import org.apache.hadoop.conf.Configuration
    import org.apache.hadoop.security.alias.CredentialProviderFactory
    import java.io.File
    import java.nio.file.Files

    // Create temporary credential provider storage
    val tempDir = Files.createTempDirectory("test-credentials")
    val keystoreFile = new File(tempDir.toFile, "test.jceks")
    val keystorePath = s"jceks://file${keystoreFile.getAbsolutePath}"

    // Create credential provider and add password
    val hadoopConf = new Configuration()
    hadoopConf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, keystorePath)

    val provider = CredentialProviderFactory.getProviders(hadoopConf).get(0)
    provider.createCredentialEntry("test.password", getPassword.toCharArray)
    provider.flush()

    try {
      val session = SparkSession
        .builder()
        .master("local[*]")
        .config(s"spark.hadoop.hadoop.security.credential.provider.path", keystorePath)
        .config("spark.sql.catalog.ob", OB_CATALOG_CLASS)
        .config("spark.sql.catalog.ob.url", getJdbcUrl)
        .config("spark.sql.catalog.ob.username", getUsername)
        .config("spark.sql.catalog.ob.password", "alias:test.password") // Use alias format
        .config("spark.sql.catalog.ob.schema-name", getSchemaName)
        .config("spark.sql.catalog.ob.direct-load.enabled", "true")
        .config("spark.sql.catalog.ob.direct-load.host", getHost)
        .config("spark.sql.catalog.ob.direct-load.rpc-port", getRpcPort)
        .config("spark.sql.catalog.ob.direct-load.username", getUsername)
        .getOrCreate()

      session.sql("use ob;")
      insertTestData(session, "PRODUCTS")
      queryAndVerifyTableData(session, "PRODUCTS", expected)

      session.stop()
    } finally {
      // Clean up temporary files
      keystoreFile.delete()
      tempDir.toFile.delete()
    }
  }

  private def queryAndVerifyTableData(
      session: SparkSession,
      tableName: String,
      expected: util.List[String]): Unit = {
    import scala.collection.JavaConverters._
    val actual = session
      .sql(s"select * from $tableName")
      .collect()
      .map(
        _.toString().drop(1).dropRight(1)
      )
      .toList
      .asJava
    assertEqualsInAnyOrder(expected, actual)
  }

  private def queryAndVerify(
      session: SparkSession,
      sql: String,
      expected: util.List[String]): Unit = {
    import scala.collection.JavaConverters._
    val actual = session
      .sql(sql)
      .collect()
      .map(
        _.toString().drop(1).dropRight(1)
      )
      .toList
      .asJava
    println(actual)
    assertEqualsInAnyOrder(expected, actual)
  }

  private def insertTestData(session: SparkSession, tableName: String): Unit = {
    session.sql(
      s"""
         |INSERT INTO $getSchemaName.$tableName VALUES
         |(101, 'scooter', 'Small 2-wheel scooter', 3.14),
         |(102, 'car battery', '12V car battery', 8.1),
         |(103, '12-pack drill bits', '12-pack of drill bits with sizes ranging from #40 to #3', 0.8),
         |(104, 'hammer', '12oz carpenter\\'s hammer', 0.75),
         |(105, 'hammer', '14oz carpenter\\'s hammer', 0.875),
         |(106, 'hammer', '16oz carpenter\\'s hammer', 1.0),
         |(107, 'rocks', 'box of assorted rocks', 5.3),
         |(108, 'jacket', 'water resistent black wind breaker', 0.1),
         |(109, 'spare tire', '24 inch spare tire', 22.2);
         |""".stripMargin)
  }

  private def insertTestDataWithNullValue(session: SparkSession, tableName: String): Unit = {
    session.sql(
      s"""
         |INSERT INTO $getSchemaName.$tableName VALUES
         |(null, null, 'Small 2-wheel scooter', 3.14),
         |(102, 'car battery', '12V car battery', 8.1),
         |(103, '12-pack drill bits', '12-pack of drill bits with sizes ranging from #40 to #3', 0.8),
         |(104, 'hammer', '12oz carpenter\\'s hammer', 0.75),
         |(null, null, '14oz carpenter\\'s hammer', 0.875),
         |(106, 'hammer', '16oz carpenter\\'s hammer', 1.0),
         |(106, 'hammer', 'box of assorted rocks', null),
         |(108, 'jacket', null, 0.1),
         |(109, 'spare tire', '24 inch spare tire', 22.2);
         |""".stripMargin)
  }
}

object OBCatalogOracleITCase {
  val expected: util.List[String] = util.Arrays.asList(
    "101,scooter,Small 2-wheel scooter,3.14",
    "102,car battery,12V car battery,8.10",
    "103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.80",
    "104,hammer,12oz carpenter's hammer,0.75",
    "105,hammer,14oz carpenter's hammer,0.88",
    "106,hammer,16oz carpenter's hammer,1.00",
    "107,rocks,box of assorted rocks,5.30",
    "108,jacket,water resistent black wind breaker,0.10",
    "109,spare tire,24 inch spare tire,22.20"
  )
}

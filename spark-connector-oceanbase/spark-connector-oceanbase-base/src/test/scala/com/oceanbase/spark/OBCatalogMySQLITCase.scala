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

import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.{AfterAll, AfterEach, Assertions, BeforeAll, BeforeEach, Test}
import org.junit.jupiter.api.function.ThrowingSupplier

import java.util

class OBCatalogMySQLITCase extends OceanBaseMySQLTestBase {

  @BeforeEach
  def initEach(): Unit = {
    initialize("sql/mysql/products.sql")
  }

  @AfterEach
  def afterEach(): Unit = {
    dropTables(
      "products",
      "products_no_pri_key",
      "products_full_pri_key",
      "products_no_int_pri_key",
      "products_unique_key",
      "products_full_unique_key",
      "products_pri_and_unique_key"
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
    insertTestData(session, "products")
    queryAndVerifyTableData(session, "products", expected)

    insertTestData(session, "products_no_pri_key")
    queryAndVerifyTableData(session, "products_no_pri_key", expected)

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
    insertTestData(session, "products")
    queryAndVerifyTableData(session, "products", expected)

    insertTestData(session, "products_no_pri_key")
    queryAndVerifyTableData(session, "products_no_pri_key", expected)

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
    val expectedDbList =
      Seq("[information_schema]", "[mysql]", "[oceanbase]", "[test]").toList.asJava
    assertEqualsInAnyOrder(expectedDbList, dbList)

    val tableList = session.sql("show tables").collect().map(_.toString()).toList.asJava
    val expectedTableList = Seq(
      "[test,products,false]",
      "[test,products_no_pri_key,false]",
      "[test,products_full_pri_key,false]",
      "[test,products_no_int_pri_key,false]",
      "[test,products_unique_key,false]",
      "[test,products_full_unique_key,false]",
      "[test,products_pri_and_unique_key,false]"
    ).toList.asJava
    assertEqualsInAnyOrder(expectedTableList, tableList)

    // test create/drop namespace
    Assertions.assertDoesNotThrow(new ThrowingSupplier[Unit] {
      override def get(): Unit = {
        session.sql("create database spark")
        session.sql("use spark")
      }
    })
    val expectedCreateDbList =
      Seq("[information_schema]", "[mysql]", "[oceanbase]", "[test]", "[spark]").toList.asJava
    val dbList1 = session.sql("show databases").collect().map(_.toString()).toList.asJava
    assertEqualsInAnyOrder(expectedCreateDbList, dbList1)
    session.sql("drop database spark")
    val dbList2 = session.sql("show databases").collect().map(_.toString()).toList.asJava
    assertEqualsInAnyOrder(expectedDbList, dbList2)

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
    insertTestData(session, "products_no_pri_key")
    queryAndVerifyTableData(session, "products_no_pri_key", expected)
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
    insertTestData(session, "products_full_pri_key")

    queryAndVerifyTableData(session, "products_full_pri_key", expected)

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

    insertTestData(session, "products")
    queryAndVerifyTableData(session, "products", expected)

    insertTestData(session, "products_no_pri_key")
    session.sql("insert overwrite products select * from products_no_pri_key")
    queryAndVerifyTableData(session, "products", expected)
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
    insertTestData(session, "products")
    // Test CTAS
    session.sql("create table test1 as select * from products")
    queryAndVerifyTableData(session, "test1", expected)

    // test bucket partition table:
    //   1. column comment test
    //   2. table comment test
    //   3. table options test
    session.sql(
      """
        |CREATE TABLE test2(
        |  user_id BIGINT COMMENT 'test_for_key',
        |  name VARCHAR(255)
        |)
        |PARTITIONED BY (bucket(16, user_id))
        |COMMENT 'test_for_table_create'
        |TBLPROPERTIES('replica_num' = 2, COMPRESSION = 'zstd_1.0', primary_key = 'user_id, name');
        |""".stripMargin)
    val showCreateTable = getShowCreateTable(s"$getSchemaName.test2")
    Assertions.assertTrue(
      showCreateTable.contains("test_for_key")
        && showCreateTable.contains("test_for_table_create")
        && showCreateTable.contains("partition by key(`user_id`)")
        && showCreateTable.contains("COMPRESSION = 'zstd_1.0'")
        && showCreateTable.contains("REPLICA_NUM = 1")
        && showCreateTable.contains("PRIMARY KEY (`user_id`, `name`)"))
    dropTables("test1", "test2")
    session.stop()
  }

  @Test
  def testString2VarcharTableCreate(): Unit = {
    val session = SparkSession
      .builder()
      .master("local[1]")
      .config("spark.sql.catalog.ob", OB_CATALOG_CLASS)
      .config("spark.sql.catalog.ob.url", getJdbcUrl)
      .config("spark.sql.catalog.ob.username", getUsername)
      .config("spark.sql.catalog.ob.password", getPassword)
      .config("spark.sql.catalog.ob.schema-name", getSchemaName)
      .config("spark.sql.catalog.ob.string-as-varchar-length", 2048)
      .config("spark.sql.defaultCatalog", "ob")
      .getOrCreate()
    session.sql("""
                  |CREATE TABLE test1(
                  |  c1 String,
                  |  c2 String
                  |);
                  |""".stripMargin)
    val showCreateTable = getShowCreateTable(s"$getSchemaName.test1")
    Assertions.assertTrue(showCreateTable.contains("varchar(2048)"))
    session.stop()

    val spark = SparkSession
      .builder()
      .master("local[1]")
      .config("spark.sql.catalog.ob", OB_CATALOG_CLASS)
      .config("spark.sql.catalog.ob.url", getJdbcUrl)
      .config("spark.sql.catalog.ob.username", getUsername)
      .config("spark.sql.catalog.ob.password", getPassword)
      .config("spark.sql.catalog.ob.schema-name", getSchemaName)
      .config("spark.sql.catalog.ob.enable-string-as-text", true.toString)
      .config("spark.sql.defaultCatalog", "ob")
      .getOrCreate()
    spark.sql("""
                |CREATE TABLE test2(
                |  c1 String,
                |  c2 String
                |);
                |""".stripMargin)
    val showCreateTableTest2 = getShowCreateTable(s"$getSchemaName.test2")
    Assertions.assertTrue(showCreateTableTest2.contains("text"))
    spark.stop()

    val ss = SparkSession
      .builder()
      .master("local[1]")
      .config("spark.sql.catalog.ob", OB_CATALOG_CLASS)
      .config("spark.sql.catalog.ob.url", getJdbcUrl)
      .config("spark.sql.catalog.ob.username", getUsername)
      .config("spark.sql.catalog.ob.password", getPassword)
      .config("spark.sql.catalog.ob.schema-name", getSchemaName)
      .config("spark.sql.catalog.ob.enable-spark-varchar-datatype", true.toString)
      .config("spark.sql.defaultCatalog", "ob")
      .getOrCreate()
    insertTestData(ss, "products")
    ss.sql("create table test3 as select * from products")
    val showCreateTableTest3 = getShowCreateTable(s"$getSchemaName.test3")
    Assertions.assertTrue(
      showCreateTableTest3.contains("varchar(255)")
        && showCreateTableTest3.contains("varchar(512)"))
    queryAndVerifyTableData(ss, "test3", expected)
    // Verify the column length under Chinese characters.
    ss.sql("create table test4(name varchar(3));")
    val showCreateTableTest4 = getShowCreateTable(s"$getSchemaName.test4")
    Assertions.assertTrue(showCreateTableTest4.contains("varchar(3)"))
    ss.sql("insert into test4 values('你好时');").show()
    queryAndVerifyTableData(ss, "test4", util.Arrays.asList("你好时"))
    ss.stop()

    dropTables("test1", "test2", "test3", "test4")
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
    insertTestData(session, "products")
    session.sql("truncate table products")
    val expect = new util.ArrayList[String]()
    queryAndVerifyTableData(session, "products", expect)

    insertTestData(session, "products_no_pri_key")
    session.sql("insert overwrite products select * from products_no_pri_key")
    queryAndVerifyTableData(session, "products", expected)

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
    insertTestData(session, "products")
    session.sql("delete from products where 1 = 0")
    queryAndVerifyTableData(session, "products", expected)

    session.sql("delete from products where id = 1")
    queryAndVerifyTableData(session, "products", expected)

    session.sql("delete from products where description is null")
    queryAndVerifyTableData(session, "products", expected)

    session.sql("delete from products where id in (101, 102, 103)")
    session.sql("delete from products where name = 'hammer'")

    session.sql("delete from products where name like 'rock%'")
    session.sql("delete from products where name like '%jack%' and id = 108 or weight = 5.3")
    session.sql("delete from products where id >= 109")

    val expect = new util.ArrayList[String]()
    queryAndVerifyTableData(session, "products", expect)

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
    insertTestData(session, "products")

    import scala.collection.JavaConverters._
    // Case limit
    val actual = session
      .sql(s"select * from products limit 3")
      .collect()
      .map(
        _.toString().drop(1).dropRight(1)
      )
      .toList
      .asJava
    val expected: util.List[String] = util.Arrays.asList(
      "101,scooter,Small 2-wheel scooter,3.1400000000",
      "102,car battery,12V car battery,8.1000000000",
      "103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.8000000000"
    )
    assertEqualsInAnyOrder(expected, actual)

    // Case top N
    val actual1 = session
      .sql(s"select * from products order by id desc nulls first limit 3")
      .collect()
      .map(
        _.toString().drop(1).dropRight(1)
      )
      .toList
      .asJava
    val expected1: util.List[String] = util.Arrays.asList(
      "109,spare tire,24 inch spare tire,22.2000000000",
      "108,jacket,water resistent black wind breaker,0.1000000000",
      "107,rocks,box of assorted rocks,5.3000000000"
    )
    assertEqualsInAnyOrder(expected1, actual1)

    val actual2 = session
      .sql(s"select * from products order by id desc nulls first, name asc nulls last limit 3")
      .collect()
      .map(
        _.toString().drop(1).dropRight(1)
      )
      .toList
      .asJava
    println(actual2)
    val expected2: util.List[String] = util.Arrays.asList(
      "109,spare tire,24 inch spare tire,22.2000000000",
      "108,jacket,water resistent black wind breaker,0.1000000000",
      "107,rocks,box of assorted rocks,5.3000000000"
    )
    assertEqualsInAnyOrder(expected2, actual2)

    session.stop()
  }

  @Test
  def testUnevenlyRead(): Unit = {
    val session = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.sql.catalog.ob", OB_CATALOG_CLASS)
      .config("spark.sql.catalog.ob.url", getJdbcUrl)
      .config("spark.sql.catalog.ob.username", getUsername)
      .config("spark.sql.catalog.ob.password", getPassword)
      .config("spark.sql.catalog.ob.jdbc.max-records-per-partition", "2")
      .config(
        s"spark.sql.catalog.ob.jdbc.$getSchemaName.products_no_int_pri_key.partition-column",
        "name")
      .config("spark.sql.catalog.ob.schema-name", getSchemaName)
      .getOrCreate()

    session.sql("use ob;")
    insertTestData(session, "products_no_int_pri_key")
    queryAndVerifyTableData(session, "products_no_int_pri_key", expected)

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
    insertTestData(session, "products_unique_key")
    queryAndVerifyTableData(session, "products_unique_key", expected)

    insertTestData(session, "products_full_unique_key")
    queryAndVerifyTableData(session, "products_full_unique_key", expected)
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
    insertTestDataWithNullValue(session, "products_unique_key")
    val expectedWithNullValue: util.List[String] = util.Arrays.asList(
      "null,null,Small 2-wheel scooter,3.1400000000",
      "102,car battery,12V car battery,8.1000000000",
      "103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.8000000000",
      "104,hammer,12oz carpenter's hammer,0.7500000000",
      "null,null,14oz carpenter's hammer,0.8750000000",
      "106,hammer,box of assorted rocks,null",
      "108,jacket,null,0.1000000000",
      "109,spare tire,24 inch spare tire,22.2000000000"
    )
    queryAndVerifyTableData(session, "products_unique_key", expectedWithNullValue)

    insertTestDataWithNullValue(session, "products_full_unique_key")
    val expectedWithNullValue1: util.List[String] = util.Arrays.asList(
      "null,null,Small 2-wheel scooter,3.1400000000",
      "102,car battery,12V car battery,8.1000000000",
      "103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.8000000000",
      "104,hammer,12oz carpenter's hammer,0.7500000000",
      "null,null,14oz carpenter's hammer,0.8750000000",
      "106,hammer,box of assorted rocks,null",
      "106,hammer,16oz carpenter's hammer,1.0000000000",
      "108,jacket,null,0.1000000000",
      "109,spare tire,24 inch spare tire,22.2000000000"
    )
    queryAndVerifyTableData(session, "products_full_unique_key", expectedWithNullValue1)
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
    insertTestData(session, "products")

    import scala.collection.JavaConverters._
    // case 1
    val expect = Seq(
      "1,102,8.1000000000",
      "3,104,1.0000000000",
      "1,109,22.2000000000",
      "1,103,0.8000000000",
      "1,108,0.1000000000",
      "1,101,3.1400000000",
      "1,107,5.3000000000").toList.asJava
    queryAndVerify(
      session,
      "select count(id), min(id), max(weight) from products group by name",
      expect)

    // case 2
    /**
     * The sql generated and push-down to oceanbase:
     *
     * SELECT /*+ PARALLEL(1) */ `name`,MIN(`id`),MAX(`weight`) FROM `test`.`products` WHERE (`id`
     * >= 101 AND `id` < 110) GROUP BY `name`
     *
     * In this case, tested and find: spark will not push down topN, but will push down aggregate
     */
    val expect1 = Seq(
      "spare tire,109,22.2000000000",
      "scooter,101,3.1400000000",
      "rocks,107,5.3000000000").toList.asJava
    queryAndVerify(
      session,
      "select name, min(id), max(weight) from products group by name order by name desc limit 3",
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
      insertTestData(session, "products")
      queryAndVerifyTableData(session, "products", expected)

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
      insertTestData(session, "products")
      queryAndVerifyTableData(session, "products", expected)

      session.stop()
    } finally {
      // Clean up temporary files
      keystoreFile.delete()
      tempDir.toFile.delete()
    }
  }

  @Test
  def testJdbcUpsertByUniqueKey(): Unit = {
    val session = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.sql.catalog.ob", OB_CATALOG_CLASS)
      .config("spark.sql.catalog.ob.url", getJdbcUrl)
      .config("spark.sql.catalog.ob.username", getUsername)
      .config("spark.sql.catalog.ob.password", getPassword)
      .config("spark.sql.catalog.ob.schema-name", getSchemaName)
      .config("spark.sql.catalog.ob.jdbc.upsert-by-unique-key", true.toString)
      .getOrCreate()

    session.sql("use ob;")
    session.sql(
      s"INSERT INTO $getSchemaName.products_pri_and_unique_key VALUES (1, 'n1', 'd1', 1.0)")
    session.sql(
      s"INSERT INTO $getSchemaName.products_pri_and_unique_key VALUES (2, 'n1', 'd2', 2.2)")

    val actual = queryTable(
      s"$getSchemaName.products_pri_and_unique_key",
      util.Arrays.asList("id", "name", "description"))
    Assertions.assertEquals(util.Arrays.asList("2,n1,d2"), actual)
    session.stop()
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

object OBCatalogMySQLITCase {
  @BeforeAll
  def setup(): Unit = {
    OceanBaseMySQLTestBase.CONTAINER.start()
  }

  @AfterAll
  def tearDown(): Unit = {
    OceanBaseMySQLTestBase.CONTAINER.stop()
  }
}

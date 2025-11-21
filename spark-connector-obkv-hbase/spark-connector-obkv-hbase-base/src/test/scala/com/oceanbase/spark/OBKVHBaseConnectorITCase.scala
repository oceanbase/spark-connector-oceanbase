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

import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.{AfterAll, AfterEach, BeforeAll, BeforeEach, Test}
import org.junit.jupiter.api.condition.{DisabledOnOs, OS}

import java.sql.ResultSet
import java.util

class OBKVHBaseConnectorITCase extends OceanBaseMySQLTestBase {
  @BeforeEach
  def before(): Unit = {
    initialize("sql/htable.sql")
  }

  @AfterEach
  def after(): Unit = {
    dropTables("htable$family1", "htable$family2")
  }

  @Test
  def testOdpDataFrameSink(): Unit = {
    val session = SparkSession.builder.master("local[*]").getOrCreate

    val newContact =
      ContactRecord("16891", "40 Ellis St.", "674-555-0110", "John Jackson", 121.11)
    val newData = Seq(newContact)
    val df = session.createDataFrame(newData).toDF()
    df.write
      .format("obkv-hbase")
      .option("odp-mode", true)
      .option("odp-ip", OceanBaseMySQLTestBase.ODP.getHost)
      .option("odp-port", OceanBaseMySQLTestBase.ODP.getRpcPort)
      .option("username", s"$getUsername#$getClusterName")
      .option("password", getPassword)
      .option("table-name", "htable")
      .option("schema-name", getSchemaName)
      .option("schema", OBKVHBaseConnectorITCase.schema)
      .save()
    session.stop()

    import scala.collection.JavaConverters._
    val expected1 = List(
      "16891,address,40 Ellis St.",
      "16891,phone,674-555-0110",
      "16891,personalName,John Jackson",
      "16891,personalPhone,121.11").asJava

    val actual1 = queryHTable("htable$family1", rowConverter)
    assertEqualsInAnyOrder(expected1, actual1)
  }

  @Test
  def testOdpSqlSink(): Unit = {
    val session = SparkSession.builder.master("local[*]").getOrCreate
    val newContact =
      ContactRecord("16891", "40 Ellis St.", "674-555-0110", "John Jackson", 121.11)
    session.createDataFrame(Seq(newContact)).createOrReplaceTempView("content")
    session.sql(s"""
                   |CREATE OR REPLACE TEMPORARY VIEW test_sink
                   |USING `obkv-hbase`
                   |OPTIONS(
                   |  "odp-mode" = "true",
                   |  "odp-ip"= "${OceanBaseMySQLTestBase.ODP.getHost}",
                   |  "odp-port" = "${OceanBaseMySQLTestBase.ODP.getRpcPort}",
                   |  "schema-name"="$getSchemaName",
                   |  "table-name"="htable",
                   |  "username"="$getUsername#$getClusterName",
                   |  "password"="$getPassword",
                   |  "schema"="${OBKVHBaseConnectorITCase.schemaWithSingleQuotes}"
                   |);
                   |""".stripMargin)
    session.sql("""
                  |INSERT INTO test_sink
                  |SELECT * FROM content;
                  |""".stripMargin)
    session.stop()

    import scala.collection.JavaConverters._
    val expected1 = List(
      "16891,address,40 Ellis St.",
      "16891,phone,674-555-0110",
      "16891,personalName,John Jackson",
      "16891,personalPhone,121.11").asJava

    val actual1 = queryHTable("htable$family1", rowConverter)
    assertEqualsInAnyOrder(expected1, actual1)
  }

  @Test
  @DisabledOnOs(
    value = Array[OS](OS.MAC),
    disabledReason = "Currently it can only be successfully run on the GitHub CI environment")
  def testDirectSqlSink(): Unit = {
    val session = SparkSession.builder.master("local[*]").getOrCreate
    val newContact =
      ContactRecord("16891", "40 Ellis St.", "674-555-0110", "John Jackson", 121.11)
    session.createDataFrame(Seq(newContact)).createOrReplaceTempView("content")
    session.sql(s"""
                   |CREATE OR REPLACE TEMPORARY VIEW test_sink
                   |USING `obkv-hbase`
                   |OPTIONS(
                   |  "url" = "${OceanBaseMySQLTestBase.getSysParameter("obconfig_url")}",
                   |  "sys.username" = "$getSysUsername",
                   |  "sys.password" =  "$getSysPassword",
                   |  "schema-name"="$getSchemaName",
                   |  "table-name"="htable",
                   |  "username"="$getUsername#$getClusterName",
                   |  "password"="$getPassword",
                   |  "schema"="${OBKVHBaseConnectorITCase.multiCFSchemaSql}"
                   |);
                   |""".stripMargin)
    session.sql("""
                  |INSERT INTO test_sink
                  |SELECT * FROM content;
                  |""".stripMargin)
    session.stop()

    import scala.collection.JavaConverters._
    val expected1 = List("16891,address,40 Ellis St.", "16891,phone,674-555-0110").asJava

    val actual1 = queryHTable("htable$family1", rowConverter)
    assertEqualsInAnyOrder(expected1, actual1)

    val expected2 = List("16891,personalName,John Jackson", "16891,personalPhone,121.11").asJava
    val actual2 = queryHTable("htable$family2", rowConverter)
    assertEqualsInAnyOrder(expected2, actual2)
  }

  protected def queryHTable(
      tableName: String,
      rowConverter: OceanBaseTestBase.RowConverter): util.List[String] = {
    queryTable(tableName, util.Arrays.asList("K", "Q", "V"), rowConverter)
  }

  def rowConverter: OceanBaseTestBase.RowConverter = new OceanBaseTestBase.RowConverter {
    override def convert(rs: ResultSet, columnCount: Int): String = {
      val k = Bytes.toString(rs.getBytes("K"))
      val q = Bytes.toString(rs.getBytes("Q"))
      val bytes = rs.getBytes("V")
      var v: String = null
      q match {
        case "address" | "phone" | "personalName" =>
          v = Bytes.toString(bytes)

        case "personalPhone" =>
          v = String.valueOf(Bytes.toDouble(bytes))
        case _ =>
          throw new RuntimeException("Unknown qualifier: " + q)
      }
      s"$k,$q,$v"
    }
  }
}

object OBKVHBaseConnectorITCase {
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

  val schema: String =
    """
      |{
      |    "rowkey": {"cf": "rowkey","col": "rowkey","type": "string"},
      |    "address": {"cf": "family1","col": "officeAddress","type": "string"},
      |    "phone": {"cf": "family1","col": "officePhone","type": "string"},
      |    "personalName": {"cf": "family1","col": "personalName","type": "string"},
      |    "personalPhone": {"cf": "family1","col": "personalPhone","type": "double"}
      |}
      |""".stripMargin

  val schemaWithSingleQuotes: String =
    """
      |{
      |    'rowkey': {'cf': 'rowkey','col': 'rowkey','type': 'string'},
      |    'address': {'cf': 'family1','col': 'address','type': 'string'},
      |    'phone': {'cf': 'family1','col': 'phone','type': 'string'},
      |    'personalName': {'cf': 'family1','col': 'personalName','type': 'string'},
      |    'personalPhone': {'cf': 'family1','col': 'personalPhone','type': 'double'}
      |}
      |""".stripMargin

  val multiCFSchemaSql: String =
    """
      |{
      |    'rowkey': {'cf': 'rowkey','col': 'rowkey','type': 'string'},
      |    'address': {'cf': 'family1','col': 'address','type': 'string'},
      |    'phone': {'cf': 'family1','col': 'phone','type': 'string'},
      |    'personalName': {'cf': 'family2','col': 'personalName','type': 'string'},
      |    'personalPhone': {'cf': 'family2','col': 'personalPhone','type': 'double'}
      |}
      |""".stripMargin
}

case class ContactRecord(
    rowkey: String,
    officeAddress: String,
    officePhone: String,
    personalName: String,
    personalPhone: Double
)

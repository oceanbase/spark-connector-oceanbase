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

import com.oceanbase.spark.OBKVHBaseE2eITCase.SINK_CONNECTOR_NAME
import com.oceanbase.spark.OceanBaseMySQLTestBase.{createSysUser, getSysParameter}
import com.oceanbase.spark.OceanBaseMySQLTestBase.getConfigServerAddress
import com.oceanbase.spark.OceanBaseTestBase.assertEqualsInAnyOrder
import com.oceanbase.spark.utils.SparkContainerTestEnvironment
import com.oceanbase.spark.utils.SparkContainerTestEnvironment.getResource

import org.apache.hadoop.hbase.util.Bytes
import org.junit.jupiter.api.{AfterAll, AfterEach, BeforeAll, BeforeEach, Disabled, Test}
import org.junit.jupiter.api.condition.DisabledIfSystemProperty
import org.junit.jupiter.api.condition.EnabledIfSystemProperty

import java.sql.ResultSet
import java.util

class OBKVHBaseE2eITCase extends SparkContainerTestEnvironment {
  @BeforeEach
  override def before(): Unit = {
    super.before()
    initialize("sql/htable.sql")
  }

  @AfterEach
  override def after(): Unit = {
    super.after()
    dropTables("htable$family1", "htable$family2")
  }

  @Test
  @DisabledIfSystemProperty(
    named = "spark_version",
    matches = "^2\\.4\\.[0-9]$",
    disabledReason = "The spark 2.x docker image fails to execute the spark-sql command."
  )
  def testInsertValues(): Unit = {
    val sqlLines: util.List[String] = new util.ArrayList[String]
    sqlLines.add(
      s"""
         |CREATE TEMPORARY VIEW test_sink (
         |  rowkey STRING,
         |  family1 STRUCT<address: STRING, phone: STRING, personalName: STRING, personalPhone: DOUBLE>
         |)
         |USING `obkv-hbase`
         |OPTIONS(
         |  "url" = "${getSysParameter("obconfig_url")}",
         |  "sys.username"= "$getSysUsername",
         |  "sys.password" = "$getSysPassword",
         |  "schema-name"="$getSchemaName",
         |  "table-name"="htable",
         |  "username"="$getUsername#$getClusterName",
         |  "password"="$getPassword"
         |);
         |""".stripMargin)
    sqlLines.add(
      """
        |INSERT INTO test_sink VALUES
        |('16891', named_struct('address', '40 Ellis St.', 'phone', '674-555-0110', 'personalName', 'John Jackson', 'personalPhone', 121.11));
        |""".stripMargin)
    submitSQLJob(sqlLines, getResource(SINK_CONNECTOR_NAME))

    import scala.collection.JavaConverters._
    val expected1 = List(
      "16891,address,40 Ellis St.",
      "16891,phone,674-555-0110",
      "16891,personalName,John Jackson",
      "16891,personalPhone,121.11"
    ).asJava

    val actual1 = queryHTable("htable$family1", rowConverter)
    assertEqualsInAnyOrder(expected1, actual1)
  }

  @Test
  @EnabledIfSystemProperty(
    named = "spark_version",
    matches = "^(2\\.4\\.[0-9])$",
    disabledReason =
      "This is because the spark 2.x docker image fails to execute the spark-sql command."
  )
  def testInsertValuesSpark2(): Unit = {
    val sqlLines: util.List[String] = new util.ArrayList[String]
    sqlLines.add(
      s"""
         |  import org.apache.spark.sql.types._
         |  val schema = StructType(Seq(
         |    StructField("rowkey", StringType),
         |    StructField("family1", StructType(Seq(
         |      StructField("address", StringType),
         |      StructField("phone", StringType),
         |      StructField("personalName", StringType),
         |      StructField("personalPhone", DoubleType)
         |    )))
         |  ))
         |  case class Family1(address: String, phone: String, personalName: String, personalPhone: Double)
         |  case class ContactRecord(rowkey: String, family1: Family1)
         |  val newContact = ContactRecord("16891", Family1("40 Ellis St.", "674-555-0110", "John Jackson", 121.11))
         |  val newData = Seq(newContact)
         |  val df = spark.createDataFrame(newData)
         |  df.write
         |    .format("obkv-hbase")
         |    .option("url", "${getSysParameter("obconfig_url")}")
         |    .option("sys.username", "$getSysUsername")
         |    .option("sys.password", "$getSysPassword")
         |    .option("username", "$getUsername#$getClusterName")
         |    .option("password", "$getPassword")
         |    .option("table-name", "htable")
         |    .option("schema-name", "$getSchemaName")
         |    .save()
         |""".stripMargin)

    submitSparkShellJob(sqlLines, getResource(SINK_CONNECTOR_NAME))

    import scala.collection.JavaConverters._
    val expected1 = List(
      "16891,address,40 Ellis St.",
      "16891,phone,674-555-0110",
      "16891,personalName,John Jackson",
      "16891,personalPhone,121.11").asJava

    val actual1 = queryHTable("htable$family1", rowConverter)
    assertEqualsInAnyOrder(expected1, actual1)
  }

  protected def queryHTable(
      tableName: String,
      rowConverter: OceanBaseTestBase.RowConverter
  ): util.List[String] = {
    queryTable(tableName, util.Arrays.asList("K", "Q", "V"), rowConverter)
  }

  def rowConverter: OceanBaseTestBase.RowConverter =
    new OceanBaseTestBase.RowConverter {
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

object OBKVHBaseE2eITCase extends SparkContainerTestEnvironment {
  private val SINK_CONNECTOR_NAME =
    "^.*spark-connector-obkv-hbase-\\d+\\.\\d+_\\d+\\.\\d+-[\\d\\.]+(?:-SNAPSHOT)?\\.jar$"

  @BeforeAll
  def setup(): Unit = {
    OceanBaseMySQLTestBase.CONFIG_SERVER.start()
    val configServerAddress = getConfigServerAddress(
      OceanBaseMySQLTestBase.CONFIG_SERVER
    )
    OceanBaseMySQLTestBase.CONTAINER
      .withEnv("OB_CONFIGSERVER_ADDRESS", configServerAddress)
      .start()
    val password = "test"
    createSysUser("proxyro", password)
  }

  @AfterAll
  def tearDown(): Unit = {
    List(
      OceanBaseMySQLTestBase.CONFIG_SERVER,
      OceanBaseMySQLTestBase.CONTAINER
    )
      .foreach(_.stop())
  }

}

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

import com.oceanbase.spark.OceanBaseCatalogE2eITCase.{MYSQL_CONNECTOR_JAVA, SINK_CONNECTOR_NAME}
import com.oceanbase.spark.OceanBaseTestBase.assertEqualsInAnyOrder
import com.oceanbase.spark.utils.SparkContainerTestEnvironment
import com.oceanbase.spark.utils.SparkContainerTestEnvironment.getResource

import org.junit.jupiter.api._
import org.junit.jupiter.api.condition.DisabledIfSystemProperty
import org.junit.jupiter.api.function.ThrowingSupplier
import org.slf4j.LoggerFactory
import org.testcontainers.containers.output.Slf4jLogConsumer

import java.util

class OceanBaseCatalogE2eITCase extends SparkContainerTestEnvironment {
  @BeforeEach
  @throws[Exception]
  override def before(): Unit = {
    super.before()
    initialize("sql/mysql/products.sql")
  }

  @AfterEach
  @throws[Exception]
  override def after(): Unit = {
    super.after()
    dropTables("products")
  }

  @Test
  @DisabledIfSystemProperty(
    named = "spark_version",
    matches = "^2\\.4\\.[0-9]$",
    disabledReason = "Catalog is only supported starting from Spark3."
  )
  def testInsertValues(): Unit = {
    val sqlLines: util.List[String] = new util.ArrayList[String]
    sqlLines.add(s"""
                    |set spark.sql.catalog.ob=com.oceanbase.spark.catalog.OceanBaseCatalog;
                    |set spark.sql.catalog.ob.url=$getJdbcUrlInContainer;
                    |set spark.sql.catalog.ob.username=$getUsername;
                    |set spark.sql.catalog.ob.password=$getPassword;
                    |set `spark.sql.catalog.ob.schema-name`=$getSchemaName;
                    |set spark.sql.defaultCatalog=ob;
                    |""".stripMargin)
    sqlLines.add(
      s"""
         |INSERT INTO $getSchemaName.products VALUES
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

    sqlLines.add("select * from products limit 10;")

    submitSQLJob(sqlLines, getResource(SINK_CONNECTOR_NAME), getResource(MYSQL_CONNECTOR_JAVA))

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
    val actual: util.List[String] = queryTable("products")
    assertEqualsInAnyOrder(expected, actual)
  }

  @Test
  @DisabledIfSystemProperty(
    named = "spark_version",
    matches = "^2\\.4\\.[0-9]$",
    disabledReason = "Catalog is only supported starting from Spark3."
  )
  def testCatalogOp(): Unit = {
    val sqlLines: util.List[String] = new util.ArrayList[String]
    sqlLines.add(s"""
                    |set spark.sql.catalog.ob=com.oceanbase.spark.catalog.OceanBaseCatalog;
                    |set spark.sql.catalog.ob.url=$getJdbcUrlInContainer;
                    |set spark.sql.catalog.ob.username=$getUsername;
                    |set spark.sql.catalog.ob.password=$getPassword;
                    |set `spark.sql.catalog.ob.schema-name`=$getSchemaName;
                    |set spark.sql.defaultCatalog=ob;
                    |""".stripMargin)

    sqlLines.add(s"""
                    |show databases;
                    |show tables;
                    |use test;
                    |select * from products limit 10;
                    |""".stripMargin)

    Assertions.assertDoesNotThrow(new ThrowingSupplier[Unit] {
      override def get(): Unit = {
        submitSQLJob(sqlLines, getResource(SINK_CONNECTOR_NAME), getResource(MYSQL_CONNECTOR_JAVA))
      }
    })
  }
}

object OceanBaseCatalogE2eITCase extends SparkContainerTestEnvironment {
  private val LOG = LoggerFactory.getLogger(classOf[OceanBaseE2eITCase])
  private val SINK_CONNECTOR_NAME =
    "^.*spark-connector-oceanbase-\\d+\\.\\d+_\\d+\\.\\d+-[\\d\\.]+(?:-SNAPSHOT)?\\.jar$"
  private val MYSQL_CONNECTOR_JAVA = "mysql-connector-java.jar"

  @BeforeAll def setup(): Unit = {
    OceanBaseMySQLTestBase.CONTAINER.withLogConsumer(new Slf4jLogConsumer(LOG)).start()
  }

  @AfterAll def tearDown(): Unit = {
    OceanBaseMySQLTestBase.CONTAINER.stop()
  }

}

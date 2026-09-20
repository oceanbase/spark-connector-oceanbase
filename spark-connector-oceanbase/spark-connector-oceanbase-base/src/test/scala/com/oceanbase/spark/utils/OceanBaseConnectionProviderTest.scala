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
package com.oceanbase.spark.utils

import com.oceanbase.spark.catalog.OceanBaseCatalog
import com.oceanbase.spark.config.OceanBaseConfig

import org.junit.jupiter.api.{Assertions, BeforeEach, Test}
import org.junit.jupiter.api.function.Executable

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.sql.{Connection, Driver, DriverManager, DriverPropertyInfo, SQLException}
import java.util.Properties
import java.util.logging.Logger

import scala.collection.mutable

class OceanBaseConnectionProviderTest {

  @BeforeEach
  def beforeEach(): Unit = {
    HAConnectionTestDriver.reset()
    HAConnectionTestDriver.register()
  }

  @Test
  def testParseJdbcUrls(): Unit = {
    val urls = OceanBaseConnectionProvider.parseJdbcUrls(
      " jdbc:obtest://proxy1/db , jdbc:obtest://proxy2/db ,, ")

    Assertions.assertEquals(Seq("jdbc:obtest://proxy1/db", "jdbc:obtest://proxy2/db"), urls)
  }

  @Test
  def testExtractDatabaseNameUsesFirstJdbcUrl(): Unit = {
    val db = OceanBaseCatalog.extractDatabaseName(
      "jdbc:oceanbase://proxy1:2881/db1,jdbc:oceanbase://proxy2:2881/db2")

    Assertions.assertEquals(Some("db1"), db)
  }

  @Test
  def testFailoverToNextJdbcUrl(): Unit = {
    HAConnectionTestDriver.failUrls += "jdbc:obtest://down/db"

    val connection = OceanBaseConnectionProvider.getConnection(
      config("jdbc:obtest://down/db,jdbc:obtest://up/db", maxRetries = "1", retryInterval = "PT0S"))

    Assertions.assertNotNull(connection)
    Assertions.assertTrue(HAConnectionTestDriver.attemptedUrls.contains("jdbc:obtest://up/db"))
    Assertions.assertEquals("jdbc:obtest://up/db", HAConnectionTestDriver.connectedUrls.last)
  }

  @Test
  def testFailedJdbcUrlIsPenalizedDuringCooldown(): Unit = {
    HAConnectionTestDriver.failUrls += "jdbc:obtest://down/db"
    Assertions.assertThrows(
      classOf[RuntimeException],
      new Executable {
        override def execute(): Unit = {
          OceanBaseConnectionProvider.getConnection(
            config(
              "jdbc:obtest://down/db",
              maxRetries = "1",
              retryInterval = "PT0S",
              cooldown = "PT1S"))
        }
      }
    )

    HAConnectionTestDriver.reset()
    val connection = OceanBaseConnectionProvider.getConnection(
      config(
        "jdbc:obtest://down/db,jdbc:obtest://up/db",
        maxRetries = "1",
        retryInterval = "PT0S",
        cooldown = "PT1S"))

    Assertions.assertNotNull(connection)
    Assertions.assertEquals(Seq("jdbc:obtest://up/db"), HAConnectionTestDriver.attemptedUrls)
  }

  @Test
  def testFailedJdbcUrlCanBeRetriedAfterCooldown(): Unit = {
    HAConnectionTestDriver.failUrls += "jdbc:obtest://down/db"
    Assertions.assertThrows(
      classOf[RuntimeException],
      new Executable {
        override def execute(): Unit = {
          OceanBaseConnectionProvider.getConnection(
            config(
              "jdbc:obtest://down/db",
              maxRetries = "1",
              retryInterval = "PT0S",
              cooldown = "PT0.05S"))
        }
      }
    )

    HAConnectionTestDriver.reset()
    Thread.sleep(80)
    val connection = OceanBaseConnectionProvider.getConnection(
      config(
        "jdbc:obtest://down/db",
        maxRetries = "1",
        retryInterval = "PT0S",
        cooldown = "PT0.05S"))

    Assertions.assertNotNull(connection)
    Assertions.assertEquals(Seq("jdbc:obtest://down/db"), HAConnectionTestDriver.connectedUrls)
  }

  private def config(
      url: String,
      maxRetries: String = "3",
      retryInterval: String = "PT0S",
      cooldown: String = "PT1S"): OceanBaseConfig = {
    val options = new java.util.HashMap[String, String]()
    options.put("url", url)
    options.put("username", "root")
    options.put("password", "password")
    options.put("jdbc.connection.max-retries", maxRetries)
    options.put("jdbc.connection.retry-interval", retryInterval)
    options.put("jdbc.connection.failed-url-cooldown", cooldown)
    new OceanBaseConfig(options)
  }
}

class HAConnectionTestDriver extends Driver {
  override def connect(url: String, info: Properties): Connection = {
    if (!acceptsURL(url)) {
      return null
    }
    HAConnectionTestDriver.recordAttempt(url)
    if (HAConnectionTestDriver.failUrls.contains(url)) {
      throw new SQLException(s"Cannot connect to $url")
    }
    HAConnectionTestDriver.recordConnection(url)
    HAConnectionTestDriver.newConnection()
  }

  override def acceptsURL(url: String): Boolean = url != null && url.startsWith("jdbc:obtest://")

  override def getPropertyInfo(url: String, info: Properties): Array[DriverPropertyInfo] =
    Array.empty

  override def getMajorVersion: Int = 1

  override def getMinorVersion: Int = 0

  override def jdbcCompliant(): Boolean = false

  override def getParentLogger: Logger = Logger.getGlobal
}

object HAConnectionTestDriver {
  val failUrls: mutable.Set[String] = mutable.Set.empty
  private val attempts: mutable.ArrayBuffer[String] = mutable.ArrayBuffer.empty
  private val connections: mutable.ArrayBuffer[String] = mutable.ArrayBuffer.empty
  private var registered = false

  def register(): Unit = synchronized {
    if (!registered) {
      DriverManager.registerDriver(new HAConnectionTestDriver)
      registered = true
    }
  }

  def reset(): Unit = synchronized {
    failUrls.clear()
    attempts.clear()
    connections.clear()
  }

  def recordAttempt(url: String): Unit = synchronized {
    attempts += url
  }

  def recordConnection(url: String): Unit = synchronized {
    connections += url
  }

  def attemptedUrls: Seq[String] = synchronized {
    attempts.toSeq
  }

  def connectedUrls: Seq[String] = synchronized {
    connections.toSeq
  }

  def newConnection(): Connection = {
    Proxy
      .newProxyInstance(
        classOf[Connection].getClassLoader,
        Array(classOf[Connection]),
        new InvocationHandler {
          override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef = {
            method.getName match {
              case "isClosed" => java.lang.Boolean.FALSE
              case "toString" => "HAConnectionTestConnection"
              case _ =>
                method.getReturnType match {
                  case java.lang.Boolean.TYPE => java.lang.Boolean.FALSE
                  case java.lang.Integer.TYPE => Integer.valueOf(0)
                  case java.lang.Long.TYPE => java.lang.Long.valueOf(0L)
                  case java.lang.Double.TYPE => java.lang.Double.valueOf(0d)
                  case java.lang.Float.TYPE => java.lang.Float.valueOf(0f)
                  case java.lang.Short.TYPE => java.lang.Short.valueOf(0.toShort)
                  case java.lang.Byte.TYPE => java.lang.Byte.valueOf(0.toByte)
                  case java.lang.Character.TYPE => Character.valueOf(0.toChar)
                  case java.lang.Void.TYPE => null
                  case _ => null
                }
            }
          }
        }
      )
      .asInstanceOf[Connection]
  }
}

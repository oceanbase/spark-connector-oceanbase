/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.oceanbase.spark.utils

import com.oceanbase.spark.config.OceanBaseConfig

import org.apache.commons.lang3.StringUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry

import java.sql.{Connection, DriverManager, SQLException}
import java.util.Properties
import java.util.concurrent.TimeUnit

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.FiniteDuration
import scala.util.Random

object OceanBaseConnectionProvider extends Logging {
  private val driverSupportSet: Set[String] =
    Set("com.mysql.jdbc.Driver", "com.mysql.cj.jdbc.Driver", "com.oceanbase.jdbc.Driver")

  private val failedUrls = TrieMap.empty[String, Long]

  def getConnection(oceanBaseConfig: OceanBaseConfig): Connection = {
    try {
      val properties = new Properties()
      properties.put("user", oceanBaseConfig.getUsername)
      properties.put("password", oceanBaseConfig.getPassword)
      properties.put("rewriteBatchedStatements", "true")

      connectWithRetry(oceanBaseConfig, properties)
    } catch {
      case e: Exception => throw new RuntimeException("Failed to obtain connection.", e)
    }
  }

  def parseJdbcUrls(rawUrl: String): Seq[String] = {
    Option(rawUrl)
      .map(_.split(",").map(_.trim).filter(StringUtils.isNotBlank).toSeq)
      .getOrElse(Seq.empty)
  }

  private def connectWithRetry(
      oceanBaseConfig: OceanBaseConfig,
      properties: Properties): Connection = {
    val jdbcUrls = parseJdbcUrls(oceanBaseConfig.getURL)
    if (jdbcUrls.isEmpty) {
      throw new SQLException("No valid JDBC URL was configured.")
    }

    val maxAttempts = oceanBaseConfig.getJdbcConnectionMaxRetries
    val retryInterval =
      FiniteDuration(oceanBaseConfig.getJdbcConnectionRetryIntervalMillis, TimeUnit.MILLISECONDS)

    var lastException: Throwable = null
    var attempt = 0
    while (attempt < maxAttempts) {
      val now = System.currentTimeMillis()
      val orderedUrls = orderedJdbcUrls(jdbcUrls, now)
      for (jdbcUrl <- orderedUrls) {
        try {
          return connectOnce(oceanBaseConfig, jdbcUrl, properties)
        } catch {
          case e: Throwable if isRetryableConnectionException(e) =>
            lastException = e
            markUrlFailed(jdbcUrl, oceanBaseConfig.getJdbcConnectionFailedUrlCooldownMillis)
          case e: Throwable =>
            throw e
        }
      }
      attempt += 1
      if (attempt < maxAttempts) {
        Thread.sleep(retryInterval.toMillis)
      }
    }

    throw new Exception(s"The operation failed after $maxAttempts attempts", lastException)
  }

  private def orderedJdbcUrls(jdbcUrls: Seq[String], now: Long): Seq[String] = {
    val (penalized, healthy) =
      Random.shuffle(jdbcUrls).partition(url => failedUrls.get(url).exists(_ > now))
    healthy ++ penalized
  }

  private def connectOnce(
      oceanBaseConfig: OceanBaseConfig,
      jdbcUrl: String,
      properties: Properties): Connection = {
    val driver = getDriver(oceanBaseConfig, jdbcUrl)
    val connection = DriverRegistry.get(driver).connect(jdbcUrl, properties)
    if (connection == null) {
      throw new SQLException(
        s"The driver could not open a JDBC connection. Check the URL: $jdbcUrl")
    }
    failedUrls.remove(jdbcUrl)
    connection
  }

  private def getDriver(oceanBaseConfig: OceanBaseConfig, jdbcUrl: String): String = {
    val configuredDriver = oceanBaseConfig.getDriver
    val driver =
      if (StringUtils.isBlank(configuredDriver)) {
        DriverManager.getDriver(jdbcUrl).getClass.getCanonicalName
      } else {
        require(
          driverSupportSet.contains(configuredDriver),
          s"Unsupported driver class: $configuredDriver")
        configuredDriver
      }
    DriverRegistry.register(driver)
    driver
  }

  private def markUrlFailed(jdbcUrl: String, cooldownMillis: Long): Unit = {
    failedUrls.put(jdbcUrl, System.currentTimeMillis() + cooldownMillis)
  }

  private def isRetryableConnectionException(exception: Throwable): Boolean = exception match {
    case _: SQLException => true
    case e if e.getCause != null => isRetryableConnectionException(e.getCause)
    case _ => false
  }
}

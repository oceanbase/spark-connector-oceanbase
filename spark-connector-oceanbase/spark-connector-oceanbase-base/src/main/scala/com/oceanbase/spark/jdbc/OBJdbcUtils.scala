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

package com.oceanbase.spark.jdbc

import com.oceanbase.spark.cfg.{ConnectionOptions, SettingUtils, SparkSettings}

import org.apache.spark.sql.jdbc.JdbcDialects

import java.sql.{Connection, DriverManager}

object OBJdbcUtils {
  val OB_MYSQL_URL = s"jdbc:mysql://%s:%d/%s"
  private val OB_ORACLE_URL = s"jdbc:oceanbase://%s:%d/%s"

  def getConnection(sparkSettings: SparkSettings): Connection = {
    val connection = DriverManager.getConnection(
      OB_MYSQL_URL.format(
        sparkSettings.getProperty(ConnectionOptions.HOST),
        sparkSettings.getIntegerProperty(ConnectionOptions.SQL_PORT),
        sparkSettings.getProperty(ConnectionOptions.SCHEMA_NAME)
      ),
      s"${sparkSettings.getProperty(ConnectionOptions.USERNAME)}",
      sparkSettings.getProperty(ConnectionOptions.PASSWORD)
    )
    connection
  }

  def getJdbcUrl(sparkSettings: SparkSettings): String = {
    var url: String = null
    if ("MYSQL".equalsIgnoreCase(getCompatibleMode(sparkSettings))) {
      url = OBJdbcUtils.OB_MYSQL_URL.format(
        sparkSettings.getProperty(ConnectionOptions.HOST),
        sparkSettings.getIntegerProperty(ConnectionOptions.SQL_PORT),
        sparkSettings.getProperty(ConnectionOptions.SCHEMA_NAME)
      )
    } else {
      JdbcDialects.registerDialect(OceanBaseOracleDialect)
      url = OBJdbcUtils.OB_ORACLE_URL.format(
        sparkSettings.getProperty(ConnectionOptions.HOST),
        sparkSettings.getIntegerProperty(ConnectionOptions.SQL_PORT),
        sparkSettings.getProperty(ConnectionOptions.SCHEMA_NAME)
      )
    }
    url
  }

  def getCompatibleMode(sparkSettings: SparkSettings): String = {
    val conn = getConnection(sparkSettings)
    val statement = conn.createStatement
    try {
      val rs = statement.executeQuery("SHOW VARIABLES LIKE 'ob_compatibility_mode'")
      if (rs.next) rs.getString("VALUE") else null
    } finally {
      statement.close()
      conn.close()
    }
  }

  def truncateTable(sparkSettings: SparkSettings): Unit = {
    val conn = getConnection(sparkSettings)
    val statement = conn.createStatement
    try {
      statement.executeUpdate(
        s"truncate table ${sparkSettings.getProperty(ConnectionOptions.SCHEMA_NAME)}.${sparkSettings
            .getProperty(ConnectionOptions.TABLE_NAME)}")
    } finally {
      statement.close()
      conn.close()
    }
  }

}

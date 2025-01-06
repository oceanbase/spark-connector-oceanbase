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

import com.oceanbase.spark.config.OceanBaseConfig

import java.sql.{Connection, DriverManager}

object OBJdbcUtils {

  def getConnection(oceanBaseConfig: OceanBaseConfig): Connection = {
    val connection = DriverManager.getConnection(
      oceanBaseConfig.getURL,
      oceanBaseConfig.getUsername,
      oceanBaseConfig.getPassword
    )
    connection
  }

  def getCompatibleMode(oceanBaseConfig: OceanBaseConfig): String = {
    val conn = getConnection(oceanBaseConfig)
    val statement = conn.createStatement
    try {
      val rs = statement.executeQuery("SHOW VARIABLES LIKE 'ob_compatibility_mode'")
      if (rs.next) rs.getString("VALUE")
      else throw new RuntimeException("Failed to obtain compatible mode of OceanBase.")
    } finally {
      statement.close()
      conn.close()
    }
  }

  def getDbTable(oceanBaseConfig: OceanBaseConfig): String = {
    if ("MySQL".equalsIgnoreCase(getCompatibleMode(oceanBaseConfig))) {
      s"`${oceanBaseConfig.getSchemaName}`.`${oceanBaseConfig.getTableName}`"
    } else {
      s""""${oceanBaseConfig.getSchemaName}"."${oceanBaseConfig.getTableName}""""
    }
  }

  def truncateTable(oceanBaseConfig: OceanBaseConfig): Unit = {
    val conn = getConnection(oceanBaseConfig)
    val statement = conn.createStatement
    try {
      statement.executeUpdate(
        s"truncate table ${oceanBaseConfig.getSchemaName}.${oceanBaseConfig.getTableName}")
    } finally {
      statement.close()
      conn.close()
    }
  }

}

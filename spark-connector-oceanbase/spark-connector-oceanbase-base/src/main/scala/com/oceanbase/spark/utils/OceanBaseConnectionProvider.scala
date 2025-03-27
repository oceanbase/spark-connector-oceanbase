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

import java.sql.{Connection, DriverManager}

object OceanBaseConnectionProvider extends Logging {
  private val driverSupportSet: Set[String] =
    Set("com.mysql.jdbc.driver", "com.mysql.cj.jdbc.driver", "com.oceanbase.jdbc.driver")

  def getConnection(oceanBaseConfig: OceanBaseConfig): Connection = {
    var driver = oceanBaseConfig.getDriver
    try {
      if (StringUtils.isBlank(driver)) {
        driver = DriverManager.getDriver(oceanBaseConfig.getURL).getClass.getCanonicalName
        DriverRegistry.register(driver)
      } else {
        require(driverSupportSet.contains(driver.toLowerCase), s"Unsupported driver class: $driver")
        DriverRegistry.register(driver)
      }
      val connection = DriverManager.getConnection(
        oceanBaseConfig.getURL,
        oceanBaseConfig.getUsername,
        oceanBaseConfig.getPassword
      )
      connection
    } catch {
      case e: Exception => throw new RuntimeException("Failed to obtain connection.", e)
    }
  }
}

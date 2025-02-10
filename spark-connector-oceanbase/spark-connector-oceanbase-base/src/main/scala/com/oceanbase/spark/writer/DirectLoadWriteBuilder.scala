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
package com.oceanbase.spark.writer

import com.oceanbase.spark.catalog.OceanBaseCatalog
import com.oceanbase.spark.config.OceanBaseConfig
import com.oceanbase.spark.utils.OBJdbcUtils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.connector.write.{BatchWrite, SupportsTruncate, V1Write, Write, WriteBuilder}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.sources.InsertableRelation

import scala.collection.JavaConverters.mapAsJavaMapConverter

@Deprecated
case class DirectLoadWriteBuilder(options: JDBCOptions) extends WriteBuilder with SupportsTruncate {
  private var isTruncate = false

  override def truncate(): WriteBuilder = {
    isTruncate = true
    this
  }

  override def build(): V1Write = new V1Write {
    // Note: Do not rewrite to lambda format to ensure compatibility with Scala 2.11 version
    override def toInsertableRelation: InsertableRelation = new InsertableRelation {
      override def insert(data: DataFrame, overwrite: Boolean): Unit = {
        if (isTruncate) {
          OBJdbcUtils.withConnection(options) {
            conn =>
              {
                OBJdbcUtils.executeStatement(
                  conn,
                  options,
                  s"TRUNCATE TABLE ${options.parameters(JDBCOptions.JDBC_TABLE_NAME)}")
              }
          }
        }
        val map = options.parameters ++ Map(
          OceanBaseConfig.SCHEMA_NAME.getKey -> options.parameters(
            OceanBaseCatalog.CURRENT_DATABASE),
          OceanBaseConfig.TABLE_NAME.getKey -> options.parameters(OceanBaseCatalog.CURRENT_TABLE)
        )
        DirectLoadWriter.savaTable(data, new OceanBaseConfig(map.asJava))
      }
    }
  }
}

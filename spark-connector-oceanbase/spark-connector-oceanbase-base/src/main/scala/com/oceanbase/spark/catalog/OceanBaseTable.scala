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
package com.oceanbase.spark.catalog

import com.oceanbase.spark.config.OceanBaseConfig
import com.oceanbase.spark.dialect.OceanBaseDialect
import com.oceanbase.spark.reader.JDBCLimitScanBuilder
import com.oceanbase.spark.reader.v2.OBJdbcScanBuilder
import com.oceanbase.spark.writer.v2.{DirectLoadWriteBuilderV2, JDBCWriteBuilder}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

import scala.collection.JavaConverters._

case class OceanBaseTable(
    ident: Identifier,
    schema: StructType,
    config: OceanBaseConfig,
    dialect: OceanBaseDialect)
  extends Table
  with SupportsRead
  with SupportsWrite {

  override def name(): String = ident.toString

  override def capabilities(): util.Set[TableCapability] = {
    util.EnumSet.of(BATCH_READ, BATCH_WRITE, TRUNCATE)
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    val mergedOptions = new JDBCOptions(
      config.getProperties.asScala.toMap ++ options.asCaseSensitiveMap().asScala)

    mergedOptions.parameters
      .get(OceanBaseConfig.ENABLE_LEGACY_BATCH_READER)
      .map(_.toBoolean) match {
      case Some(true) => JDBCLimitScanBuilder(SparkSession.active, schema, mergedOptions)
      case _ => OBJdbcScanBuilder(schema, config, dialect)
    }
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {

    if (config.getDirectLoadEnable) {
      DirectLoadWriteBuilderV2(schema, config)
    } else {
      new JDBCWriteBuilder(schema, config, dialect)
    }
  }
}

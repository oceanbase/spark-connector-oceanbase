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
import com.oceanbase.spark.dialect.{OceanBaseDialect, OceanBaseMySQLDialect, OceanBaseOracleDialect}

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

import scala.collection.JavaConverters._

object OceanBaseSourceUtils {

  case class ResolvedSourceTable(
      ident: Identifier,
      schema: StructType,
      config: OceanBaseConfig,
      dialect: OceanBaseDialect)

  def resolveTable(options: CaseInsensitiveStringMap): ResolvedSourceTable = {
    val config = normalizeConfig(options)
    val dialect = createDialect(config)
    val ident = Identifier.of(namespace(config), config.getTableName)
    val schema = OceanBaseCatalog.resolveTable(config, dialect)
    ResolvedSourceTable(ident, schema, config, dialect)
  }

  def normalizeConfig(options: CaseInsensitiveStringMap): OceanBaseConfig = {
    val properties = new util.HashMap[String, String]()
    options.asCaseSensitiveMap().asScala.foreach {
      case (key, value) =>
        properties.put(key, value)
    }

    val rawConfig = new OceanBaseConfig(properties)
    val schemaName =
      Option(rawConfig.getSchemaName)
        .filter(StringUtils.isNotBlank)
        .orElse(OceanBaseCatalog.extractDatabaseName(rawConfig.getURL))
        .getOrElse("")
    if (StringUtils.isNotBlank(schemaName)) {
      properties.put(OceanBaseConfig.SCHEMA_NAME.getKey, schemaName)
    }

    val config = new OceanBaseConfig(properties)
    val dialect = createDialect(config)
    properties.put(OceanBaseConfig.DB_TABLE, quotedTableName(config, dialect))
    new OceanBaseConfig(properties)
  }

  def createDialect(config: OceanBaseConfig): OceanBaseDialect = {
    OBJdbcUtils.getCompatibleMode(config).map(_.toLowerCase) match {
      case Some("oracle") => new OceanBaseOracleDialect
      case _ => new OceanBaseMySQLDialect
    }
  }

  private def namespace(config: OceanBaseConfig): Array[String] = {
    Option(config.getSchemaName).filter(StringUtils.isNotBlank).map(Array(_)).getOrElse(Array.empty)
  }

  private def quotedTableName(config: OceanBaseConfig, dialect: OceanBaseDialect): String = {
    (namespace(config) :+ config.getTableName).map(dialect.quoteIdentifier).mkString(".")
  }
}

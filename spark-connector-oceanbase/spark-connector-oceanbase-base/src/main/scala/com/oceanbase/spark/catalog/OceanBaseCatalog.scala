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

import com.oceanbase.spark.catalog.OceanBaseCatalog.{extractDatabaseName, resolveTable}
import com.oceanbase.spark.config.OceanBaseConfig
import com.oceanbase.spark.dialect.{OceanBaseDialect, OceanBaseMySQLDialect, OceanBaseOracleDialect}
import com.oceanbase.spark.utils.OBJdbcUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.sql.SQLException

import scala.collection.JavaConverters._
import scala.collection.mutable

class OceanBaseCatalog
  extends TableCatalog
  with SupportsNamespaces
  with SQLConfHelper
  with Logging {

  private var catalogName: Option[String] = None
  private var config: OceanBaseConfig = _
  private var dialect: OceanBaseDialect = _

  override def name(): String = {
    require(catalogName.nonEmpty, "The OceanBase catalog is not initialed")
    catalogName.get
  }

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    assert(catalogName.isEmpty, "The OceanBase catalog is already initialed")
    catalogName = Some(name)
    config = new OceanBaseConfig(options)

    // Register dialect for mysql and oracle modes.
    OBJdbcUtils.getCompatibleMode(this.config).map(_.toLowerCase) match {
      case Some("mysql") =>
        dialect = new OceanBaseMySQLDialect
      case Some("oracle") =>
        dialect = new OceanBaseOracleDialect
      case _ =>
    }
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    checkNamespace(namespace)
    val config = genNewOceanBaseConfig(this.config, namespace)
    OBJdbcUtils.withConnection(config) {
      conn =>
        val schemaPattern = if (namespace.length == 1) namespace.head else null
        val rs = conn.getMetaData
          .getTables(schemaPattern, "%", "%", Array("TABLE"));
        new Iterator[Identifier] {
          def hasNext: Boolean = rs.next()
          def next(): Identifier = Identifier.of(namespace, rs.getString("TABLE_NAME"))
        }.toArray
    }
  }

  override def tableExists(ident: Identifier): Boolean = {
    checkNamespace(ident.namespace())
    val config = genNewOceanBaseConfig(this.config, ident)
    OBJdbcUtils.unifiedCatalogException(s"Failed table existence check: $ident") {
      OBJdbcUtils.withConnection(config)(dialect.tableExists(_, config))
    }
  }

  override def dropTable(ident: Identifier): Boolean = {
    checkNamespace(ident.namespace())
    val config = genNewOceanBaseConfig(this.config, ident)
    OBJdbcUtils.withConnection(config) {
      conn =>
        try {
          dialect.dropTable(conn, getTableName(ident), config)
          true
        } catch {
          case ex: SQLException =>
            throw OceanBaseCatalogException(s"Failed to drop table ${getTableName(ident)}", ex)
        }
    }
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    checkNamespace(oldIdent.namespace())
    OBJdbcUtils.withConnection(config) {
      conn =>
        OBJdbcUtils.unifiedCatalogException(s"Failed table renaming from $oldIdent to $newIdent") {
          dialect.renameTable(conn, getTableName(oldIdent), getTableName(newIdent), config)
        }
    }
  }

  override def loadTable(ident: Identifier): Table = {
    checkNamespace(ident.namespace())
    val config = genNewOceanBaseConfig(this.config, ident)
    try {
      val schema = resolveTable(config, dialect)
      OceanBaseTable(ident, schema, config, dialect)
    } catch {
      case ex: IllegalArgumentException =>
        throw new IllegalArgumentException(
          s"OceanBase catalog failed to load table: ${ident.toString}",
          ex)
    }
  }

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: java.util.Map[String, String]): Table = {
    checkNamespace(ident.namespace())
    // TODO: Supports two-level partitioning.
    if (partitions.length >= 2) {
      throw OceanBaseCatalogException(
        s"The OceanBase catalog does not yet support secondary or higher-level partitioning")
    }

    var tableComment: String = ""
    if (!properties.isEmpty) {
      properties.asScala.foreach {
        case (k, v) =>
          k match {
            case TableCatalog.PROP_COMMENT => tableComment = v
            case TableCatalog.PROP_PROVIDER =>
              v match {
                case v if v.toLowerCase.equals("oceanbase") =>
                case _ => throw new IllegalArgumentException(s"Unknown provider: $v")
              }
            case TableCatalog.PROP_OWNER => // owner is ignored. It is default to current user name.
            case TableCatalog.PROP_LOCATION =>
              throw new UnsupportedOperationException("This syntax is not currently supported.")
            case _ =>
          }
      }
    }
    val config = genNewOceanBaseConfig(this.config, ident)
    if (tableComment.nonEmpty) {
      config.setProperty(OceanBaseConfig.TABLE_COMMENT, tableComment)
    }
    OBJdbcUtils.withConnection(config) {
      conn =>
        OBJdbcUtils.unifiedCatalogException(s"Failed table creation: $ident") {
          dialect.createTable(conn, getTableName(ident), schema, partitions, config, properties)
        }
    }

    OceanBaseTable(ident, schema, config, dialect)
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    checkNamespace(ident.namespace())
    val config = genNewOceanBaseConfig(this.config, ident)
    OBJdbcUtils.withConnection(config) {
      conn =>
        OBJdbcUtils.unifiedCatalogException(s"Failed table altering: $ident") {
          dialect.alterTable(conn, getTableName(ident), changes, config)
        }
        loadTable(ident)
    }
  }

  override def namespaceExists(namespace: Array[String]): Boolean = namespace match {
    case Array(db) =>
      OBJdbcUtils.withConnection(config)(conn => dialect.schemaExists(conn, config, db))
    case _ => false
  }

  override def listNamespaces(): Array[Array[String]] = {
    OBJdbcUtils.withConnection(config)(conn => dialect.listSchemas(conn, config))
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    namespace match {
      case Array() =>
        listNamespaces()
      case Array(_) if namespaceExists(namespace) =>
        Array()
      case _ =>
        throw new IllegalArgumentException(s"Unknown namespace: ${namespace.mkString(".")}")
    }
  }

  override def loadNamespaceMetadata(namespace: Array[String]): java.util.Map[String, String] = {
    namespace match {
      case Array(db) =>
        if (!namespaceExists(namespace)) {
          throw new IllegalArgumentException(Array(db).mkString("."))
        }
        mutable.HashMap[String, String]().asJava

      case _ =>
        throw new IllegalArgumentException(s"Unknown namespace: ${namespace.mkString(".")}")
    }
  }

  override def createNamespace(
      namespace: Array[String],
      metadata: java.util.Map[String, String]): Unit =
    namespace match {
      case Array(db) if !namespaceExists(namespace) =>
        var comment = ""
        if (!metadata.isEmpty) {
          metadata.asScala.foreach {
            case (k, v) =>
              k match {
                case SupportsNamespaces.PROP_COMMENT => comment = v
                case SupportsNamespaces.PROP_OWNER => // ignore
                case SupportsNamespaces.PROP_LOCATION =>
                  throw new UnsupportedOperationException("This syntax is not currently supported.")
                case _ =>
                  throw new UnsupportedOperationException("This syntax is not currently supported.")
              }
          }
        }
        OBJdbcUtils.withConnection(config) {
          conn =>
            OBJdbcUtils.unifiedCatalogException(s"Failed create name space: $db") {
              dialect.createSchema(conn, config, db, comment)
            }
        }

      case Array(_) =>
        throw throw new IllegalArgumentException(
          s"The namespace already exists: ${namespace.mkString(".")}")

      case _ =>
        throw throw new IllegalArgumentException(s"Unknown namespace: ${namespace.mkString(".")}")
    }

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = {
    throw new UnsupportedOperationException("OceanBase does not support alter namespace property.")
  }

  /** Note: Do not add the override keyword to ensure compatibility with older Spark versions. */
  def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean =
    namespace match {
      case Array(db) if namespaceExists(namespace) =>
        OBJdbcUtils.withConnection(config) {
          conn =>
            OBJdbcUtils.unifiedCatalogException(s"Failed drop name space: $db") {
              dialect.dropSchema(conn, config, db, cascade)
              true
            }
        }

      case _ =>
        throw new UnsupportedOperationException(namespace.mkString("."))
    }

  /** This is for compatibility with older Spark versions. */
  def dropNamespace(namespace: Array[String]): Boolean = {
    namespace match {
      case Array(db) if namespaceExists(namespace) =>
        OBJdbcUtils.withConnection(config) {
          conn =>
            OBJdbcUtils.unifiedCatalogException(s"Failed to drop namespace: $db") {
              dialect.dropSchema(conn, config, db, cascade = false)
              true
            }
        }

      case _ =>
        throw new UnsupportedOperationException(namespace.mkString("."))
    }
  }

  private def checkNamespace(namespace: Array[String]): Unit = {
    // In JDBC there is no nested database/schema
    if (namespace.length > 1) {
      throw new UnsupportedOperationException(namespace.mkString("."))

    }
  }

  private def getTableName(ident: Identifier): String = {
    (ident.namespace() :+ ident.name()).map(dialect.quoteIdentifier).mkString(".")
  }

  private def genNewOceanBaseConfig(config: OceanBaseConfig, ident: Identifier): OceanBaseConfig = {
    val properties = config.getProperties
    properties.put(OceanBaseConfig.SCHEMA_NAME.getKey, ident.namespace().mkString("."))
    properties.put(OceanBaseConfig.TABLE_NAME.getKey, ident.name())
    properties.put(OceanBaseConfig.DB_TABLE, getTableName(ident))
    new OceanBaseConfig(properties)
  }

  private def genNewOceanBaseConfig(
      config: OceanBaseConfig,
      namespace: Array[String]): OceanBaseConfig = {
    val properties = config.getProperties
    properties.put(OceanBaseConfig.SCHEMA_NAME.getKey, namespace.mkString("."))
    new OceanBaseConfig(properties)
  }

  override def defaultNamespace(): Array[String] = {
    Option(config.getSchemaName) match {
      case None =>
        extractDatabaseName(config.getURL) match {
          case None => Array[String]()
          case Some(value) => Array[String](value)
        }
      case Some(value) => Array[String](value)
    }
  }

}

object OceanBaseCatalog {

  def extractDatabaseName(jdbcUrl: String): Option[String] = {
    val pattern = "(?i)^jdbc:(mysql|oceanbase)://[^/]+/([^?]+).*".r

    jdbcUrl match {
      case pattern(_, databaseName) => Some(databaseName)
      case _ => None
    }
  }

  /** Takes a (schema, table) specification and returns the table's Catalyst schema. */
  def resolveTable(config: OceanBaseConfig, dialect: OceanBaseDialect): StructType = {
    OBJdbcUtils.withConnection(config) {
      conn =>
        OBJdbcUtils.executeQuery(conn, config, dialect.getSchemaQuery(config.getDbTable))(
          rs => OBJdbcUtils.getSchema(rs, dialect))
    }
  }
}

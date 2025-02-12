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

import com.oceanbase.spark.catalog.OceanBaseCatalog.{extractDatabaseName, preDefineMap, CURRENT_DATABASE, CURRENT_TABLE, DEFAULT_DATABASE}
import com.oceanbase.spark.config.OceanBaseConfig
import com.oceanbase.spark.dialect.{OceanBaseDialect, OceanBaseMySQLDialect, OceanBaseOracleDialect}
import com.oceanbase.spark.utils.OBJdbcUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.OceanBaseSparkDataSource.{JDBC_TXN_ISOLATION_LEVEL, OCEANBASE_DEFAULT_ISOLATION_LEVEL}
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcOptionsInWrite}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.jdbc.{JdbcDialects, OceanBaseMySQLDialect, OceanBaseOracleDialect}
import org.apache.spark.sql.reader.JDBCLimitRDD
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
  private var options: JDBCOptions = _
  private var dialect: OceanBaseDialect = _

  override def name(): String = {
    require(catalogName.nonEmpty, "The OceanBase catalog is not initialed")
    catalogName.get
  }

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    assert(catalogName.isEmpty, "The OceanBase catalog is already initialed")
    catalogName = Some(name)

    val map = options.asCaseSensitiveMap().asScala.toMap
    this.options = new JDBCOptions(map ++ preDefineMap(map))
    // Register dialect for mysql and oracle modes.
    OBJdbcUtils.getCompatibleMode(this.options).map(_.toLowerCase) match {
      case Some("mysql") =>
        dialect = new OceanBaseMySQLDialect
        // TODO: Refactor BATCH_READ implements to remove the code
        JdbcDialects.registerDialect(OceanBaseMySQLDialect)
      case Some("oracle") =>
        dialect = new OceanBaseOracleDialect
        JdbcDialects.registerDialect(OceanBaseOracleDialect)
      case _ =>
    }
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    checkNamespace(namespace)
    OBJdbcUtils.withConnection(options) {
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
    val writeOptions = new JdbcOptionsInWrite(
      options.parameters + (JDBCOptions.JDBC_TABLE_NAME -> getTableName(ident)))
    OBJdbcUtils.unifiedCatalogException(s"Failed table existence check: $ident") {
      OBJdbcUtils.withConnection(options)(dialect.tableExists(_, writeOptions))
    }
  }

  override def dropTable(ident: Identifier): Boolean = {
    checkNamespace(ident.namespace())
    OBJdbcUtils.withConnection(options) {
      conn =>
        try {
          dialect.dropTable(conn, getTableName(ident), options)
          true
        } catch {
          case ex: SQLException =>
            throw OceanBaseCatalogException(s"Failed to drop table ${getTableName(ident)}", ex)
        }
    }
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    checkNamespace(oldIdent.namespace())
    OBJdbcUtils.withConnection(options) {
      conn =>
        OBJdbcUtils.unifiedCatalogException(s"Failed table renaming from $oldIdent to $newIdent") {
          dialect.renameTable(conn, getTableName(oldIdent), getTableName(newIdent), options)
        }
    }
  }

  override def loadTable(ident: Identifier): Table = {
    checkNamespace(ident.namespace())
    val optionsWithTableName = new JDBCOptions(
      options.parameters ++ mutable.Map(
        JDBCOptions.JDBC_TABLE_NAME -> getTableName(ident),
        CURRENT_DATABASE -> ident.namespace().mkString("."),
        CURRENT_TABLE -> ident.name()))
    try {
      val schema = JDBCLimitRDD.resolveTable(optionsWithTableName)
      OceanBaseTable(ident, schema, optionsWithTableName, dialect)
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
    // TODO: Support create table with partitions
    if (partitions.nonEmpty) {
      throw new UnsupportedOperationException(
        s"OceanBase catalog current not support create partition table: ${ident.toString}")
    }

    var tableOptions = options.parameters + (JDBCOptions.JDBC_TABLE_NAME -> getTableName(ident))
    var tableComment: String = ""
    var tableProperties: String = ""
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
            case _ => tableProperties = tableProperties + " " + s"$k $v"
          }
      }
    }

    if (tableComment != "") {
      tableOptions = tableOptions + (JDBCOptions.JDBC_TABLE_COMMENT -> tableComment)
    }
    if (tableProperties != "") {
      // table property is set in JDBC_CREATE_TABLE_OPTIONS, which will be appended
      // to CREATE TABLE statement.
      // E.g., "CREATE TABLE t (name string) ENGINE InnoDB DEFAULT CHARACTER SET utf8"
      // Spark doesn't check if these table properties are supported by databases. If
      // table property is invalid, database will fail the table creation.
      tableOptions = tableOptions + (JDBCOptions.JDBC_CREATE_TABLE_OPTIONS -> tableProperties)
    }

    val writeOptions = new JdbcOptionsInWrite(tableOptions)
    val caseSensitive = SQLConf.get.caseSensitiveAnalysis
    OBJdbcUtils.withConnection(options) {
      conn =>
        OBJdbcUtils.unifiedCatalogException(s"Failed table creation: $ident") {
          dialect.createTable(conn, getTableName(ident), schema, caseSensitive, writeOptions)
        }
    }

    OceanBaseTable(ident, schema, writeOptions, dialect)
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    checkNamespace(ident.namespace())
    OBJdbcUtils.withConnection(options) {
      conn =>
        OBJdbcUtils.unifiedCatalogException(s"Failed table altering: $ident") {
          dialect.alterTable(conn, getTableName(ident), changes, options)
        }
        loadTable(ident)
    }
  }

  override def namespaceExists(namespace: Array[String]): Boolean = namespace match {
    case Array(db) =>
      OBJdbcUtils.withConnection(options)(conn => dialect.schemaExists(conn, options, db))
    case _ => false
  }

  override def listNamespaces(): Array[Array[String]] = {
    OBJdbcUtils.withConnection(options)(conn => dialect.listSchemas(conn, options))
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
        OBJdbcUtils.withConnection(options) {
          conn =>
            OBJdbcUtils.unifiedCatalogException(s"Failed create name space: $db") {
              dialect.createSchema(conn, options, db, comment)
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
        OBJdbcUtils.withConnection(options) {
          conn =>
            OBJdbcUtils.unifiedCatalogException(s"Failed drop name space: $db") {
              dialect.dropSchema(conn, options, db, cascade)
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
        OBJdbcUtils.withConnection(options) {
          conn =>
            OBJdbcUtils.unifiedCatalogException(s"Failed to drop namespace: $db") {
              dialect.dropSchema(conn, options, db, cascade = false)
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

  override def defaultNamespace(): Array[String] = {
    options.parameters.get(DEFAULT_DATABASE) match {
      case None =>
        extractDatabaseName(options.url) match {
          case None => Array[String]()
          case Some(value) => Array[String](value)
        }
      case Some(value) => Array[String](value)
    }
  }

}

object OceanBaseCatalog {
  private val FAKE_TABLE_NAME = "__invalid_table"
  private val DEFAULT_DATABASE = "schema-name"
  private val ADAPT_USERNAME = "user"
  val CURRENT_DATABASE = "current-database"
  val CURRENT_TABLE = "current-table"

  private val JDBC_PUSH_DOWN_LIMIT = "pushDownLimit"
  private val JDBC_PUSH_DOWN_OFFSET = "pushDownOffset"
  private val JDBC_PUSH_DOWN_TABLES_AMPLE = "pushDownTableSample"

  // 1. Disable agg push down 2. Disable limit & offset push down
  // 3. Enable fetchSize  4. Disable table sample 5. Enable predicates push down
  def preDefineMap(options: Map[String, String]): Map[String, String] = {
    Map(
      JDBC_PUSH_DOWN_LIMIT -> false.toString,
      JDBC_PUSH_DOWN_OFFSET -> false.toString,
      JDBCOptions.JDBC_BATCH_FETCH_SIZE -> options.getOrElse(
        JDBCOptions.JDBC_BATCH_FETCH_SIZE,
        1000.toString),
      JDBC_PUSH_DOWN_TABLES_AMPLE -> false.toString,
      JDBCOptions.JDBC_TABLE_NAME -> FAKE_TABLE_NAME,
      ADAPT_USERNAME -> options(OceanBaseConfig.USERNAME.getKey),
      JDBC_TXN_ISOLATION_LEVEL -> options
        .getOrElse(JDBC_TXN_ISOLATION_LEVEL, OCEANBASE_DEFAULT_ISOLATION_LEVEL)
    )
  }

  def extractDatabaseName(jdbcUrl: String): Option[String] = {
    val pattern = "(?i)^jdbc:(mysql|oceanbase)://[^/]+/([^?]+).*".r

    jdbcUrl match {
      case pattern(_, databaseName) => Some(databaseName)
      case _ => None
    }
  }
}

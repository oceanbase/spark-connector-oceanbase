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
package org.apache.spark.sql

import com.oceanbase.spark.catalog.OceanBaseTable
import com.oceanbase.spark.config.OceanBaseConfig
import com.oceanbase.spark.utils.{OBJdbcUtils, OceanBaseSourceUtils}
import com.oceanbase.spark.utils.OBJdbcUtils.{getCompatibleMode, getDbTable}
import com.oceanbase.spark.writer.DirectLoadWriter

import OceanBaseSparkDataSource.{buildJDBCOptions, isV1Read, writeDataViaDirectLoad, ENABLE_V2_READER, JDBC_TXN_ISOLATION_LEVEL, JDBC_URL, JDBC_USER, OCEANBASE_DEFAULT_ISOLATION_LEVEL, SHORT_NAME}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table => ConnectorTable, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns, V1Scan}
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCRelation, JdbcRelationProvider}
import org.apache.spark.sql.jdbc.{JdbcDialects, OceanBaseMySQLDialect, OceanBaseOracleDialect}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util.{EnumSet, Map => JMap, Set => JSet}

import scala.collection.JavaConverters._

class OceanBaseSparkDataSource extends JdbcRelationProvider with TableProvider {

  override def shortName(): String = SHORT_NAME

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    if (isV1Read(options)) {
      OceanBaseSparkDataSource.resolveV1Schema(options)
    } else {
      OceanBaseSourceUtils.resolveTable(options).schema
    }
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: JMap[String, String]): ConnectorTable = {
    val options = new CaseInsensitiveStringMap(properties)
    if (isV1Read(options)) {
      OceanBaseLegacyTable(schema, properties.asScala.toMap)
    } else {
      val resolved = OceanBaseSourceUtils.resolveTable(options)
      OceanBaseTable(resolved.ident, resolved.schema, resolved.config, resolved.dialect)
    }
  }

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    val oceanBaseConfig = new OceanBaseConfig(parameters.asJava)
    val jdbcOptions = buildJDBCOptions(parameters, oceanBaseConfig)._1
    val resolver = sqlContext.conf.resolver
    val timeZoneId = sqlContext.conf.sessionLocalTimeZone
    val schema = JDBCRelation.getSchema(resolver, jdbcOptions)
    val parts = JDBCRelation.columnPartition(schema, resolver, timeZoneId, jdbcOptions)
    new OceanBaseJDBCRelation(schema, parts, jdbcOptions)(sqlContext.sparkSession)
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      dataFrame: DataFrame): BaseRelation = {
    val oceanBaseConfig = new OceanBaseConfig(parameters.asJava)
    val enableDirectLoadWrite = oceanBaseConfig.getDirectLoadEnable
    if (!enableDirectLoadWrite) {
      val param = buildJDBCOptions(parameters, oceanBaseConfig)._2
      super.createRelation(sqlContext, mode, param, dataFrame)
    } else {
      writeDataViaDirectLoad(mode, dataFrame, oceanBaseConfig)
      createRelation(sqlContext, parameters)
    }
  }

}

object OceanBaseSparkDataSource {
  val SHORT_NAME: String = "oceanbase"
  val ENABLE_V2_READER = "enable_v2_reader"
  val JDBC_URL = "url"
  val JDBC_USER = "user"
  val JDBC_TXN_ISOLATION_LEVEL = "isolationLevel"
  val OCEANBASE_DEFAULT_ISOLATION_LEVEL = "READ_COMMITTED"

  def isV1Read(options: CaseInsensitiveStringMap): Boolean = {
    !options.getBoolean(ENABLE_V2_READER, true) || options.containsKey(
      JDBCOptions.JDBC_QUERY_STRING)
  }

  def resolveV1Schema(options: CaseInsensitiveStringMap): StructType = {
    val parameters = options.asCaseSensitiveMap().asScala.toMap
    val oceanBaseConfig = new OceanBaseConfig(parameters.asJava)
    val jdbcOptions = buildJDBCOptions(parameters, oceanBaseConfig)._1
    JDBCRelation.getSchema(SparkSession.active.sqlContext.conf.resolver, jdbcOptions)
  }

  def buildV1Relation(
      parameters: Map[String, String],
      schema: StructType): OceanBaseJDBCRelation = {
    val session = SparkSession.active
    val oceanBaseConfig = new OceanBaseConfig(parameters.asJava)
    val jdbcOptions = buildJDBCOptions(parameters, oceanBaseConfig)._1
    val resolver = session.sqlContext.conf.resolver
    val timeZoneId = session.sqlContext.conf.sessionLocalTimeZone
    val parts = JDBCRelation.columnPartition(schema, resolver, timeZoneId, jdbcOptions)
    new OceanBaseJDBCRelation(schema, parts, jdbcOptions)(session)
  }

  def buildJDBCOptions(
      parameters: Map[String, String],
      oceanBaseConfig: OceanBaseConfig): (JDBCOptions, Map[String, String]) = {
    var paraMap = parameters ++ Map(
      JDBC_URL -> oceanBaseConfig.getURL,
      JDBC_USER -> parameters(OceanBaseConfig.USERNAME.getKey),
      JDBC_TXN_ISOLATION_LEVEL -> parameters.getOrElse(
        JDBC_TXN_ISOLATION_LEVEL,
        OCEANBASE_DEFAULT_ISOLATION_LEVEL)
    )
    // It is not allowed to specify dbtable and query options at the same time.
    if (parameters.contains(JDBCOptions.JDBC_QUERY_STRING)) {
      paraMap =
        paraMap + (JDBCOptions.JDBC_QUERY_STRING -> parameters(JDBCOptions.JDBC_QUERY_STRING))
    } else {
      paraMap = paraMap + (JDBCOptions.JDBC_TABLE_NAME -> getDbTable(oceanBaseConfig))
    }

    // Set dialect
    getCompatibleMode(oceanBaseConfig).map(_.toLowerCase) match {
      case Some("oracle") => JdbcDialects.registerDialect(OceanBaseOracleDialect)
      case _ => JdbcDialects.registerDialect(OceanBaseMySQLDialect)
    }

    (new JDBCOptions(paraMap), paraMap)
  }

  /**
   * Writes DataFrame data using OceanBase's direct-load feature
   *   - Append mode: Directly appends data without pre-checking
   *   - Overwrite mode: Truncates target table before writing
   *   - Other modes: Throws NotImplementedError
   *
   * @param mode
   *   Spark SaveMode specifying write behavior
   * @param dataFrame
   *   DataFrame containing data to write
   * @param oceanBaseConfig
   *   Configuration for OceanBase connection
   * @note
   *   Direct load is OceanBase's high-performance data loading feature that writes directly to
   *   storage layer
   */
  def writeDataViaDirectLoad(
      mode: SaveMode,
      dataFrame: DataFrame,
      oceanBaseConfig: OceanBaseConfig): Unit = {
    mode match {
      case sql.SaveMode.Append => // do nothing
      case sql.SaveMode.Overwrite =>
        OBJdbcUtils.truncateTable(oceanBaseConfig)
      case _ =>
        throw new NotImplementedError(s"${mode.name()} mode is not currently supported.")
    }
    DirectLoadWriter.savaTable(dataFrame, oceanBaseConfig)
  }
}

case class OceanBaseLegacyTable(schema: StructType, parameters: Map[String, String])
  extends ConnectorTable
  with SupportsRead {

  override def name(): String = {
    parameters
      .get(JDBCOptions.JDBC_TABLE_NAME)
      .orElse(parameters.get(OceanBaseConfig.TABLE_NAME.getKey))
      .orElse(parameters.get(JDBCOptions.JDBC_QUERY_STRING).map(query => s"($query)"))
      .getOrElse(OceanBaseSparkDataSource.SHORT_NAME)
  }

  override def capabilities(): JSet[TableCapability] =
    EnumSet.of(TableCapability.BATCH_READ)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    val mergedParameters = parameters ++ options.asCaseSensitiveMap().asScala
    OceanBaseLegacyScanBuilder(schema, mergedParameters)
  }
}

case class OceanBaseLegacyScanBuilder(schema: StructType, parameters: Map[String, String])
  extends ScanBuilder
  with SupportsPushDownFilters
  with SupportsPushDownRequiredColumns {

  private lazy val relation = OceanBaseSparkDataSource.buildV1Relation(parameters, schema)
  private var pushedFilter = Array.empty[Filter]
  private var finalSchema = schema

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val unhandled = relation.unhandledFilters(filters)
    pushedFilter = filters.filterNot(filter => unhandled.contains(filter))
    unhandled
  }

  override def pushedFilters(): Array[Filter] = pushedFilter

  override def pruneColumns(requiredSchema: StructType): Unit = {
    val requiredCols = requiredSchema.map(_.name).toSet
    finalSchema = StructType(schema.filter(field => requiredCols.contains(field.name)))
  }

  override def build(): Scan = OceanBaseLegacyScan(relation, finalSchema, pushedFilter)
}

case class OceanBaseLegacyScan(
    relation: OceanBaseJDBCRelation,
    schema: StructType,
    pushedFilters: Array[Filter])
  extends V1Scan {

  override def readSchema(): StructType = schema

  override def toV1TableScan[T <: BaseRelation with TableScan](context: SQLContext): T = {
    new BaseRelation with TableScan {
      override def sqlContext: SQLContext = context
      override def schema: StructType = OceanBaseLegacyScan.this.schema
      override def needConversion: Boolean = relation.needConversion

      override def buildScan(): RDD[Row] =
        relation.buildScan(OceanBaseLegacyScan.this.schema.fieldNames, pushedFilters)
    }.asInstanceOf[T]
  }
}

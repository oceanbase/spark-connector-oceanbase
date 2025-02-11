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

import com.oceanbase.spark.config.OceanBaseConfig
import com.oceanbase.spark.utils.OBJdbcUtils
import com.oceanbase.spark.utils.OBJdbcUtils.{getCompatibleMode, getDbTable}
import com.oceanbase.spark.writer.DirectLoadWriter

import OceanBaseSparkDataSource.{saveTableBasedDirectLoad, JDBC_TXN_ISOLATION_LEVEL, JDBC_URL, JDBC_USER, OCEANBASE_DEFAULT_ISOLATION_LEVEL, SHORT_NAME}
import org.apache.spark.sql
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCRelation, JdbcRelationProvider}
import org.apache.spark.sql.jdbc.{JdbcDialects, OceanBaseMySQLDialect, OceanBaseOracleDialect}
import org.apache.spark.sql.sources._

import scala.collection.JavaConverters.mapAsJavaMapConverter

class OceanBaseSparkDataSource extends JdbcRelationProvider {

  override def shortName(): String = SHORT_NAME

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
      saveTableBasedDirectLoad(mode, dataFrame, oceanBaseConfig)
      createRelation(sqlContext, parameters)
    }
  }

  private def buildJDBCOptions(
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
    if ("MySQL".equalsIgnoreCase(getCompatibleMode(oceanBaseConfig))) {
      JdbcDialects.registerDialect(OceanBaseMySQLDialect)
    } else {
      JdbcDialects.registerDialect(OceanBaseOracleDialect)
    }
    (new JDBCOptions(paraMap), paraMap)
  }
}

object OceanBaseSparkDataSource {
  val SHORT_NAME: String = "oceanbase"
  val JDBC_URL = "url"
  val JDBC_USER = "user"
  val JDBC_TXN_ISOLATION_LEVEL = "isolationLevel"
  val OCEANBASE_DEFAULT_ISOLATION_LEVEL = "READ_COMMITTED"

  def saveTableBasedDirectLoad(
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

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

import com.oceanbase.spark.cfg.{ConnectionOptions, SparkSettings}
import com.oceanbase.spark.jdbc.OBJdbcUtils.{getCompatibleMode, getJdbcUrl, OB_MYSQL_URL}
import com.oceanbase.spark.sql.OceanBaseSparkSource

import OceanBaseJdbcSparkSource.SHORT_NAME
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCRelation, JdbcRelationProvider}
import org.apache.spark.sql.sources._

import scala.collection.JavaConverters.mapAsJavaMapConverter

class OceanBaseJdbcSparkSource extends JdbcRelationProvider {

  override def shortName(): String = SHORT_NAME

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    val sparkSettings = new SparkSettings(sqlContext.sparkContext.getConf)
    sparkSettings.merge(parameters.asJava)
    val jdbcOptions = buildJDBCOptions(parameters, sparkSettings)._1
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
    val sparkSettings = new SparkSettings(sqlContext.sparkContext.getConf)
    sparkSettings.merge(parameters.asJava)
    val enableDirectLoadWrite = sparkSettings.getBooleanProperty(
      ConnectionOptions.ENABLE_DIRECT_LOAD_WRITE,
      ConnectionOptions.ENABLE_DIRECT_LOAD_WRITE_DEFAULT)
    if (!enableDirectLoadWrite) {
      val param = buildJDBCOptions(parameters, sparkSettings)._2
      super.createRelation(sqlContext, mode, param, dataFrame)
    } else {
      OceanBaseSparkSource.createDirectLoadRelation(sqlContext, mode, dataFrame, sparkSettings)
      createRelation(sqlContext, parameters)
    }
  }

  private def buildJDBCOptions(
      parameters: Map[String, String],
      sparkSettings: SparkSettings): (JDBCOptions, Map[String, String]) = {
    val url: String = getJdbcUrl(sparkSettings)
    val table: String = parameters(ConnectionOptions.TABLE_NAME)
    val paraMap = parameters ++ Map(
      "url" -> url,
      "dbtable" -> table,
      "user" -> parameters(ConnectionOptions.USERNAME),
      "isolationLevel" -> "READ_COMMITTED"
    )
    (new JDBCOptions(url, table, paraMap), paraMap)
  }
}

object OceanBaseJdbcSparkSource {
  val SHORT_NAME: String = "oceanbase"
}

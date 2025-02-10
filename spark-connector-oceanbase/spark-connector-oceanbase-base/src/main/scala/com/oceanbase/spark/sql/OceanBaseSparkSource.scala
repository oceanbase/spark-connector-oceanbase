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
package com.oceanbase.spark.sql

import com.oceanbase.spark.config.OceanBaseConfig
import com.oceanbase.spark.utils.OBJdbcUtils
import com.oceanbase.spark.writer.DirectLoadWriter

import OceanBaseSparkSource.{createDirectLoadRelation, SHORT_NAME}
import org.apache.spark.internal.Logging
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider}

import scala.collection.JavaConverters.mapAsJavaMapConverter

@Deprecated
private[sql] class OceanBaseSparkSource
  extends DataSourceRegister
  with RelationProvider
  with CreatableRelationProvider
  with Logging {

  override def shortName(): String = SHORT_NAME

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    new OceanBaseRelation(sqlContext, parameters)
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      dataFrame: DataFrame): BaseRelation = {
    val oceanBaseConfig = new OceanBaseConfig(parameters.asJava)
    createDirectLoadRelation(sqlContext, mode, dataFrame, oceanBaseConfig)

    createRelation(sqlContext, parameters)
  }

}

object OceanBaseSparkSource {
  val SHORT_NAME: String = "oceanbase"

  def createDirectLoadRelation(
      sqlContext: SQLContext,
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

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

package com.oceanbase.spark

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

class OBKVHBaseSparkSource
  extends DataSourceRegister
  with RelationProvider
  with SchemaRelationProvider
  with CreatableRelationProvider {

  override def shortName(): String = "obkv-hbase"

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    throw new UnsupportedOperationException(
      "OBKV-HBase connector requires a user-specified schema. " +
        "Please use CREATE TEMPORARY VIEW with schema definition.")
  }

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation = {
    HBaseRelation(parameters, Some(schema))(sqlContext)
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    val relation = HBaseRelation(parameters, Some(data.schema))(sqlContext)
    relation.insert(data, overwrite = false)
    relation
  }
}

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

package com.oceanbase.spark.read

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.connector.read.V1Scan
import org.apache.spark.sql.reader.OBJdbcRelation
import org.apache.spark.sql.sources.{BaseRelation, Filter, TableScan}
import org.apache.spark.sql.types.StructType

case class OBJDBCLimitScan(
    relation: OBJdbcRelation,
    prunedSchema: StructType,
    pushedFilters: Array[Filter],
    pushedAggregateColumn: Array[String] = Array(),
    groupByColumns: Option[Array[String]])
  extends V1Scan {

  override def readSchema(): StructType = prunedSchema

  override def toV1TableScan[T <: BaseRelation with TableScan](context: SQLContext): T = {
    new BaseRelation with TableScan {
      override def sqlContext: SQLContext = context
      override def schema: StructType = prunedSchema
      override def needConversion: Boolean = relation.needConversion
      override def buildScan(): RDD[Row] = {
        val columnList = if (groupByColumns.isEmpty) {
          prunedSchema.map(_.name).toArray
        } else {
          pushedAggregateColumn
        }
        relation.buildScan(columnList, prunedSchema, pushedFilters, groupByColumns)
      }
    }.asInstanceOf[T]
  }

  override def description(): String = {
    val (aggString, groupByString) = if (groupByColumns.nonEmpty) {
      val groupByColumnsLength = groupByColumns.get.length
      (
        seqToString(pushedAggregateColumn.drop(groupByColumnsLength)),
        seqToString(pushedAggregateColumn.take(groupByColumnsLength)))
    } else {
      ("[]", "[]")
    }
    super.description() + ", prunedSchema: " + seqToString(prunedSchema) +
      ", PushedFilters: " + seqToString(pushedFilters) +
      ", PushedAggregates: " + aggString + ", PushedGroupBy: " + groupByString
  }

  private def seqToString(seq: Seq[Any]): String = seq.mkString("[", ", ", "]")
}

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

package com.oceanbase.spark.reader.v2

import com.oceanbase.spark.config.OceanBaseConfig
import com.oceanbase.spark.utils.OBJdbcUtils

import org.apache.spark.sql.connector.read.InputPartition

import java.util.Objects

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/** Data corresponding to one partition of a JDBCLimitRDD. */
case class OBMySQLPartition(partitionClause: String, limitOffsetClause: String, idx: Int)
  extends InputPartition {}

object OBMySQLPartition {

  private val EMPTY_STRING = ""
  private val PARTITION_QUERY_FORMAT = "PARTITION(%s)"

  def columnPartition(config: OceanBaseConfig): Array[InputPartition] = {
    // Determine whether a partition table.
    val obPartInfos: Array[OBPartInfo] = obtainPartInfo(config)
    require(obPartInfos.nonEmpty, "Failed to obtain partition info of table")

    if (obPartInfos.length == 1 && Objects.isNull(obPartInfos(0).partName)) {
      // For non-partition table
      computeForNonPartTable(config)
    } else {
      // For partition table
      computeForPartTable(config, obPartInfos)
    }
  }

  private val calLimit: Long => Long = {
    case count if count <= 100000 => 10000
    case count if count > 100000 && count <= 10000000 => 100000
    case count if count > 10000000 && count <= 100000000 => 200000
    case count if count > 100000000 && count <= 1000000000 => 250000
    case _ => 500000
  }

  private def obtainPartInfo(config: OceanBaseConfig): Array[OBPartInfo] = {
    val arrayBuilder = new mutable.ArrayBuilder.ofRef[OBPartInfo]
    OBJdbcUtils.withConnection(config) {
      conn =>
        {
          val statement = conn.createStatement()
          val sql =
            s"""
               |select
               |  TABLE_SCHEMA, TABLE_NAME, PARTITION_NAME, SUBPARTITION_NAME
               |from
               |  information_schema.partitions
               |where
               |      TABLE_SCHEMA = '${config.getSchemaName}'
               |  and TABLE_NAME = '${config.getTableName}';
               |""".stripMargin
          try {
            val rs = statement.executeQuery(sql)
            while (rs.next()) {
              arrayBuilder += OBPartInfo(
                rs.getString(1),
                rs.getString(2),
                rs.getString(3),
                rs.getString(4))
            }
          } finally {
            statement.close()
          }
        }
    }
    arrayBuilder.result()
  }

  private def computeForNonPartTable(config: OceanBaseConfig): Array[InputPartition] = {
    val count: Long = obtainCount(config, EMPTY_STRING)
    require(count >= 0, "Total must be a positive number")
    computeQueryPart(count, EMPTY_STRING, config).asInstanceOf[Array[InputPartition]]
  }

  private def computeForPartTable(
      config: OceanBaseConfig,
      obPartInfos: Array[OBPartInfo]): Array[InputPartition] = {
    val arr = new ArrayBuffer[OBMySQLPartition]()
    obPartInfos.foreach(
      obPartInfo => {
        val partitionName = obPartInfo.subPartName match {
          case x if Objects.isNull(x) => PARTITION_QUERY_FORMAT.format(obPartInfo.partName)
          case _ => PARTITION_QUERY_FORMAT.format(obPartInfo.subPartName)
        }
        val count = obtainCount(config, partitionName)
        val partitions = computeQueryPart(count, partitionName, config)
        arr ++= partitions
      })

    arr.zipWithIndex.map {
      case (partInfo, index) =>
        OBMySQLPartition(partInfo.partitionClause, partInfo.limitOffsetClause, index)
    }.toArray
  }

  private def obtainCount(config: OceanBaseConfig, partName: String) = {
    OBJdbcUtils.withConnection(config) {
      conn =>
        {
          val statement = conn.createStatement()
          val tableName = config.getDbTable
          val sql = s"SELECT count(1) AS cnt FROM $tableName $partName"
          try {
            val rs = statement.executeQuery(sql)
            if (rs.next())
              rs.getLong(1)
            else
              throw new RuntimeException(s"Failed to obtain count of $tableName.")
          } finally {
            statement.close()
          }
        }
    }
  }

  private def computeQueryPart(
      count: Long,
      partitionClause: String,
      config: OceanBaseConfig): Array[OBMySQLPartition] = {
    val step = config.getJdbcMaxRecordsPrePartition.orElse(calLimit(count))
    require(count >= 0, "Total must be a positive number")

    // Note: Now when count is 0, skip the spark query and return directly.
    val numberOfSteps = Math.ceil(count.toDouble / step).toInt
    (0 until numberOfSteps)
      .map(i => (i * step, step, i))
      .map {
        case (offset, limit, index) =>
          OBMySQLPartition(partitionClause, s"LIMIT $offset,$limit", idx = index)
      }
      .toArray
  }
}

case class OBPartInfo(tableSchema: String, tableName: String, partName: String, subPartName: String)

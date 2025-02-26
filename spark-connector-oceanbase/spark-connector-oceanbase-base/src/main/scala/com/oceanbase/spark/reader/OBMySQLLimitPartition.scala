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

package com.oceanbase.spark.reader

import com.oceanbase.spark.catalog.OceanBaseCatalog
import com.oceanbase.spark.utils.OBJdbcUtils

import org.apache.spark.Partition
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

import java.util.Objects

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/** Data corresponding to one partition of a JDBCLimitRDD. */
case class OBMySQLLimitPartition(partitionClause: String, limitOffsetClause: String, idx: Int)
  extends Partition {
  override def index: Int = idx
}

object OBMySQLLimitPartition {

  private val EMPTY_STRING = ""
  private val PARTITION_QUERY_FORMAT = "PARTITION(%s)"

  def columnPartition(jdbcOptions: JDBCOptions): Array[Partition] = {
    // Determine whether a partition table.
    val obPartInfos: Array[OBPartInfo] = obtainPartInfo(jdbcOptions)
    require(obPartInfos.nonEmpty, "Failed to obtain partition info of table")

    if (obPartInfos.length == 1 && Objects.isNull(obPartInfos(0).partName)) {
      // For non-partition table
      computeForNonPartTable(jdbcOptions)
    } else {
      // For partition table
      computeForPartTable(jdbcOptions, obPartInfos)
    }
  }

  private val calLimit: Long => Long = {
    case count if count <= 100000 => 10000
    case count if count > 100000 && count <= 10000000 => 100000
    case count if count > 10000000 && count <= 100000000 => 200000
    case count if count > 100000000 && count <= 1000000000 => 250000
    case _ => 500000
  }

  private def obtainPartInfo(jdbcOptions: JDBCOptions): Array[OBPartInfo] = {
    val arrayBuilder = new mutable.ArrayBuilder.ofRef[OBPartInfo]
    OBJdbcUtils.withConnection(jdbcOptions) {
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
               |      TABLE_SCHEMA = '${jdbcOptions.parameters(OceanBaseCatalog.CURRENT_DATABASE)}'
               |  and TABLE_NAME = '${jdbcOptions.parameters(OceanBaseCatalog.CURRENT_TABLE)}';
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

  private def computeForNonPartTable(jdbcOptions: JDBCOptions): Array[Partition] = {
    val count: Long = obtainCount(jdbcOptions, EMPTY_STRING)
    require(count >= 0, "Total must be a positive number")
    computeQueryPart(count, EMPTY_STRING).asInstanceOf[Array[Partition]]
  }

  private def computeForPartTable(
      jdbcOptions: JDBCOptions,
      obPartInfos: Array[OBPartInfo]): Array[Partition] = {
    val arr = new ArrayBuffer[OBMySQLLimitPartition]()
    obPartInfos.foreach(
      obPartInfo => {
        val partitionName = obPartInfo.subPartName match {
          case x if Objects.isNull(x) => PARTITION_QUERY_FORMAT.format(obPartInfo.partName)
          case _ => PARTITION_QUERY_FORMAT.format(obPartInfo.subPartName)
        }
        val count = obtainCount(jdbcOptions, partitionName)
        val partitions = computeQueryPart(count, partitionName)
        arr ++= partitions
      })

    arr.zipWithIndex.map {
      case (partInfo, index) =>
        OBMySQLLimitPartition(partInfo.partitionClause, partInfo.limitOffsetClause, index)
    }.toArray
  }

  private def obtainCount(jdbcOptions: JDBCOptions, partName: String) = {
    OBJdbcUtils.withConnection(jdbcOptions) {
      conn =>
        {
          val statement = conn.createStatement()
          val tableName = jdbcOptions.parameters(JDBCOptions.JDBC_TABLE_NAME)
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
      partitionClause: String): Array[OBMySQLLimitPartition] = {
    val step = calLimit(count)
    require(count >= 0, "Total must be a positive number")

    // Note: Now when count is 0, skip the spark query and return directly.
    val numberOfSteps = Math.ceil(count.toDouble / step).toInt
    (0 until numberOfSteps)
      .map(i => (i * step, step, i))
      .map {
        case (offset, limit, index) =>
          OBMySQLLimitPartition(partitionClause, s"LIMIT $offset,$limit", idx = index)
      }
      .toArray
  }
}

case class OBPartInfo(tableSchema: String, tableName: String, partName: String, subPartName: String)

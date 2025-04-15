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
import com.oceanbase.spark.dialect.{OceanBaseDialect, PriKeyColumnInfo}
import com.oceanbase.spark.utils.OBJdbcUtils

import org.apache.spark.sql.connector.read.InputPartition

import java.util.Objects

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/** Data corresponding to one partition of a JDBCLimitRDD. */
case class OBMySQLPartition(
    partitionClause: String,
    limitOffsetClause: String,
    whereClause: String,
    useHiddenPKColumn: Boolean = false,
    idx: Int)
  extends InputPartition {}

object OBMySQLPartition {

  private val EMPTY_STRING = ""
  private val PARTITION_QUERY_FORMAT = "PARTITION(%s)"
  private val INT_DATE_TYPE_SEQ = Seq("int", "bigint")
  private val AUTO_INCREMENT = "auto_increment"
  private val HIDDEN_PK_INCREMENT = "__pk_increment"

  def columnPartition(config: OceanBaseConfig, dialect: OceanBaseDialect): Array[InputPartition] = {
    // Determine whether a partition table.
    val obPartInfos: Array[OBPartInfo] = obtainPartInfo(config)
    require(obPartInfos.nonEmpty, "Failed to obtain partition info of table")

    // Check key and non-primary key tables
    val priKeyColInfos = dialect.getPriKeyInfo(config.getSchemaName, config.getTableName, config)
    if (null != priKeyColInfos && priKeyColInfos.nonEmpty) {
      // primary key table => OceanBase: All parts of a PRIMARY KEY must be NOT NULL
      var finalIntPriKey: PriKeyColumnInfo = null

      // 1. find all int columns
      val intKeys = priKeyColInfos.filter(priKey => INT_DATE_TYPE_SEQ.contains(priKey.dataType))
      // 2. check auto_increment
      val autoIncrementSeq = intKeys.filter(priKey => priKey.extra.contains(AUTO_INCREMENT)).toSeq
      if (autoIncrementSeq.nonEmpty) {
        finalIntPriKey = autoIncrementSeq.head
      } else {
        if (intKeys.length == 1) {
          finalIntPriKey = intKeys.head
        } else if (intKeys.length >= 2) {
          val bigintKeys = intKeys.filter(priKey => priKey.dataType.equals("bigint")).toSeq
          if (bigintKeys.nonEmpty) {
            finalIntPriKey = bigintKeys.head
          } else {
            finalIntPriKey = intKeys.head
          }
        }
      }

      // No primary key column of int type
      if (null == finalIntPriKey) {
        if (config.getEnableRewriteQuerySql) {
          // TODO: Rewriting query SQL through inner join to optimize deep paging performance.
          // https://www.oceanbase.com/docs/enterprise-oceanbase-database-cn-10000000000884779
          legacyLimitOffsetPartition(config, obPartInfos)
        } else {
          legacyLimitOffsetPartition(config, obPartInfos)
        }
      } else {
        if (obPartInfos.length == 1 && Objects.isNull(obPartInfos(0).partName)) {
          // For non-partition table
          computeSparkPartInfoForIntPriKeyNonPartTable(config, finalIntPriKey.columnName)
        } else {
          // For partition table
          computeSparkPartInfoForPartTable(config, obPartInfos, finalIntPriKey.columnName)
        }
      }

    } else { // non-primary key table
      if (obPartInfos.length == 1 && Objects.isNull(obPartInfos(0).partName)) {
        // For non-partition table
        computeSparkPartInfoForIntPriKeyNonPartTable(config, HIDDEN_PK_INCREMENT)
      } else {
        // For partition table
        computeSparkPartInfoForPartTable(config, obPartInfos, HIDDEN_PK_INCREMENT)
      }
    }
  }

  private def legacyLimitOffsetPartition(
      config: OceanBaseConfig,
      obPartInfos: Array[OBPartInfo]) = {
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
        OBMySQLPartition(
          partInfo.partitionClause,
          partInfo.limitOffsetClause,
          whereClause = null,
          useHiddenPKColumn = false,
          index)
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
          OBMySQLPartition(
            partitionClause,
            s"LIMIT $offset,$limit",
            whereClause = null,
            idx = index)
      }
      .toArray
  }

  // ========== Int Primary Key Section ==========
  private def computeSparkPartInfoForPartTable(
      config: OceanBaseConfig,
      obPartInfos: Array[OBPartInfo],
      priKeyColumnName: String): Array[InputPartition] = {
    val arr = new ArrayBuffer[OBMySQLPartition]()
    obPartInfos.foreach(
      obPartInfo => {
        val partitionName = obPartInfo.subPartName match {
          case x if Objects.isNull(x) => PARTITION_QUERY_FORMAT.format(obPartInfo.partName)
          case _ => PARTITION_QUERY_FORMAT.format(obPartInfo.subPartName)
        }
        val keyTableInfo = obtainIntPriKeyTableInfo(config, partitionName, priKeyColumnName)
        val partitions =
          computeIntPriKeySparkPart(keyTableInfo, partitionName, priKeyColumnName, config)
        arr ++= partitions
      })
    arr.zipWithIndex.map {
      case (partInfo, index) =>
        OBMySQLPartition(
          partInfo.partitionClause,
          limitOffsetClause = EMPTY_STRING,
          whereClause = partInfo.whereClause,
          useHiddenPKColumn = partInfo.useHiddenPKColumn,
          index)
    }.toArray
  }

  /**
   * Computes Spark partitions for integer primary key tables
   *
   * @param keyTableInfo
   *   Table information containing min/max IDs and row count
   * @param partitionClause
   *   Original partition clause (if exists)
   * @param config
   *   OceanBase configuration
   * @return
   *   Array of optimized MySQL partitions
   */
  private def computeIntPriKeySparkPart(
      keyTableInfo: IntPriKeyTableInfo,
      partitionClause: String,
      priKeyColumnName: String,
      config: OceanBaseConfig): Array[OBMySQLPartition] = {
    // Return empty array if table has no rows
    if (keyTableInfo.count <= 0) {
      return Array.empty[OBMySQLPartition]
    }
    // Calculate desired rows per partition based on configuration
    val desiredRowsPerPartition = calLimit(keyTableInfo.count)
    // Calculate number of partitions (rounded up with minimum 1)
    val numPartitions =
      Math.ceil(keyTableInfo.count.toDouble / desiredRowsPerPartition).toInt.max(1)

    // Calculate ID range and step size between partitions
    val idRange = keyTableInfo.max - keyTableInfo.min
    // Use ceiling division to ensure full coverage
    val step = (idRange + numPartitions - 1) / numPartitions
    var useHiddenPKColumn = false
    if (priKeyColumnName.equals(HIDDEN_PK_INCREMENT))
      useHiddenPKColumn = true
    (0 until numPartitions).map {
      i =>
        val lower = keyTableInfo.min + i * step
        val upper =
          if (i == numPartitions - 1) keyTableInfo.max + 1
          else lower + step // Ensure upper bound includes max value for last partition
        val whereClause = s"($priKeyColumnName >= $lower AND $priKeyColumnName < $upper)"
        OBMySQLPartition(
          partitionClause = partitionClause,
          limitOffsetClause = EMPTY_STRING,
          whereClause = whereClause,
          useHiddenPKColumn = useHiddenPKColumn,
          idx = i)
    }.toArray
  }

  private def computeSparkPartInfoForIntPriKeyNonPartTable(
      config: OceanBaseConfig,
      priKeyColumnName: String): Array[InputPartition] = {
    val priKeyColumnInfo = obtainIntPriKeyTableInfo(config, EMPTY_STRING, priKeyColumnName)
    if (priKeyColumnInfo.count <= 0) Array.empty
    computeIntPriKeySparkPart(priKeyColumnInfo, EMPTY_STRING, priKeyColumnName, config)
      .asInstanceOf[Array[InputPartition]]
  }

  private def obtainIntPriKeyTableInfo(
      config: OceanBaseConfig,
      partName: String,
      priKeyColumnName: String) = {
    OBJdbcUtils.withConnection(config) {
      conn =>
        {
          val statement = conn.createStatement()
          val tableName = config.getDbTable
          val sql =
            s"""
              SELECT /*+ opt_param('hidden_column_visible', 'true') */
                count(1) AS cnt, min($priKeyColumnName), max($priKeyColumnName)
              FROM $tableName $partName
             """
          try {
            val rs = statement.executeQuery(sql)
            if (rs.next())
              IntPriKeyTableInfo(rs.getLong(1), rs.getLong(2), rs.getLong(3))
            else
              throw new RuntimeException(s"Failed to obtain count of $tableName.")
          } finally {
            statement.close()
          }
        }
    }
  }

  private case class IntPriKeyTableInfo(count: Long, min: Long, max: Long)

}

case class OBPartInfo(tableSchema: String, tableName: String, partName: String, subPartName: String)

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
import com.oceanbase.spark.utils.{ConfigUtils, OBJdbcUtils}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.InputPartition

import java.sql.Connection
import java.util.{Objects, Optional}
import java.util.concurrent.{Executors, TimeUnit}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration

/** Data corresponding to one partition of a JDBCLimitRDD. */
case class OBMySQLPartition(
    partitionClause: String,
    limitOffsetClause: String,
    whereClause: String,
    useHiddenPKColumn: Boolean = false,
    unevenlyWhereValue: Seq[Object] = Seq[Object](),
    idx: Int)
  extends InputPartition {}

object OBMySQLPartition extends Logging {

  private val EMPTY_STRING = ""
  private val PARTITION_QUERY_FORMAT = "PARTITION(%s)"
  private val INT_DATE_TYPE_SEQ = Seq("int", "bigint")
  private val AUTO_INCREMENT = "auto_increment"
  private val HIDDEN_PK_INCREMENT = "__pk_increment"

  def columnPartition(config: OceanBaseConfig, dialect: OceanBaseDialect): Array[InputPartition] = {
    // In this method, create and close the connection uniformly to reuse the connection.
    OBJdbcUtils.withConnection(config) {
      connection =>
        {
          // Determine whether a partition table.
          val obPartInfos: Array[OBPartInfo] = obtainPartInfo(connection, config)
          require(obPartInfos.nonEmpty, "Failed to obtain partition info of table")

          // Check key and non-primary key tables
          val priKeyColInfos =
            dialect.getPriKeyInfo(connection, config.getSchemaName, config.getTableName, config)
          if (null != priKeyColInfos && priKeyColInfos.nonEmpty) {
            val finalIntPriKey: PriKeyColumnInfo = getIntPriKeyColumn(priKeyColInfos)

            // No primary key column of int type
            var configPartitionColumn = config.getJdbcReaderPartitionColumn(dialect)
            // Find the specified partition column in the spark runtime configuration.
            val partColConfig = String.format(
              OceanBaseConfig.SPECIFY_PK_TABLE_PARTITION_COLUMN.getKey,
              dialect.unQuoteIdentifier(config.getDbTable))
            configPartitionColumn = Optional.ofNullable(
              configPartitionColumn.orElse(ConfigUtils.findFromRuntimeConf(partColConfig)))
            if (config.getDisableIntPkTableUseWherePartition) {
              limitOffsetPartitionWay(connection, config, obPartInfos)
            } else if (null == finalIntPriKey) {
              // No-int pK table
              val priKeyColumnName = configPartitionColumn.orElse(priKeyColInfos.head.columnName)
              whereUnevenlySizedPartitionWay(connection, config, obPartInfos, priKeyColumnName)
            } else {
              val info = obtainIntPriKeyTableInfo(
                connection,
                config,
                EMPTY_STRING,
                finalIntPriKey.columnName)
              val priKeyDepth = info.max - info.min
              // Check the uniformity of the integer primary key value distribution. TODO：Refine logic of Check the uniformity
              if ((priKeyDepth >= info.count * 2) || (priKeyDepth * 2 <= info.count)) {
                whereUnevenlySizedPartitionWay(
                  connection,
                  config,
                  obPartInfos,
                  configPartitionColumn.orElse(finalIntPriKey.columnName))
              } else {
                whereEvenlySizedPartitionWay(
                  connection,
                  config,
                  obPartInfos,
                  finalIntPriKey.columnName)
              }
            }
          } else { // non-primary key table
            whereEvenlySizedPartitionWay(connection, config, obPartInfos, HIDDEN_PK_INCREMENT)
          }
        }
    }
  }

  private def limitOffsetPartitionWay(
      connection: Connection,
      config: OceanBaseConfig,
      obPartInfos: Array[OBPartInfo]) = {
    if (obPartInfos.length == 1 && Objects.isNull(obPartInfos(0).partName)) {
      // For non-partition table
      computeOffsetLimitPartInfoForNonPartTable(connection, config)
    } else {
      // For partition table
      computeForOffsetLimitPartInfoForPartTable(connection, config, obPartInfos)
    }
  }

  private def whereEvenlySizedPartitionWay(
      connection: Connection,
      config: OceanBaseConfig,
      obPartInfos: Array[OBPartInfo],
      priKeyColumnName: String) = {
    if (obPartInfos.length == 1 && Objects.isNull(obPartInfos(0).partName)) {
      // For non-partition table
      computeWherePartInfoForNonPartTable(connection, config, priKeyColumnName)
    } else {
      // For partition table
      computeWherePartInfoForPartTable(connection, config, obPartInfos, priKeyColumnName)
    }
  }

  private def whereUnevenlySizedPartitionWay(
      connection: Connection,
      config: OceanBaseConfig,
      obPartInfos: Array[OBPartInfo],
      priKeyColumnName: String) = {

    if (obPartInfos.length == 1 && Objects.isNull(obPartInfos(0).partName)) {
      // For non-partition table
      computeUnevenlyWherePartInfoForNonPartTable(connection, config, priKeyColumnName)
    } else {
      // For partition table
      computeUnevenlyWherePartInfoForPartTable(config, obPartInfos, priKeyColumnName)
    }
  }

  private val calPartitionSize: Long => Long = {
    case count if count <= 100000 => 10000
    case count if count > 100000 && count <= 10000000 => 100000
    case count if count > 10000000 && count <= 100000000 => 200000
    case count if count > 100000000 && count <= 1000000000 => 250000
    case _ => 500000
  }

  private def getIntPriKeyColumn(priKeyColInfos: ArrayBuffer[PriKeyColumnInfo]) = {
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
    finalIntPriKey
  }

  private def obtainPartInfo(conn: Connection, config: OceanBaseConfig): Array[OBPartInfo] = {
    val arrayBuilder = new mutable.ArrayBuilder.ofRef[OBPartInfo]
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

    arrayBuilder.result()
  }

  private def computeOffsetLimitPartInfoForNonPartTable(
      connection: Connection,
      config: OceanBaseConfig): Array[InputPartition] = {
    val count: Long = obtainCount(connection, config, EMPTY_STRING)
    require(count >= 0, "Total must be a positive number")
    computeQueryPart(count, EMPTY_STRING, config).asInstanceOf[Array[InputPartition]]
  }

  private def computeForOffsetLimitPartInfoForPartTable(
      connection: Connection,
      config: OceanBaseConfig,
      obPartInfos: Array[OBPartInfo]): Array[InputPartition] = {
    val arr = new ArrayBuffer[OBMySQLPartition]()
    obPartInfos.foreach(
      obPartInfo => {
        val partitionName = obPartInfo.subPartName match {
          case x if Objects.isNull(x) => PARTITION_QUERY_FORMAT.format(obPartInfo.partName)
          case _ => PARTITION_QUERY_FORMAT.format(obPartInfo.subPartName)
        }
        val count = obtainCount(connection, config, partitionName)
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
          idx = index)
    }.toArray
  }

  private def obtainCount(connection: Connection, config: OceanBaseConfig, partName: String) = {

    val statement = connection.createStatement()
    val tableName = config.getDbTable
    val sql =
      s"SELECT /*+ PARALLEL(${config.getJdbcStatsParallelHintDegree}) ${queryTimeoutHint(
          config)} */ count(1) AS cnt FROM $tableName $partName"
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

  private def computeQueryPart(
      count: Long,
      partitionClause: String,
      config: OceanBaseConfig): Array[OBMySQLPartition] = {
    val step = config.getJdbcMaxRecordsPrePartition.orElse(calPartitionSize(count))
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

  // ========== Evenly Sized Where Partition Section ==========
  private def computeWherePartInfoForPartTable(
      connection: Connection,
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
        val keyTableInfo =
          obtainIntPriKeyTableInfo(connection, config, partitionName, priKeyColumnName)
        val partitions =
          computeWhereSparkPart(keyTableInfo, partitionName, priKeyColumnName, config)
        arr ++= partitions
      })
    arr.zipWithIndex.map {
      case (partInfo, index) =>
        OBMySQLPartition(
          partInfo.partitionClause,
          limitOffsetClause = EMPTY_STRING,
          whereClause = partInfo.whereClause,
          useHiddenPKColumn = partInfo.useHiddenPKColumn,
          idx = index)
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
  private def computeWhereSparkPart(
      keyTableInfo: IntPriKeyTableInfo,
      partitionClause: String,
      priKeyColumnName: String,
      config: OceanBaseConfig): Array[OBMySQLPartition] = {
    // Return empty array if table has no rows
    if (keyTableInfo.count <= 0) {
      return Array.empty[OBMySQLPartition]
    }
    // Calculate desired rows per partition based on configuration
    val desiredRowsPerPartition =
      config.getJdbcMaxRecordsPrePartition.orElse(calPartitionSize(keyTableInfo.count))
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

  private def computeWherePartInfoForNonPartTable(
      connection: Connection,
      config: OceanBaseConfig,
      priKeyColumnName: String): Array[InputPartition] = {
    val priKeyColumnInfo =
      obtainIntPriKeyTableInfo(connection, config, EMPTY_STRING, priKeyColumnName)
    if (priKeyColumnInfo.count <= 0) Array.empty
    computeWhereSparkPart(priKeyColumnInfo, EMPTY_STRING, priKeyColumnName, config)
      .asInstanceOf[Array[InputPartition]]
  }

  private def obtainIntPriKeyTableInfo(
      connection: Connection,
      config: OceanBaseConfig,
      partName: String,
      priKeyColumnName: String) = {
    val statement = connection.createStatement()
    val tableName = config.getDbTable
    val useHiddenPKColHint =
      if (priKeyColumnName.equals(HIDDEN_PK_INCREMENT))
        s", opt_param('hidden_column_visible', 'true') "
      else EMPTY_STRING
    val hint =
      s"/*+ PARALLEL(${config.getJdbcStatsParallelHintDegree}) $useHiddenPKColHint ${queryTimeoutHint(config)} */"

    val sql =
      s"""
              SELECT $hint
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

  private case class IntPriKeyTableInfo(count: Long, min: Long, max: Long)

  // ========== Unevenly Sized Where Partition Section ==========
  private def computeUnevenlyWherePartInfoForNonPartTable(
      conn: Connection,
      config: OceanBaseConfig,
      priKeyColumnName: String): Array[InputPartition] = {
    val unevenlyPriKeyTableInfo =
      obtainUnevenlyPriKeyTableInfo(conn, config, EMPTY_STRING, priKeyColumnName)
    if (unevenlyPriKeyTableInfo.count <= 0)
      Array.empty
    else
      computeUnevenlyWhereSparkPart(
        conn,
        unevenlyPriKeyTableInfo,
        EMPTY_STRING,
        priKeyColumnName,
        config)
        .asInstanceOf[Array[InputPartition]]
  }

  private def computeUnevenlyWherePartInfoForPartTable(
      config: OceanBaseConfig,
      obPartInfos: Array[OBPartInfo],
      priKeyColumnName: String): Array[InputPartition] = {
    val startTime = System.nanoTime()

    // Create custom thread pool with optimized parallelism
    val maxParallelism = config.getJdbcPartitionComputeParallelism
    val partitionCount = obPartInfos.length
    val parallelism = Math.min(partitionCount, maxParallelism)
    val executor = Executors.newFixedThreadPool(parallelism)
    val executionContext = ExecutionContext.fromExecutor(executor)

    try {
      val futures = obPartInfos.map(
        obPartInfo => {
          Future {
            val conn = OBJdbcUtils.getConnection(config)
            try {
              val partitionName = obPartInfo.subPartName match {
                case x if Objects.isNull(x) => PARTITION_QUERY_FORMAT.format(obPartInfo.partName)
                case _ => PARTITION_QUERY_FORMAT.format(obPartInfo.subPartName)
              }
              val unevenlyPriKeyTableInfo =
                obtainUnevenlyPriKeyTableInfo(conn, config, partitionName, priKeyColumnName)
              val partitions =
                computeUnevenlyWhereSparkPart(
                  conn,
                  unevenlyPriKeyTableInfo,
                  partitionName,
                  priKeyColumnName,
                  config)
              partitions
            } finally {
              conn.close()
            }
          }(executionContext)
        })
      val arr = futures.flatMap(
        future => {
          Await.result(future, Duration(10, TimeUnit.MINUTES))
        })
      val endTime = System.nanoTime()
      logInfo(
        s"Partition computation completed with parallelism=$parallelism, time cost: ${(endTime - startTime) / 1000000} ms")

      arr.zipWithIndex.map {
        case (partInfo, index) =>
          OBMySQLPartition(
            partInfo.partitionClause,
            limitOffsetClause = EMPTY_STRING,
            whereClause = partInfo.whereClause,
            useHiddenPKColumn = partInfo.useHiddenPKColumn,
            unevenlyWhereValue = partInfo.unevenlyWhereValue,
            idx = index
          )
      }.toArray
    } finally {
      // Shutdown thread pool
      executor.shutdown()
      if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
        executor.shutdownNow()
        logWarning("Thread pool did not terminate gracefully, forcing shutdown")
      }
    }
  }

  /**
   * Computes Spark partitions for unevenly primary key tables
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
  private def computeUnevenlyWhereSparkPart(
      conn: Connection,
      keyTableInfo: UnevenlyPriKeyTableInfo,
      partitionClause: String,
      priKeyColumnName: String,
      config: OceanBaseConfig): Array[OBMySQLPartition] = {
    // Return empty array if table has no rows
    if (keyTableInfo.count <= 0) {
      return Array.empty[OBMySQLPartition]
    }
    // Calculate desired rows per partition based on configuration
    val desiredRowsPerPartition =
      config.getJdbcMaxRecordsPrePartition.orElse(calPartitionSize(keyTableInfo.count))
    var previousChunkEnd = keyTableInfo.min
    var chunkEnd = nextChunkEnd(
      conn,
      desiredRowsPerPartition,
      previousChunkEnd,
      keyTableInfo.max,
      partitionClause,
      priKeyColumnName,
      config)
    val arrayBuffer = ArrayBuffer[OBMySQLPartition]()
    var idx = 0
    if (Objects.nonNull(chunkEnd)) {
      val whereClause = s"($priKeyColumnName < ?)"
      arrayBuffer += OBMySQLPartition(
        partitionClause = partitionClause,
        limitOffsetClause = EMPTY_STRING,
        whereClause = whereClause,
        unevenlyWhereValue = Seq(chunkEnd),
        idx = idx)
    }
    while (Objects.nonNull(chunkEnd)) {
      previousChunkEnd = chunkEnd
      chunkEnd = nextChunkEnd(
        conn,
        desiredRowsPerPartition,
        previousChunkEnd,
        keyTableInfo.max,
        partitionClause,
        priKeyColumnName,
        config)
      if (Objects.nonNull(chunkEnd)) {
        val whereClause =
          s"($priKeyColumnName >= ? AND $priKeyColumnName < ?)"
        idx = idx + 1
        arrayBuffer += OBMySQLPartition(
          partitionClause = partitionClause,
          limitOffsetClause = EMPTY_STRING,
          whereClause = whereClause,
          unevenlyWhereValue = Seq(previousChunkEnd, chunkEnd),
          idx = idx)
      }
    }
    if (Objects.isNull(chunkEnd)) {
      val whereClause = s"($priKeyColumnName >= ?)"
      idx = idx + 1
      arrayBuffer += OBMySQLPartition(
        partitionClause = partitionClause,
        limitOffsetClause = EMPTY_STRING,
        whereClause = whereClause,
        unevenlyWhereValue = Seq(previousChunkEnd),
        idx = idx)
    }
    arrayBuffer.toArray
  }

  private def nextChunkEnd(
      conn: Connection,
      chunkSize: Long,
      previousChunkEnd: Object,
      max: Object,
      partitionClause: String,
      priKeyColumnName: String,
      config: OceanBaseConfig): Object = {
    var chunkEnd: Object =
      queryNextChunkMax(
        conn,
        chunkSize,
        previousChunkEnd,
        partitionClause,
        priKeyColumnName,
        config)
    if (Objects.isNull(chunkEnd)) return chunkEnd
    if (Objects.equals(previousChunkEnd, chunkEnd)) {
      // we don't allow equal chunk start and end,
      // should query the next one larger than chunkEnd
      chunkEnd = queryMin(conn, chunkEnd, partitionClause, priKeyColumnName, config)

      // queryMin will return null when the chunkEnd is the max value,// queryMin will return null when the chunkEnd is the max value,
      // this will happen when the mysql table ignores the capitalization.// this will happen when the mysql table ignores the capitalization.
      // see more detail at the test MySqlConnectorITCase#testReadingWithMultiMaxValue.// see more detail at the test MySqlConnectorITCase#testReadingWithMultiMaxValue.
      // In the test, the max value of order_id will return 'e' and when we get the chunkEnd =// In the test, the max value of order_id will return 'e' and when we get the chunkEnd =
      // 'E',// 'E',
      // this method will return 'E' and will not return null.// this method will return 'E' and will not return null.
      // When this method is invoked next time, queryMin will return null here.// When this method is invoked next time, queryMin will return null here.
      // So we need return null when we reach the max value here.// So we need return null when we reach the max value here.
      if (chunkEnd == null) return null
    }
    if (compare(chunkEnd, max) >= 0) null
    else chunkEnd
  }

  private def queryNextChunkMax(
      conn: Connection,
      chunkSize: Long,
      includedLowerBound: Object,
      partitionClause: String,
      priKeyColumnName: String,
      config: OceanBaseConfig): Object = {
    val tableName = config.getDbTable
    val hint =
      s"/*+ PARALLEL(${config.getJdbcStatsParallelHintDegree}) ${queryTimeoutHint(config)} */"
    val sql =
      s"""
              SELECT
                $hint MAX($priKeyColumnName) AS chunk_high
              FROM
                (
                  SELECT * FROM $tableName $partitionClause WHERE $priKeyColumnName > ? ORDER BY $priKeyColumnName ASC LIMIT $chunkSize
                );
             """
    val statement = conn.prepareStatement(sql)
    try {
      statement.setObject(1, includedLowerBound)
      val rs = statement.executeQuery()
      if (rs.next())
        rs.getObject(1)
      else
        throw new RuntimeException("Failed to query next chunk max.")
    } finally {
      statement.close()
    }
  }

  private def queryMin(
      conn: Connection,
      includedLowerBound: Object,
      partitionClause: String,
      priKeyColumnName: String,
      config: OceanBaseConfig): Object = {
    val tableName = config.getDbTable
    val hint =
      s"/*+ PARALLEL(${config.getJdbcStatsParallelHintDegree}) ${queryTimeoutHint(config)} */"
    val sql =
      s"""
              SELECT $hint
                MIN($priKeyColumnName)
              FROM $tableName $partitionClause
                WHERE $priKeyColumnName > ?
             """
    val statement = conn.prepareStatement(sql)
    try {
      statement.setObject(1, includedLowerBound)
      val rs = statement.executeQuery()
      if (rs.next())
        rs.getObject(1)
      else
        throw new RuntimeException("Failed to query next chunk max.")
    } finally {
      statement.close()
    }
  }

  private def obtainUnevenlyPriKeyTableInfo(
      conn: Connection,
      config: OceanBaseConfig,
      partName: String,
      priKeyColumnName: String) = {
    val statement = conn.createStatement()
    val tableName = config.getDbTable
    val hint =
      s"/*+ PARALLEL(${config.getJdbcStatsParallelHintDegree}) ${queryTimeoutHint(config)} */"
    val sql =
      s"""
              SELECT $hint
                count(1) AS cnt, min($priKeyColumnName), max($priKeyColumnName)
              FROM $tableName $partName
             """
    try {
      val rs = statement.executeQuery(sql)
      if (rs.next())
        UnevenlyPriKeyTableInfo(rs.getLong(1), rs.getObject(2), rs.getObject(3))
      else
        throw new RuntimeException(s"Failed to obtain count of $tableName.")
    } finally {
      statement.close()
    }
  }

  private def compare(obj1: Any, obj2: Any): Int = (obj1, obj2) match {
    case (c1: Comparable[_], c2) if c1.getClass == c2.getClass =>
      c1.asInstanceOf[Comparable[Any]].compareTo(c2)
    case _ =>
      obj1.toString.compareTo(obj2.toString)
  }

  def queryTimeoutHint(config: OceanBaseConfig): String = if (
    config.getQueryTimeoutHintDegree > 0
  ) {
    s", query_timeout(${config.getQueryTimeoutHintDegree}) "
  } else {
    ""
  }

  private case class UnevenlyPriKeyTableInfo(count: Long, min: Object, max: Object)
}

case class OBPartInfo(tableSchema: String, tableName: String, partName: String, subPartName: String)

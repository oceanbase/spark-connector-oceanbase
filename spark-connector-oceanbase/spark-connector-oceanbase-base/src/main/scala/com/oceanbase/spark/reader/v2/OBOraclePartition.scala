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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.InputPartition

import java.sql.Connection
import java.util.Objects

import scala.collection.mutable.ArrayBuffer

case class OBOraclePartition(
    partitionClause: String,
    limitOffsetClause: String,
    whereClause: String,
    useHiddenPKColumn: Boolean = false,
    unevenlyWhereValue: Seq[Object] = Seq[Object](),
    idx: Int)
  extends InputPartition {}

object OBOraclePartition extends Logging {

  private val EMPTY_STRING = ""
  private val PARTITION_QUERY_FORMAT = "PARTITION(%s)"
  private val HIDDEN_PK_INCREMENT = "__pk_increment"

  private def normalizePkNameForSql(column: String): String = {
    if (column == null) column
    else if (column == HIDDEN_PK_INCREMENT) "\"__pk_increment\""
    else column // Column name from dialect.getPriKeyInfo is already quoted
  }

  def columnPartition(config: OceanBaseConfig, dialect: OceanBaseDialect): Array[InputPartition] = {
    OBJdbcUtils
      .withConnection(config) {
        connection =>
          val obPartInfos: Array[OBOraclePartInfo] = obtainPartInfo(connection, config)

          val priKeyColInfos =
            dialect.getPriKeyInfo(connection, config.getSchemaName, config.getTableName, config)

          if (priKeyColInfos == null || priKeyColInfos.isEmpty) {
            // Non-primary key table: use hidden pk column evenly sized where partitioning
            if (obPartInfos.isEmpty) {
              computeWherePartInfoForNonPartTable(connection, config, HIDDEN_PK_INCREMENT)
            } else {
              computeWherePartInfoForPartTable(connection, config, obPartInfos, HIDDEN_PK_INCREMENT)
            }
          } else {
            val numericPriKey: PriKeyColumnInfo = selectNumericPriKey(priKeyColInfos)
            val priKeyColumnName =
              if (numericPriKey != null) numericPriKey.columnName
              else priKeyColInfos.head.columnName

            if (obPartInfos.isEmpty) {
              if (isNumericType(numericPriKey)) {
                computeWherePartInfoForNonPartTable(connection, config, priKeyColumnName)
              } else {
                computeUnevenlyWherePartInfoForNonPartTable(connection, config, priKeyColumnName)
              }
            } else {
              if (isNumericType(numericPriKey)) {
                computeWherePartInfoForPartTable(connection, config, obPartInfos, priKeyColumnName)
              } else {
                computeUnevenlyWherePartInfoForPartTable(config, obPartInfos, priKeyColumnName)
              }
            }
          }
      }
      .asInstanceOf[Array[InputPartition]]
  }

  private def obtainPartInfo(
      connection: Connection,
      config: OceanBaseConfig): Array[OBOraclePartInfo] = {
    val subPartSql =
      s"""
         |SELECT
         |  PARTITION_NAME,
         |  SUBPARTITION_NAME
         |FROM
         |  ALL_TAB_SUBPARTITIONS
         |WHERE
         |  TABLE_OWNER = '${config.getSchemaName}'
         |  AND TABLE_NAME = '${config.getTableName}'
         |ORDER BY PARTITION_NAME, SUBPARTITION_POSITION
         |""".stripMargin

    val subPartitions = ArrayBuffer[OBOraclePartInfo]()
    OBJdbcUtils.executeQuery(connection, config, subPartSql) {
      rs =>
        while (rs.next()) {
          subPartitions += OBOraclePartInfo(
            rs.getString("PARTITION_NAME"),
            rs.getString("SUBPARTITION_NAME"))
        }
        subPartitions
    }

    if (subPartitions.nonEmpty) return subPartitions.toArray

    val partSql =
      s"""
         |SELECT
         |  PARTITION_NAME
         |FROM
         |  ALL_TAB_PARTITIONS
         |WHERE
         |  TABLE_OWNER = '${config.getSchemaName}'
         |  AND TABLE_NAME = '${config.getTableName}'
         |ORDER BY PARTITION_POSITION
         |""".stripMargin

    val partitions = ArrayBuffer[OBOraclePartInfo]()
    OBJdbcUtils.executeQuery(connection, config, partSql) {
      rs =>
        while (rs.next()) {
          partitions += OBOraclePartInfo(rs.getString("PARTITION_NAME"), null)
        }
        partitions
    }

    partitions.toArray
  }

  private def obtainCount(
      connection: Connection,
      config: OceanBaseConfig,
      partName: String): Long = {
    val statement = connection.createStatement()
    val tableName = config.getDbTable
    val sql =
      s"SELECT /*+ PARALLEL(${config.getJdbcStatsParallelHintDegree}) ${queryTimeoutHint(
          config)} */ count(1) AS cnt FROM $tableName $partName"
    try {
      val rs = statement.executeQuery(sql)
      if (rs.next()) rs.getLong(1)
      else throw new RuntimeException(s"Failed to obtain count of $tableName.")
    } finally {
      statement.close()
    }
  }

  private val calPartitionSize: Long => Long = {
    case count if count <= 100000 => 10000
    case count if count > 100000 && count <= 10000000 => 100000
    case count if count > 10000000 && count <= 100000000 => 200000
    case count if count > 100000000 && count <= 1000000000 => 250000
    case _ => 500000
  }

  private def isNumericType(priKey: PriKeyColumnInfo): Boolean = {
    if (priKey == null) return false
    val t = Option(priKey.dataType).map(_.toUpperCase).getOrElse("")
    t.contains("NUMBER") || t == "INTEGER"
  }

  private def selectNumericPriKey(
      priKeyColInfos: ArrayBuffer[PriKeyColumnInfo]): PriKeyColumnInfo = {
    if (priKeyColInfos == null || priKeyColInfos.isEmpty) return null
    val numeric = priKeyColInfos.find(info => isNumericType(info))
    numeric.orNull
  }

  private def computeWherePartInfoForNonPartTable(
      connection: Connection,
      config: OceanBaseConfig,
      priKeyColumnName: String): Array[InputPartition] = {
    val pkForSql = normalizePkNameForSql(priKeyColumnName)
    val priKeyColumnInfo =
      obtainIntPriKeyTableInfo(connection, config, EMPTY_STRING, pkForSql)
    if (priKeyColumnInfo.count <= 0) Array.empty
    computeWhereSparkPart(priKeyColumnInfo, EMPTY_STRING, pkForSql, config)
      .asInstanceOf[Array[InputPartition]]
  }

  private def computeWherePartInfoForPartTable(
      connection: Connection,
      config: OceanBaseConfig,
      obPartInfos: Array[OBOraclePartInfo],
      priKeyColumnName: String): Array[InputPartition] = {
    val arr = ArrayBuffer[OBOraclePartition]()
    obPartInfos.foreach {
      obPartInfo =>
        val partitionName = if (obPartInfo.subPartName != null) {
          PARTITION_QUERY_FORMAT.format(obPartInfo.subPartName)
        } else {
          PARTITION_QUERY_FORMAT.format(obPartInfo.partName)
        }
        val pkForSql = normalizePkNameForSql(priKeyColumnName)
        val keyTableInfo =
          obtainIntPriKeyTableInfo(connection, config, partitionName, pkForSql)
        val partitions =
          computeWhereSparkPart(keyTableInfo, partitionName, pkForSql, config)
        arr ++= partitions
    }
    arr.zipWithIndex.map {
      case (partInfo, index) =>
        OBOraclePartition(
          partInfo.partitionClause,
          limitOffsetClause = EMPTY_STRING,
          whereClause = partInfo.whereClause,
          useHiddenPKColumn = partInfo.useHiddenPKColumn,
          unevenlyWhereValue = partInfo.unevenlyWhereValue,
          idx = index
        )
    }.toArray
  }

  private case class IntPriKeyTableInfo(count: Long, min: Long, max: Long)

  private def obtainIntPriKeyTableInfo(
      connection: Connection,
      config: OceanBaseConfig,
      partName: String,
      priKeyColumnName: String): IntPriKeyTableInfo = {
    val statement = connection.createStatement()
    val tableName = config.getDbTable
    val useHiddenPKColHint =
      if (priKeyColumnName.replace("\"", "") == HIDDEN_PK_INCREMENT)
        s", opt_param('hidden_column_visible', 'true') "
      else EMPTY_STRING
    val hint =
      s"/*+ PARALLEL(${config.getJdbcStatsParallelHintDegree}) $useHiddenPKColHint ${queryTimeoutHint(config)} */"

    val sql =
      s"""
         |SELECT
         |  $hint count(1) AS cnt, min(%s), max(%s)
         |FROM $tableName $partName
         |""".stripMargin
        .format(normalizePkNameForSql(priKeyColumnName), normalizePkNameForSql(priKeyColumnName))
    try {
      val rs = statement.executeQuery(sql)
      if (rs.next()) IntPriKeyTableInfo(rs.getLong(1), rs.getLong(2), rs.getLong(3))
      else throw new RuntimeException(s"Failed to obtain count of $tableName.")
    } finally {
      statement.close()
    }
  }

  private def computeWhereSparkPart(
      keyTableInfo: IntPriKeyTableInfo,
      partitionClause: String,
      priKeyColumnName: String,
      config: OceanBaseConfig): Array[OBOraclePartition] = {
    if (keyTableInfo.count <= 0) return Array.empty[OBOraclePartition]

    val desiredRowsPerPartition =
      config.getJdbcMaxRecordsPrePartition.orElse(calPartitionSize(keyTableInfo.count))
    val numPartitions =
      Math.ceil(keyTableInfo.count.toDouble / desiredRowsPerPartition).toInt.max(1)
    val idRange = keyTableInfo.max - keyTableInfo.min
    val step = (idRange + numPartitions - 1) / numPartitions
    val useHidden = priKeyColumnName.replace("\"", "") == HIDDEN_PK_INCREMENT

    (0 until numPartitions).map {
      i =>
        val lower = keyTableInfo.min + i * step
        val upper = if (i == numPartitions - 1) keyTableInfo.max + 1 else lower + step
        val whereClause = s"($priKeyColumnName >= $lower AND $priKeyColumnName < $upper)"
        OBOraclePartition(
          partitionClause = partitionClause,
          limitOffsetClause = EMPTY_STRING,
          whereClause = whereClause,
          useHiddenPKColumn = useHidden,
          unevenlyWhereValue = Seq.empty,
          idx = i)
    }.toArray
  }

  private case class UnevenlyPriKeyTableInfo(count: Long, min: Object, max: Object)

  private def obtainUnevenlyPriKeyTableInfo(
      conn: Connection,
      config: OceanBaseConfig,
      partName: String,
      priKeyColumnName: String): UnevenlyPriKeyTableInfo = {
    val statement = conn.createStatement()
    val tableName = config.getDbTable
    val hint =
      s"/*+ PARALLEL(${config.getJdbcStatsParallelHintDegree}) ${queryTimeoutHint(config)} */"
    val sql =
      s"""
         |SELECT $hint
         |  count(1) AS cnt, min(%s), max(%s)
         |FROM $tableName $partName
         |""".stripMargin
        .format(normalizePkNameForSql(priKeyColumnName), normalizePkNameForSql(priKeyColumnName))
    try {
      val rs = statement.executeQuery(sql)
      if (rs.next()) UnevenlyPriKeyTableInfo(rs.getLong(1), rs.getObject(2), rs.getObject(3))
      else throw new RuntimeException(s"Failed to obtain count of $tableName.")
    } finally {
      statement.close()
    }
  }

  private def computeUnevenlyWherePartInfoForNonPartTable(
      conn: Connection,
      config: OceanBaseConfig,
      priKeyColumnName: String): Array[InputPartition] = {
    val unevenlyPriKeyTableInfo =
      obtainUnevenlyPriKeyTableInfo(
        conn,
        config,
        EMPTY_STRING,
        normalizePkNameForSql(priKeyColumnName))
    if (unevenlyPriKeyTableInfo.count <= 0) Array.empty
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
      obPartInfos: Array[OBOraclePartInfo],
      priKeyColumnName: String): Array[InputPartition] = {
    val arr = ArrayBuffer[OBOraclePartition]()
    obPartInfos.foreach {
      obPartInfo =>
        val conn = OBJdbcUtils.getConnection(config)
        try {
          val partitionName = if (obPartInfo.subPartName != null) {
            PARTITION_QUERY_FORMAT.format(obPartInfo.subPartName)
          } else {
            PARTITION_QUERY_FORMAT.format(obPartInfo.partName)
          }
          val unevenlyPriKeyTableInfo =
            obtainUnevenlyPriKeyTableInfo(
              conn,
              config,
              partitionName,
              normalizePkNameForSql(priKeyColumnName))
          val partitions = computeUnevenlyWhereSparkPart(
            conn,
            unevenlyPriKeyTableInfo,
            partitionName,
            normalizePkNameForSql(priKeyColumnName),
            config)
          arr ++= partitions
        } finally {
          conn.close()
        }
    }
    arr.zipWithIndex.map {
      case (partInfo, index) =>
        OBOraclePartition(
          partInfo.partitionClause,
          limitOffsetClause = EMPTY_STRING,
          whereClause = partInfo.whereClause,
          useHiddenPKColumn = partInfo.useHiddenPKColumn,
          unevenlyWhereValue = partInfo.unevenlyWhereValue,
          idx = index
        )
    }.toArray
  }

  private def computeUnevenlyWhereSparkPart(
      conn: Connection,
      keyTableInfo: UnevenlyPriKeyTableInfo,
      partitionClause: String,
      priKeyColumnName: String,
      config: OceanBaseConfig): Array[OBOraclePartition] = {
    if (keyTableInfo.count <= 0) return Array.empty[OBOraclePartition]

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
    val arrayBuffer = ArrayBuffer[OBOraclePartition]()
    var idx = 0
    if (Objects.nonNull(chunkEnd)) {
      val whereClause = s"($priKeyColumnName < ?)"
      arrayBuffer += OBOraclePartition(
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
        val whereClause = s"($priKeyColumnName >= ? AND $priKeyColumnName < ?)"
        idx = idx + 1
        arrayBuffer += OBOraclePartition(
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
      arrayBuffer += OBOraclePartition(
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
      includedLowerBound: Object,
      max: Object,
      partitionClause: String,
      priKeyColumnName: String,
      config: OceanBaseConfig): Object = {
    var chunkEnd: Object = queryNextChunkMax(
      conn,
      chunkSize,
      includedLowerBound,
      partitionClause,
      priKeyColumnName,
      config)
    if (Objects.isNull(chunkEnd)) return chunkEnd
    if (compare(chunkEnd, max) >= 0) null else chunkEnd
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
    val pkForSql = normalizePkNameForSql(priKeyColumnName)
    val sql =
      s"""
         |SELECT MAX(%s) AS chunk_high FROM (
         |  SELECT %s FROM %s %s WHERE %s > ? ORDER BY %s ASC
         |) WHERE ROWNUM <= %d
         |""".stripMargin.format(
        pkForSql,
        pkForSql,
        tableName,
        partitionClause,
        pkForSql,
        pkForSql,
        chunkSize)
    // attach hint at the beginning of inner SELECT
    val finalSql = sql.replaceFirst("SELECT ", s"SELECT $hint ")
    val statement = conn.prepareStatement(finalSql)
    try {
      statement.setObject(1, includedLowerBound)
      val rs = statement.executeQuery()
      if (rs.next()) rs.getObject(1)
      else throw new RuntimeException("Failed to query next chunk max.")
    } finally {
      statement.close()
    }
  }

  private def compare(obj1: Any, obj2: Any): Int = (obj1, obj2) match {
    case (c1: Comparable[_], c2) if c1.getClass == c2.getClass =>
      c1.asInstanceOf[Comparable[Any]].compareTo(c2)
    case _ => obj1.toString.compareTo(obj2.toString)
  }

  private def queryTimeoutHint(config: OceanBaseConfig): String = if (
    config.getQueryTimeoutHintDegree > 0
  ) {
    s", query_timeout(${config.getQueryTimeoutHintDegree}) "
  } else {
    ""
  }
}

case class OBOraclePartInfo(partName: String, subPartName: String)

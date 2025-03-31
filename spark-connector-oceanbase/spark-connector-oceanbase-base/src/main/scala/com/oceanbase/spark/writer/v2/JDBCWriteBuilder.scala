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
package com.oceanbase.spark.writer.v2

import com.oceanbase.spark.catalog.OceanBaseCatalogException
import com.oceanbase.spark.config.OceanBaseConfig
import com.oceanbase.spark.dialect.OceanBaseDialect
import com.oceanbase.spark.utils.OBJdbcUtils

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.metric.CustomMetric
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriter, DataWriterFactory, PhysicalWriteInfo, SupportsOverwrite, SupportsTruncate, Write, WriteBuilder, WriterCommitMessage}
import org.apache.spark.sql.sources.{AlwaysTrue, Filter}
import org.apache.spark.sql.types.StructType

import scala.util.{Failure, Try}

class JDBCWriteBuilder(schema: StructType, config: OceanBaseConfig, dialect: OceanBaseDialect)
  extends WriteBuilder
  with SupportsOverwrite
  with SupportsTruncate {
  override def build(): Write = new JDBCWrite(schema, config, dialect)

  override def overwrite(filters: Array[Filter]): WriteBuilder = {
    if (filters.length == 1 && filters.head.isInstanceOf[AlwaysTrue]) {
      Try {
        OBJdbcUtils.withConnection(config) {
          conn =>
            OBJdbcUtils.executeStatement(conn, config, dialect.getTruncateQuery(config.getDbTable))
        }
      } match {
        case Failure(exception) =>
          throw OceanBaseCatalogException(
            s"Failed to truncate table ${config.getDbTable}",
            exception)
        case _ =>
      }
    } else {
      throw OceanBaseCatalogException(s"Currently only overwrite full data is supported.")
    }

    this
  }
}

class JDBCWrite(schema: StructType, config: OceanBaseConfig, dialect: OceanBaseDialect)
  extends Write {
  override def toBatch: BatchWrite = new JDBCBatchWrite(schema, config, dialect)

  override def supportedCustomMetrics(): Array[CustomMetric] = Array()
}

class JDBCBatchWrite(schema: StructType, config: OceanBaseConfig, dialect: OceanBaseDialect)
  extends BatchWrite
  with DataWriterFactory {

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = this

  override def commit(messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(messages: Array[WriterCommitMessage]): Unit = {}

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    new JDBCWriter(schema: StructType, config, dialect)
  }
}

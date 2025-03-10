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

import com.oceanbase.spark.config.OceanBaseConfig
import com.oceanbase.spark.directload.{DirectLoader, DirectLoadUtils}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriter, DataWriterFactory, PhysicalWriteInfo, WriteBuilder, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

/** Direct-load writing implementation based on Spark DataSource V2 API. */
case class DirectLoadWriteBuilderV2(schema: StructType, config: OceanBaseConfig)
  extends WriteBuilder {
  override def buildForBatch(): BatchWrite = {
    new DirectLoadBatchWrite(schema, config)
  }
}

/** This will be performed on the Driver side and no serialization is required. */
class DirectLoadBatchWrite(schema: StructType, oceanBaseConfig: OceanBaseConfig)
  extends BatchWrite {

  private val directLoader: DirectLoader =
    DirectLoadUtils.buildDirectLoaderFromSetting(oceanBaseConfig)
  private val executionId: String = directLoader.begin()
  oceanBaseConfig.set(OceanBaseConfig.DIRECT_LOAD_EXECUTION_ID, executionId)

  override def createBatchWriterFactory(
      info: PhysicalWriteInfo
  ): DataWriterFactory = {

    new DirectLoadDataWriterFactory(schema: StructType, oceanBaseConfig)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    directLoader.commit()
    directLoader.close()
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    directLoader.close()
  }
}

class DirectLoadDataWriterFactory(
    schema: StructType,
    oceanBaseConfig: OceanBaseConfig
) extends DataWriterFactory {

  override def createWriter(
      partitionId: Int,
      taskId: Long
  ): DataWriter[InternalRow] = {
    new DirectLoadWriteV2(schema, oceanBaseConfig)
  }
}

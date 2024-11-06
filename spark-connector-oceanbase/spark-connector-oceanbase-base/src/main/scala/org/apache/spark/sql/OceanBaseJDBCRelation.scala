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
package org.apache.spark.sql

import com.oceanbase.spark.cfg.ConnectionOptions

import org.apache.spark.{sql, Partition}
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCRelation}
import org.apache.spark.sql.types.StructType

class OceanBaseJDBCRelation(
    override val schema: StructType,
    override val parts: Array[Partition],
    override val jdbcOptions: JDBCOptions)(@transient override val sparkSession: SparkSession)
  extends JDBCRelation(schema, parts, jdbcOptions)(sparkSession) {

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    if (
      jdbcOptions.parameters
        .getOrElse(
          ConnectionOptions.ENABLE_DIRECT_LOAD_WRITE,
          s"${ConnectionOptions.ENABLE_DIRECT_LOAD_WRITE_DEFAULT}")
        .toBoolean
    ) {
      data.write
        .format(OceanBaseJdbcSparkSource.SHORT_NAME)
        .options(jdbcOptions.parameters)
        .mode(if (overwrite) sql.SaveMode.Overwrite else sql.SaveMode.Append)
        .save()
    } else {
      super.insert(data, overwrite)
    }

  }
}

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

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.connector.expressions.{BucketTransform, DaysTransform, HoursTransform, IdentityTransform, Transform, YearsTransform}
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite
import org.apache.spark.sql.jdbc.OceanBaseMySQLDialect

object ExprUtils extends SQLConfHelper with Serializable {

  def toOBMySQLPartition(transform: Transform, options: JdbcOptionsInWrite): String =
    transform match {
      case bucket: BucketTransform =>
        val identities = bucket.columns
          .map(col => OceanBaseMySQLDialect.quoteIdentifier(col.fieldNames().head))
          .mkString(",")
        s"PARTITION BY KEY($identities) PARTITIONS ${bucket.numBuckets.value()}".stripMargin
      case _: YearsTransform | _: DaysTransform | _: HoursTransform | _: IdentityTransform =>
        throw new UnsupportedOperationException("OceanBase does not support dynamic partitions.")
      case other: Transform =>
        throw new UnsupportedOperationException(s"Unsupported transform: ${other.name()}")
    }
}

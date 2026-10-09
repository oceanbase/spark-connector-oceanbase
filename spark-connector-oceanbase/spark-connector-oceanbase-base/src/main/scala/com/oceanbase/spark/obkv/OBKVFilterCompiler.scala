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

package com.oceanbase.spark.obkv

import com.alipay.oceanbase.rpc.filter.{ObCompareOp, ObTableFilter, ObTableFilterList, ObTableValueFilter}
import org.apache.spark.sql.sources._

import scala.collection.mutable

/**
 * Compiles Spark [[Filter]] instances into OBKV server-side filters ([[ObTableFilter]]).
 *
 * Filters on non-primary-key columns are converted to [[ObTableValueFilter]] for server-side
 * evaluation. Filters that cannot be pushed down are returned as unhandled filters for Spark-side
 * evaluation.
 */
class OBKVFilterCompiler(primaryKeys: Array[String]) {

  private val primaryKeyColumns: Set[String] = primaryKeys.toSet

  /** Compiles an array of Spark Filters into OBKV server-side filters. */
  def compile(filters: Array[Filter]): OBKVFilterCompiler.CompileResult = {
    val compiledFilters = mutable.ArrayBuffer[ObTableFilter]()
    val unhandled = mutable.ArrayBuffer[Filter]()

    for (filter <- filters) {
      compileFilter(filter) match {
        case Some(compiled) => compiledFilters += compiled
        case None => unhandled += filter
      }
    }

    val serverFilter = if (compiledFilters.size == 1) {
      compiledFilters.head
    } else if (compiledFilters.size > 1) {
      new ObTableFilterList(ObTableFilterList.operator.AND, compiledFilters.toArray: _*)
    } else {
      null
    }

    OBKVFilterCompiler.CompileResult(serverFilter, unhandled.toArray)
  }

  private def compileFilter(filter: Filter): Option[ObTableFilter] = {
    filter match {
      case EqualTo(attr, value) =>
        Some(new ObTableValueFilter(ObCompareOp.EQ, attr, value))
      case GreaterThan(attr, value) =>
        Some(new ObTableValueFilter(ObCompareOp.GT, attr, value))
      case GreaterThanOrEqual(attr, value) =>
        Some(new ObTableValueFilter(ObCompareOp.GE, attr, value))
      case LessThan(attr, value) =>
        Some(new ObTableValueFilter(ObCompareOp.LT, attr, value))
      case LessThanOrEqual(attr, value) =>
        Some(new ObTableValueFilter(ObCompareOp.LE, attr, value))
      case IsNull(attr) =>
        Some(new ObTableValueFilter(ObCompareOp.IS, attr, null))
      case IsNotNull(attr) =>
        Some(new ObTableValueFilter(ObCompareOp.IS_NOT, attr, null))
      case And(left, right) =>
        for {
          l <- compileFilter(left)
          r <- compileFilter(right)
        } yield new ObTableFilterList(ObTableFilterList.operator.AND, l, r)
      case Or(left, right) =>
        for {
          l <- compileFilter(left)
          r <- compileFilter(right)
        } yield new ObTableFilterList(ObTableFilterList.operator.OR, l, r)
      case _: Not =>
        // OBKV does not natively support NOT, skip it
        None
      case _ => None
    }
  }

  /** Checks if a filter references a primary key column. */
  def isPrimaryKeyFilter(filter: Filter): Boolean = {
    filter match {
      case EqualTo(attr, _) => primaryKeyColumns.contains(attr)
      case GreaterThan(attr, _) => primaryKeyColumns.contains(attr)
      case GreaterThanOrEqual(attr, _) => primaryKeyColumns.contains(attr)
      case LessThan(attr, _) => primaryKeyColumns.contains(attr)
      case LessThanOrEqual(attr, _) => primaryKeyColumns.contains(attr)
      case _ => false
    }
  }
}

object OBKVFilterCompiler {

  /** The result of compiling Spark filters. */
  case class CompileResult(serverFilter: ObTableFilter, unhandledFilters: Array[Filter])
}

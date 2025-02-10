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
package com.oceanbase.spark.utils

import java.util.concurrent.TimeUnit

import scala.annotation.tailrec
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}

object RetryUtils {

  /**
   * Automatically retries a potentially failing operation
   *
   * @param maxAttempts
   *   Maximum number of attempts (including initial execution), default 3
   * @param initialDelay
   *   Initial retry delay (will change with attempts), default 1 second
   * @param backoffPolicy
   *   Backoff policy function (params: current attempt, last delay), default exponential backoff
   * @param retryCondition
   *   Predicate to determine if exception should be retried, default retry all exceptions
   * @param block
   *   Block containing the operation that might fail
   * @tparam T
   *   Return type of the operation
   * @return
   *   Successful result or throws last exception
   */
  def retry[T](
      maxAttempts: Int = 3,
      initialDelay: FiniteDuration = FiniteDuration(1, TimeUnit.SECONDS),
      backoffPolicy: (Int, FiniteDuration) => FiniteDuration = (_, delay) => delay * 2,
      retryCondition: Throwable => Boolean = _ => true
  )(block: => T): T = {

    require(maxAttempts > 0, "The maximum number of attempts must be greater than 0")

    @tailrec
    def retryLoop(attempt: Int, currentDelay: FiniteDuration): T = {
      Try(block) match {
        case Success(result) =>
          result

        case Failure(e) if attempt < maxAttempts - 1 && retryCondition(e) =>
          Thread.sleep(currentDelay.toMillis)
          val nextDelay = backoffPolicy(attempt + 1, currentDelay)
          retryLoop(attempt + 1, nextDelay)

        case Failure(e) =>
          throw new Exception(s"The operation failed after $maxAttempts attempts", e)
      }
    }

    retryLoop(0, initialDelay)
  }
}

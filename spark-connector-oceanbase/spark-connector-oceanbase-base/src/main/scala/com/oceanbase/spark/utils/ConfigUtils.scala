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
import org.apache.hadoop.security.alias.CredentialProviderFactory
import org.apache.spark.sql.SparkSession

object ConfigUtils {
  def findFromRuntimeConf(key: String): String = {
    SparkSession.active.conf.getAll.find {
      case (k, _) if k.contains(key) => true
      case _ => false
    } match {
      case Some((_, v)) => v
      case _ => null
    }
  }

  def getCredentialFromAlias(alias: String): String = {
    val providers =
      CredentialProviderFactory.getProviders(SparkSession.active.sparkContext.hadoopConfiguration)
    if (providers == null || providers.isEmpty) {
      throw new RuntimeException("No credential provider is configured or loaded.")
    }

    var credential: String = null
    // for scala 2.11 compatibility
    val iterator = providers.iterator()
    while (iterator.hasNext) {
      val provider = iterator.next()
      val entry = provider.getCredentialEntry(alias)
      if (entry != null) {
        credential = entry.getCredential.mkString
      }
    }
    if (credential == null) {
      throw new RuntimeException(s"Alias '$alias' not found in credential provider.")
    }
    credential
  }
}

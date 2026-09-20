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
package com.oceanbase.spark.config

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.alias.CredentialProviderFactory
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse}
import org.junit.jupiter.api.Test

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.file.{Files, Path}

import scala.collection.JavaConverters._

class CredentialAliasTest {

  @Test
  def testResolvedPasswordCanBeReadWithoutActiveSparkSession(): Unit = {
    val tempDir = Files.createTempDirectory("test-credentials")
    val keystoreFile = tempDir.resolve("test.jceks")
    val keystorePath = s"jceks://file${keystoreFile.toAbsolutePath}"
    val alias = "credential.alias.serialization.test"
    val password = "test-secret"

    val hadoopConf = new Configuration()
    hadoopConf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, keystorePath)
    val provider = CredentialProviderFactory.getProviders(hadoopConf).get(0)
    provider.createCredentialEntry(alias, password.toCharArray)
    provider.flush()

    try {
      val config = new OceanBaseConfig(
        Map(OceanBaseConfig.PASSWORD.getKey -> s"alias:$alias").asJava)
      config.resolvePasswordAlias(hadoopConf)

      assertEquals(password, config.getRawString(OceanBaseConfig.PASSWORD.getKey))

      val output = new ByteArrayOutputStream()
      val objectOutput = new ObjectOutputStream(output)
      objectOutput.writeObject(config)
      objectOutput.close()

      val objectInput = new ObjectInputStream(new ByteArrayInputStream(output.toByteArray))
      val restored = objectInput.readObject().asInstanceOf[OceanBaseConfig]
      objectInput.close()

      assertFalse(restored.getRawString(OceanBaseConfig.PASSWORD.getKey).startsWith("alias:"))
      assertEquals(password, restored.getPassword)
    } finally {
      deleteRecursively(tempDir)
    }
  }

  private def deleteRecursively(path: Path): Unit = {
    val paths = Files.walk(path)
    try {
      paths.iterator().asScala.toSeq.reverse.foreach(Files.deleteIfExists)
    } finally {
      paths.close()
    }
  }
}

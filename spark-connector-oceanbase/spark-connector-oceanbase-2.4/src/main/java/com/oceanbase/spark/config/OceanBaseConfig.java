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

package com.oceanbase.spark.config;

import java.io.Serializable;
import java.util.Map;


import org.apache.commons.lang3.StringUtils;

public class OceanBaseConfig extends Config implements Serializable {
    public static final ConfigEntry<String> URL =
            new ConfigBuilder("url")
                    .doc("The connection URL")
                    .version(ConfigConstants.VERSION_1_0_0)
                    .stringConf()
                    .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
                    .create();

    public static final ConfigEntry<String> USERNAME =
            new ConfigBuilder("username")
                    .doc("The username")
                    .version(ConfigConstants.VERSION_1_0_0)
                    .stringConf()
                    .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
                    .create();

    public static final ConfigEntry<String> PASSWORD =
            new ConfigBuilder("password")
                    .doc("The password")
                    .version(ConfigConstants.VERSION_1_0_0)
                    .stringConf()
                    .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
                    .create();

    public static final ConfigEntry<String> SCHEMA_NAME =
            new ConfigBuilder("schema-name")
                    .doc("The schema name or database name")
                    .version(ConfigConstants.VERSION_1_0_0)
                    .stringConf()
                    .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
                    .create();

    public static final ConfigEntry<String> TABLE_NAME =
            new ConfigBuilder("table-name")
                    .doc("The table name")
                    .version(ConfigConstants.VERSION_1_0_0)
                    .stringConf()
                    .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
                    .create();

    public static final ConfigEntry<Boolean> DIRECT_LOAD_ENABLE =
            new ConfigBuilder("direct-load.enabled")
                    .doc("Enable direct-load writing")
                    .version(ConfigConstants.VERSION_1_0_0)
                    .booleanConf()
                    .createWithDefault(false);

    public static final ConfigEntry<String> DIRECT_LOAD_HOST =
            new ConfigBuilder("direct-load.host")
                    .doc("The direct-load host")
                    .version(ConfigConstants.VERSION_1_0_0)
                    .stringConf()
                    .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
                    .create();

    public static final ConfigEntry<Integer> DIRECT_LOAD_RPC_PORT =
            new ConfigBuilder("direct-load.rpc-port")
                    .doc("Rpc port number used in direct-load")
                    .version(ConfigConstants.VERSION_1_0_0)
                    .intConf()
                    .checkValue(port -> port > 0, ConfigConstants.POSITIVE_NUMBER_ERROR_MSG)
                    .createWithDefault(2882);

    public static final ConfigEntry<Integer> DIRECT_LOAD_PARALLEL =
            new ConfigBuilder("direct-load.parallel")
                    .doc(
                            "The parallel of the direct-load server. This parameter determines how much CPU resources the server uses to process this import task")
                    .version(ConfigConstants.VERSION_1_0_0)
                    .intConf()
                    .checkValue(port -> port > 0, ConfigConstants.POSITIVE_NUMBER_ERROR_MSG)
                    .createWithDefault(8);

    public static final ConfigEntry<String> DIRECT_LOAD_USERNAME =
            new ConfigBuilder("direct-load.username")
                    .doc("The username used in direct-load")
                    .version(ConfigConstants.VERSION_1_1_0)
                    .stringConf()
                    .create();

    public static final ConfigEntry<Integer> DIRECT_LOAD_WRITE_THREAD_NUM =
            new ConfigBuilder("direct-load.write-thread-num")
                    .doc("The number of threads to use when direct-load client are writing.")
                    .version(ConfigConstants.VERSION_1_1_0)
                    .intConf()
                    .checkValue(port -> port > 0, ConfigConstants.POSITIVE_NUMBER_ERROR_MSG)
                    .createWithDefault(1);

    public static final ConfigEntry<String> DIRECT_LOAD_EXECUTION_ID =
            new ConfigBuilder("direct-load.execution-id")
                    .doc("The execution id")
                    .version(ConfigConstants.VERSION_1_0_0)
                    .stringConf()
                    .create();

    public static final ConfigEntry<String> DIRECT_LOAD_DUP_ACTION =
            new ConfigBuilder("direct-load.dup-action")
                    .doc(
                            "Action when there is duplicated record of direct-load task. Can be STOP_ON_DUP, REPLACE or IGNORE")
                    .version(ConfigConstants.VERSION_1_0_0)
                    .stringConf()
                    .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
                    .createWithDefault("REPLACE");

    public static final ConfigEntry<Long> DIRECT_LOAD_TIMEOUT =
            new ConfigBuilder("direct-load.timeout")
                    .doc("The timeout for direct-load task.")
                    .version(ConfigConstants.VERSION_1_0_0)
                    .longConf()
                    .createWithDefault(70000L);

    public static final ConfigEntry<Long> DIRECT_LOAD_HEARTBEAT_TIMEOUT =
            new ConfigBuilder("direct-load.heartbeat-timeout")
                    .doc("Client heartbeat timeout in direct-load task")
                    .version(ConfigConstants.VERSION_1_0_0)
                    .longConf()
                    .createWithDefault(60000L);

    public static final ConfigEntry<Long> DIRECT_LOAD_HEARTBEAT_INTERVAL =
            new ConfigBuilder("direct-load.heartbeat-interval")
                    .doc("Client heartbeat interval in direct-load task")
                    .version(ConfigConstants.VERSION_1_0_0)
                    .longConf()
                    .createWithDefault(10L);

    public static final ConfigEntry<String> DIRECT_LOAD_LOAD_METHOD =
            new ConfigBuilder("direct-load.load-method")
                    .doc("The direct-load load mode: full, inc, inc_replace")
                    .version(ConfigConstants.VERSION_1_0_0)
                    .stringConf()
                    .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
                    .createWithDefault("full");

    public static final ConfigEntry<Integer> DIRECT_LOAD_MAX_ERROR_ROWS =
            new ConfigBuilder("Maximum tolerable number of error rows")
                    .doc("direct-load.max-error-rows")
                    .version(ConfigConstants.VERSION_1_0_0)
                    .intConf()
                    .checkValue(size -> size >= 0, "The value must be greater than or equal to 0")
                    .createWithDefault(0);

    public static final ConfigEntry<Integer> DIRECT_LOAD_BATCH_SIZE =
            new ConfigBuilder("direct-load-batch-size")
                    .doc("The batch size write to OceanBase one time")
                    .version(ConfigConstants.VERSION_1_0_0)
                    .intConf()
                    .checkValue(size -> size > 0, ConfigConstants.POSITIVE_NUMBER_ERROR_MSG)
                    .createWithDefault(10240);

    public static final ConfigEntry<Integer> DIRECT_LOAD_TASK_PARTITION_SIZE =
            new ConfigBuilder("direct-load.task-partition-size")
                    .doc("The number of partitions corresponding to the Writing task")
                    .version(ConfigConstants.VERSION_1_0_0)
                    .intConf()
                    .create();

    public static final ConfigEntry<Boolean> DIRECT_LOAD_TASK_USE_REPARTITION =
            new ConfigBuilder("direct-load.task-use-repartition")
                    .doc(
                            "Whether to use repartition mode to control the number of partitions written by OceanBase")
                    .version(ConfigConstants.VERSION_1_0_0)
                    .booleanConf()
                    .createWithDefault(false);

    public OceanBaseConfig(Map<String, String> properties) {
        super();
        loadFromMap(properties, k -> true);
    }

    public String getURL() {
        return get(URL);
    }

    public String getUsername() {
        return get(USERNAME);
    }

    public String getPassword() {
        return get(PASSWORD);
    }

    public String getSchemaName() {
        return get(SCHEMA_NAME);
    }

    public String getTableName() {
        return get(TABLE_NAME);
    }

    public Boolean getDirectLoadEnable() {
        return get(DIRECT_LOAD_ENABLE);
    }

    public String getDirectLoadHost() {
        return get(DIRECT_LOAD_HOST);
    }

    public int getDirectLoadPort() {
        return get(DIRECT_LOAD_RPC_PORT);
    }

    public String getDirectLoadExecutionId() {
        return get(DIRECT_LOAD_EXECUTION_ID);
    }

    public int getDirectLoadParallel() {
        return get(DIRECT_LOAD_PARALLEL);
    }

    public String getDirectLoadUserName() {
        return get(DIRECT_LOAD_USERNAME);
    }

    public int getDirectLoadWriteThreadNum() {
        return get(DIRECT_LOAD_WRITE_THREAD_NUM);
    }

    public int getBatchSize() {
        return get(DIRECT_LOAD_BATCH_SIZE);
    }

    public long getDirectLoadMaxErrorRows() {
        return get(DIRECT_LOAD_MAX_ERROR_ROWS);
    }

    public String getDirectLoadDupAction() {
        return get(DIRECT_LOAD_DUP_ACTION);
    }

    public long getDirectLoadTimeout() {
        return get(DIRECT_LOAD_TIMEOUT);
    }

    public long getDirectLoadHeartbeatTimeout() {
        return get(DIRECT_LOAD_HEARTBEAT_TIMEOUT);
    }

    public long getDirectLoadHeartbeatInterval() {
        return get(DIRECT_LOAD_HEARTBEAT_INTERVAL);
    }

    public String getDirectLoadLoadMethod() {
        return get(DIRECT_LOAD_LOAD_METHOD);
    }

    public Integer getDirectLoadTaskPartitionSize() {
        return get(DIRECT_LOAD_TASK_PARTITION_SIZE);
    }

    public boolean getDirectLoadUseRepartition() {
        return get(DIRECT_LOAD_TASK_USE_REPARTITION);
    }
}

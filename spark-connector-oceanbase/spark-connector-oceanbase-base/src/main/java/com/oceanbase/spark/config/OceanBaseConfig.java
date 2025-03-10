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
import java.time.Duration;
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

    public static final ConfigEntry<Duration> DIRECT_LOAD_TIMEOUT =
            new ConfigBuilder("direct-load.timeout")
                    .doc("The timeout for direct-load task.")
                    .version(ConfigConstants.VERSION_1_0_0)
                    .durationConf()
                    .createWithDefault(Duration.ofDays(7));

    public static final ConfigEntry<Duration> DIRECT_LOAD_HEARTBEAT_TIMEOUT =
            new ConfigBuilder("direct-load.heartbeat-timeout")
                    .doc("Client heartbeat timeout in direct-load task")
                    .version(ConfigConstants.VERSION_1_0_0)
                    .durationConf()
                    .createWithDefault(Duration.ofSeconds(60));

    public static final ConfigEntry<Duration> DIRECT_LOAD_HEARTBEAT_INTERVAL =
            new ConfigBuilder("direct-load.heartbeat-interval")
                    .doc("Client heartbeat interval in direct-load task")
                    .version(ConfigConstants.VERSION_1_0_0)
                    .durationConf()
                    .createWithDefault(Duration.ofSeconds(10));

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

    // ======== JDBC Related =========
    public static final ConfigEntry<String> DRIVER =
            new ConfigBuilder("driver")
                    .doc("The class name of the JDBC driver to use to connect to this URL.")
                    .version(ConfigConstants.VERSION_1_1_0)
                    .stringConf()
                    .create();

    public static final ConfigEntry<Integer> JDBC_QUERY_TIMEOUT =
            new ConfigBuilder("jdbc.query-timeout")
                    .doc(
                            "The number of seconds the driver will wait for a Statement object to execute to the given number of seconds. Zero means there is no limit. In the write path, this option depends on how JDBC drivers implement the API setQueryTimeout.")
                    .version(ConfigConstants.VERSION_1_1_0)
                    .intConf()
                    .checkValue(value -> value >= 0, ConfigConstants.POSITIVE_NUMBER_ERROR_MSG)
                    .createWithDefault(0);

    public static final ConfigEntry<Integer> JDBC_FETCH_SIZE =
            new ConfigBuilder("jdbc.fetch-size")
                    .doc(
                            "The JDBC fetch size, which determines how many rows to fetch per round trip.")
                    .version(ConfigConstants.VERSION_1_1_0)
                    .intConf()
                    .checkValue(value -> value >= 0, ConfigConstants.POSITIVE_NUMBER_ERROR_MSG)
                    .createWithDefault(100);

    public static final ConfigEntry<Integer> JDBC_BATCH_SIZE =
            new ConfigBuilder("jdbc.batch-size")
                    .doc(
                            "The JDBC batch size, which determines how many rows to insert per round trip. This can help performance on JDBC drivers. This option applies only to writing.")
                    .version(ConfigConstants.VERSION_1_1_0)
                    .intConf()
                    .checkValue(value -> value >= 0, ConfigConstants.POSITIVE_NUMBER_ERROR_MSG)
                    .createWithDefault(1024);

    public static final ConfigEntry<Boolean> JDBC_PUSH_DOWN_PREDICATE =
            new ConfigBuilder("jdbc.pushDownPredicate")
                    .doc(
                            "The option to enable or disable predicate push-down into the JDBC data source. The default value is true, in which case Spark will push down filters to the JDBC data source as much as possible.")
                    .version(ConfigConstants.VERSION_1_1_0)
                    .booleanConf()
                    .createWithDefault(false);

    public static final String DB_TABLE = "dbTable";
    public static final String TABLE_COMMENT = "tableComment";
    public static final String ENABLE_LEGACY_BATCH_READER = "enable_legacy_batch_reader";

    public OceanBaseConfig(Map<String, String> properties) {
        super();
        loadFromMap(properties, k -> true);
    }

    public Map<String, String> getProperties() {
        return super.configMap;
    }

    public void setProperty(String key, String value) {
        super.configMap.put(key, value);
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

    public int getDirectLoadBatchSize() {
        return get(DIRECT_LOAD_BATCH_SIZE);
    }

    public long getDirectLoadMaxErrorRows() {
        return get(DIRECT_LOAD_MAX_ERROR_ROWS);
    }

    public String getDirectLoadDupAction() {
        return get(DIRECT_LOAD_DUP_ACTION);
    }

    public long getDirectLoadTimeout() {
        return get(DIRECT_LOAD_TIMEOUT).toMillis();
    }

    public long getDirectLoadHeartbeatTimeout() {
        return get(DIRECT_LOAD_HEARTBEAT_TIMEOUT).toMillis();
    }

    public long getDirectLoadHeartbeatInterval() {
        return get(DIRECT_LOAD_HEARTBEAT_INTERVAL).toMillis();
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

    public Integer getJdbcFetchSize() {
        return get(JDBC_FETCH_SIZE);
    }

    public Integer getJdbcBatchSize() {
        return get(JDBC_BATCH_SIZE);
    }

    public Integer getJdbcQueryTimeout() {
        return get(JDBC_QUERY_TIMEOUT);
    }

    public String getDriver() {
        return get(DRIVER);
    }

    public String getDbTable() {
        return getProperties().get(DB_TABLE);
    }

    public String getTableComment() {
        return getProperties().get(TABLE_COMMENT);
    }

    public Boolean getPushDownPredicate() {
        return get(JDBC_PUSH_DOWN_PREDICATE);
    }
}

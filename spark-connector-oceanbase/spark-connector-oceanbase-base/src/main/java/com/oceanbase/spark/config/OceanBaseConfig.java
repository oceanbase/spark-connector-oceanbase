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

import com.oceanbase.spark.dialect.OceanBaseDialect;
import com.oceanbase.spark.utils.ConfigUtils;

import java.io.Serializable;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;


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

    public static final ConfigEntry<Boolean> DIRECT_LOAD_ODP_MODE =
            new ConfigBuilder("direct-load.odp-mode")
                    .doc("Whether to use ODP proxy for direct-load")
                    .version(ConfigConstants.VERSION_1_4_0)
                    .booleanConf()
                    .createWithDefault(false);

    public static final ConfigEntry<String> DIRECT_LOAD_USERNAME =
            new ConfigBuilder("direct-load.username")
                    .doc("The username used in direct-load")
                    .version(ConfigConstants.VERSION_1_1_0)
                    .stringConf()
                    .create();

    public static final ConfigEntry<Integer> DIRECT_LOAD_PARALLEL =
            new ConfigBuilder("direct-load.parallel")
                    .doc(
                            "The parallel of the direct-load server. This parameter determines how much CPU resources the server uses to process this import task")
                    .version(ConfigConstants.VERSION_1_0_0)
                    .intConf()
                    .checkValue(port -> port > 0, ConfigConstants.POSITIVE_NUMBER_ERROR_MSG)
                    .createWithDefault(8);

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
            new ConfigBuilder("direct-load.batch-size")
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

    public static final ConfigEntry<Boolean> JDBC_ENABLE_AUTOCOMMIT =
            new ConfigBuilder("jdbc.enable-autocommit")
                    .doc("Declare whether to enable autocommit when writing data using JDBC.")
                    .version(ConfigConstants.VERSION_1_3_0)
                    .booleanConf()
                    .createWithDefault(false);

    public static final ConfigEntry<Boolean> JDBC_PUSH_DOWN_PREDICATE =
            new ConfigBuilder("jdbc.push-down-predicate")
                    .doc(
                            "The option to enable or disable predicate push-down into the JDBC data source. The default value is true, in which case Spark will push down filters to the JDBC data source as much as possible.")
                    .version(ConfigConstants.VERSION_1_1_0)
                    .booleanConf()
                    .createWithDefault(false);

    public static final ConfigEntry<Boolean> JDBC_PUSH_DOWN_AGGREGATE =
            new ConfigBuilder("jdbc.push-down-aggregate")
                    .doc(
                            "The option to enable or disable aggregate push-down into the JDBC data source.")
                    .version(ConfigConstants.VERSION_1_3_0)
                    .booleanConf()
                    .createWithDefault(true);

    public static final ConfigEntry<Integer> JDBC_PARALLEL_HINT_DEGREE =
            new ConfigBuilder("jdbc.parallel-hint-degree")
                    .doc(
                            "The SQL statements sent by Spark to OB will automatically carry PARALLEL Hint. This parameter can be used to adjust the parallelism, and the default value is 1.")
                    .version(ConfigConstants.VERSION_1_1_0)
                    .intConf()
                    .createWithDefault(1);

    // https://www.oceanbase.com/docs/enterprise-oceanbase-database-cn-10000000000881388
    public static final ConfigEntry<Integer> JDBC_QUERY_TIMEOUT_HINT_DEGREE =
            new ConfigBuilder("jdbc.query-timeout-hint-degree")
                    .doc(
                            "The SQL statements sent by Spark to OB will automatically carry query_timeout Hint. Set this parameter to specify the timeout in microseconds. The default is -1, which means query_timeout hint is not set.")
                    .version(ConfigConstants.VERSION_1_2_0)
                    .intConf()
                    .createWithDefault(-1);

    public static final ConfigEntry<Integer> JDBC_STATISTICS_PARALLEL_HINT_DEGREE =
            new ConfigBuilder("jdbc.statistics-parallel-hint-degree")
                    .doc(
                            "Controls the parallelism level for statistical queries (e.g., COUNT, MIN, MAX) by adding /*+ PARALLEL(N) */ hint to generated SQL.")
                    .version(ConfigConstants.VERSION_1_2_0)
                    .intConf()
                    .createWithDefault(4);

    public static final ConfigEntry<Integer> JDBC_PARTITION_COMPUTE_PARALLELISM =
            new ConfigBuilder("jdbc.partition-compute-parallelism")
                    .doc(
                            "Controls the parallelism level for partition computation. This parameter determines the number of threads used when computing partitions for partitioned tables. Higher values can improve performance for tables with many partitions.")
                    .version(ConfigConstants.VERSION_1_3_0)
                    .intConf()
                    .createWithDefault(32);

    public static final ConfigEntry<Long> JDBC_MAX_RECORDS_PER_PARTITION =
            new ConfigBuilder("jdbc.max-records-per-partition")
                    .doc(
                            "When Spark reads OB, the maximum number of data records that can be used as a Spark partition.")
                    .version(ConfigConstants.VERSION_1_1_0)
                    .longConf()
                    .create();

    public static final ConfigEntry<Boolean> JDBC_ENABLE_REWRITE_QUERY_SQL =
            new ConfigBuilder("jdbc.enable-rewrite-query-sql")
                    .doc(
                            "Whether to enable rewriting query SQL through inner join to optimize deep paging performance.")
                    .version(ConfigConstants.VERSION_1_1_0)
                    .booleanConf()
                    .createWithDefault(false);

    public static final ConfigEntry<Boolean> JDBC_ENABLE_PUSH_DOWN_LIMIT =
            new ConfigBuilder("jdbc.enable-pushdown-limit")
                    .doc("Whether to enable pushdown of LIMIT clause to OceanBase.")
                    .version(ConfigConstants.VERSION_1_2_0)
                    .booleanConf()
                    .createWithDefault(true);

    public static final ConfigEntry<Boolean> JDBC_ENABLE_PUSH_DOWN_TOP_N =
            new ConfigBuilder("jdbc.enable-pushdown-top-n")
                    .doc(
                            "Whether to enable pushdown of ORDER BY ... LIMIT N (Top-N) queries to OceanBase. This configuration only takes effect when 'jdbc.enable-pushdown-limit' is true.")
                    .version(ConfigConstants.VERSION_1_2_0)
                    .booleanConf()
                    .createWithDefault(true);

    public static final ConfigEntry<Boolean> DISABLE_PK_TABLE_USE_WHERE_PARTITION =
            new ConfigBuilder("jdbc.disable-pk-table-use-where-partition")
                    .doc(
                            "When true, primary key tables will be prohibited from using WHERE clause partitioning.")
                    .version(ConfigConstants.VERSION_1_2_0)
                    .booleanConf()
                    .createWithDefault(false);

    public static final ConfigEntry<String> SPECIFY_PK_TABLE_PARTITION_COLUMN =
            new ConfigBuilder("jdbc.%s.partition-column")
                    .doc(
                            "You can manually specify the primary key table partition column, and by default, one will be automatically selected from the primary key columns.")
                    .version(ConfigConstants.VERSION_1_2_0)
                    .stringConf()
                    .create();

    public static final ConfigEntry<Integer> THE_LENGTH_STRING_TO_VARCHAR_TABLE_CREATE =
            new ConfigBuilder("string-as-varchar-length")
                    .doc(
                            "Defines the length of VARCHAR type when mapping String types during table creation. Default: 1024.")
                    .version(ConfigConstants.VERSION_1_2_0)
                    .intConf()
                    .createWithDefault(1024);

    public static final ConfigEntry<Boolean> ENABLE_STRING_TO_TEXT =
            new ConfigBuilder("enable-string-as-text")
                    .doc(
                            "When this option is true, the string type of spark will be converted to text type of OceanBase when creating a table.")
                    .version(ConfigConstants.VERSION_1_2_0)
                    .booleanConf()
                    .createWithDefault(false);

    public static final ConfigEntry<Boolean> ENABLE_SPARK_VARCHAR_DATA_TYPE =
            new ConfigBuilder("enable-spark-varchar-datatype")
                    .doc(
                            "When this option is true, the varchar type of OceanBase will be converted to spark's varchar type. Note that spark varchar type is an experimental feature.")
                    .version(ConfigConstants.VERSION_1_2_0)
                    .booleanConf()
                    .createWithDefault(false);

    public static final ConfigEntry<Boolean> ENABLE_ALWAYS_NULLABLE =
            new ConfigBuilder("enable-always-nullable")
                    .doc(
                            "Forces all fields to be marked as nullable during schema inference, regardless of the database metadata's nullability constraints. This provides a safety net for handling data sources with incomplete metadata or implicit null values.")
                    .version(ConfigConstants.VERSION_1_2_0)
                    .booleanConf()
                    .createWithDefault(true);

    public static final ConfigEntry<Boolean> JDBC_UPSERT_BY_UNIQUE_KEY =
            new ConfigBuilder("jdbc.upsert-by-unique-key")
                    .doc(
                            "When a table has both primary key and unique key index, this option controls which key to use for determining conflict detection. "
                                    + "If true, uses unique key for conflict detection and updates all columns except unique key columns (including primary key columns). "
                                    + "If false (default), uses primary key for conflict detection and updates all columns except primary key columns.")
                    .version(ConfigConstants.VERSION_1_4_0)
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
        String password = get(PASSWORD);
        if (StringUtils.isNotBlank(password) && password.startsWith("alias:")) {
            try {
                return ConfigUtils.getCredentialFromAlias(password.substring(6));
            } catch (Exception e) {
                throw new RuntimeException(
                        String.format(
                                "Failed to get password from hadoop credential alias: %s",
                                password),
                        e);
            }
        }
        return password;
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

    public Boolean getDirectLoadOdpMode() {
        return get(DIRECT_LOAD_ODP_MODE);
    }

    public String getDirectLoadUserName() {
        return get(DIRECT_LOAD_USERNAME);
    }

    public String getDirectLoadExecutionId() {
        return get(DIRECT_LOAD_EXECUTION_ID);
    }

    public int getDirectLoadParallel() {
        return get(DIRECT_LOAD_PARALLEL);
    }

    public int getDirectLoadWriteThreadNum() {
        return get(DIRECT_LOAD_WRITE_THREAD_NUM);
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

    public Boolean getJdbcEnableAutoCommit() {
        return get(JDBC_ENABLE_AUTOCOMMIT);
    }

    public Integer getJdbcQueryTimeout() {
        return get(JDBC_QUERY_TIMEOUT);
    }

    public String getDriver() {
        return get(DRIVER);
    }

    public Boolean getEnableRewriteQuerySql() {
        return get(JDBC_ENABLE_REWRITE_QUERY_SQL);
    }

    public Boolean getEnablePushdownLimit() {
        return get(JDBC_ENABLE_PUSH_DOWN_LIMIT);
    }

    public Boolean getEnablePushdownTopN() {
        return get(JDBC_ENABLE_PUSH_DOWN_TOP_N);
    }

    public Boolean getDisableIntPkTableUseWherePartition() {
        return get(DISABLE_PK_TABLE_USE_WHERE_PARTITION);
    }

    public Integer getLengthString2Varchar() {
        return get(THE_LENGTH_STRING_TO_VARCHAR_TABLE_CREATE);
    }

    public Boolean getEnableString2Text() {
        return get(ENABLE_STRING_TO_TEXT);
    }

    public Boolean getEnableSparkVarcharDataType() {
        return get(ENABLE_SPARK_VARCHAR_DATA_TYPE);
    }

    public Boolean getEnableAlwaysNullable() {
        return get(ENABLE_ALWAYS_NULLABLE);
    }

    public Boolean getJdbcUpsertByUniqueKey() {
        return get(JDBC_UPSERT_BY_UNIQUE_KEY);
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

    public Boolean getPushDownAggregate() {
        return get(JDBC_PUSH_DOWN_AGGREGATE);
    }

    public Integer getJdbcParallelHintDegree() {
        return get(JDBC_PARALLEL_HINT_DEGREE);
    }

    public Integer getQueryTimeoutHintDegree() {
        return get(JDBC_QUERY_TIMEOUT_HINT_DEGREE);
    }

    public Integer getJdbcStatsParallelHintDegree() {
        return get(JDBC_STATISTICS_PARALLEL_HINT_DEGREE);
    }

    public Integer getJdbcPartitionComputeParallelism() {
        return get(JDBC_PARTITION_COMPUTE_PARALLELISM);
    }

    public Optional<Long> getJdbcMaxRecordsPrePartition() {
        return Optional.ofNullable(get(JDBC_MAX_RECORDS_PER_PARTITION));
    }

    public Optional<String> getJdbcReaderPartitionColumn(OceanBaseDialect dialect) {
        String configPartCol =
                String.format(
                        SPECIFY_PK_TABLE_PARTITION_COLUMN.getKey(),
                        dialect.unQuoteIdentifier(getDbTable()));
        return getProperties().entrySet().stream()
                .filter(entry -> entry.getKey().contains(configPartCol))
                .map(entry -> dialect.quoteIdentifier(entry.getValue()))
                .findFirst();
    }
}

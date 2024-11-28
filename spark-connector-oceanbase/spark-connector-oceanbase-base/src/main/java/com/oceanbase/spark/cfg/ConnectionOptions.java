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

package com.oceanbase.spark.cfg;

public interface ConnectionOptions {

    String HOST = "host";
    String SQL_PORT = "sql-port";
    String USERNAME = "username";
    String PASSWORD = "password";
    String SCHEMA_NAME = "schema-name";
    String TABLE_NAME = "table-name";
    String DRIVER = "driver";
    String DRIVER_DEFAULT = "com.mysql.cj.jdbc.Driver";

    /* Direct-load config */
    String ENABLE_DIRECT_LOAD_WRITE = "direct-load.enabled";
    boolean ENABLE_DIRECT_LOAD_WRITE_DEFAULT = false;
    String RPC_PORT = "direct-load.rpc-port";
    String PARALLEL = "direct-load.parallel";
    String EXECUTION_ID = "direct-load.execution-id";
    String DUP_ACTION = "direct-load.dup-action";
    String HEARTBEAT_TIMEOUT = "direct-load.heartbeat-timeout";
    String HEARTBEAT_INTERVAL = "direct-load.heartbeat-interval";
    String LOAD_METHOD = "direct-load.load-method";
    String TIMEOUT = "direct-load.timeout";
    String MAX_ERROR_ROWS = "direct-load.max-error-rows";
    String BATCH_SIZE = "direct-load-batch-size";
    int BATCH_SIZE_DEFAULT = 10240;
    String SINK_TASK_PARTITION_SIZE = "direct-load.task.partition.size";
    /**
     * Set direct-load task partition size. If you set a small coalesce size, and you don't have the
     * action operations, this may result in the same parallelism in your computation. To avoid
     * this, you can use repartition operations. This will add a shuffle step, but means the current
     * upstream partitions will be executed in parallel.
     */
    String SINK_TASK_USE_REPARTITION = "direct-load.task.use.repartition";

    boolean SINK_TASK_USE_REPARTITION_DEFAULT = false;
}

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

package com.oceanbase.spark.directload;

import java.io.Serializable;


import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadConnection;
import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadManager;
import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadStatement;
import com.alipay.oceanbase.rpc.direct_load.exception.ObDirectLoadException;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObLoadDupActionType;
import org.apache.commons.lang3.StringUtils;

/** The builder for {@link DirectLoader}. */
public class DirectLoaderBuilder implements Serializable {

    private String host;
    private int port;

    private String user;
    private String tenant;
    private String password;

    private String schema;
    private String table;

    /** Server-side parallelism. */
    private int parallel = 8;

    private long maxErrorCount = 0L;

    private ObLoadDupActionType duplicateKeyAction = ObLoadDupActionType.REPLACE;

    /** The overall timeout of the direct load task */
    private long timeout = 1000L * 1000 * 1000;

    private long heartBeatTimeout = 60 * 1000;

    private long heartBeatInterval = 10 * 1000;

    /** Direct load mode: full, inc, inc_replace. */
    private String directLoadMethod = "full";

    private String executionId;

    public DirectLoaderBuilder host(String host) {
        this.host = host;
        return this;
    }

    public DirectLoaderBuilder port(int port) {
        this.port = port;
        return this;
    }

    public DirectLoaderBuilder user(String user) {
        this.user = user;
        return this;
    }

    public DirectLoaderBuilder tenant(String tenant) {
        this.tenant = tenant;
        return this;
    }

    public DirectLoaderBuilder password(String password) {
        this.password = password;
        return this;
    }

    public DirectLoaderBuilder schema(String schema) {
        this.schema = schema;
        return this;
    }

    public DirectLoaderBuilder table(String table) {
        this.table = table;
        return this;
    }

    public DirectLoaderBuilder parallel(Integer parallel) {
        this.parallel = parallel;
        return this;
    }

    public DirectLoaderBuilder maxErrorCount(Long maxErrorCount) {
        this.maxErrorCount = maxErrorCount;
        return this;
    }

    public DirectLoaderBuilder duplicateKeyAction(String duplicateKeyAction) {
        this.duplicateKeyAction = ObLoadDupActionType.valueOf(duplicateKeyAction);
        return this;
    }

    public DirectLoaderBuilder directLoadMethod(String directLoadMethod) {
        this.directLoadMethod = directLoadMethod;
        return this;
    }

    public DirectLoaderBuilder timeout(Long timeout) {
        this.timeout = timeout;
        return this;
    }

    public DirectLoaderBuilder heartBeatTimeout(Long heartBeatTimeout) {
        this.heartBeatTimeout = heartBeatTimeout;
        return this;
    }

    public DirectLoaderBuilder heartBeatInterval(Long heartBeatInterval) {
        this.heartBeatInterval = heartBeatInterval;
        return this;
    }

    public DirectLoaderBuilder executionId(String executionId) {
        this.executionId = executionId;
        return this;
    }

    public DirectLoader build() {
        try {
            ObDirectLoadConnection obDirectLoadConnection = buildConnection(parallel);
            ObDirectLoadStatement obDirectLoadStatement = buildStatement(obDirectLoadConnection);
            if (StringUtils.isNotBlank(executionId)) {
                return new DirectLoader(
                        this,
                        String.format("%s.%s", schema, table),
                        obDirectLoadStatement,
                        obDirectLoadConnection,
                        executionId);
            } else {
                return new DirectLoader(
                        this,
                        String.format("%s.%s", schema, table),
                        obDirectLoadStatement,
                        obDirectLoadConnection);
            }
        } catch (ObDirectLoadException e) {
            throw new RuntimeException("Fail to obtain direct-load connection.", e);
        }
    }

    private ObDirectLoadConnection buildConnection(int writeThreadNum)
            throws ObDirectLoadException {
        return ObDirectLoadManager.getConnectionBuilder()
                .setServerInfo(host, port)
                .setLoginInfo(tenant, user, password, schema)
                .setHeartBeatInfo(heartBeatTimeout, heartBeatInterval)
                .enableParallelWrite(writeThreadNum)
                .build();
    }

    private ObDirectLoadStatement buildStatement(ObDirectLoadConnection connection)
            throws ObDirectLoadException {
        return connection
                .getStatementBuilder()
                .setTableName(table)
                .setDupAction(duplicateKeyAction)
                .setParallel(parallel)
                .setQueryTimeout(timeout)
                .setMaxErrorRowCount(maxErrorCount)
                .setLoadMethod(directLoadMethod)
                .build();
    }
}

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

package com.oceanbase.spark.obkv;

import com.oceanbase.spark.config.OceanBaseConfig;


import com.alipay.oceanbase.rpc.ObTableClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class for creating and managing OBKV client instances. */
public class OBKVClientUtils {

    private static final Logger LOG = LoggerFactory.getLogger(OBKVClientUtils.class);

    /**
     * Creates and initializes an {@link ObTableClient} from the given config.
     *
     * <p>Supports both direct connection mode and ODP proxy mode.
     *
     * @param config the OceanBase configuration
     * @return an initialized ObTableClient
     */
    public static ObTableClient createClient(OceanBaseConfig config) {
        return createClient(config, null);
    }

    /**
     * Creates and initializes an {@link ObTableClient} from the given config with explicit primary
     * keys.
     *
     * @param config the OceanBase configuration
     * @param primaryKeys primary key column names (used in Catalog mode where obkv.primary-key is
     *     not configured)
     * @return an initialized ObTableClient
     */
    public static ObTableClient createClient(OceanBaseConfig config, String[] primaryKeys) {
        try {
            ObTableClient client = new ObTableClient();
            client.setFullUserName(config.getObkvFullUserName());
            client.setPassword(config.getObkvPassword());

            if (config.getObkvOdpMode()) {
                client.setOdpMode(true);
                client.setOdpAddr(config.getObkvOdpAddr());
                client.setOdpPort(config.getObkvOdpPort());
                client.setDatabase(config.getSchemaName());
            } else {
                client.setParamURL(config.getObkvParamUrl());
                String sysUserName = config.getObkvSysUserName();
                if (sysUserName != null && !sysUserName.isEmpty()) {
                    client.setSysUserName(sysUserName);
                    client.setSysPassword(config.getObkvSysPassword());
                }
            }

            client.setRpcConnectTimeout(config.getObkvRpcConnectTimeout());
            client.setRpcExecuteTimeout(config.getObkvRpcExecuteTimeout());

            // Register row key elements: use explicit primaryKeys if provided,
            // otherwise fall back to config
            String tableName = config.getTableName();
            if (primaryKeys != null && primaryKeys.length > 0) {
                client.addRowKeyElement(tableName, primaryKeys);
            } else {
                String primaryKey = config.getObkvPrimaryKey();
                if (primaryKey != null && !primaryKey.isEmpty()) {
                    String[] pkColumns = primaryKey.split(",");
                    for (int i = 0; i < pkColumns.length; i++) {
                        pkColumns[i] = pkColumns[i].trim();
                    }
                    client.addRowKeyElement(tableName, pkColumns);
                }
            }

            client.init();
            LOG.info("OBKV client initialized successfully");
            return client;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create OBKV client", e);
        }
    }

    /**
     * Safely closes an {@link ObTableClient}.
     *
     * @param client the client to close, may be null
     */
    public static void closeClient(ObTableClient client) {
        if (client != null) {
            try {
                client.close();
                LOG.info("OBKV client closed successfully");
            } catch (Exception e) {
                LOG.warn("Failed to close OBKV client", e);
            }
        }
    }
}

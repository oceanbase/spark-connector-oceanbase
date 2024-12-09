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

import com.oceanbase.spark.config.OBKVHbaseConfig;

import java.util.Properties;


import com.alipay.oceanbase.hbase.OHTableClient;
import com.alipay.oceanbase.hbase.constants.OHConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HTableClientUtils {
    private static final Logger LOG = LoggerFactory.getLogger(HTableClientUtils.class);

    public static Table getHTableClient(OBKVHbaseConfig config) {
        try {
            OHTableClient tableClient = new OHTableClient(config.getTableName(), getConfig(config));
            tableClient.init();
            return tableClient;
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize OHTableClient", e);
        }
    }

    private static Configuration getConfig(OBKVHbaseConfig config) {

        Configuration conf = new Configuration();
        if (config.getOdpMode()) {
            conf.setBoolean(OHConstants.HBASE_OCEANBASE_ODP_MODE, config.getOdpMode());
            conf.set(OHConstants.HBASE_OCEANBASE_ODP_ADDR, config.getOdpIp());
            conf.setInt(OHConstants.HBASE_OCEANBASE_ODP_PORT, config.getOdpPort());
            conf.set(OHConstants.HBASE_OCEANBASE_DATABASE, config.getSchemaName());
        } else {
            String paramUrl =
                    String.format("%s&database=%s", config.getURL(), config.getSchemaName());
            LOG.debug("Set paramURL for database {} to {}", config.getSchemaName(), paramUrl);
            conf.set(OHConstants.HBASE_OCEANBASE_PARAM_URL, paramUrl);
            conf.set(OHConstants.HBASE_OCEANBASE_SYS_USER_NAME, config.getSysUserName());
            conf.set(OHConstants.HBASE_OCEANBASE_SYS_PASSWORD, config.getSysPassword());
        }
        conf.set(OHConstants.HBASE_OCEANBASE_FULL_USER_NAME, config.getUsername());
        conf.set(OHConstants.HBASE_OCEANBASE_PASSWORD, config.getPassword());
        Properties hbaseProperties = config.getHBaseProperties();
        if (hbaseProperties != null) {
            for (String name : hbaseProperties.stringPropertyNames()) {
                conf.set(name, hbaseProperties.getProperty(name));
            }
        }
        return conf;
    }
}

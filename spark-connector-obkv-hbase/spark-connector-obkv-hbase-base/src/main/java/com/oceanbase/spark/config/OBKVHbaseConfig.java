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

import java.util.Map;
import java.util.Properties;


import org.apache.commons.lang3.StringUtils;

public class OBKVHbaseConfig extends Config {
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

    public static final ConfigEntry<String> SYS_USERNAME =
            new ConfigBuilder("sys.username")
                    .doc("The username of system tenant")
                    .version(ConfigConstants.VERSION_1_0_0)
                    .stringConf()
                    .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
                    .create();

    public static final ConfigEntry<String> SYS_PASSWORD =
            new ConfigBuilder("sys.password")
                    .doc("The password of system tenant")
                    .version(ConfigConstants.VERSION_1_0_0)
                    .stringConf()
                    .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
                    .create();

    public static final ConfigEntry<String> HBASE_PROPERTIES =
            new ConfigBuilder("hbase.properties")
                    .doc("Properties to configure 'obkv-hbase-client-java'")
                    .version(ConfigConstants.VERSION_1_0_0)
                    .stringConf()
                    .create();

    public static final ConfigEntry<Boolean> ODP_MODE =
            new ConfigBuilder("odp-mode")
                    .doc("Whether to use ODP to connect to OBKV")
                    .version(ConfigConstants.VERSION_1_0_0)
                    .booleanConf()
                    .createWithDefault(false);

    public static final ConfigEntry<String> ODP_IP =
            new ConfigBuilder("odp-ip")
                    .doc("ODP IP address")
                    .version(ConfigConstants.VERSION_1_0_0)
                    .stringConf()
                    .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
                    .create();

    public static final ConfigEntry<Integer> ODP_PORT =
            new ConfigBuilder("odp-port")
                    .doc("ODP rpc port")
                    .version(ConfigConstants.VERSION_1_0_0)
                    .intConf()
                    .checkValue(port -> port > 0, ConfigConstants.POSITIVE_NUMBER_ERROR_MSG)
                    .createWithDefault(2885);

    public static final ConfigEntry<Integer> BATCH_SIZE =
            new ConfigBuilder("batch-size")
                    .doc("The batch size write to OceanBase one time")
                    .version(ConfigConstants.VERSION_1_0_0)
                    .intConf()
                    .checkValue(size -> size > 0, ConfigConstants.POSITIVE_NUMBER_ERROR_MSG)
                    .createWithDefault(10_000);

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

    public String getSysUserName() {
        return get(SYS_USERNAME);
    }

    public String getSysPassword() {
        return get(SYS_PASSWORD);
    }

    public Properties getHBaseProperties() {
        return parseProperties(get(HBASE_PROPERTIES));
    }

    public Boolean getOdpMode() {
        return get(ODP_MODE);
    }

    public String getOdpIp() {
        return get(ODP_IP);
    }

    public Integer getOdpPort() {
        return get(ODP_PORT);
    }

    public Integer getBatchSize() {
        return get(BATCH_SIZE);
    }

    public OBKVHbaseConfig(Map<String, String> properties) {
        super();
        loadFromMap(properties, k -> true);
    }

    private Properties parseProperties(String propsStr) {
        if (StringUtils.isBlank(propsStr)) {
            return null;
        }
        Properties props = new Properties();
        for (String propStr : propsStr.split(";")) {
            if (StringUtils.isBlank(propStr)) {
                continue;
            }
            String[] pair = propStr.trim().split("=");
            if (pair.length != 2) {
                throw new IllegalArgumentException("properties must have one key value pair");
            }
            props.put(pair[0].trim(), pair[1].trim());
        }
        return props;
    }
}

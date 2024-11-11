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

import com.oceanbase.spark.cfg.ConnectionOptions;
import com.oceanbase.spark.cfg.OceanBaseUserInfo;
import com.oceanbase.spark.cfg.SparkSettings;

/** The utils of {@link DirectLoader} */
public class DirectLoadUtils {

    public static DirectLoader buildDirectLoaderFromSetting(SparkSettings settings) {
        try {
            OceanBaseUserInfo userInfo = OceanBaseUserInfo.parse(settings);
            return new DirectLoaderBuilder()
                    .host(settings.getProperty(ConnectionOptions.HOST))
                    .port(settings.getIntegerProperty(ConnectionOptions.RPC_PORT))
                    .user(userInfo.getUser())
                    .password(settings.getProperty(ConnectionOptions.PASSWORD))
                    .tenant(userInfo.getTenant())
                    .schema(settings.getProperty(ConnectionOptions.SCHEMA_NAME))
                    .table(settings.getProperty(ConnectionOptions.TABLE_NAME))
                    .executionId(settings.getProperty(ConnectionOptions.EXECUTION_ID))
                    .duplicateKeyAction(settings.getProperty(ConnectionOptions.DUP_ACTION))
                    .maxErrorCount(settings.getLongProperty(ConnectionOptions.MAX_ERROR_ROWS))
                    .timeout(settings.getLongProperty(ConnectionOptions.TIMEOUT))
                    .heartBeatTimeout(settings.getLongProperty(ConnectionOptions.HEARTBEAT_TIMEOUT))
                    .heartBeatInterval(
                            settings.getLongProperty(ConnectionOptions.HEARTBEAT_INTERVAL))
                    .directLoadMethod(settings.getProperty(ConnectionOptions.LOAD_METHOD))
                    .parallel(settings.getIntegerProperty(ConnectionOptions.PARALLEL))
                    .build();
        } catch (Exception e) {
            throw new RuntimeException("Fail to build DirectLoader.", e);
        }
    }
}

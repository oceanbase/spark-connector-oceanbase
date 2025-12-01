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

import com.oceanbase.spark.config.OceanBaseConfig;
import com.oceanbase.spark.config.OceanBaseUserInfo;

/** The utils of {@link DirectLoader} */
public class DirectLoadUtils {

    public static DirectLoader buildDirectLoaderFromSetting(OceanBaseConfig oceanBaseConfig) {
        try {
            OceanBaseUserInfo userInfo = OceanBaseUserInfo.parse(oceanBaseConfig.getUsername());
            String directLoadUserName = oceanBaseConfig.getDirectLoadUserName();
            if (directLoadUserName != null && !directLoadUserName.isEmpty()) {
                userInfo = OceanBaseUserInfo.parse(directLoadUserName);
            }

            String username = "";
            if (oceanBaseConfig.getDirectLoadOdpMode()) {
                if (directLoadUserName != null && !directLoadUserName.isEmpty()) {
                    username = directLoadUserName;
                } else {
                    username = oceanBaseConfig.getUsername();
                }
            } else {
                username = userInfo.getUser();
            }

            return new DirectLoaderBuilder()
                    .host(oceanBaseConfig.getDirectLoadHost())
                    .port(oceanBaseConfig.getDirectLoadPort())
                    .user(username)
                    .password(oceanBaseConfig.getPassword())
                    .tenant(userInfo.getTenant())
                    .schema(oceanBaseConfig.getSchemaName())
                    .table(oceanBaseConfig.getTableName())
                    .executionId(oceanBaseConfig.getDirectLoadExecutionId())
                    .duplicateKeyAction(oceanBaseConfig.getDirectLoadDupAction())
                    .maxErrorCount(oceanBaseConfig.getDirectLoadMaxErrorRows())
                    .timeout(oceanBaseConfig.getDirectLoadTimeout())
                    .heartBeatTimeout(oceanBaseConfig.getDirectLoadHeartbeatTimeout())
                    .heartBeatInterval(oceanBaseConfig.getDirectLoadHeartbeatInterval())
                    .directLoadMethod(oceanBaseConfig.getDirectLoadLoadMethod())
                    .parallel(oceanBaseConfig.getDirectLoadParallel())
                    .writeThreadNum(oceanBaseConfig.getDirectLoadWriteThreadNum())
                    .build();
        } catch (Exception e) {
            throw new RuntimeException("Fail to build DirectLoader.", e);
        }
    }
}

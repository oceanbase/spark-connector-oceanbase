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

/** Constants used for configuration. */
public interface ConfigConstants {

    /** The value of messages used to indicate that the configuration is not set. */
    String NOT_BLANK_ERROR_MSG = "The value can't be blank";

    /**
     * The value of messages used to indicate that the configuration should be a positive number.
     */
    String POSITIVE_NUMBER_ERROR_MSG = "The value must be a positive number";

    String VERSION_1_0_0 = "1.0";
    String VERSION_1_1_0 = "1.1";
}

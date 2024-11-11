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

import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;


import org.apache.commons.lang3.StringUtils;

public abstract class Settings {
    public abstract String getProperty(String name);

    public abstract void setProperty(String name, String value);

    public abstract Properties asProperties();

    public abstract Settings copy();

    public String getProperty(String name, String defaultValue) {
        String value = getProperty(name);
        if (StringUtils.isEmpty(value)) {
            return defaultValue;
        }
        return value;
    }

    public Integer getIntegerProperty(String name) {
        return getIntegerProperty(name, null);
    }

    public Integer getIntegerProperty(String name, Integer defaultValue) {
        if (getProperty(name) != null) {
            return Integer.parseInt(getProperty(name));
        }

        return defaultValue;
    }

    public Long getLongProperty(String name) {
        return getLongProperty(name, null);
    }

    public Long getLongProperty(String name, Long defaultValue) {
        if (getProperty(name) != null) {
            return Long.parseLong(getProperty(name));
        }

        return defaultValue;
    }

    public Boolean getBooleanProperty(String name) {
        return getBooleanProperty(name, null);
    }

    public Boolean getBooleanProperty(String name, Boolean defaultValue) {
        if (getProperty(name) != null) {
            return Boolean.valueOf(getProperty(name));
        }
        return defaultValue;
    }

    public Settings merge(Properties properties) {
        if (properties == null || properties.isEmpty()) {
            return this;
        }

        Enumeration<?> propertyNames = properties.propertyNames();

        for (; propertyNames.hasMoreElements(); ) {
            Object prop = propertyNames.nextElement();
            if (prop instanceof String) {
                Object value = properties.get(prop);
                setProperty((String) prop, value.toString());
            }
        }

        return this;
    }

    public Settings merge(Map<String, String> map) {
        if (map == null || map.isEmpty()) {
            return this;
        }

        for (Map.Entry<String, String> entry : map.entrySet()) {
            setProperty(entry.getKey(), entry.getValue());
        }

        return this;
    }

    @Override
    public int hashCode() {
        return asProperties().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        return asProperties().equals(((Settings) obj).asProperties());
    }
}

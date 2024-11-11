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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Config class is responsible for managing configuration settings. */
public abstract class Config implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(Config.class);

    private final ConcurrentMap<String, String> configMap;

    private final Map<String, DeprecatedConfig> deprecatedConfigMap;

    private final DeprecatedConfig[] deprecatedConfigs = {};

    /** Constructs a Config instance and loads default configurations. */
    public Config() {
        deprecatedConfigMap = new HashMap<>();
        for (DeprecatedConfig config : deprecatedConfigs) {
            deprecatedConfigMap.put(config.key, config);
        }

        configMap = new ConcurrentHashMap<>();
    }

    /**
     * Gets the value of a configuration entry.
     *
     * @param entry The configuration entry to retrieve.
     * @param <T> The type of the configuration value.
     * @return The value of the configuration entry.
     * @throws NoSuchElementException If the configuration entry is not found.
     */
    public <T> T get(ConfigEntry<T> entry) throws NoSuchElementException {
        return entry.readFrom(configMap);
    }

    /**
     * Retrieves the raw string value associated with the specified configuration key.
     *
     * @param key The configuration key for which the raw string value is requested.
     * @return The raw string value associated with the given configuration key, or null if the key
     *     is not found.
     */
    public String getRawString(String key) {
        return configMap.get(key);
    }

    /**
     * Retrieves the raw string value associated with the specified configuration key, providing a
     * default value if the key is not found.
     *
     * @param key The configuration key for which the raw string value is requested.
     * @param defaultValue The default value to be returned if the key is not found.
     * @return The raw string value associated with the given configuration key, or the provided
     *     default value if the key is not found.
     */
    public String getRawString(String key, String defaultValue) {
        return configMap.getOrDefault(key, defaultValue);
    }

    /**
     * Retrieves a map containing all configuration entries.
     *
     * @return An unmodifiable map containing all configuration entries.
     */
    public Map<String, String> getAllConfig() {
        return new HashMap<>(configMap);
    }

    /**
     * Sets the value of a configuration entry.
     *
     * @param entry The configuration entry for which the value needs to be set.
     * @param value The new value to be assigned to the configuration entry.
     * @param <T> The type of the configuration value.
     */
    public <T> void set(ConfigEntry<T> entry, T value) {
        if (entry.isDeprecated()
                && deprecatedConfigMap.containsKey(entry.getKey())
                && LOG.isWarnEnabled()) {
            LOG.warn(
                    "Config {} is deprecated since {}. {}",
                    entry.getKey(),
                    deprecatedConfigMap.get(entry.getKey()).version,
                    deprecatedConfigMap.get(entry.getKey()).deprecationMessage);
        }

        if (value == null && LOG.isWarnEnabled()) {
            LOG.warn("Config {} value to set is null, ignore setting to Config.", entry.getKey());
            return;
        }

        entry.writeTo(configMap, value);
    }

    /**
     * Loads configurations from a map.
     *
     * @param map The map containing configuration key-value pairs.
     * @param predicate The keys only match the predicate will be loaded to configMap
     */
    public void loadFromMap(Map<String, String> map, Predicate<String> predicate) {
        map.forEach(
                (k, v) -> {
                    String trimmedK = k.trim();
                    String trimmedV = v.trim();
                    if (!trimmedK.isEmpty() && !trimmedV.isEmpty()) {
                        if (predicate.test(trimmedK)) {
                            configMap.put(trimmedK, trimmedV);
                        }
                    }
                });
    }

    /** The DeprecatedConfig class represents a configuration entry that has been deprecated. */
    private static class DeprecatedConfig {
        private final String key;
        private final String version;
        private final String deprecationMessage;

        /**
         * Constructs a DeprecatedConfig instance.
         *
         * @param key The key of the deprecated configuration.
         * @param version The version in which the configuration was deprecated.
         * @param deprecationMessage Message to indicate the deprecation warning.
         */
        private DeprecatedConfig(String key, String version, String deprecationMessage) {
            this.key = key;
            this.version = version;
            this.deprecationMessage = deprecationMessage;
        }
    }
}

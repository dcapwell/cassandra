/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.config;

import java.beans.IntrospectionException;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import org.apache.commons.lang3.SystemUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.error.YAMLException;
import org.yaml.snakeyaml.introspector.MissingProperty;
import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.introspector.PropertyUtils;

public class YamlConfigurationLoader implements ConfigurationLoader
{
    private static final Logger logger = LoggerFactory.getLogger(YamlConfigurationLoader.class);

    private final static String DEFAULT_CONFIGURATION = "cassandra.yaml";

    /**
     * Inspect the classpath to find storage configuration file
     */
    @VisibleForTesting
    public static URL getStorageConfigURL() throws ConfigurationException
    {
        String configUrl = System.getProperty("cassandra.config");
        if (configUrl == null)
            configUrl = DEFAULT_CONFIGURATION;

        URL url;
        try
        {
            url = new URL(configUrl);
            url.openStream().close(); // catches well-formed but bogus URLs
        }
        catch (Exception e)
        {
            ClassLoader loader = DatabaseDescriptor.class.getClassLoader();
            url = loader.getResource(configUrl);
            if (url == null)
            {
                String required = "file:" + File.separator + File.separator;
                if (!configUrl.startsWith(required))
                    throw new ConfigurationException(String.format(
                        "Expecting URI in variable: [cassandra.config]. Found[%s]. Please prefix the file with [%s%s] for local " +
                        "files and [%s<server>%s] for remote files. If you are executing this from an external tool, it needs " +
                        "to set Config.setClientMode(true) to avoid loading configuration.",
                        configUrl, required, File.separator, required, File.separator));
                throw new ConfigurationException("Cannot locate " + configUrl + ".  If this is a local file, please confirm you've provided " + required + File.separator + " as a URI prefix.");
            }
        }

        logger.info("Configuration location: {}", url);

        return url;
    }

    private static URL storageConfigURL;

    @Override
    public Config loadConfig() throws ConfigurationException
    {
        if (storageConfigURL == null)
            storageConfigURL = getStorageConfigURL();
        return loadConfig(storageConfigURL);
    }

    public Config loadConfig(URL url) throws ConfigurationException
    {
        try
        {
            logger.debug("Loading settings from {}", url);
            byte[] configBytes;
            try (InputStream is = url.openStream())
            {
                configBytes = ByteStreams.toByteArray(is);
            }
            catch (IOException e)
            {
                // getStorageConfigURL should have ruled this out
                throw new AssertionError(e);
            }

            Constructor constructor = new CustomConstructor(Config.class);
            PropertiesChecker propertiesChecker = new PropertiesChecker();
            constructor.setPropertyUtils(propertiesChecker);
            Yaml yaml = new Yaml(constructor);
            Config result = loadConfig(yaml, configBytes);
            propertiesChecker.check();
            return result;
        }
        catch (YAMLException e)
        {
            throw new ConfigurationException("Invalid yaml: " + url, e);
        }
    }

    static class CustomConstructor extends Constructor
    {
        CustomConstructor(Class<?> theRoot)
        {
            super(theRoot);

            TypeDescription seedDesc = new TypeDescription(ParameterizedClass.class);
            seedDesc.putMapPropertyType("parameters", String.class, String.class);
            addTypeDescription(seedDesc);
        }

        @Override
        protected List<Object> createDefaultList(int initSize)
        {
            return Lists.newCopyOnWriteArrayList();
        }

        @Override
        protected Map<Object, Object> createDefaultMap()
        {
            return Maps.newConcurrentMap();
        }

        @Override
        protected Set<Object> createDefaultSet(int initSize)
        {
            return Sets.newConcurrentHashSet();
        }

        @Override
        protected Set<Object> createDefaultSet()
        {
            return Sets.newConcurrentHashSet();
        }
    }

    private Config loadConfig(Yaml yaml, byte[] configBytes)
    {
        Config config = yaml.loadAs(new ByteArrayInputStream(configBytes), Config.class);
        // If the configuration file is empty yaml will return null. In this case we should use the default
        // configuration to avoid hitting a NPE at a later stage.
        return config == null ? new Config() : config;
    }

    /**
     * Utility class to check that there are no extra properties and that properties that are not null by default
     * are not set to null.
     */
    private static class PropertiesChecker extends PropertyUtils
    {
        private final Set<String> missingProperties = new HashSet<>();

        private final Set<String> nullProperties = new HashSet<>();

        private final ImmutableMap<String, String> MAP_OLD_TO_NEW_PARAMETERS = ImmutableMap.<String, String>builder()
            .put("max_hint_window_in_ms", "max_hint_window")
            .put("native_transport_idle_timeout_in_ms", "native_transport_idle_timeout")
            .put("request_timeout_in_ms", "request_timeout")
            .put("read_request_timeout_in_ms", "read_request_timeout_in_ms")
            .put("range_request_timeout_in_ms", "range_request_timeout")
            .put("write_request_timeout_in_ms", "write_request_timeout")
            .put("counter_write_request_timeout_in_ms", "counter_write_request_timeout")
            .put("cas_contention_timeout_in_ms", "cas_contention_timeout")
            .put("truncate_request_timeout_in_ms", "truncate_request_timeout")
            .put("streaming_keep_alive_period_in_secs", "streaming_keep_alive_period")
            .put("slow_query_log_timeout_in_ms", "slow_query_log_timeout")
            .put("internode_tcp_connect_timeout_in_ms", "internode_tcp_connect_timeout")
            .put("internode_tcp_user_timeout_in_ms", "internode_tcp_user_timeout")
            .put("commitlog_sync_batch_window_in_ms", "commitlog_sync_batch_window")
            .put("commitlog_sync_group_window_in_ms", "commitlog_sync_group_window")
            .put("commitlog_sync_period_in_ms", "commitlog_sync_period")
            .put("periodic_commitlog_sync_lag_block_in_ms", "periodic_commitlog_sync_lag_block")
            .put("cdc_free_space_check_interval_ms", "cdc_free_space_check_interval")
            .put("dynamic_snitch_update_interval_in_ms", "dynamic_snitch_update_interval")
            .put("dynamic_snitch_reset_interval_in_ms", "dynamic_snitch_reset_interval")
            .put("gc_log_threshold_in_ms", "gc_log_threshold")
            .put("hints_flush_period_in_ms", "hints_flush_period")
            .put("gc_warn_threshold_in_ms", "gc_warn_threshold")
            .put("tracetype_query_ttl_in_s", "tracetype_query_ttl")
            .put("tracetype_repair_ttl_in_s", "tracetype_repair_ttl")
            .put("permissions_validity_in_ms", "permissions_validity")
            .put("permissions_update_interval_in_ms", "permissions_update_interval")
            .put("roles_validity_in_ms", "roles_validity")
            .put("roles_update_interval_in_ms", "roles_update_interval")
            .put("credentials_validity_in_ms", "credentials_validity")
            .put("credentials_update_interval_in_ms", "credentials_update_interval")
            .put("index_summary_resize_interval_in_minutes", "index_summary_resize_interval")

            .put("max_hints_file_size_in_mb", "max_hints_file_size")
            .put("memtable_heap_space_in_mb", "memtable_heap_space")
            .put("memtable_offheap_space_in_mb", "memtable_offheap_space")
            .put("repair_session_space_in_mb", "repair_session_space")
            .put("internode_application_send_queue_capacity_in_bytes", "internode_application_send_queue_capacity")
            .put("internode_application_send_queue_reserve_endpoint_capacity_in_bytes", "internode_application_send_queue_reserve_endpoint_capacity")
            .put("internode_application_send_queue_reserve_global_capacity_in_bytes", "internode_application_send_queue_reserve_global_capacity")
            .put("internode_application_receive_queue_capacity_in_bytes", "internode_application_receive_queue_capacity")
            .put("internode_application_receive_queue_reserve_endpoint_capacity_in_bytes", "internode_application_receive_queue_reserve_endpoint_capacity")
            .put("internode_application_receive_queue_reserve_global_capacity_in_bytes", "internode_application_receive_queue_reserve_global_capacity")
            .put("max_native_transport_frame_size_in_mb", "max_native_transport_frame_size")
            .put("native_transport_frame_block_size_in_kb", "native_transport_frame_block_size")
            .put("max_value_size_in_mb", "max_value_size")
            .put("column_index_size_in_kb", "column_index_size")
            .put("column_index_cache_size_in_kb", "column_index_cache_size")
            .put("batch_size_warn_threshold_in_kb", "batch_size_warn_threshold")
            .put("batch_size_fail_threshold_in_kb", "batch_size_fail_threshold")
            .put("compaction_large_partition_warning_threshold_mb", "compaction_large_partition_warning_threshold")
            .put("commitlog_total_space_in_mb", "commitlog_total_space")
            .put("commitlog_segment_size_in_mb", "commitlog_segment_size")
            .put("max_mutation_size_in_kb", "max_mutation_size")
            .put("cdc_total_space_in_mb", "cdc_total_space")
            .put("hinted_handoff_throttle_in_kb", "hinted_handoff_throttle")
            .put("batchlog_replay_throttle_in_kb", "trickle_fsync_interval")
            .put("sstable_preemptive_open_interval_in_mb", "sstable_preemptive_open_interval")
            .put("counter_cache_size_in_mb", "counter_cache_size")
            .put("file_cache_size_in_mb", "file_cache_size")
            .put("index_summary_capacity_in_mb", "index_summary_capacity")
            .put("prepared_statements_cache_size_mb", "prepared_statements_cache_size")
            .put("key_cache_size_in_mb", "key_cache_size")
            .put("row_cache_size_in_mb", "row_cache_size")

            .put("compaction_throughput_mb_per_sec", "compaction_throughput")
            .put("stream_throughput_outbound_megabits_per_sec", "stream_throughput_outbound")
            .put("inter_dc_stream_throughput_outbound_megabits_per_sec", "inter_dc_stream_throughput_outbound")

            .put("enable_user_defined_functions", "user_defined_functions_enabled")
            .put("enable_scripted_user_defined_functions", "scripted_user_defined_functions_enabled")
            .put("cross_node_timeout", "internode_timeout")
            .put("native_transport_max_threads", "max_native_transport_threads")
            .put("native_transport_max_frame_size_in_mb", "max_native_transport_frame_size_in_mb")
            .put("native_transport_max_concurrent_connections", "max_native_transport_concurrent_connections_in_mb")
            .put("native_transport_max_concurrent_connections_per_ip", "max_native_transport_concurrent_connections_per_ip")
            .put("otc_coalescing_strategy", "outbound_connection_coalescing_strategy")
            .put("otc_coalescing_window_us", "outbound_connection_coalescing_window_us")
            .put("otc_coalescing_enough_coalesced_messages", "outbound_connection_coalescing_enough_coalesced_messages")
            .put("otc_backlog_expiration_interval_ms", "outbound_connection_backlog_expiration_interval_ms")
            .put("enable_legacy_ssl_storage_port", "legacy_ssl_storage_port_enabled")
            .put("enable_materialized_views", "materialized_views_enabled")
            .put("enable_sasi_indexes", "sasi_indexes_enabled")
            .put("enable_transient_replication", "transient_replication_enabled")
            .build();

        public PropertiesChecker()
        {
            setSkipMissingProperties(true);
        }

        @Override
        public Property getProperty(Class<? extends Object> type, String name) throws IntrospectionException
        {
            final Property result;
            if(MAP_OLD_TO_NEW_PARAMETERS.containsKey(name))
            {
                result = super.getProperty(type, MAP_OLD_TO_NEW_PARAMETERS.get(name));
                logger.warn("{} parameter has a new name and value format. For more information, please refer to NEWS.txt", name);
            }
            else
            {
                result = super.getProperty(type, name);
            }

            if (result instanceof MissingProperty)
            {
                missingProperties.add(result.getName());
            }

            return new Property(result.getName(), result.getType())
            {
                @Override
                public void set(Object object, Object value) throws Exception
                {
                    if (value == null && get(object) != null)
                    {
                        nullProperties.add(getName());
                    }

                    result.set(object, value);
                }

                @Override
                public Class<?>[] getActualTypeArguments()
                {
                    return result.getActualTypeArguments();
                }

                @Override
                public Object get(Object object)
                {
                    return result.get(object);
                }
            };
        }

        public void check() throws ConfigurationException
        {
            if (!nullProperties.isEmpty())
            {
                throw new ConfigurationException("Invalid yaml. Those properties " + nullProperties + " are not valid", false);
            }

            if (!missingProperties.isEmpty())
            {
                throw new ConfigurationException("Invalid yaml. Please remove properties " + missingProperties + " from your cassandra.yaml", false);
            }
        }
    }
}

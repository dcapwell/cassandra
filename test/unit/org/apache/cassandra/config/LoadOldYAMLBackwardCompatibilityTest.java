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

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(OrderedJUnit4ClassRunner.class)
public class LoadOldYAMLBackwardCompatibilityTest
{
    @BeforeClass
    public static void setupDatabaseDescriptor()
    {
        System.setProperty("cassandra.config", "cassandra-old.yaml");
        DatabaseDescriptor.daemonInitialization();
    }

    // CASSANDRA-15234
    @Test
    public void testConfigurationLoaderBackwardCompatibility()
    {

        Config config = DatabaseDescriptor.loadConfig();

        //Confirm parameters were successfully read with the default values in cassandra-old.yaml
        assertEquals(Duration.inMilliseconds(10800000), config.max_hint_window);
        assertEquals(Duration.inMilliseconds(0), config.native_transport_idle_timeout);
        assertEquals(Duration.inMilliseconds(10000), config.request_timeout);
        assertEquals(Duration.inMilliseconds(5000), config.read_request_timeout);
        assertEquals(Duration.inMilliseconds(10000), config.range_request_timeout);
        assertEquals(Duration.inMilliseconds(2000), config.write_request_timeout);
        assertEquals(Duration.inMilliseconds(5000), config.counter_write_request_timeout);
        assertEquals(Duration.inMilliseconds(1000), config.cas_contention_timeout);
        assertEquals(Duration.inMilliseconds(60000), config.truncate_request_timeout);
        assertEquals(Duration.inSeconds(300), config.streaming_keep_alive_period);
        assertEquals(Duration.inMilliseconds(500), config.slow_query_log_timeout);
        assertEquals(Duration.inMilliseconds(2000), config.internode_tcp_connect_timeout);
        assertEquals(Duration.inMilliseconds(30000), config.internode_tcp_user_timeout);
        assertNull(config.commitlog_sync_group_window);
        assertEquals(Duration.inMilliseconds(0), config.commitlog_sync_period);
        assertNull(config.periodic_commitlog_sync_lag_block);  //Integer
        assertEquals(Duration.inMilliseconds(250), config.cdc_free_space_check_interval);
        assertEquals(Duration.inMilliseconds(100), config.dynamic_snitch_update_interval);
        assertEquals(Duration.inMilliseconds(600000), config.dynamic_snitch_reset_interval);
        assertEquals(Duration.inMilliseconds(200), config.gc_log_threshold);
        assertEquals(Duration.inMilliseconds(10000), config.hints_flush_period);
        assertEquals(Duration.inMilliseconds(1000), config.gc_warn_threshold);
        assertEquals(Duration.inSeconds(86400), config.tracetype_query_ttl);
        assertEquals(Duration.inSeconds(604800), config.tracetype_repair_ttl);
        assertEquals(Duration.inMilliseconds(2000), config.permissions_validity);
        assertNull(config.permissions_update_interval);
        assertEquals(Duration.inMilliseconds(2000), config.roles_validity);
        assertNull(config.roles_update_interval);
        assertEquals(Duration.inMilliseconds(2000), config.credentials_validity);
        assertNull(config.credentials_update_interval);
        assertEquals(Duration.inMinutes(60), config.index_summary_resize_interval);
    }
}

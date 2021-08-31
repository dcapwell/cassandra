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

package org.apache.cassandra.distributed.test.trackwarnings;

import java.io.IOException;
import java.util.List;

import org.junit.BeforeClass;

import org.apache.cassandra.config.DatabaseDescriptor;

import static org.assertj.core.api.Assertions.assertThat;

public class ClientLocalReadSizeWarningTest extends AbstractClientSizeWarning
{
    @BeforeClass
    public static void setupClass() throws IOException
    {
        AbstractClientSizeWarning.setupClass();

        // setup threshold after init to avoid driver issues loading
        // the test uses a rather small limit, which causes driver to fail while loading metadata
        CLUSTER.stream().forEach(i -> i.runOnInstance(() -> {
            // disable coordinator version
            DatabaseDescriptor.setClientLargeReadWarnThresholdKB(0);
            DatabaseDescriptor.setClientLargeReadAbortThresholdKB(0);

            DatabaseDescriptor.setLocalReadTooLargeWarningThresholdKb(1);
            DatabaseDescriptor.setLocalReadTooLargeAbortThresholdKb(2);
        }));
    }

    @Override
    protected void assertWarnings(List<String> warnings)
    {
        assertThat(warnings).hasSize(1);
        assertThat(warnings.get(0)).contains("(see local_read_too_large_warning_threshold_kb)").contains("and issued local read size warnings for query");
    }

    @Override
    protected void assertAbortWarnings(List<String> warnings)
    {
        assertThat(warnings).hasSize(1);
        assertThat(warnings.get(0)).contains("(see local_read_too_large_abort_threshold_kb)").contains("aborted the query");
    }

    @Override
    protected long totalWarnings()
    {
        return CLUSTER.stream().mapToLong(i -> i.metrics().getCounter("org.apache.cassandra.metrics.keyspace.ClientLocalReadSizeTooLargeWarnings." + KEYSPACE)).sum();
    }

    @Override
    protected long totalAborts()
    {
        return CLUSTER.stream().mapToLong(i -> i.metrics().getCounter("org.apache.cassandra.metrics.keyspace.ClientLocalReadSizeTooLargeAborts." + KEYSPACE)).sum();
    }
}

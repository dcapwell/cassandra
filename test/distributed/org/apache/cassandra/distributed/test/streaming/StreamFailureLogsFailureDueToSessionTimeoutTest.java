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

package org.apache.cassandra.distributed.test.streaming;

import java.io.IOException;
import java.util.concurrent.ForkJoinPool;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;

public class StreamFailureLogsFailureDueToSessionTimeoutTest extends AbstractStreamFailureLogs
{
    @Test
    public void failureDueToSessionTimeout() throws IOException
    {
        streamTimeoutTest("Session timed out");
    }

    protected void streamTimeoutTest(String reason) throws IOException
    {
        try (Cluster cluster = Cluster.build(2)
                                      .withInstanceInitializer(BBStreamTimeoutHelper::install)
                                      .withConfig(c -> c.with(Feature.values())
                                                        // when die, this will try to halt JVM, which is easier to validate in the test
                                                        // other levels require checking state of the subsystems
                                                        .set("stream_transfer_task_timeout", "1ms"))
                                      .start())
        {

            init(cluster);
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY)"));

            ForkJoinPool.commonPool().execute(() -> triggerStreaming(cluster, true));
            State.STREAM_IS_RUNNING.await();
            logger.info("Streaming is running... time to wake it up");
            State.UNBLOCK_STREAM.signal();

            IInvokableInstance failingNode = cluster.get(1);

            searchForLog(failingNode, reason);
        }
    }
}

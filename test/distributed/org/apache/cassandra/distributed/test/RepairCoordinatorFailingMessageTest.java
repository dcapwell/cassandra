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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.net.Verb;

import static org.apache.cassandra.distributed.api.IMessageFilters.Matcher.of;

public class RepairCoordinatorFailingMessageTest extends DistributedTestBase implements Serializable
{
    private static Cluster CLUSTER;

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.clientInitialization();
    }

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        // streaming requires networking ATM
        // streaming also requires gossip or isn't setup properly
        CLUSTER = init(Cluster.build(2)
                              .withConfig(c -> c.with(Feature.NETWORK)
                                                .with(Feature.GOSSIP))
                              .start());
    }

    @AfterClass
    public static void teardownCluster()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    //TODO failure reply murkle tree
    //TODO failure reply murkle tree IR


    @Test(timeout = 1 * 60 * 1000)
    public void validationFailure()
    {
        CLUSTER.schemaChange("CREATE TABLE " + KEYSPACE + ".validationfailure (key text, value text, PRIMARY KEY (key))");
        IMessageFilters.Filter filter = CLUSTER.verbs(Verb.VALIDATION_REQ).messagesMatching(of(m -> {
            throw new RuntimeException("validation fail");
        })).drop();
        try
        {
            NodeToolResult result = CLUSTER.get(1).nodetoolResult("repair", KEYSPACE, "validationfailure", "--full");
            result.asserts()
                  .notOk()
                  .errorContains("Some repair failed")
                  .notificationContains(NodeToolResult.ProgressEventType.ERROR, "Some repair failed")
                  .notificationContains(NodeToolResult.ProgressEventType.COMPLETE, "finished with error");
        }
        finally
        {
            filter.off();
        }
    }

    @Test(timeout = 1 * 60 * 1000)
    public void validationParticipentCrashesAndComesBack()
    {
        // Test what happens when a participant restarts in the middle of validation
        // Currently this isn't recoverable but could be.
        // TODO since this is a real restart, how would I test "long pause"? Can't send SIGSTOP since same procress
        CLUSTER.schemaChange("CREATE TABLE " + KEYSPACE + ".validationparticipentcrashesandcomesback (key text, value text, PRIMARY KEY (key))");
        AtomicReference<Future<Void>> participantShutdown = new AtomicReference<>();
        IMessageFilters.Filter filter = CLUSTER.verbs(Verb.VALIDATION_REQ).to(2).messagesMatching(of(m -> {
            // the nice thing about this is that this lambda is "capturing" and not "transfer", what this means is that
            // this lambda isn't serialized and any object held isn't copied.
            participantShutdown.set(CLUSTER.get(2).shutdown());
            return true; // drop it so this node doesn't reply before shutdown.
        })).drop();
        try
        {
            // since nodetool is blocking, need to handle participantShutdown in the background
            CompletableFuture<Void> recovered = CompletableFuture.runAsync(() -> {
                try {
                    while (participantShutdown.get() == null) {
                        // event not happened, wait for it
                        TimeUnit.MILLISECONDS.sleep(100);
                    }
                    Future<Void> f = participantShutdown.get();
                    f.get(); // wait for shutdown to complete
                    CLUSTER.get(2).startup(CLUSTER);
                } catch (Exception e) {
                    if (e instanceof RuntimeException) {
                        throw (RuntimeException) e;
                    }
                    throw new RuntimeException(e);
                }
            });
            NodeToolResult result = CLUSTER.get(1).nodetoolResult("repair", KEYSPACE, "validationparticipentcrashesandcomesback", "--full");
            recovered.join(); // if recovery didn't happen then the results are not what are being tested, so block here first
            result.asserts()
                  .notOk()
                  .errorContains("Some repair failed")
                  .notificationContains(NodeToolResult.ProgressEventType.ERROR, "Some repair failed")
                  .notificationContains(NodeToolResult.ProgressEventType.COMPLETE, "finished with error");
        }
        finally
        {
            filter.off();
            try {
                CLUSTER.get(2).startup(CLUSTER);
            } catch (Exception e) {
                // if you call startup twice it is allowed to fail, so ignore it... hope this didn't brike the other tests =x
            }
        }
    }

    @Test(timeout = 1 * 60 * 1000)
    public void validationIrFailure()
    {
        CLUSTER.schemaChange("CREATE TABLE " + KEYSPACE + ".validationirfailure (key text, value text, PRIMARY KEY (key))");
        IMessageFilters.Filter filter = CLUSTER.verbs(Verb.VALIDATION_REQ).messagesMatching(of(m -> {
            throw new RuntimeException("validation fail");
        })).drop();
        try
        {
            NodeToolResult result = CLUSTER.get(1).nodetoolResult("repair", KEYSPACE, "validationirfailure");
            result.asserts()
                  .notOk()
                  .errorContains("Some repair failed")
                  .notificationContains(NodeToolResult.ProgressEventType.ERROR, "Some repair failed")
                  .notificationContains(NodeToolResult.ProgressEventType.COMPLETE, "finished with error");
        }
        finally
        {
            filter.off();
        }
    }

    @Test(timeout = 1 * 60 * 1000)
    public void streamFailure()
    {
        CLUSTER.schemaChange("CREATE TABLE " + KEYSPACE + ".streamfailure (key text, value text, PRIMARY KEY (key))");
        // there needs to be a difference to cause streaming to happen, so add to one node
        CLUSTER.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".streamfailure (key) VALUES (?)", "some data");
        IMessageFilters.Filter filter = CLUSTER.verbs(Verb.SYNC_REQ).messagesMatching(of(m -> {
            throw new RuntimeException("stream fail");
        })).drop();
        CLUSTER.get(1).runOnInstance(() -> {
            DatabaseDescriptor.repair_local_sync_enabled(false);
        });
        try
        {
            NodeToolResult result = CLUSTER.get(1).nodetoolResult("repair", KEYSPACE, "streamfailure", "--full");
            result.asserts()
                  .notOk()
                  .errorContains("Some repair failed")
                  .notificationContains(NodeToolResult.ProgressEventType.ERROR, "Some repair failed")
                  .notificationContains(NodeToolResult.ProgressEventType.COMPLETE, "finished with error");
        }
        finally
        {
            filter.off();
            CLUSTER.get(1).runOnInstance(() -> {
                DatabaseDescriptor.repair_local_sync_enabled(true);
            });
        }
    }

    @Test(timeout = 1 * 60 * 1000)
    public void streamIrFailure()
    {
        CLUSTER.schemaChange("CREATE TABLE " + KEYSPACE + ".streamirfailure (key text, value text, PRIMARY KEY (key))");
        // there needs to be a difference to cause streaming to happen, so add to one node
        CLUSTER.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".streamirfailure (key) VALUES (?)", "some data");
        IMessageFilters.Filter filter = CLUSTER.verbs(Verb.SYNC_REQ).messagesMatching(of(m -> {
            throw new RuntimeException("stream fail");
        })).drop();
        CLUSTER.get(1).runOnInstance(() -> {
            DatabaseDescriptor.repair_local_sync_enabled(false);
        });
        try
        {
            NodeToolResult result = CLUSTER.get(1).nodetoolResult("repair", KEYSPACE, "streamirfailure");
            result.asserts()
                  .notOk()
                  .errorContains("Some repair failed")
                  .notificationContains(NodeToolResult.ProgressEventType.ERROR, "Some repair failed")
                  .notificationContains(NodeToolResult.ProgressEventType.COMPLETE, "finished with error");
        }
        finally
        {
            filter.off();
            CLUSTER.get(1).runOnInstance(() -> {
                DatabaseDescriptor.repair_local_sync_enabled(true);
            });
        }
    }

    @Test(timeout = 1 * 60 * 1000)
    public void prepareIrFailure()
    {
        CLUSTER.schemaChange("CREATE TABLE " + KEYSPACE + ".prepareirfailure (key text, value text, PRIMARY KEY (key))");
        IMessageFilters.Filter filter = CLUSTER.verbs(Verb.PREPARE_CONSISTENT_REQ).messagesMatching(of(m -> {
            throw new RuntimeException("prepare fail");
        })).drop();
        try
        {
            NodeToolResult result = CLUSTER.get(1).nodetoolResult("repair", KEYSPACE, "prepareirfailure");
            result.asserts()
                  .notOk()
                  .errorContains("error prepare fail")
                  .notificationContains(NodeToolResult.ProgressEventType.ERROR, "error prepare fail")
                  .notificationContains(NodeToolResult.ProgressEventType.COMPLETE, "finished with error");
        }
        finally
        {
            filter.off();
        }
    }
}

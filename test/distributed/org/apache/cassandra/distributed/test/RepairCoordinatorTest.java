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
import java.net.UnknownHostException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.api.LongTokenRange;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.api.NodeToolResult.ProgressEventType;
import org.apache.cassandra.distributed.impl.MessageFilters;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.distributed.api.IMessageFilters.Matcher.of;

//TODO JMX to make sure no over counting
//TODO check system tables
//TODO test START event is seen; this is a regression with the patch
//TODO paramaterized test for incremental or full
public class RepairCoordinatorTest extends DistributedTestBase implements Serializable
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

    @Test(timeout = 1 * 60 * 1000)
    public void simple() {
        CLUSTER.schemaChange("CREATE TABLE " + KEYSPACE + ".simple (key text, PRIMARY KEY (key))");
        CLUSTER.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".simple (key) VALUES (?)", ConsistencyLevel.ANY, "some text");

        NodeToolResult result = CLUSTER.get(2).nodetoolResult("repair", KEYSPACE, "simple", "--full");
        result.asserts()
              .ok()
              .notificationContains(ProgressEventType.START, "Starting repair command")
              .notificationContains(ProgressEventType.START, "repairing keyspace " + KEYSPACE + " with repair options")
              .notificationContains(ProgressEventType.SUCCESS, "Repair completed successfully")
              .notificationContains(ProgressEventType.COMPLETE, "finished");
    }

    @Test(timeout = 1 * 60 * 1000)
    public void simpleIr() {
        CLUSTER.schemaChange("CREATE TABLE " + KEYSPACE + ".simpleir (key text, PRIMARY KEY (key))");
        CLUSTER.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".simpleir (key) VALUES (?)", ConsistencyLevel.ANY, "some text");

        NodeToolResult result = CLUSTER.get(2).nodetoolResult("repair", KEYSPACE, "simpleir");
        result.asserts()
              .ok()
              .notificationContains(ProgressEventType.SUCCESS, "Repair completed successfully")
              .notificationContains(ProgressEventType.COMPLETE, "finished");
    }

    @Test(timeout = 1 * 60 * 1000)
    public void missingKeyspace()
    {
        NodeToolResult result = CLUSTER.get(2).nodetoolResult("repair", "doesnotexist");
        result.asserts()
              .notOk()
              .errorContains("Keyspace [doesnotexist] does not exist.");
    }

    @Test(timeout = 1 * 60 * 1000)
    public void missingTable()
    {
        NodeToolResult result = CLUSTER.get(2).nodetoolResult("repair", KEYSPACE, "doesnotexist");
        result.asserts()
              .notOk()
              .errorContains("failed with error Unknown keyspace/cf pair (distributed_test_keyspace.doesnotexist)")
              .notificationContains(ProgressEventType.ERROR, "failed with error Unknown keyspace/cf pair (distributed_test_keyspace.doesnotexist)")
              .notificationContains(ProgressEventType.COMPLETE, "finished with error");
    }

    @Test(timeout = 1 * 60 * 1000)
    public void noTablesToRepair()
    {
        // index CF currently don't support repair, so they get dropped when listed
        // this is done in this test to cause the keyspace to have 0 tables to repair, which causes repair to no-op
        // early and skip.
        CLUSTER.schemaChange("CREATE TABLE " + KEYSPACE + ".withindex (key text, value text, PRIMARY KEY (key))");
        CLUSTER.schemaChange("CREATE INDEX value ON " + KEYSPACE + ".withindex (value)");
        // if CF has a . in it, it is assumed to be a 2i which rejects repairs
        NodeToolResult result = CLUSTER.get(2).nodetoolResult("repair", KEYSPACE, "withindex.value");
        result.asserts()
              .ok()
              .notificationContains("Empty keyspace")
              .notificationContains("skipping repair: " + KEYSPACE)
              .notificationContains(ProgressEventType.SUCCESS, "Empty keyspace") // will fail since success isn't returned; only complete
              .notificationContains(ProgressEventType.COMPLETE, "finished"); // will fail since it doesn't do this
    }

    @Test(timeout = 1 * 60 * 1000)
    public void intersectingRange()
    {
        // this test exists to show that this case will cause repair to finish; success or failure isn't imporant
        // if repair is enhanced to allow intersecting ranges w/ local then this test will fail saying that we expected
        // repair to fail but it didn't, this would be fine and this test should be updated to reflect the new
        // semantic
        CLUSTER.schemaChange("CREATE TABLE " + KEYSPACE + ".intersectingrange (key text, value text, PRIMARY KEY (key))");

        //TODO dtest api for this?
        LongTokenRange tokenRange = CLUSTER.get(2).callOnInstance(() -> {
            Set<Range<Token>> ranges = StorageService.instance.getLocalReplicas(KEYSPACE).ranges();
            Range<Token> range = Iterables.getFirst(ranges, null);
            long left = (long) range.left.getTokenValue();
            long right = (long) range.right.getTokenValue();
            return new LongTokenRange(left, right);
        });
        LongTokenRange intersectingRange = new LongTokenRange(tokenRange.maxInclusive - 7, tokenRange.maxInclusive + 7);

        NodeToolResult result = CLUSTER.get(2).nodetoolResult("repair", KEYSPACE, "intersectingrange",
                                                              "--start-token", Long.toString(intersectingRange.minExclusive),
                                                              "--end-token", Long.toString(intersectingRange.maxInclusive));
        result.asserts()
              .notOk()
              .errorContains("Requested range " + intersectingRange + " intersects a local range (" + tokenRange + ") but is not fully contained in one")
              .notificationContains(ProgressEventType.ERROR, "Requested range " + intersectingRange + " intersects a local range (" + tokenRange + ") but is not fully contained in one")
              .notificationContains(ProgressEventType.COMPLETE, "finished with error");
    }

    @Test(timeout = 1 * 60 * 1000)
    public void unknownHost()
    {
        CLUSTER.schemaChange("CREATE TABLE " + KEYSPACE + ".unknownhost (key text, value text, PRIMARY KEY (key))");

        NodeToolResult result = CLUSTER.get(2).nodetoolResult("repair", KEYSPACE, "unknownhost", "--in-hosts", "thisreally.should.not.exist.apache.org");
        result.asserts()
              .notOk()
              .errorContains("Unknown host specified thisreally.should.not.exist.apache.org")
              .notificationContains(ProgressEventType.ERROR, "Unknown host specified thisreally.should.not.exist.apache.org")
              .notificationContains(ProgressEventType.COMPLETE, "finished with error");
    }

    @Test(timeout = 1 * 60 * 1000)
    public void desiredHostNotCoordinator()
    {
        // current limitation is that the coordinator must be apart of the repair, so as long as that exists this test
        // verifies that the validation logic will termniate the repair properly
        CLUSTER.schemaChange("CREATE TABLE " + KEYSPACE + ".desiredhostnotcoordinator (key text, value text, PRIMARY KEY (key))");

        NodeToolResult result = CLUSTER.get(2).nodetoolResult("repair", KEYSPACE, "desiredhostnotcoordinator", "--in-hosts", "localhost");
        result.asserts()
              .notOk()
              .errorContains("The current host must be part of the repair")
              .notificationContains(ProgressEventType.ERROR, "The current host must be part of the repair")
              .notificationContains(ProgressEventType.COMPLETE, "finished with error");
    }

    @Test(timeout = 1 * 60 * 1000)
    public void onlyCoordinator()
    {
        // this is very similar to ::desiredHostNotCoordinator but has the difference that the only host to do repair
        // is the coordinator
        CLUSTER.schemaChange("CREATE TABLE " + KEYSPACE + ".onlycoordinator (key text, value text, PRIMARY KEY (key))");

        NodeToolResult result = CLUSTER.get(1).nodetoolResult("repair", KEYSPACE, "onlycoordinator", "--in-hosts", "localhost");
        result.asserts()
              .notOk()
              .errorContains("Specified hosts [localhost] do not share range")
              .notificationContains(ProgressEventType.ERROR, "Specified hosts [localhost] do not share range")
              .notificationContains(ProgressEventType.COMPLETE, "finished with error");
    }

    @Test(timeout = 1 * 60 * 1000)
    public void replicationFactorOne()
    {
        // In the case of rf=1 repair fails to create a cmd handle so node tool exists early
        CLUSTER.schemaChange("CREATE KEYSPACE replicationfactor WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
        CLUSTER.schemaChange("CREATE TABLE replicationfactor.one (key text, value text, PRIMARY KEY (key))");

        NodeToolResult result = CLUSTER.get(1).nodetoolResult("repair", "replicationfactor", "one");
        result.asserts()
              .ok();
    }

    @Test(timeout = 1 * 60 * 1000)
    public void prepareRPCTimeout()
    {
        CLUSTER.schemaChange("CREATE TABLE " + KEYSPACE + ".preparerpctimeout (key text, value text, PRIMARY KEY (key))");
        MessageFilters.Filter filter = CLUSTER.verbs(Verb.PREPARE_MSG).drop();
        try
        {
            NodeToolResult result = CLUSTER.get(1).nodetoolResult("repair", KEYSPACE, "preparerpctimeout");
            result.asserts()
                  .notOk()
                  .errorContains("Got negative replies from endpoints [127.0.0.2:7012]")
                  .notificationContains(ProgressEventType.ERROR, "Got negative replies from endpoints [127.0.0.2:7012]")
                  .notificationContains(ProgressEventType.COMPLETE, "finished with error");
        }
        finally
        {
            filter.off();
        }
    }

    @Test(timeout = 1 * 60 * 1000)
    public void neighbourDown() throws InterruptedException, ExecutionException
    {
        CLUSTER.schemaChange("CREATE TABLE " + KEYSPACE + ".neighbourdown (key text, value text, PRIMARY KEY (key))");
        Future<Void> shutdownFuture = CLUSTER.get(2).shutdown();
        String downNodeAddress = CLUSTER.get(2).callOnInstance(() -> FBUtilities.getBroadcastAddressAndPort().toString());
        try
        {
            // wait for the node to stop
            shutdownFuture.get();
            // wait for the failure detector to detect this
            CLUSTER.get(1).runOnInstance(() -> {
                InetAddressAndPort neighbor;
                try
                {
                    neighbor = InetAddressAndPort.getByName(downNodeAddress);
                }
                catch (UnknownHostException e)
                {
                    throw new RuntimeException(e);
                }
                while (FailureDetector.instance.isAlive(neighbor))
                    Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
            });

            NodeToolResult result = CLUSTER.get(1).nodetoolResult("repair", KEYSPACE, "neighbourdown");
            result.asserts()
                  .notOk()
                  .errorContains("Endpoint not alive")
                  .notificationContains(ProgressEventType.ERROR, "Endpoint not alive")
                  .notificationContains(ProgressEventType.COMPLETE, "finished with error");
        }
        finally
        {
            CLUSTER.get(2).startup(CLUSTER);
        }
    }

    @Test(timeout = 1 * 60 * 1000)
    public void prepareFailure()
    {
        CLUSTER.schemaChange("CREATE TABLE " + KEYSPACE + ".preparefailure (key text, value text, PRIMARY KEY (key))");
        IMessageFilters.Filter filter = CLUSTER.verbs(Verb.PREPARE_MSG).messagesMatching(of(m -> {
            throw new RuntimeException("prepare fail");
        })).drop();
        try
        {
            NodeToolResult result = CLUSTER.get(1).nodetoolResult("repair", KEYSPACE, "preparefailure", "--full");
            result.asserts()
                  .notOk()
                  //TODO error message may be better to improve since its user facing
                  .errorContains("Got negative replies from endpoints")
                  .notificationContains(ProgressEventType.ERROR, "Got negative replies from endpoints")
                  .notificationContains(ProgressEventType.COMPLETE, "finished with error");
        }
        finally
        {
            filter.off();
        }
    }

    @Test(timeout = 1 * 60 * 1000)
    public void snapshotFailure()
    {
        CLUSTER.schemaChange("CREATE TABLE " + KEYSPACE + ".snapshotfailure (key text, value text, PRIMARY KEY (key))");
        IMessageFilters.Filter filter = CLUSTER.verbs(Verb.SNAPSHOT_MSG).messagesMatching(of(m -> {
            throw new RuntimeException("snapshot fail");
        })).drop();
        try
        {
            NodeToolResult result = CLUSTER.get(1).nodetoolResult("repair", KEYSPACE, "snapshotfailure", "--full", "--sequential");
            result.asserts()
                  .notOk()
                  .errorContains("Some repair failed")
                  .notificationContains(ProgressEventType.ERROR, "Some repair failed")
                  .notificationContains(ProgressEventType.COMPLETE, "finished with error");
        }
        finally
        {
            filter.off();
        }
    }

    // IR doesn't use the snapshot message, so don't need to test it
}

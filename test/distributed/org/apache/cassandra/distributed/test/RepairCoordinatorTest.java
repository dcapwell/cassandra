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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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
import org.apache.cassandra.distributed.test.DistributedRepairUtils.RepairType;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static java.lang.String.format;
import static org.apache.cassandra.distributed.api.IMessageFilters.Matcher.of;
import static org.apache.cassandra.distributed.test.DistributedRepairUtils.assertParentRepairFailed;
import static org.apache.cassandra.distributed.test.DistributedRepairUtils.assertParentRepairFailedWithMessageContains;
import static org.apache.cassandra.distributed.test.DistributedRepairUtils.assertParentRepairNotExist;
import static org.apache.cassandra.distributed.test.DistributedRepairUtils.assertParentRepairSuccess;
import static org.apache.cassandra.distributed.test.DistributedRepairUtils.getRepairExceptions;

@RunWith(Parameterized.class)
public class RepairCoordinatorTest extends DistributedTestBase implements Serializable
{
    private static Cluster CLUSTER;

    private final RepairType repairType;

    public RepairCoordinatorTest(RepairType repairType)
    {
        this.repairType = repairType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> messages()
    {
        List<Object[]> tests = new ArrayList<>();
        for (RepairType type : RepairType.values())
        {
            tests.add(new Object[] { type });
        }
        return tests;
    }

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
        String table = tableName("simple");
        CLUSTER.schemaChange(format("CREATE TABLE %s.%s (key text, PRIMARY KEY (key))", KEYSPACE, table));
        CLUSTER.coordinator(1).execute(format("INSERT INTO %s.%s (key) VALUES (?)", KEYSPACE, table), ConsistencyLevel.ANY, "some text");

        long repairExceptions = getRepairExceptions(CLUSTER, 2);
        NodeToolResult result = repair(2, KEYSPACE, table);
        result.asserts()
              .ok()
              .notificationContains(ProgressEventType.START, "Starting repair command")
              .notificationContains(ProgressEventType.START, "repairing keyspace " + KEYSPACE + " with repair options")
              .notificationContains(ProgressEventType.SUCCESS, "Repair completed successfully")
              .notificationContains(ProgressEventType.COMPLETE, "finished");

        assertParentRepairSuccess(CLUSTER, KEYSPACE, table);

        Assert.assertEquals(repairExceptions, getRepairExceptions(CLUSTER, 2));
    }

    @Test(timeout = 1 * 60 * 1000)
    public void missingKeyspace()
    {
        // as of this moment the check is done in nodetool so the JMX notifications are not imporant
        // nor is the history stored
        long repairExceptions = getRepairExceptions(CLUSTER, 2);
        NodeToolResult result = repair(2, "doesnotexist");
        result.asserts()
              .notOk()
              .errorContains("Keyspace [doesnotexist] does not exist.");


        Assert.assertEquals(repairExceptions, getRepairExceptions(CLUSTER, 2));

        //TODO verify ParentRepair missing for keyspace
    }

    @Test(timeout = 1 * 60 * 1000)
    public void missingTable()
    {
        long repairExceptions = getRepairExceptions(CLUSTER, 2);
        NodeToolResult result = repair(2, KEYSPACE, "doesnotexist");
        result.asserts()
              .notOk()
              .errorContains("failed with error Unknown keyspace/cf pair (distributed_test_keyspace.doesnotexist)")
              // Start notification is ignored since this is checked during setup (aka before start)
              .notificationContains(ProgressEventType.ERROR, "failed with error Unknown keyspace/cf pair (distributed_test_keyspace.doesnotexist)")
              .notificationContains(ProgressEventType.COMPLETE, "finished with error");

        assertParentRepairNotExist(CLUSTER, KEYSPACE, "doesnotexist");

        Assert.assertEquals(repairExceptions + 1, getRepairExceptions(CLUSTER, 2));
    }

    @Test(timeout = 1 * 60 * 1000)
    public void noTablesToRepair()
    {
        // index CF currently don't support repair, so they get dropped when listed
        // this is done in this test to cause the keyspace to have 0 tables to repair, which causes repair to no-op
        // early and skip.
        String table = tableName("withindex");
        CLUSTER.schemaChange(format("CREATE TABLE %s.%s (key text, value text, PRIMARY KEY (key))", KEYSPACE, table));
        CLUSTER.schemaChange(format("CREATE INDEX value_%s ON %s.%s (value)", postfix(), KEYSPACE, table));

        long repairExceptions = getRepairExceptions(CLUSTER, 2);
        // if CF has a . in it, it is assumed to be a 2i which rejects repairs
        NodeToolResult result = repair(2, KEYSPACE, table + ".value");
        result.asserts()
              .ok()
              .notificationContains("Empty keyspace")
              .notificationContains("skipping repair: " + KEYSPACE)
              // Start notification is ignored since this is checked during setup (aka before start)
              .notificationContains(ProgressEventType.SUCCESS, "Empty keyspace") // will fail since success isn't returned; only complete
              .notificationContains(ProgressEventType.COMPLETE, "finished"); // will fail since it doesn't do this


        assertParentRepairNotExist(CLUSTER, KEYSPACE, table + ".value");

        // this is actually a SKIP and not a FAILURE, so shouldn't increment
        Assert.assertEquals(repairExceptions, getRepairExceptions(CLUSTER, 2));
    }

    @Test(timeout = 1 * 60 * 1000)
    public void intersectingRange()
    {
        // this test exists to show that this case will cause repair to finish; success or failure isn't imporant
        // if repair is enhanced to allow intersecting ranges w/ local then this test will fail saying that we expected
        // repair to fail but it didn't, this would be fine and this test should be updated to reflect the new
        // semantic
        String table = tableName("intersectingrange");
        CLUSTER.schemaChange(format("CREATE TABLE %s.%s (key text, value text, PRIMARY KEY (key))", KEYSPACE, table));

        //TODO dtest api for this?
        LongTokenRange tokenRange = CLUSTER.get(2).callOnInstance(() -> {
            Set<Range<Token>> ranges = StorageService.instance.getLocalReplicas(KEYSPACE).ranges();
            Range<Token> range = Iterables.getFirst(ranges, null);
            long left = (long) range.left.getTokenValue();
            long right = (long) range.right.getTokenValue();
            return new LongTokenRange(left, right);
        });
        LongTokenRange intersectingRange = new LongTokenRange(tokenRange.maxInclusive - 7, tokenRange.maxInclusive + 7);

        long repairExceptions = getRepairExceptions(CLUSTER, 2);
        NodeToolResult result = repair(2, KEYSPACE, table,
                                       "--start-token", Long.toString(intersectingRange.minExclusive),
                                       "--end-token", Long.toString(intersectingRange.maxInclusive));
        result.asserts()
              .notOk()
              .errorContains("Requested range " + intersectingRange + " intersects a local range (" + tokenRange + ") but is not fully contained in one")
              .notificationContains(ProgressEventType.START, "Starting repair command")
              .notificationContains(ProgressEventType.START, "repairing keyspace " + KEYSPACE + " with repair options")
              .notificationContains(ProgressEventType.ERROR, "Requested range " + intersectingRange + " intersects a local range (" + tokenRange + ") but is not fully contained in one")
              .notificationContains(ProgressEventType.COMPLETE, "finished with error");

        assertParentRepairNotExist(CLUSTER, KEYSPACE, table);

        Assert.assertEquals(repairExceptions + 1, getRepairExceptions(CLUSTER, 2));
    }

    @Test(timeout = 1 * 60 * 1000)
    public void unknownHost()
    {
        String table = tableName("unknownhost");
        CLUSTER.schemaChange(format("CREATE TABLE %s.%s (key text, value text, PRIMARY KEY (key))", KEYSPACE, table));

        long repairExceptions = getRepairExceptions(CLUSTER, 2);
        NodeToolResult result = repair(2, KEYSPACE, table, "--in-hosts", "thisreally.should.not.exist.apache.org");
        result.asserts()
              .notOk()
              .errorContains("Unknown host specified thisreally.should.not.exist.apache.org")
              .notificationContains(ProgressEventType.START, "Starting repair command")
              .notificationContains(ProgressEventType.START, "repairing keyspace " + KEYSPACE + " with repair options")
              .notificationContains(ProgressEventType.ERROR, "Unknown host specified thisreally.should.not.exist.apache.org")
              .notificationContains(ProgressEventType.COMPLETE, "finished with error");

        assertParentRepairNotExist(CLUSTER, KEYSPACE, table);

        Assert.assertEquals(repairExceptions + 1, getRepairExceptions(CLUSTER, 2));
    }

    @Test(timeout = 1 * 60 * 1000)
    public void desiredHostNotCoordinator()
    {
        // current limitation is that the coordinator must be apart of the repair, so as long as that exists this test
        // verifies that the validation logic will termniate the repair properly
        String table = tableName("desiredhostnotcoordinator");
        CLUSTER.schemaChange(format("CREATE TABLE %s.%s (key text, value text, PRIMARY KEY (key))", KEYSPACE, table));

        long repairExceptions = getRepairExceptions(CLUSTER, 2);
        NodeToolResult result = repair(2, KEYSPACE, table, "--in-hosts", "localhost");
        result.asserts()
              .notOk()
              .errorContains("The current host must be part of the repair")
              .notificationContains(ProgressEventType.START, "Starting repair command")
              .notificationContains(ProgressEventType.START, "repairing keyspace " + KEYSPACE + " with repair options")
              .notificationContains(ProgressEventType.ERROR, "The current host must be part of the repair")
              .notificationContains(ProgressEventType.COMPLETE, "finished with error");

        assertParentRepairNotExist(CLUSTER, KEYSPACE, table);

        Assert.assertEquals(repairExceptions + 1, getRepairExceptions(CLUSTER, 2));
    }

    @Test(timeout = 1 * 60 * 1000)
    public void onlyCoordinator()
    {
        // this is very similar to ::desiredHostNotCoordinator but has the difference that the only host to do repair
        // is the coordinator
        String table = tableName("onlycoordinator");
        CLUSTER.schemaChange(format("CREATE TABLE %s.%s (key text, value text, PRIMARY KEY (key))", KEYSPACE, table));

        long repairExceptions = getRepairExceptions(CLUSTER, 2);
        NodeToolResult result = repair(1, KEYSPACE, table, "--in-hosts", "localhost");
        result.asserts()
              .notOk()
              .errorContains("Specified hosts [localhost] do not share range")
              .notificationContains(ProgressEventType.START, "Starting repair command")
              .notificationContains(ProgressEventType.START, "repairing keyspace " + KEYSPACE + " with repair options")
              .notificationContains(ProgressEventType.ERROR, "Specified hosts [localhost] do not share range")
              .notificationContains(ProgressEventType.COMPLETE, "finished with error");

        assertParentRepairNotExist(CLUSTER, KEYSPACE, table);

        //TODO should this be marked as fail to match others?  Should they not be marked?
        Assert.assertEquals(repairExceptions, getRepairExceptions(CLUSTER, 2));
    }

    @Test(timeout = 1 * 60 * 1000)
    public void replicationFactorOne()
    {
        // In the case of rf=1 repair fails to create a cmd handle so node tool exists early
        String table = tableName("one");
        // since cluster is shared and this test gets called multiple times, need "IF NOT EXISTS" so the second+ attempt
        // does not fail
        CLUSTER.schemaChange("CREATE KEYSPACE IF NOT EXISTS replicationfactor WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
        CLUSTER.schemaChange(format("CREATE TABLE replicationfactor.%s (key text, value text, PRIMARY KEY (key))", table));

        long repairExceptions = getRepairExceptions(CLUSTER, 1);
        NodeToolResult result = repair(1, "replicationfactor", table);
        result.asserts()
              .ok();

        assertParentRepairNotExist(CLUSTER, KEYSPACE, table);

        Assert.assertEquals(repairExceptions, getRepairExceptions(CLUSTER, 1));
    }

    @Test(timeout = 1 * 60 * 1000)
    public void prepareRPCTimeout()
    {
        String table = tableName("preparerpctimeout");
        CLUSTER.schemaChange(format("CREATE TABLE %s.%s (key text, value text, PRIMARY KEY (key))", KEYSPACE, table));
        MessageFilters.Filter filter = CLUSTER.verbs(Verb.PREPARE_MSG).drop();
        try
        {
            long repairExceptions = getRepairExceptions(CLUSTER, 1);
            NodeToolResult result = repair(1, KEYSPACE, table);
            result.asserts()
                  .notOk()
                  .errorContains("Got negative replies from endpoints [127.0.0.2:7012]")
                  .notificationContains(ProgressEventType.START, "Starting repair command")
                  .notificationContains(ProgressEventType.START, "repairing keyspace " + KEYSPACE + " with repair options")
                  .notificationContains(ProgressEventType.ERROR, "Got negative replies from endpoints [127.0.0.2:7012]")
                  .notificationContains(ProgressEventType.COMPLETE, "finished with error");

            assertParentRepairFailedWithMessageContains(CLUSTER, KEYSPACE, table, "Got negative replies from endpoints [127.0.0.2:7012]");

            Assert.assertEquals(repairExceptions + 1, getRepairExceptions(CLUSTER, 1));
        }
        finally
        {
            filter.off();
        }
    }

    @Test(timeout = 1 * 60 * 1000)
    public void neighbourDown() throws InterruptedException, ExecutionException
    {
        String table = tableName("neighbourdown");
        CLUSTER.schemaChange(format("CREATE TABLE %s.%s (key text, value text, PRIMARY KEY (key))", KEYSPACE, table));
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

            long repairExceptions = getRepairExceptions(CLUSTER, 1);
            NodeToolResult result = repair(1, KEYSPACE, table);
            result.asserts()
                  .notOk()
                  .errorContains("Endpoint not alive")
                  .notificationContains(ProgressEventType.START, "Starting repair command")
                  .notificationContains(ProgressEventType.START, "repairing keyspace " + KEYSPACE + " with repair options")
                  .notificationContains(ProgressEventType.ERROR, "Endpoint not alive")
                  .notificationContains(ProgressEventType.COMPLETE, "finished with error");

            Assert.assertEquals(repairExceptions + 1, getRepairExceptions(CLUSTER, 1));
        }
        finally
        {
            CLUSTER.get(2).startup(CLUSTER);
        }

        // make sure to call outside of the try/finally so the node is up so we can actually query
        assertParentRepairFailed(CLUSTER, KEYSPACE, table);
    }

    @Test(timeout = 1 * 60 * 1000)
    public void prepareFailure()
    {
        String table = tableName("preparefailure");
        CLUSTER.schemaChange(format("CREATE TABLE %s.%s (key text, value text, PRIMARY KEY (key))", KEYSPACE, table));
        IMessageFilters.Filter filter = CLUSTER.verbs(Verb.PREPARE_MSG).messagesMatching(of(m -> {
            throw new RuntimeException("prepare fail");
        })).drop();
        try
        {
            long repairExceptions = getRepairExceptions(CLUSTER, 1);
            NodeToolResult result = repair(1, KEYSPACE, table);
            result.asserts()
                  .notOk()
                  //TODO error message may be better to improve since its user facing
                  .errorContains("Got negative replies from endpoints")
                  .notificationContains(ProgressEventType.START, "Starting repair command")
                  .notificationContains(ProgressEventType.START, "repairing keyspace " + KEYSPACE + " with repair options")
                  .notificationContains(ProgressEventType.ERROR, "Got negative replies from endpoints")
                  .notificationContains(ProgressEventType.COMPLETE, "finished with error");

            Assert.assertEquals(repairExceptions + 1, getRepairExceptions(CLUSTER, 1));
            assertParentRepairFailed(CLUSTER, KEYSPACE, table);
        }
        finally
        {
            filter.off();
        }
    }

    @Test(timeout = 1 * 60 * 1000)
    public void snapshotFailure()
    {
        // IR doesn't use snapshot message, so can ignore that case; mostly running it out of simplicity (and maybe one day it does)
        String table = tableName("snapshotfailure");
        CLUSTER.schemaChange(format("CREATE TABLE %s.%s (key text, value text, PRIMARY KEY (key))", KEYSPACE, table));
        IMessageFilters.Filter filter = CLUSTER.verbs(Verb.SNAPSHOT_MSG).messagesMatching(of(m -> {
            throw new RuntimeException("snapshot fail");
        })).drop();
        try
        {
            long repairExceptions = getRepairExceptions(CLUSTER, 1);
            NodeToolResult result = repair(1, KEYSPACE, table, "--sequential");
            result.asserts()
                  .notOk()
                  .errorContains("Some repair failed")
                  .notificationContains(ProgressEventType.START, "Starting repair command")
                  .notificationContains(ProgressEventType.START, "repairing keyspace " + KEYSPACE + " with repair options")
                  .notificationContains(ProgressEventType.ERROR, "Some repair failed")
                  .notificationContains(ProgressEventType.COMPLETE, "finished with error");

            Assert.assertEquals(repairExceptions + 1, getRepairExceptions(CLUSTER, 1));
            assertParentRepairFailed(CLUSTER, KEYSPACE, table);
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
        String table = tableName("validationparticipentcrashesandcomesback");
        CLUSTER.schemaChange(format("CREATE TABLE %s.%s (key text, value text, PRIMARY KEY (key))", KEYSPACE, table));
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

            long repairExceptions = getRepairExceptions(CLUSTER, 1);
            NodeToolResult result = repair(1, KEYSPACE, table);
            recovered.join(); // if recovery didn't happen then the results are not what are being tested, so block here first
            result.asserts()
                  .notOk()
                  .errorContains("Some repair failed")
                  .notificationContains(NodeToolResult.ProgressEventType.ERROR, "Some repair failed")
                  .notificationContains(NodeToolResult.ProgressEventType.COMPLETE, "finished with error");

            Assert.assertEquals(repairExceptions + 1, getRepairExceptions(CLUSTER, 1));
            assertParentRepairFailed(CLUSTER, KEYSPACE, table);
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

    private String tableName(String prefix) {
        return prefix + "_" + postfix();
    }

    private String postfix()
    {
        return repairType.name().toLowerCase();
    }

    private NodeToolResult repair(int node, String... args) {
        return DistributedRepairUtils.repair(CLUSTER, node, repairType, args);
    }
}

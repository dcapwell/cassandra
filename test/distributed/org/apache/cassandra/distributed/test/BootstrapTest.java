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

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.impl.DistributedTestSnitch;
import org.apache.cassandra.distributed.impl.RowUtil;
import org.apache.cassandra.distributed.shared.Byteman;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.shared.Shared;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.concurrent.SimpleCondition;

import static org.apache.cassandra.distributed.action.GossipHelper.decomission;
import static org.apache.cassandra.distributed.action.GossipHelper.disseminateGossipState;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

//TODO class is slow, so starts to push the limits of the 6m timeout, might be best to split it =(
//TODO would be great if we could split CI based off test function rather than class, would solve the above problem (how python dtest works)
public class BootstrapTest extends TestBaseImpl implements Serializable
{
    @Test
    public void bootstrapTest() throws Throwable
    {
        int originalNodeCount = 2;
        int expandedNodeCount = originalNodeCount + 1;

        try (Cluster cluster = builder().withNodes(originalNodeCount)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(expandedNodeCount))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(expandedNodeCount, "dc0", "rack0"))
                                        .withConfig(config -> config.with(NETWORK, GOSSIP))
                                        .start())
        {
            populate(cluster,0, 100);

            IInstanceConfig config = cluster.newInstanceConfig();
            config.set("auto_bootstrap", true);
            IInvokableInstance newInstance = cluster.bootstrap(config);
            withJoinRing(false, () -> newInstance.startup(cluster));

            newInstance.nodetoolResult("join").asserts().success();

            for (Map.Entry<Integer, Long> e : count(cluster).entrySet())
                Assert.assertEquals("Node " + e.getKey() + " has incorrect row state", e.getValue().longValue(), 100L);
        }
    }

    @Test
    public void testPendingWrites() throws Throwable
    {
        int originalNodeCount = 2;
        int expandedNodeCount = originalNodeCount + 1;

        Byteman byteman = Byteman.createFromScripts("test/resources/byteman/stream_blocker.btm");
        try (Cluster cluster = builder().withNodes(originalNodeCount)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(expandedNodeCount))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(expandedNodeCount, "dc0", "rack0"))
                                        .withConfig(config -> config.with(NETWORK, GOSSIP))
                                        .withInstanceInitializer((cl, node) -> {
                                            switch (node)
                                            {
                                                case 1:
                                                case 2:
                                                    byteman.install(cl);
                                            }
                                        })
                                        .start())
        {
            populate(cluster, 0, 100);
            IInstanceConfig config = cluster.newInstanceConfig();
            config.set("auto_bootstrap", true);
            IInvokableInstance newInstance = cluster.bootstrap(config);
            withJoinRing(false, () -> newInstance.startup(cluster));

            // join in the background; the bootstrap should hang waiting for the test to unblock
            //TODO update dtest api to expose async nodetool to avoid needing a seperate thread to block on
            CompletableFuture<Void> joining = CompletableFuture.runAsync(() -> newInstance.nodetoolResult("join").asserts().success());

            // wait for join to start, pending ranges should be updated by now
            BootstrapBlocker.awaitStreaming();

            // Make sure the node joins the ring and that some ranges are pending
            cluster.get(1).acceptsOnInstance((InetSocketAddress ip) -> {

                Set<InetAddressAndPort> set = new HashSet<>();
                for (Map.Entry<Range<Token>, EndpointsForRange.Builder> e : StorageService.instance.getTokenMetadata().getPendingRanges(KEYSPACE))
                {
                    set.addAll(e.getValue().build().endpoints());
                }
                Assert.assertEquals(set.size(), 1);
                Assert.assertTrue(String.format("%s should contain %s", set, ip),
                                  set.contains(DistributedTestSnitch.toCassandraInetAddressAndPort(ip)));
            }).accept(newInstance.broadcastAddress());

            populate(cluster, 100, 150);

            // unblock the bootstrap and wait for it to complete
            BootstrapBlocker.signal();
            joining.join();

            // node3 knows that bootstrap is complete but node1 and node2 may not have this yet
            // to avoid flaky tests disseminate the GossipState cross the cluster
            IntStream.of(1, 2).forEach(i -> disseminateGossipState(cluster.get(i), newInstance));

            // validate no pending ranges
            cluster.get(1).acceptsOnInstance((InetSocketAddress ip) -> {
                Set<InetAddressAndPort> set = new HashSet<>();
                for (Map.Entry<Range<Token>, EndpointsForRange.Builder> e : StorageService.instance.getTokenMetadata().getPendingRanges(KEYSPACE))
                    set.addAll(e.getValue().build().endpoints());
                assert set.size() == 0 : set;
            }).accept(newInstance.broadcastAddress());

            for (Map.Entry<Integer, Long> e : count(cluster).entrySet())
                Assert.assertEquals("Node " + e.getKey() + " has incorrect row state; present was " + e.getValue().longValue(), e.getValue().longValue(), 150L);
        }
    }

    @Test
    public void internodeConnectionsDuringDecom() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(4)
                                        .withConfig(config -> config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL))
                                        .start())
        {
            populate(cluster, 0, 100);

            decomission(cluster.get(1));

            cluster.filters().allVerbs().from(1).messagesMatching((i, i1, iMessage) -> {
                throw new AssertionError("Decomissioned node should not send any messages");
            }).drop();


            Map<Integer, Long> connectionAttempts = new HashMap<>();
            long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10);

            // Wait 10 seconds and check if there are any new connection attempts to the decomissioned node
            while (System.currentTimeMillis() <= deadline)
            {
                for (int i = 2; i <= cluster.size(); i++)
                {
                    Object[][] res = cluster.get(i).executeInternal("SELECT active_connections, connection_attempts FROM system_views.internode_outbound WHERE address = '127.0.0.1' AND port = 7012");
                    Assert.assertEquals(1, res.length);
                    Assert.assertEquals(0L, ((Long) res[0][0]).longValue());
                    long attempts = ((Long) res[0][1]).longValue();
                    if (connectionAttempts.get(i) == null)
                        connectionAttempts.put(i, attempts);
                    else
                        Assert.assertEquals(connectionAttempts.get(i), (Long) attempts);
                }
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
            }
        }
    }

    @Test
    public void clientConnectionsDuringDecom() throws Throwable
    {
        ExecutorService executor = Executors.newFixedThreadPool(1);
        try (Cluster cluster = builder().withNodes(4)
                                        .withConfig(config -> config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL))
                                        .start())
        {
            populate(cluster, 0, 100);

            try (com.datastax.driver.core.Cluster client = com.datastax.driver.core.Cluster.builder().addContactPoint("127.0.0.1")
                                                                                           .withLoadBalancingPolicy(new RoundRobinPolicy())
                                                                                           .build();
                 Session session = client.connect())
            {
                CompletableFuture.runAsync(() -> {
                    while (!Thread.currentThread().isInterrupted())
                    {
                        if (client.getMetadata().getAllHosts().size() == cluster.size())
                            return;
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
                    }
                }, executor).get(1, TimeUnit.MINUTES);

                decomission(cluster.get(1));

                Assert.assertEquals(cluster.size() - 1, client.getMetadata().getAllHosts().size());
                for (int i = 0; i < 100; i++)
                {
                    Statement select = new SimpleStatement("select * from " + KEYSPACE + ".tbl WHERE pk = " + i).setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.ALL);
                    final ResultSet resultSet = session.execute(select);
                    assertRows(RowUtil.toObjects(resultSet), row(i, i, i));
                }
                Assert.assertEquals(cluster.size() - 1, client.getMetadata().getAllHosts().size());
            }
        }
    }

    public static void populate(ICluster cluster, int from, int to)
    {
        cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + 3 + "};");
        cluster.schemaChange("CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
        for (int i = from; i < to; i++)
        {
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?, ?, ?)",
                                           ConsistencyLevel.QUORUM,
                                           i, i, i);
        }
    }

    public static Map<Integer, Long> count(ICluster cluster)
    {
        return IntStream.rangeClosed(1, cluster.size())
                        .boxed()
                        .collect(Collectors.toMap(nodeId -> nodeId,
                                                  nodeId -> (Long) cluster.get(nodeId).executeInternal("SELECT count(*) FROM " + KEYSPACE + ".tbl")[0][0]));
    }


    public static void withJoinRing(boolean value, Runnable r)
    {
        String prop = "cassandra.join_ring";
        String before = System.getProperty(prop);
        try
        {
            System.setProperty(prop, Boolean.toString(value));
            r.run();
        }
        finally
        {
            System.setProperty(prop, before == null ? "true" : before);
        }
    }

    @Before
    public void resetBlocked()
    {
        BootstrapBlocker.reset();
    }

    @Shared
    public static final class BootstrapBlocker
    {
        private static final SimpleCondition STREAMING = new SimpleCondition();
        private static final SimpleCondition TEST = new SimpleCondition();

        public static boolean block()
        {
            TEST.signalAll();
            try
            {
                STREAMING.await();
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
            return true;
        }

        public static void awaitStreaming() throws InterruptedException
        {
            TEST.await();
        }

        public static void reset()
        {
            TEST.reset();
            STREAMING.reset();
        }

        public static void signal()
        {
            STREAMING.signalAll();
        }
    }
}
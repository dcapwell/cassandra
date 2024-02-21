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

package org.apache.cassandra.service.accord;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.google.common.collect.Maps;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.api.Key;
import accord.impl.SizeOfIntersectionSorter;
import accord.local.Node;
import accord.local.PreLoadContext;
import accord.local.SaveStatus;
import accord.messages.PreAccept;
import accord.primitives.FullKeyRoute;
import accord.primitives.FullRangeRoute;
import accord.primitives.FullRoute;
import accord.primitives.Keys;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.AccordGens;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.transformations.AddAccordTable;
import org.apache.cassandra.utils.Pair;
import org.assertj.core.api.Assertions;

import static accord.utils.Property.qt;
import static org.apache.cassandra.service.accord.AccordTestUtils.createTxn;

public class AccordCommandStoreFuzzTest extends CQLTester
{
    static
    {
        CassandraRelevantProperties.TEST_ACCORD_STORE_THREAD_CHECKS_ENABLED.setBoolean(false);
        // since this test does frequent truncates, the info table gets updated and forced flushed... which is 90% of the cost of this test...
        // this flag disables that flush
        CassandraRelevantProperties.UNSAFE_SYSTEM.setBoolean(true);
        // The plan is to migrate away from SAI, so rather than hacking around timeout issues; just disable for now
        CassandraRelevantProperties.SAI_TEST_DISABLE_TIMEOUT.setBoolean(true);
    }

    private static TableMetadata intTbl, reverseTokenTbl;
    private static Node.Id nodeId;

    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();
        DatabaseDescriptor.setIncrementalBackupsEnabled(false);
    }

    @Before
    public void init()
    {
        if (intTbl != null)
            return;
        createKeyspace("CREATE KEYSPACE test WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 2 }");
        createTable("test", "CREATE TABLE test.tbl1 (pk int PRIMARY KEY, value int)");
        intTbl = Schema.instance.getTableMetadata("test", "tbl1");
        AddAccordTable.addTable(intTbl.id);

        createTable("test", "CREATE TABLE test.tbl2 (pk vector<bigint, 2> PRIMARY KEY, value int)");
        reverseTokenTbl = Schema.instance.getTableMetadata("test", "tbl2");
        AddAccordTable.addTable(reverseTokenTbl.id);

        nodeId = AccordTopology.tcmIdToAccord(ClusterMetadata.current().myNodeId());

        ServerTestUtils.markCMS();
    }

    @Test
    public void emptyTxns()
    {
        qt().withExamples(10).check(rs -> {
           clearSystemTables();
            try (var instance = new SimulatedAccordCommandStore(rs))
            {
                for (int i = 0, examples = 100; i < examples; i++)
                {
                    TxnId id = AccordGens.txnIds().next(rs);
                    instance.process(PreLoadContext.contextFor(id), (safe) -> {
                        var safeCommand = safe.get(id, id, Ranges.EMPTY);
                        var command = safeCommand.current();
                        Assertions.assertThat(command.saveStatus()).isEqualTo(SaveStatus.Uninitialised);
                        return null;
                    });
                }
            }

        });
    }

    @Test
    public void keyConflicts()
    {
        TableMetadata tbl = intTbl;
        int numSamples = 100;

        qt().withExamples(10).check(rs -> {
            clearSystemTables();
            int key = rs.nextInt();
            PartitionKey pk = new PartitionKey(tbl.id, tbl.partitioner.decorateKey(Int32Type.instance.decompose(key)));
            Keys keys = Keys.of(pk);
            FullKeyRoute route = keys.toRoute(pk.toUnseekable());
            Txn txn = createTxn(wrapInTxn("INSERT INTO " + tbl + "(pk, value) VALUES (?, ?)"), Arrays.asList(key, 42));
            try (var instance = new SimulatedAccordCommandStore(rs))
            {
                List<TxnId> conflicts = new ArrayList<>(numSamples);
                boolean concurrent = rs.nextBoolean();
                List<AsyncResult<?>> asyncs = !concurrent ? null : new ArrayList<>(numSamples);
                for (int i = 0; i < numSamples; i++)
                {
                    instance.maybeCacheEvict(keys, Ranges.EMPTY);
                    if (concurrent)
                    {
                        var pair = assertPreAcceptAsync(instance, txn, route, keyConflicts(conflicts, keys));
                        conflicts.add(pair.left);
                        asyncs.add(pair.right);
                    }
                    else
                    {
                        conflicts.add(assertPreAccept(instance, txn, route, keyConflicts(conflicts, keys)));
                    }
                }
                if (concurrent)
                {
                    instance.processAll();
                    safeBlock(asyncs);
                }
            }
        });
    }

    @Test
    public void concurrentRangePartialKeyMatch()
    {
        var tbl = reverseTokenTbl;
        int numSamples = 250;
        int numConflictKeyTxns = 10;

        qt().withExamples(10).check(rs -> {
            clearSystemTables();
            try (var instance = new SimulatedAccordCommandStore(rs))
            {
                long token = rs.nextLong(Long.MIN_VALUE  + 1, Long.MAX_VALUE);
                Ranges partialRange = Ranges.of(tokenRange(tbl.id, token - 1, token));
                long outOfRangeToken = token - 10;
                if (outOfRangeToken == Long.MIN_VALUE) // if this wraps around that is fine, just can't be min
                    outOfRangeToken++;
                Key key = new PartitionKey(tbl.id, tbl.partitioner.decorateKey(LongToken.keyForToken(token)));
                Key outOfRangeKey = new PartitionKey(tbl.id, tbl.partitioner.decorateKey(LongToken.keyForToken(outOfRangeToken)));
                Txn keyTxn = createTxn(wrapInTxn("INSERT INTO " + tbl + "(pk, value) VALUES (?, ?)",
                                                 "INSERT INTO " + tbl + "(pk, value) VALUES (?, ?)"),
                                       Arrays.asList(LongToken.keyForToken(token), 42,
                                                     LongToken.keyForToken(outOfRangeToken), 42));
                Keys keys = (Keys) keyTxn.keys();
                FullRoute<?> keyRoute = keys.toRoute(keys.get(0).toUnseekable());

                Txn conflictingKeyTxn = createTxn(wrapInTxn("INSERT INTO " + tbl + "(pk, value) VALUES (?, ?)"),
                                                  Arrays.asList(LongToken.keyForToken(outOfRangeToken), 42));
                Keys conflictingKeys = (Keys) conflictingKeyTxn.keys();
                FullRoute<?> conflictingRoute = conflictingKeys.toRoute(conflictingKeys.get(0).toUnseekable());

                FullRangeRoute rangeRoute = partialRange.toRoute(keys.get(0).toUnseekable());
                Txn rangeTxn = createTxn(Txn.Kind.ExclusiveSyncPoint, partialRange);

                List<TxnId> keyConflicts = new ArrayList<>(numSamples);
                List<TxnId> outOfRangeKeyConflicts = new ArrayList<>(numSamples);
                List<TxnId> rangeConflicts = new ArrayList<>(numSamples);
                List<AsyncResult<?>> asyncs = new ArrayList<>(numSamples * 2 + numSamples * numConflictKeyTxns);
                List<TxnId> asyncIds = new ArrayList<>(numSamples * 2 + numSamples * numConflictKeyTxns);
                for (int i = 0; i < numSamples; i++)
                {
                    instance.maybeCacheEvict((Keys) keyTxn.keys(), partialRange);
                    for (int j = 0; j < numConflictKeyTxns; j++)
                    {
                        var p = enqueuePreAccept(instance, conflictingKeyTxn, conflictingRoute);
                        outOfRangeKeyConflicts.add(p.left);
                        asyncs.add(p.right);
                        asyncIds.add(p.left);
                    }

                    var k = assertPreAcceptAsync(instance, keyTxn, keyRoute, Map.of(key, keyConflicts, outOfRangeKey, outOfRangeKeyConflicts), Collections.emptyMap());
                    keyConflicts.add(k.left);
                    outOfRangeKeyConflicts.add(k.left);
                    asyncs.add(k.right);
                    asyncIds.add(k.left);

                    var r = assertPreAcceptAsync(instance, rangeTxn, rangeRoute, Map.of(key, keyConflicts), rangeConflicts(rangeConflicts, partialRange));
                    rangeConflicts.add(r.left);
                    asyncs.add(r.right);
                    asyncIds.add(r.left);
                }
                instance.processAll();
                safeBlock(asyncs, asyncIds);
            }
        });
    }

    @Test
    public void keysAllOverConflictingWithRange()
    {
        var tbl = reverseTokenTbl;
        Ranges wholeRange = Ranges.of(fullRange(tbl.id));
        FullRangeRoute rangeRoute = wholeRange.toRoute(wholeRange.get(0).end());
        Txn rangeTxn = createTxn(Txn.Kind.ExclusiveSyncPoint, wholeRange);
        int numSamples = 300;

        qt().withExamples(10).check(rs -> {
            clearSystemTables();
            try (var instance = new SimulatedAccordCommandStore(rs))
            {
                Map<Key, List<TxnId>> keyConflicts = new HashMap<>();
                List<TxnId> rangeConflicts = new ArrayList<>(numSamples);
                boolean concurrent = rs.nextBoolean();
                List<AsyncResult<?>> asyncs = !concurrent ? null : new ArrayList<>(numSamples * 2);
                for (int i = 0; i < numSamples; i++)
                {
                    long token = rs.nextLong(Long.MIN_VALUE  + 1, Long.MAX_VALUE);
                    Key key = new PartitionKey(tbl.id, tbl.partitioner.decorateKey(LongToken.keyForToken(token)));
                    Txn keyTxn = createTxn(wrapInTxn("INSERT INTO " + tbl + "(pk, value) VALUES (?, ?)"),
                                           Arrays.asList(LongToken.keyForToken(token), 42));
                    Keys keys = (Keys) keyTxn.keys();
                    FullRoute<?> keyRoute = keys.toRoute(keys.get(0).toUnseekable());

                    instance.maybeCacheEvict((Keys) keyTxn.keys(), wholeRange);

                    if (concurrent)
                    {
                        var k = assertPreAcceptAsync(instance, keyTxn, keyRoute, Map.of(key, keyConflicts.computeIfAbsent(key, ignore -> new ArrayList<>())), Collections.emptyMap());
                        keyConflicts.get(key).add(k.left);
                        asyncs.add(k.right);

                        var r = assertPreAcceptAsync(instance, rangeTxn, rangeRoute, keyConflicts, rangeConflicts(rangeConflicts, wholeRange));
                        rangeConflicts.add(r.left);
                        asyncs.add(r.right);
                    }
                    else
                    {
                        var k = assertPreAccept(instance, keyTxn, keyRoute, Map.of(key, keyConflicts.computeIfAbsent(key, ignore -> new ArrayList<>())), Collections.emptyMap());
                        keyConflicts.get(key).add(k);
                        rangeConflicts.add(assertPreAccept(instance, rangeTxn, rangeRoute, keyConflicts, rangeConflicts(rangeConflicts, wholeRange)));
                    }
                }
                if (concurrent)
                {
                    instance.processAll();
                    safeBlock(asyncs);
                }
            }
        });
    }

    @Test
    public void simpleRangeConflicts()
    {
        var tbl = reverseTokenTbl;
        Ranges wholeRange = Ranges.of(fullRange(tbl.id));
        int numSamples = 100;

        qt().withExamples(10).check(rs -> {
            clearSystemTables();
            try (var instance = new SimulatedAccordCommandStore(rs))
            {
                long token = rs.nextLong(Long.MIN_VALUE  + 1, Long.MAX_VALUE);
                ByteBuffer key = LongToken.keyForToken(token);
                PartitionKey pk = new PartitionKey(tbl.id, tbl.partitioner.decorateKey(key));
                Keys keys = Keys.of(pk);
                FullKeyRoute keyRoute = keys.toRoute(pk.toUnseekable());
                Txn keyTxn = createTxn(wrapInTxn("INSERT INTO " + tbl + "(pk, value) VALUES (?, ?)"), Arrays.asList(key, 42));

                Ranges partialRange = Ranges.of(tokenRange(tbl.id, token - 1, token));
                boolean useWholeRange = rs.nextBoolean();
                Ranges ranges = useWholeRange ? wholeRange : partialRange;
                FullRangeRoute rangeRoute = ranges.toRoute(pk.toUnseekable());
                Txn rangeTxn = createTxn(Txn.Kind.ExclusiveSyncPoint, ranges);

                List<TxnId> keyConflicts = new ArrayList<>(numSamples);
                List<TxnId> rangeConflicts = new ArrayList<>(numSamples);
                boolean concurrent = rs.nextBoolean();
                List<AsyncResult<?>> asyncs = !concurrent ? null : new ArrayList<>(numSamples * 2);
                for (int i = 0; i < numSamples; i++)
                {
                    instance.maybeCacheEvict(keys, ranges);
                    if (concurrent)
                    {
                        var k = assertPreAcceptAsync(instance, keyTxn, keyRoute, keyConflicts(keyConflicts, keys));
                        keyConflicts.add(k.left);
                        asyncs.add(k.right);
                        var r = assertPreAcceptAsync(instance, rangeTxn, rangeRoute, keyConflicts(keyConflicts, keys), rangeConflicts(rangeConflicts, ranges));
                        rangeConflicts.add(r.left);
                        asyncs.add(r.right);
                    }
                    else
                    {
                        keyConflicts.add(assertPreAccept(instance, keyTxn, keyRoute, keyConflicts(keyConflicts, keys)));
                        rangeConflicts.add(assertPreAccept(instance, rangeTxn, rangeRoute, keyConflicts(keyConflicts, keys), rangeConflicts(rangeConflicts, ranges)));
                    }
                }
                if (concurrent)
                {
                    instance.processAll();
                    safeBlock(asyncs);
                }
            }
        });
    }

    @Test
    public void expandingRangeConflicts()
    {
        var tbl = reverseTokenTbl;
        int numSamples = 100;

        qt().withExamples(10).check(rs -> {
            clearSystemTables();
            try (var instance = new SimulatedAccordCommandStore(rs))
            {
                long token = rs.nextLong(Long.MIN_VALUE + numSamples + 1, Long.MAX_VALUE - numSamples);
                ByteBuffer key = LongToken.keyForToken(token);
                PartitionKey pk = new PartitionKey(tbl.id, tbl.partitioner.decorateKey(key));
                Keys keys = Keys.of(pk);
                FullKeyRoute keyRoute = keys.toRoute(pk.toUnseekable());
                Txn keyTxn = createTxn(wrapInTxn("INSERT INTO " + tbl + "(pk, value) VALUES (?, ?)"), Arrays.asList(key, 42));

                List<TxnId> keyConflicts = new ArrayList<>(numSamples);
                Map<Range, List<TxnId>> rangeConflicts = new HashMap<>();
                boolean concurrent = rs.nextBoolean();
                List<AsyncResult<?>> asyncs = !concurrent ? null : new ArrayList<>(numSamples);
                for (int i = 0; i < numSamples; i++)
                {
                    Ranges partialRange = Ranges.of(tokenRange(tbl.id, token - i - 1, token + i));
                    FullRangeRoute rangeRoute = partialRange.toRoute(pk.toUnseekable());
                    Txn rangeTxn = createTxn(Txn.Kind.ExclusiveSyncPoint, partialRange);
                    try
                    {
                        instance.maybeCacheEvict(keys, partialRange);
                        if (concurrent)
                        {
                            var pair = assertPreAcceptAsync(instance, keyTxn, keyRoute, keyConflicts(keyConflicts, keys));
                            keyConflicts.add(pair.left);
                            asyncs.add(pair.right);

                            pair = assertPreAcceptAsync(instance, rangeTxn, rangeRoute, keyConflicts(keyConflicts, keys), rangeConflicts);
                            rangeConflicts.put(partialRange.get(0), Collections.singletonList(pair.left));
                            asyncs.add(pair.right);
                        }
                        else
                        {
                            keyConflicts.add(assertPreAccept(instance, keyTxn, keyRoute, keyConflicts(keyConflicts, keys)));
                            rangeConflicts.put(partialRange.get(0), Collections.singletonList(assertPreAccept(instance, rangeTxn, rangeRoute, keyConflicts(keyConflicts, keys), rangeConflicts)));
                        }
                    }
                    catch (Throwable t)
                    {
                        AssertionError error = new AssertionError("Unexpected error: i=" + i + ", token=" + token + ", range=" + partialRange.get(0));
                        t.addSuppressed(error);
                        throw t;
                    }
                }
                if (concurrent)
                {
                    instance.processAll();
                    safeBlock(asyncs);
                }
            }
        });
    }

    @Test
    public void overlappingRangeConflicts()
    {
        var tbl = reverseTokenTbl;
        int numSamples = 100;

        qt().withExamples(10).check(rs -> {
            clearSystemTables();
            try (var instance = new SimulatedAccordCommandStore(rs))
            {
                long token = rs.nextLong(Long.MIN_VALUE + numSamples + 1, Long.MAX_VALUE - numSamples);
                ByteBuffer key = LongToken.keyForToken(token);
                PartitionKey pk = new PartitionKey(tbl.id, tbl.partitioner.decorateKey(key));
                Keys keys = Keys.of(pk);
                FullKeyRoute keyRoute = keys.toRoute(pk.toUnseekable());
                Txn keyTxn = createTxn(wrapInTxn("INSERT INTO " + tbl + "(pk, value) VALUES (?, ?)"), Arrays.asList(key, 42));

                Range left = tokenRange(tbl.id, token - 10, token + 5);
                Range right = tokenRange(tbl.id, token - 5, token + 10);

                List<TxnId> keyConflicts = new ArrayList<>(numSamples);
                Map<Range, List<TxnId>> rangeConflicts = new HashMap<>();
                rangeConflicts.put(left, new ArrayList<>());
                rangeConflicts.put(right, new ArrayList<>());
                for (int i = 0; i < numSamples; i++)
                {
                    Ranges partialRange = Ranges.of(rs.nextBoolean() ? left : right);
                    try
                    {
                        instance.maybeCacheEvict(keys, partialRange);
                        keyConflicts.add(assertPreAccept(instance, keyTxn, keyRoute, keyConflicts(keyConflicts, keys)));

                        FullRangeRoute rangeRoute = partialRange.toRoute(pk.toUnseekable());
                        Txn rangeTxn = createTxn(Txn.Kind.ExclusiveSyncPoint, partialRange);
                        rangeConflicts.get(partialRange.get(0)).add(assertPreAccept(instance, rangeTxn, rangeRoute, keyConflicts(keyConflicts, keys), rangeConflicts));
                    }
                    catch (Throwable t)
                    {
                        AssertionError error = new AssertionError("Unexpected error: i=" + i + ", token=" + token + ", range=" + partialRange.get(0));
                        t.addSuppressed(error);
                        throw t;
                    }
                }
            }
        });
    }

    private static void safeBlock(List<AsyncResult<?>> asyncs) throws InterruptedException, ExecutionException
    {
        int counter = 0;
        for (var chain : asyncs)
        {
            Assertions.assertThat(chain.isDone())
                      .describedAs("The %dth async task is blocked!", counter++)
                      .isTrue();
            AsyncChains.getBlocking(chain);
        }
    }

    private static void safeBlock(List<AsyncResult<?>> asyncs, List<?> details) throws InterruptedException, ExecutionException
    {
        int counter = 0;
        for (var chain : asyncs)
        {
            Assertions.assertThat(chain.isDone())
                      .describedAs("The %dth async task %s is blocked!", counter, details.get(counter++))
                      .isTrue();
            AsyncChains.getBlocking(chain);
        }
    }

    private static TokenRange fullRange(TableId id)
    {
        return new TokenRange(AccordRoutingKey.SentinelKey.min(id), AccordRoutingKey.SentinelKey.max(id));
    }

    private static TokenRange tokenRange(TableId id, long start, long end)
    {
        return new TokenRange(start == Long.MIN_VALUE ? AccordRoutingKey.SentinelKey.min(id) : tokenKey(id, start), tokenKey(id, end));
    }

    private static AccordRoutingKey.TokenKey tokenKey(TableId id, long token)
    {
        return new AccordRoutingKey.TokenKey(id, new LongToken(token));
    }

    private static <K> Map<K, List<TxnId>> keyConflicts(Map<K, List<TxnId>> conflicts, Map<K, Integer> keySizes)
    {
        Map<K, List<TxnId>> kc = Maps.newHashMapWithExpectedSize(conflicts.size());
        for (Map.Entry<K, List<TxnId>> e : conflicts.entrySet())
        {
            if (!keySizes.containsKey(e.getKey()))
                continue;
            int size = keySizes.get(e.getKey());
            if (size == 0)
                continue;
            kc.put(e.getKey(), e.getValue().subList(0, size));
        }
        return kc;
    }

    private static Map<Key, List<TxnId>> keyConflicts(List<TxnId> list, Keys keys)
    {
        Map<Key, List<TxnId>> kc = Maps.newHashMapWithExpectedSize(keys.size());
        for (Key key : keys)
            kc.put(key, list);
        return kc;
    }

    private static Map<Range, List<TxnId>> rangeConflicts(List<TxnId> list, Ranges ranges)
    {
        Map<Range, List<TxnId>> kc = Maps.newHashMapWithExpectedSize(ranges.size());
        for (Range range : ranges)
            kc.put(range, list);
        return kc;
    }

    private static TxnId assertPreAccept(SimulatedAccordCommandStore instance,
                                         Txn txn, FullRoute<?> route,
                                         Map<Key, List<TxnId>> keyConflicts) throws ExecutionException, InterruptedException
    {
        return assertPreAccept(instance, txn, route, keyConflicts, Collections.emptyMap());
    }

    private static TxnId assertPreAccept(SimulatedAccordCommandStore instance,
                                         Txn txn, FullRoute<?> route,
                                         Map<Key, List<TxnId>> keyConflicts,
                                         Map<Range, List<TxnId>> rangeConflicts) throws ExecutionException, InterruptedException
    {
        var pair = assertPreAcceptAsync(instance, txn, route, keyConflicts, rangeConflicts);
        instance.processAll();
        AsyncChains.getBlocking(pair.right);

        return pair.left;
    }

    private static Pair<TxnId, AsyncResult<?>> assertPreAcceptAsync(SimulatedAccordCommandStore instance,
                                                                    Txn txn, FullRoute<?> route,
                                                                    Map<Key, List<TxnId>> keyConflicts)
    {
        return assertPreAcceptAsync(instance, txn, route, keyConflicts, Collections.emptyMap());
    }

    private static Pair<TxnId, AsyncResult<?>> assertPreAcceptAsync(SimulatedAccordCommandStore instance,
                                                                    Txn txn, FullRoute<?> route,
                                                                    Map<Key, List<TxnId>> keyConflicts,
                                                                    Map<Range, List<TxnId>> rangeConflicts)
    {
        Map<Key, Integer> keySizes = Maps.newHashMapWithExpectedSize(keyConflicts.size());
        for (Map.Entry<Key, List<TxnId>> e : keyConflicts.entrySet())
            keySizes.put(e.getKey(), e.getValue().size());
        Map<Range, Integer> rangeSizes = Maps.newHashMapWithExpectedSize(rangeConflicts.size());
        for (Map.Entry<Range, List<TxnId>> e : rangeConflicts.entrySet())
            rangeSizes.put(e.getKey(), e.getValue().size());
        var pair = enqueuePreAccept(instance, txn, route);
        return Pair.create(pair.left, pair.right.map(success -> {
            assertDeps(keyConflicts(keyConflicts, keySizes), keyConflicts(rangeConflicts, rangeSizes), success);
            return null;
        }).beginAsResult());
    }

    private static Pair<TxnId, AsyncResult<PreAccept.PreAcceptOk>> enqueuePreAccept(SimulatedAccordCommandStore instance, Txn txn, FullRoute<?> route)
    {
        TxnId txnId = new TxnId(instance.timeService.epoch(), instance.timeService.now(), txn.kind(), txn.keys().domain(), nodeId);
        var preAccept = new PreAccept(instance.nodeId, new Topologies.Single(SizeOfIntersectionSorter.SUPPLIER, instance.topology), txnId, txn, route);
        return Pair.create(txnId, instance.processAsync(preAccept, safe -> {
            var reply = preAccept.apply(safe);
            Assertions.assertThat(reply.isOk()).isTrue();
            return (PreAccept.PreAcceptOk) reply;
        }));
    }

    private static void assertDeps(Map<Key, List<TxnId>> keyConflicts,
                                   Map<Range, List<TxnId>> rangeConflicts,
                                   PreAccept.PreAcceptOk success)
    {
        if (rangeConflicts.isEmpty())
        {
            Assertions.assertThat(success.deps.rangeDeps.isEmpty()).describedAs("Txn %s rangeDeps was not empty", success.txnId).isTrue();
        }
        else
        {
            Assertions.assertThat(success.deps.rangeDeps.rangeCount()).describedAs("Txn %s Expected ranges size", success.txnId).isEqualTo(rangeConflicts.size());
            AssertionError errors = null;
            for (int i = 0; i < rangeConflicts.size(); i++)
            {
                try
                {
                    var range = success.deps.rangeDeps.range(i);
                    Assertions.assertThat(rangeConflicts).describedAs("Txn %s had an unexpected range", success.txnId).containsKey(range);
                    var conflict = success.deps.rangeDeps.txnIdsForRangeIndex(i);
                    List<TxnId> expectedConflict = rangeConflicts.get(range);
                    Assertions.assertThat(conflict).describedAs("Txn %s Expected range %s to have different conflicting txns", success.txnId, range).isEqualTo(expectedConflict);
                }
                catch (AssertionError e)
                {
                    if (errors == null)
                        errors = e;
                    else
                        errors.addSuppressed(e);
                }
            }
            if (errors != null)
                throw errors;
        }
        if (keyConflicts.isEmpty())
        {
            Assertions.assertThat(success.deps.keyDeps.isEmpty()).describedAs("Txn %s keyDeps was not empty", success.txnId).isTrue();
        }
        else
        {
            Assertions.assertThat(success.deps.keyDeps.keys()).describedAs("Txn %s Keys", success.txnId).isEqualTo(Keys.of(keyConflicts.keySet()));
            for (var key : keyConflicts.keySet())
                Assertions.assertThat(success.deps.keyDeps.txnIds(key)).describedAs("Txn %s for key %s", success.txnId, key).isEqualTo(keyConflicts.get(key));
        }
    }

    private static void clearSystemTables()
    {
        for (var store : Keyspace.open(SchemaConstants.ACCORD_KEYSPACE_NAME).getColumnFamilyStores())
            store.truncateBlockingWithoutSnapshot();
    }
}

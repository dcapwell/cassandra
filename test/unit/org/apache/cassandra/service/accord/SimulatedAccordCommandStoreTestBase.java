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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.google.common.collect.Maps;
import org.junit.Before;
import org.junit.BeforeClass;

import accord.api.Key;
import accord.local.Node;
import accord.messages.PreAccept;
import accord.primitives.FullRoute;
import accord.primitives.Keys;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.transformations.AddAccordTable;
import org.apache.cassandra.utils.Pair;
import org.assertj.core.api.Assertions;

public abstract class SimulatedAccordCommandStoreTestBase extends CQLTester
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

    protected static TableMetadata intTbl, reverseTokenTbl;
    protected static Node.Id nodeId;

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

    protected static void safeBlock(List<AsyncResult<?>> asyncs) throws InterruptedException, ExecutionException
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

    protected static void safeBlock(List<AsyncResult<?>> asyncs, List<?> details) throws InterruptedException, ExecutionException
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

    protected static TokenRange fullRange(TableId id)
    {
        return new TokenRange(AccordRoutingKey.SentinelKey.min(id), AccordRoutingKey.SentinelKey.max(id));
    }

    protected static TokenRange tokenRange(TableId id, long start, long end)
    {
        return new TokenRange(start == Long.MIN_VALUE ? AccordRoutingKey.SentinelKey.min(id) : tokenKey(id, start), tokenKey(id, end));
    }

    protected static AccordRoutingKey.TokenKey tokenKey(TableId id, long token)
    {
        return new AccordRoutingKey.TokenKey(id, new Murmur3Partitioner.LongToken(token));
    }

    protected static <K> Map<K, List<TxnId>> keyConflicts(Map<K, List<TxnId>> conflicts, Map<K, Integer> keySizes)
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

    protected static Map<Key, List<TxnId>> keyConflicts(List<TxnId> list, Keys keys)
    {
        Map<Key, List<TxnId>> kc = Maps.newHashMapWithExpectedSize(keys.size());
        for (Key key : keys)
            kc.put(key, list);
        return kc;
    }

    protected static Map<Range, List<TxnId>> rangeConflicts(List<TxnId> list, Ranges ranges)
    {
        Map<Range, List<TxnId>> kc = Maps.newHashMapWithExpectedSize(ranges.size());
        for (Range range : ranges)
            kc.put(range, list);
        return kc;
    }

    protected static TxnId assertPreAccept(SimulatedAccordCommandStore instance,
                                           Txn txn, FullRoute<?> route,
                                           Map<Key, List<TxnId>> keyConflicts) throws ExecutionException, InterruptedException
    {
        return assertPreAccept(instance, txn, route, keyConflicts, Collections.emptyMap());
    }

    protected static TxnId assertPreAccept(SimulatedAccordCommandStore instance,
                                           Txn txn, FullRoute<?> route,
                                           Map<Key, List<TxnId>> keyConflicts,
                                           Map<Range, List<TxnId>> rangeConflicts) throws ExecutionException, InterruptedException
    {
        var pair = assertPreAcceptAsync(instance, txn, route, keyConflicts, rangeConflicts);
        instance.processAll();
        AsyncChains.getBlocking(pair.right);

        return pair.left;
    }

    protected static Pair<TxnId, AsyncResult<?>> assertPreAcceptAsync(SimulatedAccordCommandStore instance,
                                                                      Txn txn, FullRoute<?> route,
                                                                      Map<Key, List<TxnId>> keyConflicts)
    {
        return assertPreAcceptAsync(instance, txn, route, keyConflicts, Collections.emptyMap());
    }

    protected static Pair<TxnId, AsyncResult<?>> assertPreAcceptAsync(SimulatedAccordCommandStore instance,
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
        var pair = instance.enqueuePreAccept(txn, route);
        return Pair.create(pair.left, pair.right.map(success -> {
            assertDeps(keyConflicts(keyConflicts, keySizes), keyConflicts(rangeConflicts, rangeSizes), success);
            return null;
        }).beginAsResult());
    }

    protected static void assertDeps(Map<Key, List<TxnId>> keyConflicts,
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

    protected static void clearSystemTables()
    {
        for (var store : Keyspace.open(SchemaConstants.ACCORD_KEYSPACE_NAME).getColumnFamilyStores())
            store.truncateBlockingWithoutSnapshot();
    }
}
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
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.junit.Test;

import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.CommonAttributes;
import accord.local.Node;
import accord.local.SaveStatus;
import accord.local.Status;
import accord.messages.Commit;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.KeyDeps;
import accord.primitives.Keys;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.RangeDeps;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.Observable;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaProvider;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CassandraGenerators;
import org.assertj.core.api.Assertions;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import static accord.utils.Property.qt;
import static org.apache.cassandra.service.accord.AccordTestUtils.createTxn;
import static org.apache.cassandra.utils.AbstractTypeGenerators.getTypeSupport;
import static org.apache.cassandra.utils.AccordGenerators.fromQT;

public class AccordKeyspaceTest extends CQLTester.InMemory
{
    static
    {
        // since this test does frequent truncates, the info table gets updated and forced flushed... which is 90% of the cost of this test...
        // this flag disables that flush
        CassandraRelevantProperties.UNSAFE_SYSTEM.setBoolean(true);
    }

    @Test
    public void serde()
    {
        AtomicLong now = new AtomicLong();

        String tableName = createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c))");
        TableId tableId = Schema.instance.getTableMetadata(KEYSPACE, tableName).id;
        Ranges scope = Ranges.of(new TokenRange(AccordRoutingKey.SentinelKey.min(tableId), AccordRoutingKey.SentinelKey.max(tableId)));

        AccordCommandStore store = AccordTestUtils.createAccordCommandStore(now::incrementAndGet, KEYSPACE, tableName);

        TxnId id = new TxnId(Timestamp.fromValues(1, 42, new Node.Id(1)), Txn.Kind.Read, Routable.Domain.Key);

        Txn txn = createTxn(wrapInTxn(String.format("SELECT * FROM %s.%s WHERE k=? LIMIT 1", KEYSPACE, tableName)), Collections.singletonList(42));

        PartialTxn partialTxn = txn.slice(scope, true);
        RoutingKey routingKey = partialTxn.keys().get(0).asKey().toUnseekable();
        FullRoute<?> route = partialTxn.keys().toRoute(routingKey);
        Deps deps = new Deps(KeyDeps.none((Keys) txn.keys()), RangeDeps.NONE);
        PartialDeps partialDeps = deps.slice(scope);


        CommonAttributes.Mutable common = new CommonAttributes.Mutable(id);
        common.partialTxn(partialTxn);
        common.route(route);
        common.partialDeps(deps.slice(scope));
        common.durability(Status.Durability.NotDurable);
        Command.WaitingOn waitingOn = null;

        Command.Committed committed = Command.SerializerSupport.committed(common, SaveStatus.Committed, id, Ballot.ZERO, Ballot.ZERO, waitingOn);
        AccordSafeCommand safeCommand = new AccordSafeCommand(AccordTestUtils.loaded(id, null));
        safeCommand.set(committed);

        Commit commit = Commit.SerializerSupport.create(id, route.slice(scope), 1, Commit.Kind.StableFastPath, Ballot.ZERO, id, partialTxn.keys(), partialTxn, partialDeps, route, null);
        store.appendToJournal(commit);

        Mutation mutation = AccordKeyspace.getCommandMutation(store, safeCommand, 42);
        mutation.apply();

        Command loaded = AccordKeyspace.loadCommand(store, id);
        Assertions.assertThat(loaded).isEqualTo(committed);
    }

    @Test
    public void name()
    {
    }

    @Test
    public void findOverlappingKeys()
    {
        class Key implements Comparable<Key>
        {
            private final int storeId;
            private final PartitionKey key;

            Key(int storeId, PartitionKey key)
            {
                this.storeId = storeId;
                this.key = key;
            }

            @Override
            public boolean equals(Object o)
            {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                Key key1 = (Key) o;
                return storeId == key1.storeId && Objects.equals(key, key1.key);
            }

            @Override
            public int hashCode()
            {
                return Objects.hash(storeId, key);
            }

            @Override
            public int compareTo(Key o)
            {
                int rc = Integer.compare(storeId, o.storeId);
                if (rc == 0)
                    rc = key.compareTo(o.key);
                return rc;
            }

            @Override
            public String toString()
            {
                return "Key{" +
                       "storeId=" + storeId +
                       ", key=" + key +
                       '}';
            }
        }
        var tableIdGen = fromQT(CassandraGenerators.TABLE_ID_GEN);
        var partitionGen = fromQT(CassandraGenerators.partitioners());
        qt().withSeed(5157752473231082120L).check(rs -> {
            clearSystemTables();
            TreeMap<TableId, IPartitioner> tables = new TreeMap<>();
            int numTables = rs.nextInt(1, 3);
            for (int i = 0; i < numTables; i++)
            {
                var tableId = tableIdGen.next(rs);
                while (tables.containsKey(tableId))
                    tableId = tableIdGen.next(rs);
                tables.put(tableId, partitionGen.next(rs));
            }
            AccordKeyspace.schema = Mockito.mock(SchemaProvider.class);
            Mockito.when(AccordKeyspace.schema.getTablePartitioner(Mockito.any())).thenAnswer((Answer<IPartitioner>) invocationOnMock -> tables.get(invocationOnMock.getArgument(0)));
            int numStores = rs.nextInt(1, 3);
            SortedSet<Key> keys = new TreeSet<>();
            for (int i = 0, numKeys = rs.nextInt(10, 20); i < numKeys; i++)
            {
                Key key;
                do
                {
                    int store = rs.nextInt(0, numStores);
                    TableId tableId = rs.pick(tables.keySet());
                    IPartitioner partitioner = tables.get(tableId);
                    ByteBuffer data = !(partitioner instanceof LocalPartitioner) ? Int32Type.instance.decompose(rs.nextInt())
                                      : fromQT(getTypeSupport(partitioner.getTokenValidator()).bytesGen()).next(rs);
                    PartitionKey pk = new PartitionKey(tableId, tables.get(tableId).decorateKey(data));
                    key = new Key(store, pk);
                }
                while (!keys.add(key));
                new Mutation(AccordKeyspace.getCommandsForKeyPartitionUpdate(key.storeId, key.key, 42, ByteBufferUtil.EMPTY_BYTE_BUFFER)).apply();
            }
            TreeMap<Integer, Integer> storeSizes = new TreeMap<>();
            for (var k : keys)
                storeSizes.compute(k.storeId, (ignore, accum) -> accum == null ? 1 : accum++);
            for (int i = 0, queries = rs.nextInt(1, 5); i < queries; i++)
            {
                int store = rs.pick(storeSizes.keySet());
                int finalStore1 = store;
                var keysForStore = keys.stream()
                                       .filter(k -> k.storeId == finalStore1)
                                       .map(k -> k.key)
                                       .collect(Collectors.toList());

                int offset;
                int offsetEnd;
                if (keysForStore.size() == 1)
                {
                    offset = 0;
                    offsetEnd = 1;
                }
                else
                {
                    offset = rs.nextInt(0, keysForStore.size());
                    offsetEnd = rs.nextInt(offset, keysForStore.size()) + 1;
                }
                List<PartitionKey> subset = keysForStore.subList(offset, offsetEnd);
                PartitionKey start = subset.get(0);
                PartitionKey end = subset.get(subset.size() - 1);

                int finalStore = store;
                AsyncChain<List<PartitionKey>> map = Observable.asChain(callback -> AccordKeyspace.findAllKeysBetween(finalStore, start.toUnseekable(), true, end.toUnseekable(), true, callback));
                List<PartitionKey> actual = AsyncChains.getUnchecked(map);
                Assertions.assertThat(actual).isEqualTo(subset);
            }
        });
    }

    protected static void clearSystemTables()
    {
        //TODO (testing, maintaince): move this to AccordKeyspace?  Now that there is the serializes to clean up that will be an issue...
        for (var store : Keyspace.open(SchemaConstants.ACCORD_KEYSPACE_NAME).getColumnFamilyStores())
            store.truncateBlockingWithoutSnapshot();

        AccordKeyspace.TABLE_SERIALIZERS.clear();
        AccordKeyspace.schema = Schema.instance;
    }
}
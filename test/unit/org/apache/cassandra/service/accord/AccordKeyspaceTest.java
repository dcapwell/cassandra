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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
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
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.schema.MemtableParams;
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

        // make sure blob is always the same
        CassandraRelevantProperties.TEST_BLOB_SHARED_SEED.setInt(42);
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
        var tableIdGen = fromQT(CassandraGenerators.TABLE_ID_GEN);
        var partitionGen = fromQT(CassandraGenerators.partitioners());

        var sstableFormats = DatabaseDescriptor.getSSTableFormats();
        List<String> sstableFormatNames = new ArrayList<>(sstableFormats.keySet());
        sstableFormatNames.sort(Comparator.naturalOrder());

        List<String> memtableFormats = MemtableParams.knownDefinitions().stream()
                                                     .filter(name -> !name.startsWith("test_") && !name.equals("default"))
                                                     .sorted()
                                                     .collect(Collectors.toList());

        qt().withSeed(-3892767246556534332L).check(rs -> {
            clearSystemTables();
            // control SSTable format
            DatabaseDescriptor.setSelectedSSTableFormat(sstableFormats.get("bti"));
            // control memtable format
            Keyspace.open("system_accord").initCf(Schema.instance.getTableMetadata("system_accord", "commands_for_key").unbuild()
                                                                 .memtable(MemtableParams.get("trie"))
                                                                 .build(), false);

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
            TreeMap<Integer, SortedSet<PartitionKey>> storesToKeys = new TreeMap<>();
            for (int i = 0, numKeys = rs.nextInt(10, 20); i < numKeys; i++)
            {
                int store = rs.nextInt(0, numStores);
                var keys = storesToKeys.computeIfAbsent(store, ignore -> new TreeSet<>());
                PartitionKey pk = null;
                // LocalPartitioner may have a type with a very small domain (boolean, vector<boolean, 1>, etc.), so need to bound the attempts
                // else this will loop forever...
                for (int attempt = 0; attempt < 10; attempt++)
                {
                    TableId tableId = rs.pick(tables.keySet());
                    IPartitioner partitioner = tables.get(tableId);
                    ByteBuffer data = !(partitioner instanceof LocalPartitioner) ? Int32Type.instance.decompose(rs.nextInt())
                                                                                 : fromQT(getTypeSupport(partitioner.getTokenValidator()).bytesGen()).next(rs);
                    PartitionKey key = new PartitionKey(tableId, tables.get(tableId).decorateKey(data));
                    if (keys.add(key))
                    {
                        pk = key;
                        break;
                    }
                }
                if (pk != null)
                {
                    try
                    {
                        new Mutation(AccordKeyspace.getCommandsForKeyPartitionUpdate(store, pk, 42, ByteBufferUtil.EMPTY_BYTE_BUFFER)).apply();
                    }
                    catch (IllegalArgumentException e)
                    {
                        // Sometimes the types are too large (LocalPartitioner) so the mutation gets rejected... just ignore those cases
                        // Length 69912 > max length 65535
                        String msg = e.getMessage();
                        if (msg != null && msg.startsWith("Length ") && msg.endsWith("> max length 65535"))
                        {
                            // failed to add
                            keys.remove(pk);
                            continue;
                        }
                        throw e;
                    }
                }
            }
            Keyspace.open("system_accord").getColumnFamilyStore("commands_for_key").forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

            TreeMap<Integer, SortedSet<ByteBuffer>> expectedCqlStoresToKeys = new TreeMap<>();
            for (var e : storesToKeys.entrySet())
            {
                int store = e.getKey();
                expectedCqlStoresToKeys.put(store, new TreeSet<>(e.getValue().stream().map(p -> AccordKeyspace.serializeRoutingKey(p.toUnseekable())).collect(Collectors.toList())));
            }

            // make sure no data loss... when this test was written sstable had all the rows but the sstable didn't... this
            // is mostly a santity check to detect that case early
            var resultSet = execute("SELECT store_id, key_token FROM system_accord.commands_for_key ALLOW FILTERING");
            TreeMap<Integer, SortedSet<ByteBuffer>> cqlStoresToKeys = new TreeMap<>();
            for (var row : resultSet)
            {
                int storeId = row.getInt("store_id");
                ByteBuffer bb = row.getBytes("key_token");
                cqlStoresToKeys.computeIfAbsent(storeId, ignore -> new TreeSet<>()).add(bb);
            }
            Assertions.assertThat(cqlStoresToKeys).isEqualTo(expectedCqlStoresToKeys);

            for (int i = 0, queries = rs.nextInt(1, 5); i < queries; i++)
            {
                int store = rs.pick(storesToKeys.keySet());
                var keysForStore = new ArrayList<>(storesToKeys.get(store));

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
                List<PartitionKey> expected = keysForStore.subList(offset, offsetEnd);
                PartitionKey start = expected.get(0);
                PartitionKey end = expected.get(expected.size() - 1);

                AsyncChain<List<PartitionKey>> map = Observable.asChain(callback -> AccordKeyspace.findAllKeysBetween(store, start.toUnseekable(), true, end.toUnseekable(), true, callback));
                List<PartitionKey> actual = AsyncChains.getUnchecked(map);
                Assertions.assertThat(actual).isEqualTo(expected);
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
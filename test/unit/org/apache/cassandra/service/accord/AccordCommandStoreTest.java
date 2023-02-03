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

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Key;
import accord.api.Result;
import accord.impl.CommandsForKey;
import accord.impl.CommandsForKeys;
import accord.local.Command;
import accord.local.PreLoadContext;
import accord.local.SaveStatus;
import accord.primitives.Ballot;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.utils.Pair;

import static accord.local.Status.Durability.Durable;
import static accord.utils.async.AsyncChains.awaitUninterruptibly;
import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;
import static org.apache.cassandra.service.accord.AccordTestUtils.Commands.preaccepted;
import static org.apache.cassandra.service.accord.AccordTestUtils.ballot;
import static org.apache.cassandra.service.accord.AccordTestUtils.createAccordCommandStore;
import static org.apache.cassandra.service.accord.AccordTestUtils.createPartialTxn;
import static org.apache.cassandra.service.accord.AccordTestUtils.timestamp;
import static org.apache.cassandra.service.accord.AccordTestUtils.txnId;

public class AccordCommandStoreTest
{
    private static final Logger logger = LoggerFactory.getLogger(AccordCommandStoreTest.class);

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1),
                                    parse("CREATE TABLE tbl (k int, c int, v int, primary key (k, c))", "ks"));
        StorageService.instance.initServer();
    }

    @Before
    public void setUp() throws Exception
    {
        Keyspace.open(SchemaConstants.ACCORD_KEYSPACE_NAME).getColumnFamilyStores().forEach(ColumnFamilyStore::truncateBlocking);
    }

    @Test
    public void commandLoadSave() throws Throwable
    {
        AtomicLong clock = new AtomicLong(0);
        PartialTxn depTxn = createPartialTxn(0);
        Key key = (Key)depTxn.keys().get(0);
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");

        PartialDeps dependencies;
        try (PartialDeps.Builder builder = PartialDeps.builder(depTxn.covering()))
        {
            builder.add(key, txnId(1, clock.incrementAndGet(), 1));
            dependencies = builder.build();
        }

        QueryProcessor.executeInternal("INSERT INTO ks.tbl (k, c, v) VALUES (0, 0, 1)");
        TxnId oldTxnId1 = txnId(1, clock.incrementAndGet(), 1);
        TxnId oldTxnId2 = txnId(1, clock.incrementAndGet(), 1);
        TxnId oldTimestamp = txnId(1, clock.incrementAndGet(), 1);
        TxnId txnId = txnId(1, clock.incrementAndGet(), 1);

        AccordTestUtils.CommandAttributes attrs = new AccordTestUtils.CommandAttributes(txnId);
        PartialTxn txn = createPartialTxn(0);
        attrs.homeKey(key.toUnseekable());
        attrs.progressKey(key.toUnseekable());
        attrs.durability(Durable);
        Ballot promised = ballot(1, clock.incrementAndGet(), 1);
        Ballot accepted = ballot(1, clock.incrementAndGet(), 1);
        Timestamp executeAt = timestamp(1, clock.incrementAndGet(), 1);
        attrs.partialDeps(dependencies);
        ImmutableSortedSet<TxnId> waitingOnCommit = ImmutableSortedSet.of(oldTxnId1);
        ImmutableSortedMap<Timestamp, TxnId > waitingOnApply = ImmutableSortedMap.of(oldTimestamp, oldTxnId2);
        attrs.listeners(ImmutableSet.of(Command.listener(oldTxnId1)));
        Pair<Writes, Result> result = AccordTestUtils.processTxnResult(commandStore, txnId, txn, executeAt);
        Command command = Command.SerializerSupport.executed(attrs, SaveStatus.Applied, executeAt, promised, accepted,
                                                             waitingOnCommit, waitingOnApply, result.left, result.right);

        command.markActive();
        AccordKeyspace.getCommandMutation(commandStore, null, command, commandStore.nextSystemTimestampMicros()).apply();
        logger.info("E: {}", command);
        Command actual = AccordKeyspace.loadCommand(commandStore, txnId);
        actual.markActive();
        logger.info("A: {}", actual);

        Assert.assertEquals(command, actual);
    }

    @Test
    public void commandsForKeyLoadSave()
    {
        AtomicLong clock = new AtomicLong(0);
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");
        Timestamp maxTimestamp = timestamp(1, clock.incrementAndGet(), 1);

        PartialTxn txn = createPartialTxn(1);
        PartitionKey key = (PartitionKey) Iterables.getOnlyElement(txn.keys());
        TxnId txnId1 = txnId(1, clock.incrementAndGet(), 1);
        TxnId txnId2 = txnId(1, clock.incrementAndGet(), 1);

        Command command1 = preaccepted(txnId1, txn, timestamp(1, clock.incrementAndGet(), 1));
        command1.markActive();
        Command command2 = preaccepted(txnId2, txn, timestamp(1, clock.incrementAndGet(), 1));
        command2.markActive();

        AtomicReference<CommandsForKey> finalCfk = new AtomicReference<>();
        awaitUninterruptibly(commandStore.execute(PreLoadContext.contextFor(key), safeStore -> {

            CommandsForKey cfk = safeStore.commandsForKey(key);
            CommandsForKey.Update update = safeStore.beginUpdate(cfk);
            update.updateMax(maxTimestamp);
            cfk = update.complete();

            cfk = CommandsForKey.updateLastExecutionTimestamps(cfk, safeStore, txnId1, true);
            Assert.assertEquals(txnId1.hlc(), cfk.timestampMicrosFor(txnId1, true));

            cfk = CommandsForKey.updateLastExecutionTimestamps(cfk, safeStore, txnId2, true);
            Assert.assertEquals(txnId2.hlc(), cfk.timestampMicrosFor(txnId2, true));

            Assert.assertEquals(txnId2, cfk.lastExecutedTimestamp());
            Assert.assertEquals(txnId2.hlc(), cfk.lastExecutedMicros());


            CommandsForKeys.register(safeStore, safeStore.commandsForKey(key), command1);
            CommandsForKeys.register(safeStore, safeStore.commandsForKey(key), command2);
            finalCfk.set(safeStore.commandsForKey(key));
        }));


        CommandsForKey cfk = finalCfk.get();
        cfk.markActive();
        AccordKeyspace.getCommandsForKeyMutation(commandStore, null, cfk, commandStore.nextSystemTimestampMicros()).apply();
        logger.info("E: {}", cfk);
        CommandsForKey actual = AccordKeyspace.loadCommandsForKey(commandStore, key);
        actual.markActive();
        logger.info("A: {}", actual);

        Assert.assertEquals(cfk, actual);
    }
}

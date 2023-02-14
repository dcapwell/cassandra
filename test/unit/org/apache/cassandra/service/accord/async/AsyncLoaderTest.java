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

package org.apache.cassandra.service.accord.async;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.primitives.PartialTxn;
import accord.primitives.RoutableKey;
import accord.primitives.TxnId;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.AccordLiveCommand;
import org.apache.cassandra.service.accord.AccordLiveCommandsForKey;
import org.apache.cassandra.service.accord.AccordStateCache;
import org.apache.cassandra.service.accord.AccordTestUtils.TestableLoad;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.utils.concurrent.AsyncPromise;

import static java.util.Collections.singleton;
import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;
import static org.apache.cassandra.service.accord.AccordTestUtils.Commands.notWitnessed;
import static org.apache.cassandra.service.accord.AccordTestUtils.commandsForKey;
import static org.apache.cassandra.service.accord.AccordTestUtils.createAccordCommandStore;
import static org.apache.cassandra.service.accord.AccordTestUtils.createPartialTxn;
import static org.apache.cassandra.service.accord.AccordTestUtils.execute;
import static org.apache.cassandra.service.accord.AccordTestUtils.liveCommand;
import static org.apache.cassandra.service.accord.AccordTestUtils.liveCommandsForKey;
import static org.apache.cassandra.service.accord.AccordTestUtils.txnId;

public class AsyncLoaderTest
{
    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1),
                                    parse("CREATE TABLE tbl (k int, c int, v int, primary key (k, c))", "ks"));
        StorageService.instance.initServer();
    }

    /**
     * Loading a cached resource shoudln't block
     */
    @Test
    public void cachedTest()
    {
        AtomicLong clock = new AtomicLong(0);
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");
        AccordStateCache.Instance<TxnId, AccordLiveCommand> commandCache = commandStore.commandCache();
        commandStore.executeBlocking(() -> commandStore.setCacheSize(1024));

        AccordStateCache.Instance<RoutableKey, AccordLiveCommandsForKey> cfkCache = commandStore.commandsForKeyCache();
        TxnId txnId = txnId(1, clock.incrementAndGet(), 1);
        PartialTxn txn = createPartialTxn(0);
        PartitionKey key = (PartitionKey) Iterables.getOnlyElement(txn.keys());

        // acquire / release
        commandCache.referenceAndLoad(txnId, (id, consumer) -> {
            consumer.accept(liveCommand(notWitnessed(txnId, txn)));;
            return AsyncResults.success(null);
        });
        AccordLiveCommand command = commandCache.getActive(txnId);
        Assert.assertNotNull(command);
        commandCache.release(txnId, command);

        cfkCache.referenceAndLoad(key, (k, consumer) -> {
            consumer.accept(liveCommandsForKey(commandsForKey(key)));
            return AsyncResults.success(null);
        });
        AccordLiveCommandsForKey cfk = cfkCache.getActive(key);
        Assert.assertNotNull(cfk);
        cfkCache.release(key, cfk);

        AsyncLoader loader = new AsyncLoader(commandStore, singleton(txnId), singleton(key));

        // everything is cached, so the loader should return immediately
        commandStore.executeBlocking(() -> {
            boolean result = loader.load((o, t) -> Assert.fail());
            Assert.assertTrue(result);
        });

        Assert.assertSame(command, commandCache.getActive(txnId));
        Assert.assertSame(cfk, cfkCache.getActive(key));
    }

    /**
     * Loading a cached resource should block
     */
    @Test
    public void loadTest()
    {
        AtomicLong clock = new AtomicLong(0);
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");
        AccordStateCache.Instance<TxnId, AccordLiveCommand> commandCache = commandStore.commandCache();
        AccordStateCache.Instance<RoutableKey, AccordLiveCommandsForKey> cfkCacche = commandStore.commandsForKeyCache();
        TxnId txnId = txnId(1, clock.incrementAndGet(), 1);
        PartialTxn txn = createPartialTxn(0);
        PartitionKey key = (PartitionKey) Iterables.getOnlyElement(txn.keys());

        // create / persist
        AccordLiveCommand liveCommand = liveCommand(txnId);
        liveCommand.set(notWitnessed(txnId, txn));
        AccordKeyspace.getCommandMutation(commandStore, liveCommand, commandStore.nextSystemTimestampMicros()).apply();
        AccordLiveCommandsForKey cfk = liveCommandsForKey(key);
        cfk.set(commandsForKey(key));
        AccordKeyspace.getCommandsForKeyMutation(commandStore, cfk, commandStore.nextSystemTimestampMicros()).apply();

        // resources are on disk only, so the loader should suspend...
        AsyncLoader loader = new AsyncLoader(commandStore, singleton(txnId), singleton(key));
        AsyncPromise<Void> cbFired = new AsyncPromise<>();
        commandStore.executeBlocking(() -> {
            boolean result = loader.load((o, t) -> {
                Assert.assertNull(t);
                cbFired.setSuccess(null);
            });
            Assert.assertFalse(result);
        });

        cbFired.awaitUninterruptibly(1, TimeUnit.SECONDS);

        // then return immediately after the callback has fired
        commandStore.executeBlocking(() -> {
            boolean result = loader.load((o, t) -> Assert.fail());
            Assert.assertTrue(result);
        });
    }

    /**
     * Test when some resources are cached and others need to be loaded
     */
    @Test
    public void partialLoadTest()
    {
        AtomicLong clock = new AtomicLong(0);
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");
        AccordStateCache.Instance<TxnId, AccordLiveCommand> commandCache = commandStore.commandCache();
        AccordStateCache.Instance<RoutableKey, AccordLiveCommandsForKey> cfkCacche = commandStore.commandsForKeyCache();
        TxnId txnId = txnId(1, clock.incrementAndGet(), 1);
        PartialTxn txn = createPartialTxn(0);
        PartitionKey key = (PartitionKey) Iterables.getOnlyElement(txn.keys());

        // acquire /release, create / persist
        commandCache.referenceAndLoad(txnId, (id, consumer) -> {
            consumer.accept(liveCommand(notWitnessed(txnId, txn)));;
            return AsyncResults.success(null);
        });
        AccordLiveCommand command = commandCache.getActive(txnId);
        Assert.assertNotNull(command);
        commandCache.release(txnId, command);

        AccordLiveCommandsForKey cfk = liveCommandsForKey(key);
        cfk.set(commandsForKey(key));
        AccordKeyspace.getCommandsForKeyMutation(commandStore, cfk, commandStore.nextSystemTimestampMicros()).apply();

        // resources are on disk only, so the loader should suspend...
        AsyncLoader loader = new AsyncLoader(commandStore, singleton(txnId), singleton(key));
        AsyncPromise<Void> cbFired = new AsyncPromise<>();
        commandStore.executeBlocking(() -> {
            boolean result = loader.load((o, t) -> {
                Assert.assertNull(t);
                cbFired.setSuccess(null);
            });
            Assert.assertFalse(result);
        });

        cbFired.awaitUninterruptibly(1, TimeUnit.SECONDS);

        // then return immediately after the callback has fired
        commandStore.executeBlocking(() -> {
            boolean result = loader.load((o, t) -> Assert.fail());
            Assert.assertTrue(result);
        });
    }

    /**
     * If another process is loading a resource, piggyback on it's future
     */
    @Test
    public void inProgressLoadTest()
    {
        AtomicLong clock = new AtomicLong(0);
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");
        AccordStateCache.Instance<TxnId, AccordLiveCommand> commandCache = commandStore.commandCache();
        AccordStateCache.Instance<RoutableKey, AccordLiveCommandsForKey> cfkCache = commandStore.commandsForKeyCache();
        TxnId txnId = txnId(1, clock.incrementAndGet(), 1);
        PartialTxn txn = createPartialTxn(0);
        PartitionKey key = (PartitionKey) Iterables.getOnlyElement(txn.keys());

        // acquire / release
        AccordLiveCommand liveCommand = liveCommand(txnId);
        liveCommand.set(notWitnessed(txnId, txn));
        TestableLoad<TxnId, AccordLiveCommand> load = new TestableLoad<>();
        commandCache.referenceAndLoad(txnId, load);
        Assert.assertTrue(commandCache.isReferenced(txnId));
        Assert.assertFalse(commandCache.isLoaded(txnId));

        TestableLoad<RoutableKey, AccordLiveCommandsForKey> cfkLoad = new TestableLoad<>();
        cfkCache.referenceAndLoad(key, cfkLoad);
        AccordLiveCommandsForKey liveCfk = liveCommandsForKey(key);
        liveCfk.set(commandsForKey(key));
        cfkLoad.complete(liveCfk);
        AccordLiveCommandsForKey cfk = cfkCache.getActive(key);
        Assert.assertNotNull(cfk);
        cfkCache.release(key, cfk);

        AsyncLoader loader = new AsyncLoader(commandStore, singleton(txnId), singleton(key));

        // since there's a read future associated with the txnId, we'll wait for it to load
        AsyncPromise<Void> cbFired = new AsyncPromise<>();
        commandStore.executeBlocking(() -> {
            boolean result = loader.load((o, t) -> {
                Assert.assertNull(t);
                cbFired.setSuccess(null);
            });
            Assert.assertFalse(result);
        });

        Assert.assertFalse(cbFired.isSuccess());
        load.complete(liveCommand);
        cbFired.awaitUninterruptibly(1, TimeUnit.SECONDS);
        Assert.assertTrue(cbFired.isSuccess());

        // then return immediately after the callback has fired
        commandStore.executeBlocking(() -> {
            boolean result = loader.load((o, t) -> Assert.fail());
            Assert.assertTrue(result);
        });
    }

    @Test
    public void failedLoadTest() throws Throwable
    {
        AtomicLong clock = new AtomicLong(0);
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");
        TxnId txnId1 = txnId(1, clock.incrementAndGet(), 1);
        TxnId txnId2 = txnId(1, clock.incrementAndGet(), 1);

        AsyncResult.Settable<Void> promise1 = AsyncResults.settable();
        AtomicReference<Consumer<AccordLiveCommand>> consumer1 = new AtomicReference<>();
        AsyncResult.Settable<Void> promise2 = AsyncResults.settable();
        AtomicReference<Consumer<AccordLiveCommand>> consumer2 = new AtomicReference<>();
        AsyncResult.Settable<Void> callback = AsyncResults.settable();
        RuntimeException failure = new RuntimeException();

        execute(commandStore, () -> {
            AtomicInteger loadCalls = new AtomicInteger();
            AsyncLoader loader = new AsyncLoader(commandStore, ImmutableList.of(txnId1, txnId2), Collections.emptyList()){

                @Override
                AccordStateCache.LoadFunction<TxnId, AccordLiveCommand> loadCommandFunction()
                {
                    return (txnId, consumer) -> {
                        loadCalls.incrementAndGet();
                        if (txnId.equals(txnId1))
                        {
                            consumer1.set(consumer);;
                            return promise1;
                        }
                        if (txnId.equals(txnId2))
                        {
                            consumer2.set(consumer);
                            return promise2;
                        }
                        throw new AssertionError("Unknown txnId: " + txnId);
                    };
                }
            };

            boolean result = loader.load((u, t) -> {
                Assert.assertFalse(callback.isDone());
                Assert.assertNull(u);
                Assert.assertEquals(failure, t);
                callback.trySuccess(null);
            });
            Assert.assertFalse(result);
            Assert.assertEquals(2, loadCalls.get());
        });

        promise1.tryFailure(failure);
        AsyncResults.awaitUninterruptibly(callback);
    }
}

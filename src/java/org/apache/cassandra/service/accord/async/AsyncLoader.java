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

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.primitives.RoutableKey;
import accord.primitives.TxnId;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.AccordLiveCommand;
import org.apache.cassandra.service.accord.AccordLiveCommandsForKey;
import org.apache.cassandra.service.accord.AccordLiveState;
import org.apache.cassandra.service.accord.AccordStateCache;
import org.apache.cassandra.service.accord.AccordStateCache.LoadFunction;
import org.apache.cassandra.service.accord.api.PartitionKey;

import static accord.utils.async.AsyncResults.ofRunnable;

public class AsyncLoader
{
    private static final Logger logger = LoggerFactory.getLogger(AsyncLoader.class);
    enum State
    {
        INITIALIZED,
        SETUP,
        LOADING,
        FINISHED
    }

    private State state = State.INITIALIZED;
    private final AccordCommandStore commandStore;

    private final List<TxnId> txnIds;
    private final List<RoutableKey> keys;

    protected AsyncResult<?> readResult;

    public AsyncLoader(AccordCommandStore commandStore, Iterable<TxnId> txnIds, Iterable<RoutableKey> keys)
    {
        this.commandStore = commandStore;
        this.txnIds = Lists.newArrayList(txnIds);
        this.keys = Lists.newArrayList(keys);
    }

    private <K, V extends AccordLiveState<?>> List<AsyncChain<Void>> referenceAndDispatchReads(Iterable<K> keys,
                                                                                               AccordStateCache.Instance<K, V> cache,
                                                                                               LoadFunction<K, V> loadFunction,
                                                                                               List<AsyncChain<Void>> results,
                                                                                               Object callback)
    {
        for (K key : keys)
        {
            AsyncResult<Void> result = cache.referenceAndLoad(key, loadFunction);
            if (result == null)
                continue;

            if (results == null)
                results = new ArrayList<>();

            results.add(result);
        }

        return results;
    }

    @VisibleForTesting
    LoadFunction<TxnId, AccordLiveCommand> loadCommandFunction(Object callback)
    {
        return (txnId, consumer) -> ofRunnable(Stage.READ.executor(), () -> {
            try
            {
                logger.trace("Starting load of {} for {}", txnId, callback);
                AccordLiveCommand command = AccordKeyspace.loadCommand(commandStore, txnId);
                logger.trace("Completed load of {} for {}", txnId, callback);
                consumer.accept(command);
            }
            catch (Throwable t)
            {
                logger.error("Exception loading {} for {}", txnId, callback, t);
                throw t;
            }
        });
    }

    @VisibleForTesting
    LoadFunction<RoutableKey, AccordLiveCommandsForKey> loadCommandsPerKeyFunction(Object callback)
    {
        return (key, consumer) -> ofRunnable(Stage.READ.executor(), () -> {
            try
            {
                logger.trace("Starting load of {} for {}", key, callback);
                AccordLiveCommandsForKey cfk = AccordKeyspace.loadCommandsForKey(commandStore, (PartitionKey) key);
                logger.trace("Completed load of {} for {}", key, callback);
                consumer.accept(cfk);
            }
            catch (Throwable t)
            {
                logger.error("Exception loading {} for {}", key, callback, t);
                throw t;
            }
        });
    }

    private AsyncResult<Void> referenceAndDispatchReads(Object callback)
    {
        List<AsyncChain<Void>> results = null;

        results = referenceAndDispatchReads(txnIds,
                                            commandStore.commandCache(),
                                            loadCommandFunction(callback),
                                            results,
                                            callback);

        results = referenceAndDispatchReads(keys,
                                            commandStore.commandsForKeyCache(),
                                            loadCommandsPerKeyFunction(callback),
                                            results,
                                            callback);

        return results != null ? AsyncResults.reduce(results, (a, b ) -> null).beginAsResult() : null;
    }

    @VisibleForTesting
    void state(State state)
    {
        this.state = state;
    }

    public boolean load(BiConsumer<Object, Throwable> callback)
    {
        logger.trace("Running load for {} with state {}: {} {}", callback, state, txnIds, keys);
        commandStore.checkInStoreThread();
        switch (state)
        {
            case INITIALIZED:
                state(State.SETUP);
            case SETUP:
                readResult = referenceAndDispatchReads(callback);
                state(State.LOADING);
            case LOADING:
                if (readResult != null)
                {
                    if (readResult.isSuccess())
                    {
                        logger.trace("Read result succeeded for {}", callback);
                        readResult = null;
                    }
                    else
                    {
                        logger.trace("Adding callback for read result: {}", callback);
                        readResult.addCallback(callback, commandStore.executor());
                        break;
                    }
                }
                state(State.FINISHED);
            case FINISHED:
                break;
            default:
                throw new IllegalStateException("Unexpected state: " + state);
        }

        logger.trace("Exiting load for {} with state {}: {} {}", callback, state, txnIds, keys);
        return state == State.FINISHED;
    }
}

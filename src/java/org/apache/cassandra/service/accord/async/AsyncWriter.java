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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.primitives.RoutableKey;
import accord.primitives.TxnId;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.AccordLiveCommand;
import org.apache.cassandra.service.accord.AccordLiveCommandsForKey;
import org.apache.cassandra.service.accord.AccordLiveState;
import org.apache.cassandra.service.accord.AccordSafeCommandStore;
import org.apache.cassandra.service.accord.AccordStateCache;

import static accord.utils.async.AsyncResults.ofRunnable;

public class AsyncWriter
{
    private static final Logger logger = LoggerFactory.getLogger(AsyncWriter.class);

    enum State
    {
        INITIALIZED,
        SETUP,
        SAVING,
        FINISHED
    }

    private State state = State.INITIALIZED;
    protected AsyncResult<Void> writeResult;
    private final AccordCommandStore commandStore;
    final AccordStateCache.Instance<TxnId, AccordLiveCommand> commandCache;
    final AccordStateCache.Instance<RoutableKey, AccordLiveCommandsForKey> cfkCache;

    public AsyncWriter(AccordCommandStore commandStore)
    {
        this.commandStore = commandStore;
        this.commandCache = commandStore.commandCache();
        this.cfkCache = commandStore.commandsForKeyCache();
    }

    private interface StateMutationFunction<V extends AccordLiveState<?>>
    {
        Mutation apply(AccordCommandStore commandStore, V value, long timestamp);
    }

    private static <K, V extends AccordLiveState<?>> List<AsyncChain<Void>> dispatchWrites(Map<K, V> values,
                                                                                           AccordStateCache.Instance<K, V> cache,
                                                                                           StateMutationFunction<V> mutationFunction,
                                                                                           long timestamp,
                                                                                           AccordCommandStore commandStore,
                                                                                           List<AsyncChain<Void>> results,
                                                                                           Object callback)
    {
        values.forEach((key, value) -> {
            if (!value.hasUpdate())
                return;
            Mutation mutation = mutationFunction.apply(commandStore, value, timestamp);
            if (logger.isTraceEnabled())
                logger.trace("Dispatching mutation for {} for {}, {} -> {}", key, callback, value.current(), mutation);
            AsyncResult<Void> result = ofRunnable(Stage.MUTATION.executor(), () -> {
                try
                {
                    if (logger.isTraceEnabled())
                        logger.trace("Applying mutation for {} for {}: {}", key, callback, mutation);
                    mutation.apply();
                    if (logger.isTraceEnabled())
                        logger.trace("Completed applying mutation for {} for {}: {}", key, callback, mutation);
                }
                catch (Throwable t)
                {
                    logger.error(String.format("Exception applying mutation for %s for %s: %s", key, callback, mutation), t);
                    throw t;
                }
            });
            cache.addSaveResult(key, result);
            results.add(result);
        });

        return results;
    }

    private AsyncResult<Void> maybeDispatchWrites(AccordSafeCommandStore context, Object callback) throws IOException
    {
        if (context.commands().isEmpty() && context.commandsForKey().isEmpty())
            return null;

        List<AsyncChain<Void>> results = new ArrayList<>(context.commands().size() + context.commandsForKey().size());

        long timestamp = commandStore.nextSystemTimestampMicros();
        results = dispatchWrites(context.commands(),
                                 commandStore.commandCache(),
                                 AccordKeyspace::getCommandMutation,
                                 timestamp,
                                 commandStore,
                                 results,
                                 callback);

        results = dispatchWrites(context.commandsForKey(),
                                 commandStore.commandsForKeyCache(),
                                 AccordKeyspace::getCommandsForKeyMutation,
                                 timestamp,
                                 commandStore,
                                 results,
                                 callback);

        return !results.isEmpty() ? AsyncResults.reduce(results, (a, b) -> null).beginAsResult() : null;
    }

    @VisibleForTesting
    void setState(State state)
    {
        this.state = state;
    }

    public boolean save(AccordSafeCommandStore context, BiConsumer<Object, Throwable> callback)
    {
        logger.trace("Running save for {} with state {}", callback, state);
        commandStore.checkInStoreThread();
        try
        {
            switch (state)
            {
                case INITIALIZED:
                    setState(State.SETUP);
                case SETUP:
                    writeResult = maybeDispatchWrites(context, callback);

                    setState(State.SAVING);
                case SAVING:
                    if (writeResult != null && !writeResult.isSuccess())
                    {
                        logger.trace("Adding callback for write result: {}", callback);
                        writeResult.addCallback(callback, commandStore.executor());
                        break;
                    }
                    context.commands().keySet().forEach(commandStore.commandCache()::cleanupSaveResult);
                    context.commandsForKey().keySet().forEach(commandStore.commandsForKeyCache()::cleanupSaveResult);
                    setState(State.FINISHED);
                case FINISHED:
                    break;
                default:
                    throw new IllegalStateException("Unexpected state: " + state);
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        logger.trace("Exiting save for {} with state {}", callback, state);
        return state == State.FINISHED;
    }

}

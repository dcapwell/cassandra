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
import java.util.function.BiConsumer;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.primitives.RoutableKey;
import accord.primitives.TxnId;
import accord.utils.Invariants;
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

    public interface StateMutationFunction<V extends AccordLiveState<?>>
    {
        Mutation apply(AccordCommandStore commandStore, V value, long timestamp);
    }

    private static <K, V extends AccordLiveState<?>> List<AsyncChain<Void>> dispatchWrites(AsyncContext<K, V> context,
                                                                                           AccordStateCache.Instance<K, V> cache,
                                                                                           StateMutationFunction<V> mutationFunction,
                                                                                           long timestamp,
                                                                                           AccordCommandStore commandStore,
                                                                                           List<AsyncChain<Void>> results)
    {
        context.forEachUpdated((key, value) -> {
            Invariants.checkArgument(value.hasUpdate());
            Mutation mutation = mutationFunction.apply(commandStore, value, timestamp);
            if (logger.isTraceEnabled())
                logger.trace("Dispatching mutation for {}, {} -> {}", key, value.current(), mutation);
            AsyncResult<Void> result = ofRunnable(Stage.MUTATION.executor(), () -> {
                try
                {
                    if (logger.isTraceEnabled())
                        logger.trace("Applying mutation for {}: {}", key, mutation);
                    mutation.apply();
                    if (logger.isTraceEnabled())
                        logger.trace("Completed applying mutation for {}: {}", key, mutation);
                }
                catch (Throwable t)
                {
                    logger.error(String.format("Exception applying mutation for %s: %s", key, mutation), t);
                    throw t;
                }
            });
            cache.addSaveResult(key, result);
            results.add(result);
        });

        return results;
    }

    protected StateMutationFunction<AccordLiveCommand> writeCommandFunction()
    {
        return AccordKeyspace::getCommandMutation;
    }

    protected StateMutationFunction<AccordLiveCommandsForKey> writeCommandForKeysFunction()
    {
        return AccordKeyspace::getCommandsForKeyMutation;
    }

    private AsyncResult<Void> maybeDispatchWrites(AsyncOperation.Context context) throws IOException
    {
        if (context.commands.isEmpty() && context.commandsForKeys.isEmpty())
            return null;

        List<AsyncChain<Void>> results = new ArrayList<>(context.commands.size() + context.commandsForKeys.size());

        long timestamp = commandStore.nextSystemTimestampMicros();
        results = dispatchWrites(context.commands,
                                 commandStore.commandCache(),
                                 writeCommandFunction(),
                                 timestamp,
                                 commandStore,
                                 results);

        results = dispatchWrites(context.commandsForKeys,
                                 commandStore.commandsForKeyCache(),
                                 writeCommandForKeysFunction(),
                                 timestamp,
                                 commandStore,
                                 results);

        return !results.isEmpty() ? AsyncResults.reduce(results, (a, b) -> null).beginAsResult() : null;
    }

    @VisibleForTesting
    void setState(State state)
    {
        this.state = state;
    }

    public boolean save(AsyncOperation.Context context, BiConsumer<Object, Throwable> callback)
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
                    writeResult = maybeDispatchWrites(context);

                    setState(State.SAVING);
                case SAVING:
                    if (writeResult != null && !writeResult.isSuccess())
                    {
                        logger.trace("Adding callback for write result: {}", callback);
                        writeResult.addCallback(callback, commandStore.executor());
                        break;
                    }
                    context.commands.forEachKey(commandStore.commandCache()::cleanupSaveResult);
                    context.commandsForKeys.forEachKey(commandStore.commandsForKeyCache()::cleanupSaveResult);
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

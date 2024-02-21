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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import accord.impl.TestAgent;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.Node;
import accord.local.NodeTimeService;
import accord.local.PreLoadContext;
import accord.local.SafeCommandStore;
import accord.messages.TxnRequest;
import accord.primitives.Keys;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.RoutableKey;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.topology.Topology;
import accord.utils.RandomSource;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.concurrent.SimulatedExecutorFactory;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.metrics.AccordStateCacheMetrics;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.Generators;

import static org.apache.cassandra.utils.AccordGenerators.fromQT;

class SimulatedAccordCommandStore implements AutoCloseable
{
    private final List<Throwable> failures = new ArrayList<>();
    private final SimulatedExecutorFactory globalExecutor;
    private final CommandStore.EpochUpdateHolder updateHolder;
    private final BooleanSupplier shouldEvict;

    public final NodeTimeService timeService;
    public final AccordCommandStore store;
    public final Node.Id nodeId;
    public final Topology topology;
    public final MockJournal journal;

    SimulatedAccordCommandStore(RandomSource rs)
    {
        globalExecutor = new SimulatedExecutorFactory(rs, fromQT(Generators.TIMESTAMP_GEN.map(java.sql.Timestamp::getTime)).mapToLong(TimeUnit.MILLISECONDS::toNanos).next(rs), failures::add);
        var unorderedScheduled = globalExecutor.scheduled("ignored");
        ExecutorFactory.Global.unsafeSet(globalExecutor);
        Stage.READ.unsafeSetExecutor(unorderedScheduled);
        Stage.MUTATION.unsafeSetExecutor(unorderedScheduled);
        for (Stage stage : Arrays.asList(Stage.MISC, Stage.ACCORD_MIGRATION))
            stage.unsafeSetExecutor(globalExecutor.configureSequential("ignore").build());

        this.updateHolder = new CommandStore.EpochUpdateHolder();
        this.nodeId = AccordTopology.tcmIdToAccord(ClusterMetadata.currentNullable().myNodeId());
        this.timeService = new NodeTimeService()
        {
            private final ToLongFunction<TimeUnit> unixWrapper = NodeTimeService.unixWrapper(TimeUnit.NANOSECONDS, this::now);

            @Override
            public Node.Id id()
            {
                return nodeId;
            }

            @Override
            public long epoch()
            {
                return ClusterMetadata.current().epoch.getEpoch();
            }

            @Override
            public long now()
            {
                return globalExecutor.nanoTime();
            }

            @Override
            public long unix(TimeUnit unit)
            {
                return unixWrapper.applyAsLong(unit);
            }

            @Override
            public Timestamp uniqueNow(Timestamp atLeast)
            {
                var now = Timestamp.fromValues(epoch(), now(), nodeId);
                if (now.compareTo(atLeast) < 0)
                    throw new UnsupportedOperationException();
                return now;
            }
        };

        this.journal = new MockJournal();
        this.store = new AccordCommandStore(0,
                                            timeService,
                                            new TestAgent.RethrowAgent()
                                            {
                                                @Override
                                                public boolean isExpired(TxnId initiated, long now)
                                                {
                                                    return false;
                                                }
                                            },
                                            null,
                                            ignore -> AccordTestUtils.NOOP_PROGRESS_LOG,
                                            updateHolder,
                                            journal,
                                            new AccordStateCacheMetrics("test"));

        this.topology = AccordTopology.createAccordTopology(ClusterMetadata.current());
        var rangesForEpoch = new CommandStores.RangesForEpoch(topology.epoch(), topology.ranges(), store);
        updateHolder.add(topology.epoch(), rangesForEpoch, topology.ranges());
        updateHolder.updateGlobal(topology.ranges());

        int evictSelection = rs.nextInt(0, 3);
        switch (evictSelection)
        {
            case 0: // uniform 50/50
                shouldEvict = rs::nextBoolean;
                break;
            case 1: // variable frequency
                var freq = rs.nextFloat();
                shouldEvict = () -> rs.decide(freq);
                break;
            case 2: // fixed result
                boolean result = rs.nextBoolean();
                shouldEvict = () -> result;
                break;
            default:
                throw new IllegalStateException("Unexpected int for evict selection: " + evictSelection);
        }
    }

    public void maybeCacheEvict(Keys keys, Ranges ranges)
    {
        AccordStateCache cache = store.cache();
        cache.forEach(state -> {
            Class<?> keyType = state.key().getClass();
            if (TxnId.class.equals(keyType))
            {
                Command command = (Command) state.state().get();
                if (command.known().definition.isKnown()
                    && (command.partialTxn().keys().intersects(keys) || ranges.intersects(command.partialTxn().keys()))
                    && shouldEvict.getAsBoolean())
                    cache.maybeEvict(state);
            }
            else if (RoutableKey.class.isAssignableFrom(keyType))
            {
                //TODO (now, coverage): this is broken and conflicts with Benedict's work to make this stable... hold off testing this for now
                // this is some form of CommandsForKey... possible matches are: TimestampsForKey, DepsCommandsForKey, AllCommandsForKey,and UpdatesForKey
//                    RoutableKey key = (RoutableKey) state.key();
//                    if (keys.contains(key) && rs.nextBoolean())
//                        cache.maybeEvict(state);
            }
            else if (Range.class.isAssignableFrom(keyType))
            {
                Ranges key = Ranges.of((Range) state.key());
                if ((key.intersects(keys) || key.intersects(ranges))
                    && shouldEvict.getAsBoolean())
                    cache.maybeEvict(state);
            }
            else
            {
                throw new AssertionError("Unexpected key type: " + state.key().getClass());
            }
        });
    }

    public void checkFailures()
    {
        if (Thread.interrupted())
            failures.add(new InterruptedException());
        if (failures.isEmpty()) return;
        AssertionError error = new AssertionError("Unexpected exceptions found");
        failures.forEach(error::addSuppressed);
        failures.clear();
        throw error;
    }

    public <T> T process(TxnRequest<T> request) throws ExecutionException, InterruptedException
    {
        return process(request, request::apply);
    }

    public <T> T process(PreLoadContext loadCtx, Function<? super SafeCommandStore, T> function) throws ExecutionException, InterruptedException
    {
        var result = store.submit(loadCtx, function).beginAsResult();
        processAll();
        return AsyncChains.getBlocking(result);
    }

    public <T> AsyncResult<T> processAsync(TxnRequest<T> request)
    {
        return processAsync(request, request::apply);
    }

    public <T> AsyncResult<T> processAsync(PreLoadContext loadCtx, Function<? super SafeCommandStore, T> function)
    {
        return store.submit(loadCtx, function).beginAsResult();
    }

    public void processAll()
    {
        while (processOne())
        {
        }
    }

    private boolean processOne()
    {
        boolean result = globalExecutor.processOne();
        checkFailures();
        return result;
    }

    @Override
    public void close() throws Exception
    {
        store.shutdown();
    }
}
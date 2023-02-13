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

import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;

import javax.annotation.Nullable;

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.Key;
import accord.api.ProgressLog;
import accord.impl.AbstractSafeCommandStore;
import accord.impl.CommandsForKey;
import accord.impl.LiveCommandsForKey;
import accord.local.CommandStores.RangesForEpoch;
import accord.local.CommonAttributes;
import accord.local.NodeTimeService;
import accord.local.PreLoadContext;
import accord.local.Status;
import accord.primitives.AbstractKeys;
import accord.primitives.Keys;
import accord.primitives.Ranges;
import accord.primitives.RoutableKey;
import accord.primitives.Routables;
import accord.primitives.Seekable;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import org.apache.cassandra.service.accord.serializers.CommandsForKeySerializer;

public class AccordSafeCommandStore extends AbstractSafeCommandStore<AccordLiveCommand, AccordLiveCommandsForKey>
{
    private final AccordCommandStore commandStore;

    public AccordSafeCommandStore(PreLoadContext context,
                                  Map<TxnId, AccordLiveCommand> commands,
                                  Map<RoutableKey, AccordLiveCommandsForKey> commandsForKey,
                                  AccordCommandStore commandStore)
    {
        super(context, commands, commandsForKey);
        this.commandStore = commandStore;
    }

    @Override
    protected AccordLiveCommand getIfLoaded(TxnId txnId)
    {
        return commandStore.commandCache().referenceAndGetIfLoaded(txnId);
    }

    @Override
    protected AccordLiveCommandsForKey getIfLoaded(RoutableKey key)
    {
        return commandStore.commandsForKeyCache().referenceAndGetIfLoaded(key);
    }

    @Override
    public AccordCommandStore commandStore()
    {
        return commandStore;
    }

    @Override
    public DataStore dataStore()
    {
        return commandStore().dataStore();
    }

    @Override
    public Agent agent()
    {
        return commandStore.agent();
    }

    @Override
    public ProgressLog progressLog()
    {
        return commandStore().progressLog();
    }

    @Override
    public NodeTimeService time()
    {
        return commandStore.time();
    }

    @Override
    public RangesForEpoch ranges()
    {
        return commandStore().ranges();
    }

    @Override
    public long latestEpoch()
    {
        return commandStore().time().epoch();
    }

    @Override
    protected Timestamp maxConflict(Seekables<?, ?> keysOrRanges, Ranges slice)
    {
        // TODO: Seekables
        // TODO: efficiency
        return ((Keys)keysOrRanges).stream()
                           .map(this::maybeCommandsForKey)
                           .filter(Objects::nonNull)
                           .map(LiveCommandsForKey::current)
                           .map(CommandsForKey::max)
                           .max(Comparator.naturalOrder())
                           .orElse(Timestamp.NONE);
    }

    public <T> T mapReduce(Routables<?, ?> keysOrRanges, Function<CommandsForKey, T> map, BinaryOperator<T> reduce, T initialValue)
    {
        switch (keysOrRanges.domain()) {
            default:
                throw new AssertionError();
            case Key:
                AbstractKeys<Key, ?> keys = (AbstractKeys<Key, ?>) keysOrRanges;
                return keys.stream()
                           .map(this::commandsForKey)
                           .map(LiveCommandsForKey::current)
                           .map(map)
                           .reduce(initialValue, reduce);
            case Range:
                // TODO: implement
                throw new UnsupportedOperationException();
        }
    }

    private <O> O mapReduceForKey(Routables<?, ?> keysOrRanges, Ranges slice, BiFunction<CommandsForKey, O, O> map, O accumulate, O terminalValue)
    {
        switch (keysOrRanges.domain()) {
            default:
                throw new AssertionError();
            case Key:
                // TODO: efficiency
                AbstractKeys<Key, ?> keys = (AbstractKeys<Key, ?>) keysOrRanges;
                for (Key key : keys)
                {
                    if (!slice.contains(key)) continue;
                    LiveCommandsForKey forKey = commandsForKey(key);
                    accumulate = map.apply(forKey.current(), accumulate);
                    if (accumulate.equals(terminalValue))
                        return accumulate;
                }
                break;
            case Range:
                // TODO (required): implement
                throw new UnsupportedOperationException();
        }
        return accumulate;
    }

    @Override
    public <T> T mapReduce(Seekables<?, ?> keysOrRanges, Ranges slice, TestKind testKind, TestTimestamp testTimestamp, Timestamp timestamp, TestDep testDep, @Nullable TxnId depId, @Nullable Status minStatus, @Nullable Status maxStatus, CommandFunction<T, T> map, T accumulate, T terminalValue)
    {
        accumulate = mapReduceForKey(keysOrRanges, slice, (forKey, prev) -> {
            CommandsForKey.CommandTimeseries<?> timeseries;
            switch (testTimestamp)
            {
                default: throw new AssertionError();
                case STARTED_AFTER:
                case STARTED_BEFORE:
                    timeseries = forKey.byId();
                    break;
                case EXECUTES_AFTER:
                case MAY_EXECUTE_BEFORE:
                    timeseries = forKey.byExecuteAt();
            }
            CommandsForKey.CommandTimeseries.TestTimestamp remapTestTimestamp;
            switch (testTimestamp)
            {
                default: throw new AssertionError();
                case STARTED_AFTER:
                case EXECUTES_AFTER:
                    remapTestTimestamp = CommandsForKey.CommandTimeseries.TestTimestamp.AFTER;
                    break;
                case STARTED_BEFORE:
                case MAY_EXECUTE_BEFORE:
                    remapTestTimestamp = CommandsForKey.CommandTimeseries.TestTimestamp.BEFORE;
            }
            return timeseries.mapReduce(testKind, remapTestTimestamp, timestamp, testDep, depId, minStatus, maxStatus, map, prev, terminalValue);
        }, accumulate, terminalValue);

        return accumulate;
    }

    @Override
    public CommonAttributes completeRegistration(Seekables<?, ?> seekables, Ranges ranges, AccordLiveCommand accordLiveCommand, CommonAttributes commonAttributes)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public CommonAttributes completeRegistration(Seekable seekable, Ranges ranges, AccordLiveCommand accordLiveCommand, CommonAttributes commonAttributes)
    {
        throw new UnsupportedOperationException();
    }


    //
//    @Override
//    public void register(Seekables<?, ?> keysOrRanges, Ranges slice, Command command)
//    {
//        // TODO (required): support ranges
//        Routables.foldl((Keys)keysOrRanges, slice, (k, v, i) -> { CommandsForKeys.register(this, command, k, slice); return v; }, null);
//    }
//
//    @Override
//    public void register(Seekable keyOrRange, Ranges slice, Command command)
//    {
//        // TODO (required): support ranges
//        Key key = (Key) keyOrRange;
//        if (slice.contains(key))
//            CommandsForKeys.register(this, command, key, slice);
//    }

    @Override
    public CommandsForKey.CommandLoader<?> cfkLoader()
    {
        return CommandsForKeySerializer.loader;
    }
}

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
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.impl.CommandsSummary;
import accord.local.SafeCommandStore.CommandFunction;
import accord.local.SafeCommandStore.TestDep;
import accord.local.SafeCommandStore.TestStartedAt;
import accord.local.SafeCommandStore.TestStatus;
import accord.local.SaveStatus;
import accord.primitives.Range;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;

import static accord.local.SafeCommandStore.TestDep.ANY_DEPS;
import static accord.local.SafeCommandStore.TestDep.WITH;
import static accord.local.SafeCommandStore.TestStartedAt.ANY;
import static accord.local.SafeCommandStore.TestStatus.ANY_STATUS;
import static accord.local.Status.Stable;
import static accord.local.Status.Truncated;

public class CommandsForRange implements CommandsSummary
{
    private final Range range;
    private final Map<TxnId, DiskCommandsForRanges.Summary> map;

    public CommandsForRange(Range range, Map<TxnId, DiskCommandsForRanges.Summary> map)
    {
        this.range = range;
        this.map = map;
    }

    @Override
    public <P1, T> T mapReduceFull(TxnId testTxnId, Txn.Kind.Kinds testKind, TestStartedAt testStartedAt, TestDep testDep, TestStatus testStatus, CommandFunction<P1, T, T> map, P1 p1, T accumulate)
    {
        return mapReduce(testTxnId, testTxnId, testKind, testStartedAt, testDep, testStatus, map, p1, accumulate);
    }

    @Override
    public <P1, T> T mapReduceActive(Timestamp startedBefore, Txn.Kind.Kinds testKind, CommandFunction<P1, T, T> map, P1 p1, T accumulate)
    {
        return mapReduce(startedBefore, null, testKind, ANY, ANY_DEPS, ANY_STATUS, map, p1, accumulate);
    }

    private <P1, T> T mapReduce(@Nonnull Timestamp testTimestamp, @Nullable TxnId testTxnId, Txn.Kind.Kinds testKind, TestStartedAt testStartedAt, TestDep testDep, TestStatus testStatus, CommandFunction<P1, T, T> map, P1 p1, T accumulate)
    {
        // TODO (required): reconsider how we build this, to avoid having to provide range keys in order (or ensure our range search does this for us)
        Map<Range, List<DiskCommandsForRanges.Summary>> collect = new TreeMap<>(Range::compare);
        this.map.values().forEach((summary -> {
            if (summary.saveStatus.compareTo(SaveStatus.Erased) >= 0)
                return;

            switch (testStartedAt)
            {
                default: throw new AssertionError();
                case STARTED_AFTER:
                    if (summary.txnId.compareTo(testTimestamp) <= 0) return;
                    else break;
                case STARTED_BEFORE:
                    if (summary.txnId.compareTo(testTimestamp) >= 0) return;
                case ANY:
                    if (testDep != ANY_DEPS && (summary.executeAt == null || summary.executeAt.compareTo(testTxnId) < 0))
                        return;
            }

            switch (testStatus)
            {
                default: throw new AssertionError("Unhandled TestStatus: " + testStatus);
                case ANY_STATUS:
                    break;
                case IS_PROPOSED:
                    switch (summary.saveStatus)
                    {
                        default: return;
                        case PreCommitted:
                        case Committed:
                        case Accepted:
                    }
                    break;
                case IS_STABLE:
                    if (!summary.saveStatus.hasBeen(Stable) || summary.saveStatus.hasBeen(Truncated))
                        return;
            }

            if (!testKind.test(summary.txnId.kind()))
                return;

            if (testDep != ANY_DEPS)
            {
                if (!summary.saveStatus.known.deps.hasProposedOrDecidedDeps())
                    return;

                // TODO (required): we must ensure these txnId are limited to those we intersect in this command store
                // We are looking for transactions A that have (or have not) B as a dependency.
                // If B covers ranges [1..3] and A covers [2..3], but the command store only covers ranges [1..2],
                // we could have A adopt B as a dependency on [3..3] only, and have that A intersects B on this
                // command store, but also that there is no dependency relation between them on the overlapping
                // key range [2..2].

                // This can lead to problems on recovery, where we believe a transaction is a dependency
                // and so it is safe to execute, when in fact it is only a dependency on a different shard
                // (and that other shard, perhaps, does not know that it is a dependency - and so it is not durably known)
                // TODO (required): consider this some more
                if ((testDep == WITH) == !summary.depsIds.contains(testTxnId))
                    return;
            }

            // TODO (required): ensure we are excluding any ranges that are now shard-redundant (not sure if this is enforced yet)
            for (Range range : summary.ranges)
                collect.computeIfAbsent(range, ignore -> new ArrayList<>()).add(summary);
        }));

        for (Map.Entry<Range, List<DiskCommandsForRanges.Summary>> e : collect.entrySet())
        {
            for (DiskCommandsForRanges.Summary command : e.getValue())
                accumulate = map.apply(p1, e.getKey(), command.txnId, command.executeAt, accumulate);
        }

        return accumulate;
    }

    public boolean hasRedundant(TxnId shardRedundantBefore)
    {
        return false;
    }

    public CommandsForRange withoutRedundant(TxnId shardRedundantBefore)
    {
        return null;
    }
}
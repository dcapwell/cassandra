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

import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;

import accord.primitives.Range;
import accord.primitives.TxnId;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import org.apache.cassandra.utils.Pair;

public class AccordSafeCommandsForRange implements AccordSafeState<Range, CommandsForRange>
{
    private final AsyncResult<Pair<DiskCommandsForRanges.Watcher, NavigableMap<TxnId, DiskCommandsForRanges.Summary>>> chain;
    private final Range range;
    private boolean invalidated;
    private CommandsForRange original, current;

    public AccordSafeCommandsForRange(Range range, AsyncResult<Pair<DiskCommandsForRanges.Watcher, NavigableMap<TxnId, DiskCommandsForRanges.Summary>>> chain)
    {
        this.range = range;
        this.chain = chain;
    }

    @Override
    public CommandsForRange current()
    {
        checkNotInvalidated();
        return current;
    }

    @Override
    public void invalidate()
    {
        invalidated = true;
    }

    @Override
    public boolean invalidated()
    {
        return invalidated;
    }

    @Override
    public void set(CommandsForRange update)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public CommandsForRange original()
    {
        checkNotInvalidated();
        return original;
    }

    @Override
    public void preExecute()
    {
        checkNotInvalidated();
        Pair<DiskCommandsForRanges.Watcher, NavigableMap<TxnId, DiskCommandsForRanges.Summary>> pair = AsyncChains.getUnchecked(chain);
        pair.left.close();
        pair.left.get().entrySet().forEach(e -> pair.right.put(e.getKey(), e.getValue()));
        current = original = new CommandsForRange(range, pair.right);
    }

    @Override
    public void postExecute()
    {
        checkNotInvalidated();
    }

    @Override
    public AccordCachingState<Range, CommandsForRange> global()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AccordSafeCommandsForRange that = (AccordSafeCommandsForRange) o;
        return Objects.equals(original, that.original) && Objects.equals(current, that.current);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(original, current);
    }

    @Override
    public String toString()
    {
        return "AccordSafeCommandsForRange{" +
               "chain=" + chain +
               ", invalidated=" + invalidated +
               ", original=" + original +
               ", current=" + current +
               '}';
    }
}

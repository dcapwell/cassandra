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

import java.util.Objects;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;

import accord.api.Key;
import accord.impl.CommandsForKey;
import accord.impl.SafeCommandsForKey;
import accord.primitives.RoutableKey;
import accord.utils.async.AsyncChain;

public class AccordSafeCommandsForKey extends SafeCommandsForKey implements AccordSafeState<RoutableKey, CommandsForKey>
{
    private boolean invalidated;
    private final AccordLoadingState<RoutableKey, CommandsForKey> global;
    private CommandsForKey original;
    private CommandsForKey current;

    public AccordSafeCommandsForKey(AccordLoadingState<RoutableKey, CommandsForKey> global)
    {
        super((Key) global.key());
        this.global = global;
        this.original = null;
        this.current = null;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AccordSafeCommandsForKey that = (AccordSafeCommandsForKey) o;
        return Objects.equals(original, that.original) && Objects.equals(current, that.current);
    }

    @Override
    public int hashCode()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public CommandsForKey current()
    {
        return current;
    }

    @Override
    @VisibleForTesting
    public void set(CommandsForKey cfk)
    {
        this.current = cfk;
    }

    public CommandsForKey original()
    {
        return original;
    }

    @Override
    public long estimatedSizeOnHeap()
    {
        if (current == null)
            return 0;

        return AccordObjectSizes.commandsForKey(current);
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
    public AccordLoadingState.LoadingState loadingState()
    {
        return global.state();
    }

    @Override
    public Runnable load(Function<RoutableKey, CommandsForKey> loadFunction)
    {
        return global.load(loadFunction);
    }

    @Override
    public AsyncChain<?> listen()
    {
        return global.listen();
    }

    @Override
    public Throwable failure()
    {
        return global.failure();
    }
}

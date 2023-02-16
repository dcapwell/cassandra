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

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;

import accord.utils.Invariants;
import org.apache.cassandra.service.accord.AccordSafeState;
import org.apache.cassandra.service.accord.AccordStateCache;

import static accord.utils.Invariants.nonNull;

public class AsyncContext<K, V extends AccordSafeState<?>>
{
    private static final Object REFERENCED = new Object();

    private final Map<K, Object> values = new HashMap<>();

    public V get(K key)
    {
        Object value = values.get(key);
        return value != REFERENCED ? (V) value : null;
    }

    public void add(K key, V value)
    {
        values.put(key, nonNull(value));
    }

    @VisibleForTesting
    boolean isReferenced(K key)
    {
        return values.get(key) == REFERENCED;
    }

    @VisibleForTesting
    boolean isLoaded(K key)
    {
        Object state = values.get(key);
        return state != REFERENCED && state != null;
    }

    public boolean isEmpty()
    {
        return values.isEmpty();
    }

    public int size()
    {
        return values.size();
    }

    public void markReferenced(K key)
    {
        Invariants.checkState(!values.containsKey(key));
        values.put(key, REFERENCED);
    }

    public void getActive(AccordStateCache.Instance<K, V> cache)
    {
        values.forEach((k, state) -> values.put(k, nonNull(cache.getActive(k))));
    }

    public void releaseResources(AccordStateCache.Instance<K, V> cache)
    {
        values.forEach((k, state) -> {
            if (state == REFERENCED)
                cache.release(k);
            else
                cache.release(k, (V) state);
        });
    }

    public void revertChanges()
    {
        values.forEach((k, state) -> {
            if (state == REFERENCED)
                return;
            ((V) state).revert();
        });
    }

    public void forEachUpdated(BiConsumer<K, V> consumer)
    {
        values.forEach((key, state) -> {
            Invariants.checkState(state != REFERENCED);
            V value = (V) state;
            if (!value.hasUpdate())
                return;
            consumer.accept(key, value);
        });
    }

    public void forEachKey(Consumer<K> consumer)
    {
        values.keySet().forEach(consumer);
    }

    public void invalidate()
    {
        values.forEach((key, state) -> {
            Invariants.checkState(state != REFERENCED);
            V value = (V) state;
            value.invalidate();
        });
    }
}

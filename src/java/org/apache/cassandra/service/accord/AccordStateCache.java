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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Data;
import accord.local.Command;
import accord.local.ImmutableState;
import accord.utils.Invariants;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import org.apache.cassandra.utils.ObjectSizes;

/**
 * Cache for AccordCommand and AccordCommandsForKey, available memory is shared between the two object types.
 *
 * Supports dynamic object sizes. After each acquire/free cycle, the cacheable objects size is recomputed to
 * account for data added/removed during txn processing if it's modified flag is set
 *
 * TODO: explain how items move to and from the active pool and are evicted
 */
public class AccordStateCache
{
    private static final Logger logger = LoggerFactory.getLogger(AccordStateCache.class);

    public interface LoadFunction<K, V>
    {
        AsyncResult<Void> apply(K key, Consumer<V> valueConsumer);
    }

    public interface ItemAccessor<K, V>
    {
        long estimatedSizeOnHeap(V value);
        boolean isEmpty(V value);
    }

    private static class PendingLoad<V> implements Consumer<V>
    {
        volatile V value = null;
        final AsyncResult<Void> result;

        public <K> PendingLoad(K key, LoadFunction<K, V> function)
        {
            this.result = function.apply(key, this);
        }

        @Override
        public void accept(V v)
        {
            this.value = v;
        }
    }

    static class Node<K, V extends ImmutableState>
    {
        static final long EMPTY_SIZE = ObjectSizes.measure(new AccordStateCache.Node(null, null, null));

        final K key;
        @Nonnull Object state;
        final ItemAccessor<K, V> accessor;
        private Node<?, ?> prev;
        private Node<?, ?> next;
        private int references = 0;
        private long lastQueriedEstimatedSizeOnHeap = 0;

        public Node(K key, PendingLoad<V> load, ItemAccessor<K, V> accessor)
        {
            this.key = key;
            this.state = load;
            this.accessor = accessor;
        }

        boolean maybeFinishLoad()
        {
            if (!(state instanceof PendingLoad))
                return false;

            PendingLoad<V> load = (PendingLoad<V>) state;
            if (!load.result.isDone())
                return false;

            // if the load is completed, switch the state to the loaded value
            state = load.value;
            return true;
        }

        boolean isLoaded()
        {
            return !(state instanceof PendingLoad);
        }

        AsyncResult<Void> loadResult()
        {
            return state instanceof PendingLoad ? ((PendingLoad<V>) state).result : null;
        }

        V value()
        {
            Invariants.checkState(isLoaded() && state != null);
            return (V) state;
        }

        void value(V v)
        {
            Invariants.checkState(isLoaded());
            state = v;
        }

        long estimatedSizeOnHeap()
        {
            long result = EMPTY_SIZE;
            if (isLoaded() && !accessor.isEmpty(value()))
            {
                boolean wasDormant = value().isDormant();
                try
                {
                    if (wasDormant)
                        value().markActive();
                    result += accessor.estimatedSizeOnHeap(value());
                }
                finally
                {
                    if (wasDormant)
                        value().markDormant();
                }
            }
            lastQueriedEstimatedSizeOnHeap = result;
            return result;
        }

        long estimatedSizeOnHeapDelta()
        {
            long prevSize = lastQueriedEstimatedSizeOnHeap;
            return estimatedSizeOnHeap() - prevSize;
        }

        K key()
        {
            return key;
        }
    }

    static class Stats
    {
        private long queries;
        private long hits;
        private long misses;
    }

    private static class NamedMap<K, V> extends HashMap<K, V>
    {
        final String name;

        public NamedMap(String name)
        {
            this.name = name;
        }
    }

    public final Map<Object, Node<?, ?>> active = new HashMap<>();
    private final Map<Object, Node<?, ?>> cache = new HashMap<>();
    private final Set<Instance<?, ?>> instances = new HashSet<>();

    private final NamedMap<Object, Node<?, ?>> pendingLoads = new NamedMap<>("pendingLoads");
    private final NamedMap<Object, AsyncResult<Void>> saveResults = new NamedMap<>("saveResults");

    private final NamedMap<Object, AsyncResult<Data>> readResults = new NamedMap<>("readResults");
    private final NamedMap<Object, AsyncResult<Void>> writeResults = new NamedMap<>("writeResults");

    Node<?, ?> head;
    Node<?, ?> tail;
    private long maxSizeInBytes;
    private long bytesCached = 0;
    private final Stats stats = new Stats();

    public AccordStateCache(long maxSizeInBytes)
    {
        this.maxSizeInBytes = maxSizeInBytes;
    }

    public void setMaxSize(long size)
    {
        maxSizeInBytes = size;
        maybeEvict();
    }

    private void unlink(Node<?, ?> node)
    {
        Node<?, ?> prev = node.prev;
        Node<?, ?> next = node.next;

        if (prev == null)
        {
            Preconditions.checkState(head == node, "previous is null but the head isnt the provided node!");
            head = next;
        }
        else
        {
            prev.next = next;
        }

        if (next == null)
        {
            Preconditions.checkState(tail == node, "next is null but the tail isnt the provided node!");
            tail = prev;
        }
        else
        {
            next.prev = prev;
        }

        node.prev = null;
        node.next = null;
    }

    private void push(Node<?, ?> node)
    {
        if (head != null)
        {
            node.prev = null;
            node.next = head;
            head.prev = node;
            head = node;
        }
        else
        {
            head = node;
            tail = node;
        }
    }

    private void updateSize(Node<?, ?> node)
    {
        bytesCached += node.estimatedSizeOnHeapDelta();
    }

    // don't evict if there's an outstanding save result. If an item is evicted then reloaded
    // before it's mutation is applied, out of date info will be loaded
    private <K, V extends ImmutableState> boolean canEvict(Node<K, V> node)
    {
        return node.isLoaded() &&
               !hasActiveAsyncResult(saveResults, node.key) &&
               !hasActiveAsyncResult(readResults, node.key) &&
               !hasActiveAsyncResult(writeResults, node.key);
    }

    private void maybeEvict()
    {
        if (bytesCached <= maxSizeInBytes)
            return;

        Node<?, ?> current = tail;
        while (current != null && bytesCached > maxSizeInBytes)
        {
            Node<?, ?> evict = current;
            current = current.prev;

            if (!canEvict(evict))
                continue;

            logger.trace("Evicting {} {}", evict.value().getClass().getSimpleName(), evict.key());
            unlink(evict);
            cache.remove(evict.key());
            bytesCached -= evict.estimatedSizeOnHeap();
        }
    }

    private static <K, V, F extends AsyncResult<V>> F getAsyncResult(NamedMap<Object, F> resultMap, K key)
    {
        F r = resultMap.get(key);
        if (r == null)
            return null;

        if (!r.isDone())
            return r;

        if (logger.isTraceEnabled())
            logger.trace("Clearing result for {} from {}: {}", key, resultMap.name, r);
        resultMap.remove(key);
        return null;
    }

    private static <K, F extends AsyncResult<?>> void setAsyncResult(Map<Object, F> resultsMap, K key, F result)
    {
        Preconditions.checkState(!resultsMap.containsKey(key));
        resultsMap.put(key, result);
    }

    private static <K, V> boolean hasActiveAsyncResult(NamedMap<Object, AsyncResult<V>> resultMap, K key)
    {
        // getResult only returns a result if it is not complete, so don't need to check if its been completed
        return getAsyncResult(resultMap, key) != null;
    }

    private static <K> void mergeAsyncResult(Map<Object, AsyncResult<Void>> resultMap, K key, AsyncResult<Void> result)
    {
        AsyncResult<Void> existing = resultMap.get(key);
        if (existing != null && !existing.isDone())
        {
            logger.trace("Merging result {} with existing {}", result, existing);
            result = AsyncResults.reduce(ImmutableList.of(existing, result), (a, b) -> null).beginAsResult();
        }

        resultMap.put(key, result);
    }

    private <K, V extends ImmutableState> Node<K, V> maybeCleanupLoad(Node<K, V> node)
    {
        if (node.maybeFinishLoad())
            updateSize(node);
        return node;
    }

    @VisibleForTesting
    private <K> void maybeCleanupLoad(K key)
    {
        Node<?, ?> node = active.get(key);
        if (node != null)
            maybeCleanupLoad(node);;
    }

    private <K> void maybeClearAsyncResult(K key)
    {
        maybeCleanupLoad(key);
        // will clear if it's done
        getAsyncResult(saveResults, key);
        getAsyncResult(readResults, key);
        getAsyncResult(writeResults, key);
    }

    public class Instance<K, V extends ImmutableState>
    {
        private final Class<K> keyClass;
        private final Class<V> valClass;
        private final ItemAccessor<K, V> accessor;
        private final Stats stats = new Stats();

        public Instance(Class<K> keyClass, Class<V> valClass, ItemAccessor<K, V> accessor)
        {
            this.keyClass = keyClass;
            this.valClass = valClass;
            this.accessor = accessor;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Instance<?, ?> instance = (Instance<?, ?>) o;
            return keyClass.equals(instance.keyClass) && valClass.equals(instance.valClass);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(keyClass, valClass);
        }

        private Node<K, V> getAndReferenceNode(K key, LoadFunction<K, V> loadFunction)
        {
            stats.queries++;
            AccordStateCache.this.stats.queries++;

            Node<K, V> node = (Node<K, V>) active.get(key);
            if (node != null)
            {
                stats.hits++;
                AccordStateCache.this.stats.hits++;
                node.references++;
                return node;
            }

            node = (Node<K, V>) cache.remove(key);

            if (node == null)
            {
                stats.misses++;
                AccordStateCache.this.stats.misses++;
                if (loadFunction == null)
                    return null;
                node = new Node<>(key, new PendingLoad<>(key, loadFunction), accessor);
                updateSize(node);
            }
            else
            {
                stats.hits++;
                AccordStateCache.this.stats.hits++;
                unlink(node);
            }

            Preconditions.checkState(node.references == 0);
            maybeEvict();

            node.references++;
            active.put(key, node);

            return node;
        }

        public V referenceAndGetIfLoaded(K key)
        {
            Node<K, V> node = getAndReferenceNode(key, null);
            if (node == null || !node.isLoaded())
                return null;
            return node.value();
        }

        public AsyncResult<Void> referenceAndLoad(K key, LoadFunction<K, V> loadFunction)
        {
            Invariants.checkArgument(loadFunction != null);
            return getAndReferenceNode(key, loadFunction).loadResult();
        }

        /**
         * Return an immutable map of instances from the given keys. The keys are assumed to be
         * loaded and referenced, therefore in the active map
         */
        public ImmutableMap<K, V> getActive(Iterable<K> keys)
        {
            Iterator<K> keyIter = keys.iterator();
            if (!keyIter.hasNext())
                return ImmutableMap.of();

            ImmutableMap.Builder<K, V> result = ImmutableMap.builder();
            while (keyIter.hasNext())
            {
                K key = keyIter.next();
                Node<K, V> node = (Node<K, V>) active.get(key);
                maybeCleanupLoad(node);
                V value = node.value();
                if (!accessor.isEmpty(value))
                    value.checkIsDormant();
                result.put(key, value);
            }

            return result.build();
        }

        @VisibleForTesting
        public V getActive(K key)
        {
            return (V) maybeCleanupLoad(active.get(key)).value();
        }

        @VisibleForTesting
        public boolean isReferenced(K key)
        {
            Node<K, V> node = (Node<K, V>) active.get(key);
            if (node == null)
                return false;
            Invariants.checkState(node.references > 0);
            return true;
        }

        @VisibleForTesting
        public boolean isLoaded(K key)
        {
            Node<K, V> node = (Node<K, V>) active.get(key);
            if (node == null)
                node = (Node<K, V>) cache.get(key);
            return node != null && node.isLoaded();
        }

        public void release(K key, V original, V current)
        {
            if (current == original)
            {
                // it's possible to load an empty object and never do anything to witness it
                if (original != null)
                {
                    original.checkIsActive();
                    original.markDormant();
                }
            }
            else
            {
                if (original != null && !accessor.isEmpty(original))
                    original.checkIsSuperseded();
                current.checkIsActive();
                current.markDormant();
            }

            logger.trace("Releasing resources for {}: {}", key, original);
            maybeClearAsyncResult(key);
            Node<K, V> node = (Node<K, V>) active.get(key);
            Invariants.checkState(node != null && node.references > 0);
            Invariants.checkState(node.isLoaded());

            Invariants.checkState(node.value() == original || (accessor.isEmpty(node.value()) && original == null));
            if (current != original)
            {
                node.value(current);
                updateSize(node);
            }

            if (--node.references == 0)
            {
                logger.trace("Moving {} from active pool to cache", key);
                active.remove(key);
                cache.put(key, node);
                push(node);
            }

            maybeEvict();
        }

        @VisibleForTesting
        boolean canEvict(K key)
        {
            return AccordStateCache.this.canEvict(active.get(key));
        }

        @VisibleForTesting
        public boolean hasLoadResult(K key)
        {
            Node<?, ?> node = active.get(key);
            return node != null && !node.isLoaded();
        }

        public void cleanupLoadResult(K key)
        {
            maybeCleanupLoad(key);
        }

        public AsyncResult<?> getSaveResult(K key)
        {
            return getAsyncResult(saveResults, key);
        }

        public void addSaveResult(K key, AsyncResult<Void> result)
        {
            logger.trace("Adding save result for {}: {}", key, result);
            mergeAsyncResult(saveResults, key, result);
        }

        public void cleanupSaveResult(K key)
        {
            getSaveResult(key);
        }

        @VisibleForTesting
        public boolean hasSaveResult(K key)
        {
            return saveResults.get(key) != null;
        }

        public AsyncResult<Data> getReadResult(K key)
        {
            return getAsyncResult(readResults, key);
        }

        public void setReadResult(K key, AsyncResult<Data> result)
        {
            setAsyncResult(readResults, key, result);
        }

        public void cleanupReadResult(K key)
        {
            getReadResult(key);
        }

        public AsyncResult<Void> getWriteResult(K key)
        {
            return getAsyncResult(writeResults, key);
        }

        public void setWriteResult(K key, AsyncResult<Void> result)
        {
            setAsyncResult(writeResults, key, result);
        }

        public void cleanupWriteResult(K key)
        {
            getWriteResult(key);
        }

        public long cacheQueries()
        {
            return stats.queries;
        }

        public long cacheHits()
        {
            return stats.hits;
        }

        public long cacheMisses()
        {
            return stats.misses;
        }
    }

    public <K, V extends ImmutableState> Instance<K, V> instance(Class<K> keyClass, Class<V> valClass, ItemAccessor<K, V> accessor)
    {
        Instance<K, V> instance = new Instance<>(keyClass, valClass, accessor);
        if (!instances.add(instance))
            throw new IllegalArgumentException(String.format("Cache instances for types %s -> %s already exists",
                                                             keyClass.getName(), valClass.getName()));
        return instance;
    }

    @VisibleForTesting
    int numActiveEntries()
    {
        return active.size();
    }

    @VisibleForTesting
    int numCachedEntries()
    {
        return cache.size();
    }

    @VisibleForTesting
    long bytesCached()
    {
        return bytesCached;
    }

    @VisibleForTesting
    boolean keyIsActive(Object key)
    {
        return active.containsKey(key);
    }

    @VisibleForTesting
    boolean keyIsCached(Object key)
    {
        return cache.containsKey(key);
    }

    @VisibleForTesting
    int references(Object key)
    {
        Node<?, ?> node = active.get(key);
        return node != null ? node.references : 0;
    }

    public long cacheQueries()
    {
        return stats.queries;
    }

    public long cacheHits()
    {
        return stats.hits;
    }

    public long cacheMisses()
    {
        return stats.misses;
    }
}

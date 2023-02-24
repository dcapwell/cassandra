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
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Data;
import accord.utils.Invariants;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import org.apache.cassandra.utils.ObjectSizes;

/**
 * Cache for AccordCommand and AccordCommandsForKey, available memory is shared between the two object types.
 *
 * Supports dynamic object sizes. After each acquire/free cycle, the cacheable objects size is recomputed to
 * account for data added/removed during txn processing if it's modified flag is set
 */
public class AccordStateCache
{
    private static final Logger logger = LoggerFactory.getLogger(AccordStateCache.class);

    public static class Node<K, V> extends AccordLoadingState<K, V>
    {
        static final long EMPTY_SIZE = ObjectSizes.measure(new AccordStateCache.Node(null));

        private Node<?, ?> prev;
        private Node<?, ?> next;
        private int references = 0;
        private long lastQueriedEstimatedSizeOnHeap = 0;

        public Node(K key)
        {
            super(key);
        }

        public int referenceCount()
        {
            return references;
        }

        boolean isLoaded()
        {
            return state() == LoadingState.LOADED;
        }

        private boolean isUnlinked()
        {
            return prev == null && next == null;
        }

        long estimatedSizeOnHeap(ToLongFunction<V> estimator)
        {
            long result = EMPTY_SIZE;
            V v;
            if (isLoaded() && (v = value()) != null)
                result += estimator.applyAsLong(v);
            lastQueriedEstimatedSizeOnHeap = result;
            return result;
        }

        long estimatedSizeOnHeapDelta(ToLongFunction<V> estimator)
        {
            long prevSize = lastQueriedEstimatedSizeOnHeap;
            return estimatedSizeOnHeap(estimator) - prevSize;
        }

        boolean shouldUpdateSize()
        {
            return isLoaded() && lastQueriedEstimatedSizeOnHeap == EMPTY_SIZE;
        }

        void maybeCleanupLoad()
        {
            state();
        }

        @Override
        public String toString()
        {
            return "Node{" + state() +
                   ", references=" + references +
                   '}';
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

    private final Map<Object, Node<?, ?>> cache = new HashMap<>();
    private final Set<Instance<?, ?, ?>> instances = new HashSet<>();

    private final NamedMap<Object, AsyncResult<Void>> saveResults = new NamedMap<>("saveResults");

    private final NamedMap<Object, AsyncResult<Data>> readResults = new NamedMap<>("readResults");
    private final NamedMap<Object, AsyncResult<Void>> writeResults = new NamedMap<>("writeResults");

    private int linked = 0;
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

    public long getMaxSize()
    {
        return maxSizeInBytes;
    }

    public void clear()
    {
        cache.clear();
        saveResults.clear();
        readResults.clear();
        writeResults.clear();
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
        linked--;
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
        linked++;
    }

    private <K, V> void updateSize(Node<K, V> node, ToLongFunction<V> estimator)
    {
        bytesCached += node.estimatedSizeOnHeapDelta(estimator);
    }

    // don't evict if there's an outstanding save result. If an item is evicted then reloaded
    // before it's mutation is applied, out of date info will be loaded
    private boolean canEvict(Node<?, ?> node)
    {
        return node.references == 0 &&
               node.isLoaded() &&
               !hasActiveAsyncResult(saveResults, node.key()) &&
               !hasActiveAsyncResult(readResults, node.key()) &&
               !hasActiveAsyncResult(writeResults, node.key());
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

            logger.info("Evicting {} {}", evict.value(), evict.key());
            unlink(evict);
            cache.remove(evict.key());
            bytesCached -= evict.lastQueriedEstimatedSizeOnHeap;
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
            result = AsyncChains.reduce(ImmutableList.of(existing, result), (a, b) -> null).beginAsResult();
        }

        resultMap.put(key, result);
    }

    @VisibleForTesting
    private <K> void maybeCleanupLoad(K key)
    {
        Node<?, ?> node = cache.get(key);
        if (node != null)
            node.maybeCleanupLoad();
    }

    private <K> void maybeClearAsyncResult(K key)
    {
        maybeCleanupLoad(key);
        // will clear if it's done
        getAsyncResult(saveResults, key);
        getAsyncResult(readResults, key);
        getAsyncResult(writeResults, key);
    }

    public class Instance<K, V, S extends AccordSafeState<K, V>>
    {
        private final Class<K> keyClass;
        private final Class<V> valClass;
        private final Function<AccordLoadingState<K, V>, S> safeRefFactory;
        private final ToLongFunction<V> heapEstimator;
        private final Stats stats = new Stats();

        public Instance(Class<K> keyClass, Class<V> valClass, Function<AccordLoadingState<K, V>, S> safeRefFactory, ToLongFunction<V> heapEstimator)
        {
            this.keyClass = keyClass;
            this.valClass = valClass;
            this.safeRefFactory = safeRefFactory;
            this.heapEstimator = heapEstimator;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Instance<?, ?, ?> instance = (Instance<?, ?, ?>) o;
            return keyClass.equals(instance.keyClass) && valClass.equals(instance.valClass);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(keyClass, valClass);
        }

        private Node<K, V> reference(K key, boolean createIfAbsent)
        {
            stats.queries++;
            AccordStateCache.this.stats.queries++;

            Node<K, V> node = (Node<K, V>) cache.get(key);
            if (node == null)
            {
                stats.misses++;
                AccordStateCache.this.stats.misses++;
                if (!createIfAbsent)
                    return null;
                node = new Node<>(key);
                // need to store ref right away, so eviction can not remove
                node.references++;
                cache.put(key, node);
                updateSize(node, heapEstimator);
                maybeEvict();
            }
            else
            {
                stats.hits++;
                AccordStateCache.this.stats.hits++;
                if (node.references == 0)
                    unlink(node);
                else
                    Invariants.checkState(node.isUnlinked());
                node.references++;
            }

            return node;
        }

        public S reference(K key)
        {
            Node<K, V> node = reference(key, true);
            return safeRefFactory.apply(node);
        }

        public S referenceAndGetIfLoaded(K key)
        {
            Node<K, V> node = reference(key, false);
            if (node == null || !node.isLoaded())
                return null;
            return safeRefFactory.apply(node);
        }

        @VisibleForTesting
        public Node<K, V> getUnsafe(K key)
        {
            return (Node<K, V>) cache.get(key);
        }

        @VisibleForTesting
        public boolean isReferenced(K key)
        {
            Node<K, V> node = (Node<K, V>) cache.get(key);
            return node != null && node.references > 0;
        }

        @VisibleForTesting
        public boolean isLoaded(K key)
        {
            Node<K, V> node = (Node<K, V>) cache.get(key);
            return node != null && node.isLoaded();
        }

        public void release(S safeRef)
        {
            K key = safeRef.global().key();
            logger.trace("Releasing resources for {}: {}", key, safeRef);
            maybeClearAsyncResult(key);
            Node<K, V> node = (Node<K, V>) cache.get(key);
            Invariants.checkState(node != null && node.references > 0, "node is null or references are zero for %s (%s)", key, node);

            Invariants.checkState(safeRef.global() == node);
            if (node.isLoaded() && (safeRef.hasUpdate() || node.shouldUpdateSize()))
            {
                node.value(safeRef.current());
                updateSize(node, heapEstimator);
            }

            if (--node.references == 0)
            {
                logger.trace("Moving {} from active pool to cache", key);
                Invariants.checkState(node.isUnlinked());
                push(node);
            }

            maybeEvict();
        }

        @VisibleForTesting
        boolean canEvict(K key)
        {
            return AccordStateCache.this.canEvict(cache.get(key));
        }

        @VisibleForTesting
        public boolean hasLoadResult(K key)
        {
            Node<?, ?> node = cache.get(key);
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

    public <K, V, S extends AccordSafeState<K, V>> Instance<K, V, S> instance(Class<K> keyClass, Class<V> valClass,
                                                                           Function<AccordLoadingState<K, V>, S> safeRefFactory,
                                                                           ToLongFunction<V> heapEstimator)
    {
        Instance<K, V, S> instance = new Instance<>(keyClass, valClass, safeRefFactory, heapEstimator);
        if (!instances.add(instance))
            throw new IllegalArgumentException(String.format("Cache instances for types %s -> %s already exists",
                                                             keyClass.getName(), valClass.getName()));
        return instance;
    }

    @VisibleForTesting
    int numReferencedEntries()
    {
        return cache.size() - linked;
    }

    @VisibleForTesting
    int numUnreferencedEntries()
    {
        return linked;
    }

    @VisibleForTesting
    int totalNumEntries()
    {
        return cache.size();
    }

    @VisibleForTesting
    long bytesCached()
    {
        return bytesCached;
    }

    @VisibleForTesting
    boolean keyIsReferenced(Object key)
    {
        Node<?, ?> node = cache.get(key);
        return node != null && node.references > 0;
    }

    @VisibleForTesting
    boolean keyIsCached(Object key)
    {
        return cache.containsKey(key);
    }

    @VisibleForTesting
    int references(Object key)
    {
        Node<?, ?> node = cache.get(key);
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

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

import org.junit.Assert;
import org.junit.Test;

import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import org.apache.cassandra.service.accord.AccordTestUtils.TestableLoad;

public class AccordStateCacheTest
{
    private static final long DEFAULT_ITEM_SIZE = 100;
    private static final long KEY_SIZE = 4;
    private static final long DEFAULT_NODE_SIZE = nodeSize(DEFAULT_ITEM_SIZE);

    private static class Item implements AccordSafeState<Integer>
    {
        boolean invalidated;
        long size = DEFAULT_ITEM_SIZE;

        Integer original;
        Integer current;

        public Item(Integer current)
        {
            this.current = current;
        }

        @Override
        public Integer original()
        {
            return original;
        }

        @Override
        public void resetOriginal()
        {
            original = current;
        }

        @Override
        public Integer current()
        {
            return current;
        }

        public void set(Integer update)
        {
            current = update;
        }

        public long estimatedSizeOnHeap()
        {
            return size + KEY_SIZE;
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
    }

    private static long emptyNodeSize()
    {
        return AccordStateCache.Node.EMPTY_SIZE;
    }

    private static long nodeSize(long itemSize)
    {
        return itemSize + KEY_SIZE + emptyNodeSize();
    }

    private static void assertCacheState(AccordStateCache cache, int active, int cached, long bytes)
    {
        Assert.assertEquals(active, cache.numActiveEntries());
        Assert.assertEquals(cached, cache.numCachedEntries());
        Assert.assertEquals(bytes, cache.bytesCached());
    }

    @Test
    public void testAcquisitionAndRelease()
    {
        AccordStateCache cache = new AccordStateCache(500);
        AccordStateCache.Instance<Integer, Item> instance = cache.instance(Integer.class, Item.class);
        assertCacheState(cache, 0, 0, 0);

        TestableLoad<Integer, Item> load1 = new TestableLoad<>();
        AsyncResult<Void> result1 = instance.referenceAndLoad(1, load1);
        assertCacheState(cache, 1, 0, emptyNodeSize());
        Assert.assertFalse(result1.isDone());
        Assert.assertNull(cache.head);
        Assert.assertNull(cache.tail);

        Item item1 = new Item(1);
        item1.size = 110;
        load1.complete(item1);
        Assert.assertTrue(result1.isDone());
        Assert.assertSame(item1, instance.getActive(1));

        instance.release(1, item1);
        assertCacheState(cache, 0, 1, nodeSize(110));
        Assert.assertSame(item1, cache.tail.value());
        Assert.assertSame(item1, cache.head.value());

        TestableLoad<Integer, Item> load2 = new TestableLoad<>();
        instance.referenceAndLoad(2, load2);
        Item item2 = new Item(2);
        load2.complete(item2);
        instance.getActive(2);
        assertCacheState(cache, 1, 1, DEFAULT_NODE_SIZE + nodeSize(110));
        instance.release(2, item2);
        assertCacheState(cache, 0, 2, DEFAULT_NODE_SIZE + nodeSize(110));

        Assert.assertSame(item1, cache.tail.value());
        Assert.assertSame(item2, cache.head.value());
    }

    @Test
    public void testRotation()
    {
        AccordStateCache cache = new AccordStateCache(DEFAULT_NODE_SIZE * 5);
        AccordStateCache.Instance<Integer, Item> instance = cache.instance(Integer.class, Item.class);
        assertCacheState(cache, 0, 0, 0);

        Item[] items = new Item[3];
        for (int i=0; i<3; i++)
        {
            TestableLoad<Integer, Item> load = new TestableLoad<>();
            AsyncResult<Void> result = instance.referenceAndLoad(i, load);
            Assert.assertNotNull(result);
            Item item = new Item(i);
            items[i] = item;
            load.complete(item);
            instance.getActive(i);
            instance.release(i, item);
        }

        Assert.assertSame(items[0], cache.tail.value());
        Assert.assertSame(items[2], cache.head.value());
        assertCacheState(cache, 0, 3, DEFAULT_NODE_SIZE * 3);

        TestableLoad<Integer, Item> load = new TestableLoad<>();
        instance.referenceAndLoad(1, load);
        load.assertNotLoaded();
        assertCacheState(cache, 1, 2, DEFAULT_NODE_SIZE * 3);

        // releasing item should return it to the head
        Item item = instance.getActive(1);
        instance.release(1, item);
        assertCacheState(cache, 0, 3, DEFAULT_NODE_SIZE * 3);
        Assert.assertSame(items[0], cache.tail.value());
        Assert.assertSame(items[1], cache.head.value());
    }

    @Test
    public void testEvictionOnAcquire()
    {
        AccordStateCache cache = new AccordStateCache(DEFAULT_NODE_SIZE * 5);
        AccordStateCache.Instance<Integer, Item> instance = cache.instance(Integer.class, Item.class);
        assertCacheState(cache, 0, 0, 0);

        Item[] items = new Item[5];
        for (int i=0; i<5; i++)
        {
            TestableLoad<Integer, Item> load = new TestableLoad<>();
            instance.referenceAndLoad(i, load);
            Item item = new Item(i);
            items[i] = item;
            load.complete(item);
            instance.getActive(i);
            instance.release(i, item);
        }

        assertCacheState(cache, 0, 5, DEFAULT_NODE_SIZE * 5);
        Assert.assertSame(items[0], cache.tail.value());
        Assert.assertSame(items[4], cache.head.value());

        TestableLoad<Integer, Item> load = new TestableLoad<>();
        instance.referenceAndLoad(5, load);
        Item item = new Item(5);
        load.complete(item);
        instance.getActive(5);
        assertCacheState(cache, 1, 4, DEFAULT_NODE_SIZE * 5);
        Assert.assertSame(items[1], cache.tail.value());
        Assert.assertSame(items[4], cache.head.value());
        Assert.assertFalse(cache.keyIsCached(0));
        Assert.assertFalse(cache.keyIsActive(0));
    }

    @Test
    public void testEvictionOnRelease()
    {
        AccordStateCache cache = new AccordStateCache(DEFAULT_NODE_SIZE * 4);
        AccordStateCache.Instance<Integer, Item> instance = cache.instance(Integer.class, Item.class);
        assertCacheState(cache, 0, 0, 0);

        Item[] items = new Item[5];
        for (int i=0; i<5; i++)
        {
            TestableLoad<Integer, Item> load = new TestableLoad<>();
            instance.referenceAndLoad(i, load);
            Item item = new Item(i);
            load.complete(item);
            instance.getActive(i);
            items[i] = item;
        }

        assertCacheState(cache, 5, 0, DEFAULT_NODE_SIZE * 5);
        Assert.assertNull(cache.head);
        Assert.assertNull(cache.tail);

        instance.release(2, items[2]);
        assertCacheState(cache, 4, 0, DEFAULT_NODE_SIZE * 4);
        Assert.assertNull(cache.head);
        Assert.assertNull(cache.tail);

        instance.release(4, items[4]);
        assertCacheState(cache, 3, 1, DEFAULT_NODE_SIZE * 4);
        Assert.assertSame(items[4], cache.tail.value());
        Assert.assertSame(items[4], cache.head.value());
    }

    @Test
    public void testMultiAcquireRelease()
    {
        AccordStateCache cache = new AccordStateCache(DEFAULT_NODE_SIZE * 4);
        AccordStateCache.Instance<Integer, Item> instance = cache.instance(Integer.class, Item.class);
        assertCacheState(cache, 0, 0, 0);

        TestableLoad<Integer, Item> load = new TestableLoad<>();
        AsyncResult<Void> result = instance.referenceAndLoad(0, load);
        Assert.assertNotNull(result);
        Item item = new Item(0);
        load.complete(item);
        instance.getActive(0);

        Assert.assertNotNull(item);
        Assert.assertEquals(1, cache.references(0));
        assertCacheState(cache, 1, 0, DEFAULT_NODE_SIZE);

        result = instance.referenceAndLoad(0, load);
        Assert.assertNull(result);
        Assert.assertEquals(2, cache.references(0));
        assertCacheState(cache, 1, 0, DEFAULT_NODE_SIZE);

        instance.release(0, item);
        assertCacheState(cache, 1, 0, DEFAULT_NODE_SIZE);
        instance.release(0, item);
        assertCacheState(cache, 0, 1, DEFAULT_NODE_SIZE);
    }

    @Test
    public void evictionBlockedOnSaveFuture()
    {
        AccordStateCache cache = new AccordStateCache(DEFAULT_NODE_SIZE * 4);
        AccordStateCache.Instance<Integer, Item> instance = cache.instance(Integer.class, Item.class);
        assertCacheState(cache, 0, 0, 0);

        Item[] items = new Item[4];
        for (int i=0; i<4; i++)
        {
            TestableLoad<Integer, Item> load = new TestableLoad<>();
            instance.referenceAndLoad(i, load);
            Item item = new Item(i);
            load.complete(item);
            instance.getActive(i);
            instance.release(i, item);
        }

        assertCacheState(cache, 0, 4, DEFAULT_NODE_SIZE * 4);

        AsyncResult<Void> saveFuture = AsyncResults.settable();
        instance.addSaveResult(0, saveFuture);
        cache.setMaxSize(0);

        // all should have been evicted except 0
        assertCacheState(cache, 0, 1, DEFAULT_NODE_SIZE);
        Assert.assertTrue(cache.keyIsCached(0));
        Assert.assertFalse(cache.keyIsCached(1));
        Assert.assertFalse(cache.keyIsCached(2));
        Assert.assertFalse(cache.keyIsCached(3));
    }

    // if a future is added and another one exists for the same key, they should be merged
    @Test
    public void testFutureMerging()
    {
        AccordStateCache cache = new AccordStateCache(500);
        AccordStateCache.Instance<Integer, Item> instance = cache.instance(Integer.class, Item.class);
        AsyncResult.Settable<Void> promise1 = AsyncResults.settable();
        AsyncResult.Settable<Void> promise2 = AsyncResults.settable();
        instance.addSaveResult(5, promise1);
        instance.addSaveResult(5, promise2);

        AsyncResult<?> future = instance.getSaveResult(5);
        Assert.assertNotSame(future, promise1);
        Assert.assertNotSame(future, promise2);

        Assert.assertFalse(future.isDone());

        promise1.setSuccess(null);
        Assert.assertFalse(future.isDone());

        promise2.setSuccess(null);
        Assert.assertTrue(future.isDone());
    }

    @Test
    public void testUpdates()
    {
        AccordStateCache cache = new AccordStateCache(500);
        AccordStateCache.Instance<Integer, Item> instance = cache.instance(Integer.class, Item.class);
        assertCacheState(cache, 0, 0, 0);

        TestableLoad<Integer, Item> load = new TestableLoad<>();
        AsyncResult<Void> result = instance.referenceAndLoad(1, load);
        assertCacheState(cache, 1, 0, emptyNodeSize());
        Assert.assertFalse(result.isDone());
        Assert.assertNull(cache.head);
        Assert.assertNull(cache.tail);

        Item original = new Item(1);
        original.size = 110;
        load.complete(original);
        Assert.assertTrue(result.isDone());
        Assert.assertSame(original, instance.getActive(1));
        assertCacheState(cache, 1, 0, nodeSize(110));

        original.set(2);
        original.size = 111;
        instance.release(1, original);
        assertCacheState(cache, 0, 1, nodeSize(111));
        Assert.assertSame(original, cache.tail.value());
        Assert.assertSame(original, cache.head.value());

    }
}

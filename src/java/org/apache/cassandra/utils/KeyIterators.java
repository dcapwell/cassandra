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

package org.apache.cassandra.utils;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class KeyIterators
{
    private KeyIterators() {}

    protected static class Reducer extends MergeIterator.Reducer<DecoratedKey, DecoratedKey>
    {
        DecoratedKey merged = null;
        @Override
        public void reduce(int idx, DecoratedKey current)
        {
            merged = current;
        }

        @Override
        protected DecoratedKey getReduced()
        {
            return merged;
        }

        @Override
        protected void onKeyChange()
        {
            merged = null;
        }
    }

    /**
     * Returns a DecoratedKey iterator for the given range. Skips reading data files for sstable formats with a partition index file
     *
     * @param range
     * @return
     */
    private static CloseableIterator<DecoratedKey> keyIterator(Memtable memtable, AbstractBounds<PartitionPosition> range)
    {

        AbstractBounds<PartitionPosition> memtableRange = range.withNewRight(memtable.metadata().partitioner.getMinimumToken().maxKeyBound());
        DataRange dataRange = new DataRange(memtableRange, new ClusteringIndexSliceFilter(Slices.ALL, false));
        UnfilteredPartitionIterator iter = memtable.partitionIterator(ColumnFilter.NONE, dataRange, SSTableReadsListener.NOOP_LISTENER);
        return new AbstractIterator<>()
        {
            @Override
            protected DecoratedKey computeNext()
            {
                while (iter.hasNext())
                {
                    DecoratedKey key = iter.next().partitionKey();
                    if (range.contains(key))
                        return key;

                    if (key.compareTo(range.right) >= 0)
                        break;

                    return key;
                }
                return endOfData();
            }

            @Override
            public void close()
            {
                try
                {
                    super.close();
                }
                finally
                {
                    iter.close();
                }
            }
        };
    }

    public static CloseableIterator<DecoratedKey> keyIterator(TableMetadata metadata, AbstractBounds<PartitionPosition> range) throws IOException
    {
        ColumnFamilyStore cfs = Keyspace.openAndGetStore(metadata);
        ColumnFamilyStore.ViewFragment view = cfs.select(View.selectLive(range));

        List<CloseableIterator<?>> closeableIterators = new ArrayList<>();
        List<Iterator<DecoratedKey>> iterators = new ArrayList<>();

        try
        {
            for (Memtable memtable : view.memtables)
            {
                CloseableIterator<DecoratedKey> iter = keyIterator(memtable, range);
                iterators.add(iter);
                closeableIterators.add(iter);
            }

            for (SSTableReader sstable : view.sstables)
            {
                CloseableIterator<DecoratedKey> iter = sstable.keyIterator(range);
                iterators.add(iter);
                closeableIterators.add(iter);
            }
        }
        catch (Throwable e)
        {
            for (CloseableIterator<?> iter: closeableIterators)
            {
                try
                {
                    iter.close();
                }
                catch (Throwable e2)
                {
                    e.addSuppressed(e2);
                }
            }
            throw e;
        }

        return MergeIterator.get(iterators, DecoratedKey::compareTo, new Reducer());
    }
}

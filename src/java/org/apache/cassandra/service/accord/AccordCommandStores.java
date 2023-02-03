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

import java.util.stream.IntStream;

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.ProgressLog;
import accord.local.CommandStores;
import accord.local.NodeTimeService;
import accord.local.PreLoadContext;
import accord.local.SafeCommandStore;
import accord.local.ShardDistributor;
import accord.primitives.Routables;
import accord.topology.Topology;
import accord.utils.MapReduceConsume;

public class AccordCommandStores extends CommandStores<AccordCommandStore>
{
    private long cacheSize;
    AccordCommandStores(NodeTimeService time, Agent agent, DataStore store,
                        ShardDistributor shardDistributor, ProgressLog.Factory progressLogFactory)
    {
        super(time, agent, store, shardDistributor, progressLogFactory, AccordCommandStore::new);
        setCacheSize(maxCacheSize());
    }

    synchronized void setCacheSize(long bytes)
    {
        cacheSize = bytes;
        refreshCacheSizes();
    }

    synchronized void refreshCacheSizes()
    {
        if (count() == 0)
            return;
        long perStore = cacheSize / count();
        // TODO (low priority, safety): we might transiently breach our limit if we increase one store before decreasing another
        forEach(commandStore -> ((AccordSafeCommandStore) commandStore).commandStore().setCacheSize(perStore));
    }

    private static long maxCacheSize()
    {
        return 5 << 20; // TODO (required): make configurable
    }

    @Override
    public synchronized void updateTopology(Topology newTopology)
    {
        super.updateTopology(newTopology);
        refreshCacheSizes();
    }

    @Override
    public synchronized void shutdown()
    {
        super.shutdown();
        //TODO shutdown isn't useful by itself, we need a way to "wait" as well.  Should be AutoCloseable or offer awaitTermination as well (think Shutdownable interface)
    }

    @Override
    public <O> void mapReduceConsume(PreLoadContext context, Routables<?, ?> keys, long minEpoch, long maxEpoch, MapReduceConsume<? super SafeCommandStore, O> mapReduceConsume)
    {
        mapReduceConsume(context, keys, minEpoch, maxEpoch, mapReduceConsume, CommandStores.AsyncMapReduceAdapter.instance());
    }

    @Override
    public <O> void mapReduceConsume(PreLoadContext context, IntStream commandStoreIds, MapReduceConsume<? super SafeCommandStore, O> mapReduceConsume)
    {
        mapReduceConsume(context, commandStoreIds, mapReduceConsume, CommandStores.AsyncMapReduceAdapter.instance());
    }
}

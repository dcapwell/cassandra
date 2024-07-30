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

package org.apache.cassandra.service.accord.serializers;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import accord.api.Key;
import accord.primitives.Deps;
import accord.primitives.PartialDeps;
import accord.primitives.Range;
import accord.primitives.Ranges;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.IVersionedSerializers;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaProvider;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.SentinelKey;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.utils.AccordGenerators;
import org.mockito.Mockito;

import static accord.utils.Property.qt;
import static org.apache.cassandra.net.MessagingService.Version.VERSION_51;

public class DepsSerializerTest
{
    static
    {
        DatabaseDescriptor.clientInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    private static final List<MessagingService.Version> SUPPORTED_VERSIONS = VERSION_51.greaterThanOrEqual();

    @Test
    public void depsSerde()
    {
        DataOutputBuffer buffer = new DataOutputBuffer();
        qt().check(rs -> {
            IPartitioner partitioner = AccordGenerators.partitioner().next(rs);
            Schema.instance = Mockito.mock(SchemaProvider.class);
            DatabaseDescriptor.setPartitionerUnsafe(partitioner);
            Mockito.when(Schema.instance.getExistingTablePartitioner(Mockito.any())).thenReturn(partitioner);
            Deps deps = AccordGenerators.depsGen(partitioner).next(rs);
            for (MessagingService.Version version : SUPPORTED_VERSIONS)
                IVersionedSerializers.testSerde(buffer, DepsSerializer.deps, deps, version.value);
        });
    }

    @Test
    public void partialDepsSerde()
    {
        DataOutputBuffer buffer = new DataOutputBuffer();
        qt().check(rs -> {
            IPartitioner partitioner = AccordGenerators.partitioner().next(rs);
            Schema.instance = Mockito.mock(SchemaProvider.class);
            DatabaseDescriptor.setPartitionerUnsafe(partitioner);
            Mockito.when(Schema.instance.getExistingTablePartitioner(Mockito.any())).thenReturn(partitioner);
            Deps deps = AccordGenerators.depsGen(partitioner).next(rs);
            PartialDeps partialDeps = toPartial(deps);
            for (MessagingService.Version version : SUPPORTED_VERSIONS)
                IVersionedSerializers.testSerde(buffer, DepsSerializer.partialDeps, partialDeps, version.value);
        });
    }

    private static PartialDeps toPartial(Deps deps)
    {
        Set<TableId> tables = new HashSet<>();
        for (Key key : deps.keyDeps.keys())
            tables.add(((PartitionKey) key).table());
        for (Key key : deps.directKeyDeps.keys())
            tables.add(((PartitionKey) key).table());
        for (Range range : deps.rangeDeps.covering())
            tables.add(((TokenRange) range).table());
        List<TokenRange> covering = new ArrayList<>(tables.size());
        for (TableId table : tables)
            covering.add(new TokenRange(SentinelKey.min(table), SentinelKey.max(table)));
        return deps.slice(Ranges.of(covering.toArray(Range[]::new)));
    }
}
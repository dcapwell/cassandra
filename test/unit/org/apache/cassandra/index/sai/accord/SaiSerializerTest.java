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

package org.apache.cassandra.index.sai.accord;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.accord.SaiSerializer.TokenOrSentinelSerializer;
import org.apache.cassandra.utils.AccordGenerators;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.CassandraGenerators;
import org.assertj.core.api.Assertions;

import static accord.utils.Property.qt;
import static org.apache.cassandra.utils.AccordGenerators.fromQT;
import static org.apache.cassandra.utils.CassandraGenerators.murmurToken;

public class SaiSerializerTest
{
    private static final TokenOrSentinelSerializer serializer = new TokenOrSentinelSerializer(Murmur3Partitioner.instance);
    static
    {
        DatabaseDescriptor.clientInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    @Test
    public void tokenSerde()
    {
        qt().forAll(fromQT(murmurToken())).check(token -> {
            var bytes = serializer.serializeRaw(token);
            Assertions.assertThat(bytes)
                      .hasSize(serializer.valueSize())
                      .hasSize(serializer.min().length)
                      .hasSize(serializer.max().length);

            Assertions.assertThat(ByteArrayUtil.compareUnsigned(serializer.min(), 0, bytes, 0, bytes.length)).isLessThan(0);
            Assertions.assertThat(ByteArrayUtil.compareUnsigned(serializer.max(), 0, bytes, 0, bytes.length)).isGreaterThan(0);

            var read = serializer.deserializeToken(bytes);
            Assertions.assertThat(read).isEqualTo(token);
        });
    }

    @Test
    public void accordRoutingKeySerde()
    {
        qt().forAll(AccordGenerators.routingKeyGen(fromQT(CassandraGenerators.TABLE_ID_GEN), fromQT(murmurToken()))).check(key -> {
            var bb = SaiSerializer.serializeRoutingKey(key);
            var read = SaiSerializer.deserializeRoutingKey(bb);
            Assertions.assertThat(read).isEqualTo(key);
        });
    }
}
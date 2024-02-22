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

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.ByteArrayAccessor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.service.accord.serializers.ByteOrderedTokenSerializers.FixedLength;
import org.apache.cassandra.utils.AccordGenerators;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.CassandraGenerators;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.assertj.core.api.Assertions;

import static accord.utils.Property.qt;
import static org.apache.cassandra.utils.AccordGenerators.fromQT;
import static org.apache.cassandra.utils.CassandraGenerators.murmurToken;

public class ByteOrderedTokenSerializersTest
{
    private static final FixedLength MURMUR = new FixedLength(Murmur3Partitioner.instance, ByteComparable.Version.OSS50);
    private static final byte[] MAX = ByteSourceInverse.readBytes(MURMUR.maxAsComparableBytes());
    private static final byte[] MIN = ByteSourceInverse.readBytes(MURMUR.minAsComparableBytes());

    static
    {
        DatabaseDescriptor.clientInitialization();
        // AccordRoutingKey$TokenKey reaches into DD to get partitioner, so need to set that up...
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    @Test
    public void fixedLengthTokenSerde()
    {
        qt().forAll(fromQT(murmurToken())).check(token -> {
            var bytes = MURMUR.serialize(token);
            // working
            // [64, -128, 64, 64, 120, -113, 47, 11, -78, -83, 121, 56]
            Assertions.assertThat(bytes)
                      .hasSize(MURMUR.valueSize())
                      .hasSize(MIN.length)
                      .hasSize(MAX.length);

            Assertions.assertThat(ByteArrayUtil.compareUnsigned(MIN, 0, bytes, 0, bytes.length)).isLessThan(0);
            Assertions.assertThat(ByteArrayUtil.compareUnsigned(MAX, 0, bytes, 0, bytes.length)).isGreaterThan(0);

            var read = MURMUR.tokenFromComparableBytes(ByteArrayAccessor.instance, bytes);
            Assertions.assertThat(read).isEqualTo(token);
        });
    }

    @Test
    public void fixedLengthAccordRoutingKeySerde()
    {
        qt().forAll(AccordGenerators.routingKeyGen(fromQT(CassandraGenerators.TABLE_ID_GEN), fromQT(murmurToken()))).check(key -> {
            var read = MURMUR.fromComparableBytes(ByteArrayAccessor.instance, MURMUR.serialize(key));
            Assertions.assertThat(read).isEqualTo(key);
        });
    }
}
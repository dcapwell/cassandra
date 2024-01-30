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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;
import java.util.function.Function;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.ByteArrayAccessor;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

public class SaiSerializer
{
    private static final TokenOrSentinelSerializer SERIALIZER = new TokenOrSentinelSerializer(DatabaseDescriptor.getPartitioner());

    public static int storeId(PrimaryKey pk)
    {
        var split = AccordKeyspace.CommandRows.splitPartitionKey(pk.partitionKey());
        return AccordKeyspace.CommandRows.getStoreId(split);
    }

    public static byte[] unwrap(AccordRoutingKey key)
    {
        return key.kindOfRoutingKey() == AccordRoutingKey.RoutingKeyKind.SENTINEL ? key.asSentinelKey().isMin ? tokenOrSentinelSerializer().min() : tokenOrSentinelSerializer().max()
                                                                                  : tokenOrSentinelSerializer().serializeRaw(key.asTokenKey().token());
    }

    public static ByteBuffer serializeRoutingKey(AccordRoutingKey key)
    {
        var uuid = key.table().asUUID();
        ByteSource[] srcs = { LongType.instance.asComparableBytes(LongType.instance.decompose(uuid.getMostSignificantBits()), ByteComparable.Version.OSS50),
                              LongType.instance.asComparableBytes(LongType.instance.decompose(uuid.getLeastSignificantBits()), ByteComparable.Version.OSS50),
                              SERIALIZER.serialize(key), };
        var bs = ByteSource.withTerminator(ByteSource.TERMINATOR, srcs);
        return ByteBuffer.wrap(ByteSourceInverse.readBytes(bs));
    }

    public static AccordRoutingKey deserializeRoutingKey(ByteBuffer bb)
    {
        var bs = ByteSource.peekable(ByteSource.fixedLength(bb));
        long[] uuidValues = new long[2];
        for (int i = 0; i < 2; i++)
        {
            if (bs.peek() == ByteSource.TERMINATOR)
                throw new IllegalArgumentException("Unable to parse bytes");
            ByteSource.Peekable component = ByteSourceInverse.nextComponentSource(bs);
            long value = LongType.instance.compose(LongType.instance.fromComparableBytes(component, ByteComparable.Version.OSS50));
            uuidValues[i] = value;
        }
        TableId tableId = TableId.fromUUID(new UUID(uuidValues[0], uuidValues[1]));

        try
        {
            return SERIALIZER.deserialize(bs,
                                          isMin -> isMin ? AccordRoutingKey.SentinelKey.min(tableId) : AccordRoutingKey.SentinelKey.max(tableId),
                                          token -> new AccordRoutingKey.TokenKey(tableId, token));
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    public static TokenOrSentinelSerializer tokenOrSentinelSerializer()
    {
        return SERIALIZER;
    }

    public static class TokenOrSentinelSerializer
    {
        private static final ByteComparable.Version VERSION = ByteComparable.Version.OSS50;
        private static final byte[] MIN_ORDER = { -1};
        private static final byte[] TOKEN_ORDER = {0};
        private static final byte[] MAX_ORDER = {1};

        private final IPartitioner partitioner;
        private final int tokenSize;
        private final byte[] minOrder, maxOrder;
        private final byte[] empty;

        public TokenOrSentinelSerializer(IPartitioner partitioner)
        {
            this.partitioner = partitioner;
            if (!partitioner.isFixedLength())
                throw new IllegalArgumentException("Unable to use partitioner " + partitioner.getClass() + "; it is not fixed-length");
            tokenSize = ByteSourceInverse.readBytes(partitioner.getMinimumToken().asComparableBytes(VERSION)).length;
//            byte[] empty = new byte[tokenSize];
            empty = new byte[tokenSize];
            minOrder = ByteSourceInverse.readBytes(ByteSource.withTerminator(ByteSource.TERMINATOR, minPrefix(), ByteSource.fixedLength(empty)));
            maxOrder = ByteSourceInverse.readBytes(ByteSource.withTerminator(ByteSource.TERMINATOR, maxPrefix(), ByteSource.fixedLength(empty)));
            if (minOrder.length != valueSize())
                throw new IllegalStateException("Min order has the wrong size: found " + minOrder.length + "; need " + valueSize());
            if (maxOrder.length != valueSize())
                throw new IllegalStateException("Max order has the wrong size: found " + maxOrder.length + "; need " + valueSize());
        }

        public int valueSize()
        {
            return 4 + tokenSize;
        }

        public byte[] min()
        {
            return minOrder;
        }

        public byte[] max()
        {
            return maxOrder;
        }

        private static ByteSource minPrefix()
        {
            return ByteSource.signedFixedLengthNumber(ByteArrayAccessor.instance, MIN_ORDER);
        }

        private static ByteSource tokenPrefix()
        {
            return ByteSource.signedFixedLengthNumber(ByteArrayAccessor.instance, TOKEN_ORDER);
        }

        private static ByteSource maxPrefix()
        {
            return ByteSource.signedFixedLengthNumber(ByteArrayAccessor.instance, MAX_ORDER);
        }

        public ByteSource serialize(Token token)
        {
            if (token.getPartitioner() != partitioner)
                throw new IllegalArgumentException("Attempted to use the wrong partitioner: given " + token.getPartitioner() + " but expected " + partitioner);
            ByteSource[] srcs = {tokenPrefix(), token.asComparableBytes(VERSION)};
            return ByteSource.withTerminator(ByteSource.TERMINATOR, srcs);
        }

        public ByteSource serialize(AccordRoutingKey key)
        {
            if (key.kindOfRoutingKey() == AccordRoutingKey.RoutingKeyKind.SENTINEL)
            {
                var sentinel = key.asSentinelKey();
                return sentinel.isMin ? ByteSource.withTerminator(ByteSource.TERMINATOR, minPrefix(), ByteSource.fixedLength(empty))
                                      : ByteSource.withTerminator(ByteSource.TERMINATOR, maxPrefix(), ByteSource.fixedLength(empty));
//                return ByteSource.fixedLength(sentinel.isMin ? min() : max());
            }
            else
            {
                return serialize(key.token());
            }
        }

        public byte[] serializeRaw(Token token)
        {
            return ByteSourceInverse.readBytes(serialize(token));
        }

        public Token deserializeToken(byte[] bytes) throws IOException
        {
            var bs = ByteSource.peekable(ByteSource.fixedLength(bytes));
            if (bs.peek() == ByteSource.TERMINATOR)
                throw new IOException("Unable to read prefix");
            ByteSource.Peekable component = ByteSourceInverse.nextComponentSource(bs);
            if (component == null)
                throw new IOException("Unable to read prefix; component was not found");

            var prefix = ByteSourceInverse.getOptionalSignedFixedLength(ByteArrayAccessor.instance, component, 1);
            if (prefix == null)
                throw new IOException("Unable to read prefix; prefix was null");
            if (!Arrays.equals(TOKEN_ORDER, prefix))
            {
                String match = Arrays.equals(MIN_ORDER, prefix) ? "min"
                               : Arrays.equals(MAX_ORDER, prefix) ? "max"
                                 : "unknown";
                throw new IOException("Attempt to read token from non-token value: was " + match);
            }

            component = ByteSourceInverse.nextComponentSource(bs);
            if (component == null)
                throw new IOException("Unable to read token; component was not found");
            return partitioner.getTokenFactory().fromComparableBytes(component, VERSION);
        }

        public AccordRoutingKey deserialize(ByteSource.Peekable bs,
                                            Function<Boolean, AccordRoutingKey> onSentinel,
                                            Function<Token, AccordRoutingKey> onToken) throws IOException
        {
            if (bs.peek() == ByteSource.TERMINATOR)
                throw new IOException("Unable to read prefix");
            ByteSource.Peekable component = ByteSourceInverse.nextComponentSource(bs);
            if (component == null)
                throw new IOException("Unable to read prefix; component was not found");
            if (component.peek() == ByteSource.NEXT_COMPONENT)
            {
                // this came from (table, token_or_sentinel)
                component = ByteSourceInverse.nextComponentSource(bs);
                if (component == null)
                    throw new IOException("Unable to read prefix; component was not found");
            }

            var prefix = ByteSourceInverse.getOptionalSignedFixedLength(ByteArrayAccessor.instance, component, 1);
            if (prefix == null)
                throw new IOException("Unable to read prefix; prefix was null");
            if (Arrays.equals(TOKEN_ORDER, prefix))
            {
                component = ByteSourceInverse.nextComponentSource(bs);
                if (component == null)
                    throw new IOException("Unable to read token; component was not found");
                return onToken.apply(partitioner.getTokenFactory().fromComparableBytes(component, VERSION));
            }
            if (Arrays.equals(MIN_ORDER, prefix))
                return onSentinel.apply(true);
            if (Arrays.equals(MAX_ORDER, prefix))
                return onSentinel.apply(false);
            throw new AssertionError("Unknown prefix");
        }
    }
}

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

import java.io.IOException;

import com.google.common.base.Preconditions;

import accord.api.Query;
import accord.api.Read;
import accord.api.Update;
import accord.local.Node;
import accord.local.SaveStatus;
import accord.local.Status;
import accord.local.Status.Durability;
import accord.primitives.Ballot;
import accord.primitives.PartialTxn;
import accord.primitives.Ranges;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.txn.TxnQuery;
import org.apache.cassandra.service.accord.txn.TxnRead;
import org.apache.cassandra.service.accord.txn.TxnUpdate;
import org.apache.cassandra.service.accord.txn.TxnWrite;
import org.apache.cassandra.utils.CastingSerializer;

public class CommandSerializers
{
    private CommandSerializers() {}

    public static final TimestampSerializer<TxnId> txnId = new TimestampSerializer<>(TxnId::fromBits);
    public static final TimestampSerializer<Timestamp> timestamp = new TimestampSerializer<>(Timestamp::fromBits);
    public static final TimestampSerializer<Ballot> ballot = new TimestampSerializer<>(Ballot::fromBits);
    public static final EnumSerializer<Txn.Kind> kind = new EnumSerializer<>(Txn.Kind.class);

    public static class TimestampSerializer<T extends Timestamp> implements IVersionedSerializer<T>
    {
        interface Factory<T extends Timestamp>
        {
            T create(long msb, long lsb, Node.Id node);
        }

        private final TimestampSerializer.Factory<T> factory;

        private TimestampSerializer(TimestampSerializer.Factory<T> factory)
        {
            this.factory = factory;
        }

        @Override
        public void serialize(T ts, DataOutputPlus out, int version) throws IOException
        {
            out.writeLong(ts.msb);
            out.writeLong(ts.lsb);
            TopologySerializers.nodeId.serialize(ts.node, out, version);
        }

        public <V> int serialize(T ts, V dst, ValueAccessor<V> accessor, int offset)
        {
            int position = offset;
            position += accessor.putLong(dst, position, ts.msb);
            position += accessor.putLong(dst, position, ts.lsb);
            position += TopologySerializers.nodeId.serialize(ts.node, dst, accessor, position);
            int size = position - offset;
            Preconditions.checkState(size == serializedSize());
            return size;
        }

        @Override
        public T deserialize(DataInputPlus in, int version) throws IOException
        {
            return factory.create(in.readLong(),
                                  in.readLong(),
                                  TopologySerializers.nodeId.deserialize(in, version));
        }

        public <V> T deserialize(V src, ValueAccessor<V> accessor, int offset)
        {
            long msb = accessor.getLong(src, offset);
            offset += TypeSizes.LONG_SIZE;
            long lsb = accessor.getLong(src, offset);
            offset += TypeSizes.LONG_SIZE;
            Node.Id node = TopologySerializers.nodeId.deserialize(src, accessor, offset);
            return factory.create(msb, lsb, node);
        }

        @Override
        public long serializedSize(T ts, int version)
        {
            return serializedSize();
        }

        public int serializedSize()
        {
            return TypeSizes.LONG_SIZE +  // ts.msb
                   TypeSizes.LONG_SIZE +  // ts.lsb
                   TopologySerializers.nodeId.serializedSize();   // ts.node
        }
    }

    public static class PartialTxnSerializer implements IVersionedSerializer<PartialTxn>
    {
        private final IVersionedSerializer<Read> readSerializer;
        private final IVersionedSerializer<Query> querySerializer;
        private final IVersionedSerializer<Update> updateSerializer;

        public PartialTxnSerializer(IVersionedSerializer<Read> readSerializer, IVersionedSerializer<Query> querySerializer, IVersionedSerializer<Update> updateSerializer)
        {
            this.readSerializer = readSerializer;
            this.querySerializer = querySerializer;
            this.updateSerializer = updateSerializer;
        }

        @Override
        public void serialize(PartialTxn txn, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.kind.serialize(txn.kind(), out, version);
            KeySerializers.ranges.serialize(txn.covering(), out, version);
            KeySerializers.seekables.serialize(txn.keys(), out, version);
            readSerializer.serialize(txn.read(), out, version);
            querySerializer.serialize(txn.query(), out, version);
            out.writeBoolean(txn.update() != null);
            if (txn.update() != null)
                updateSerializer.serialize(txn.update(), out, version);
        }

        @Override
        public PartialTxn deserialize(DataInputPlus in, int version) throws IOException
        {
            Txn.Kind kind = CommandSerializers.kind.deserialize(in, version);
            Ranges covering = KeySerializers.ranges.deserialize(in, version);
            Seekables<?, ?> keys = KeySerializers.seekables.deserialize(in, version);
            Read read = readSerializer.deserialize(in, version);
            Query query = querySerializer.deserialize(in, version);
            Update update = in.readBoolean() ? updateSerializer.deserialize(in, version) : null;
            return new PartialTxn.InMemory(covering, kind, keys, read, query, update);
        }

        @Override
        public long serializedSize(PartialTxn txn, int version)
        {
            long size = CommandSerializers.kind.serializedSize(txn.kind(), version);
            size += KeySerializers.ranges.serializedSize(txn.covering(), version);
            size += KeySerializers.seekables.serializedSize(txn.keys(), version);
            size += readSerializer.serializedSize(txn.read(), version);
            size += querySerializer.serializedSize(txn.query(), version);
            size += TypeSizes.sizeof(txn.update() != null);
            if (txn.update() != null)
                size += updateSerializer.serializedSize(txn.update(), version);
            return size;
        }
    }

    private static final IVersionedSerializer<Read> read = new CastingSerializer<>(TxnRead.class, TxnRead.serializer);
    private static final IVersionedSerializer<Query> query = new CastingSerializer<>(TxnQuery.class, TxnQuery.serializer);
    private static final IVersionedSerializer<Update> update = new CastingSerializer<>(TxnUpdate.class, TxnUpdate.serializer);

    public static final IVersionedSerializer<PartialTxn> partialTxn = new PartialTxnSerializer(read, query, update);

    public static final IVersionedSerializer<SaveStatus> saveStatus = new EnumSerializer<>(SaveStatus.class);
    public static final IVersionedSerializer<Status> status = new EnumSerializer<>(Status.class);
    public static final IVersionedSerializer<Durability> durability = new EnumSerializer<>(Durability.class);

    public static final IVersionedSerializer<Writes> writes = new IVersionedSerializer<Writes>()
    {
        @Override
        public void serialize(Writes writes, DataOutputPlus out, int version) throws IOException
        {
            txnId.serialize(writes.txnId, out, version);
            timestamp.serialize(writes.executeAt, out, version);
            KeySerializers.seekables.serialize(writes.keys, out, version);
            boolean hasWrites = writes.write != null;
            out.writeBoolean(hasWrites);
            if (hasWrites)
                TxnWrite.serializer.serialize((TxnWrite) writes.write, out, version);
        }

        @Override
        public Writes deserialize(DataInputPlus in, int version) throws IOException
        {
            return new Writes(txnId.deserialize(in, version), timestamp.deserialize(in, version),
                              KeySerializers.seekables.deserialize(in, version),
                              in.readBoolean() ? TxnWrite.serializer.deserialize(in, version) : null);
        }

        @Override
        public long serializedSize(Writes writes, int version)
        {
            long size = txnId.serializedSize(writes.txnId, version);
            size += timestamp.serializedSize(writes.executeAt, version);
            size += KeySerializers.seekables.serializedSize(writes.keys, version);
            boolean hasWrites = writes.write != null;
            size += TypeSizes.sizeof(hasWrites);
            if (hasWrites)
                size += TxnWrite.serializer.serializedSize((TxnWrite) writes.write, version);
            return size;
        }
    };
}

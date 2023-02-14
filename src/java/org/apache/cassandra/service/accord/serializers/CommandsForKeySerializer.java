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
import java.nio.ByteBuffer;

import accord.impl.CommandsForKey.CommandLoader;
import accord.local.Command;
import accord.local.SaveStatus;
import accord.primitives.PartialDeps;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.io.LocalVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.AccordSerializerVersion;

public class CommandsForKeySerializer
{
    private static final LocalVersionedSerializer<PartialDeps> depsSerializer = new LocalVersionedSerializer<>(AccordSerializerVersion.CURRENT, AccordSerializerVersion.serializer, DepsSerializer.partialDeps);
    public static final CommandLoader<ByteBuffer> loader = new AccordCFKLoader();
    private static class AccordCFKLoader implements CommandLoader<ByteBuffer>
    {
        private static final int HAS_DEPS = 0x01;

        private static final long FIXED_SIZE;
        private static final int FLAG_OFFSET;
        private static final int STATUS_OFFSET;
        private static final int TXNID_OFFSET;
        private static final int EXECUTEAT_OFFSET;
        private static final int DEPS_OFFSET;

        static
        {
            long size = 0;

            FLAG_OFFSET = (int) size;
            size += TypeSizes.BYTE_SIZE;

            STATUS_OFFSET = (int) size;
            size += TypeSizes.BYTE_SIZE;

            TXNID_OFFSET = (int) size;
            size += CommandSerializers.txnId.serializedSize();

            EXECUTEAT_OFFSET = (int) size;
            size += CommandSerializers.timestamp.serializedSize();

            DEPS_OFFSET = (int) size;
            FIXED_SIZE = size;
        }

        private int serializedSize(Command command)
        {
            return (int) (FIXED_SIZE + (command.partialDeps() != null ? depsSerializer.serializedSize(command.partialDeps()) : 0));
        }

        private static final ValueAccessor<ByteBuffer> accessor = ByteBufferAccessor.instance;

        private static byte toByte(int v)
        {
            Invariants.checkArgument(v < Byte.MAX_VALUE, "Value %d is larger than %d", v, Byte.MAX_VALUE);
            return (byte) v;
        }

        private AccordCFKLoader() {}

        @Override
        public ByteBuffer saveForCFK(Command command)
        {
            int flags = 0;

            PartialDeps deps = command.partialDeps();
            if (deps != null)
                flags |= HAS_DEPS;

            int size = serializedSize(command);
            ByteBuffer buffer = accessor.allocate(size);
            accessor.putByte(buffer, FLAG_OFFSET, toByte(flags));
            accessor.putByte(buffer, STATUS_OFFSET, toByte(command.status().ordinal()));
            CommandSerializers.txnId.serialize(command.txnId(), buffer, accessor, TXNID_OFFSET);
            CommandSerializers.timestamp.serialize(command.executeAt(), buffer, accessor, EXECUTEAT_OFFSET);
            if (deps != null)
            {
                ByteBuffer duplicate = buffer.duplicate();
                duplicate.position(DEPS_OFFSET);
                DataOutputPlus out = new DataOutputBuffer(duplicate);
                try
                {
                    depsSerializer.serialize(deps, out);
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }
            return buffer;
        }

        @Override
        public TxnId txnId(ByteBuffer data)
        {
            return CommandSerializers.txnId.deserialize(data, accessor, TXNID_OFFSET);
        }

        @Override
        public Timestamp executeAt(ByteBuffer data)
        {
            return CommandSerializers.timestamp.deserialize(data, accessor, EXECUTEAT_OFFSET);
        }

        @Override
        public SaveStatus saveStatus(ByteBuffer data)
        {
            return SaveStatus.values()[accessor.getByte(data, STATUS_OFFSET)];
        }

        @Override
        public PartialDeps partialDeps(ByteBuffer data)
        {
            byte flags = accessor.getByte(data, FLAG_OFFSET);
            if ((flags & HAS_DEPS) == 0)
                return null;
            ByteBuffer buffer = data.duplicate();
            buffer.position(data.position() + DEPS_OFFSET);
            DataInputBuffer in = new DataInputBuffer(buffer, false);
            try
            {
                return depsSerializer.deserialize(in);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
    }
}

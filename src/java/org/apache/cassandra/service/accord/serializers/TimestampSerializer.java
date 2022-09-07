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

import accord.local.Node;
import accord.txn.Timestamp;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class TimestampSerializer<T extends Timestamp> implements IVersionedSerializer<T>
{
    public static final IVersionedSerializer<Timestamp> instance = new TimestampSerializer<>(Timestamp::new);

    interface Factory<T extends Timestamp>
    {
        T create(long epoch, long real, int logical, Node.Id node);
    }

    private final Factory<T> factory;

    TimestampSerializer(Factory<T> factory)
    {
        this.factory = factory;
    }

    @Override
    public void serialize(T ts, DataOutputPlus out, int version) throws IOException
    {
        out.writeLong(ts.epoch);
        out.writeLong(ts.real);
        out.writeInt(ts.logical);
        IdSerializer.instance.serialize(ts.node, out, version);
    }

    @Override
    public T deserialize(DataInputPlus in, int version) throws IOException
    {
        return factory.create(in.readLong(),
                              in.readLong(),
                              in.readInt(),
                              IdSerializer.instance.deserialize(in, version));
    }

    @Override
    public long serializedSize(T ts, int version)
    {
        return TypeSizes.sizeof(ts.epoch)
               + TypeSizes.sizeof(ts.real)
               + TypeSizes.sizeof(ts.logical)
               + IdSerializer.instance.serializedSize(ts.node, version);
    }
}
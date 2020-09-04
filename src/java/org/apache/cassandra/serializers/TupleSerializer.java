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
package org.apache.cassandra.serializers;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;

public class TupleSerializer implements TypeSerializer<TupleType.Tuple>
{
    public final List<TypeSerializer<?>> fields;

    public TupleSerializer(List<TypeSerializer<?>> fields)
    {
        this.fields = fields;
    }

    @Override
    public ByteBuffer serialize(TupleType.Tuple tuple)
    {
        List<Object> value = tuple.values;
        ByteBuffer[] bbs = new ByteBuffer[value.size()];
        int idx = 0;
        for (Object o : value)
        {
            TypeSerializer<Object> serializer = (TypeSerializer<Object>) fields.get(idx);
            bbs[idx++] = serializer.serialize(o);
        }
        return TupleType.buildValue(bbs);
    }

    @Override
    public TupleType.Tuple deserialize(ByteBuffer bytes)
    {
        ByteBuffer[] bbs = split(bytes);
        List<Object> objects = new ArrayList<>(bbs.length);
        for (int i = 0; i < bbs.length; i++)
            objects.add(fields.get(i).deserialize(bbs[i]));
        return new TupleType.Tuple(objects);
    }

    @Override
    public void validate(ByteBuffer bytes) throws MarshalException
    {
        ByteBuffer input = bytes.duplicate();
        for (int i = 0; i < fields.size(); i++)
        {
            // we allow the input to have less fields than declared so as to support field addition.
            if (!input.hasRemaining())
                return;

            if (input.remaining() < Integer.BYTES)
                throw new MarshalException(String.format("Not enough bytes to read size of %dth component", i));

            int size = input.getInt();

            // size < 0 means null value
            if (size < 0)
                continue;

            if (input.remaining() < size)
                throw new MarshalException(String.format("Not enough bytes to read %dth component", i));

            ByteBuffer field = ByteBufferUtil.readBytes(input, size);
            fields.get(i).validate(field);
        }

        // We're allowed to get less fields than declared, but not more
        if (input.hasRemaining())
            throw new MarshalException("Invalid remaining data after end of tuple value");
    }

    @Override
    public String toString(TupleType.Tuple tuple)
    {
        List<Object> value = tuple.values;
        StringBuilder sb = new StringBuilder();
        boolean isFirst = true;
        sb.append('(');
        int offset = 0;
        for (Object element : value)
        {
            if (isFirst)
                isFirst = false;
            else
                sb.append(", ");
            sb.append(((TypeSerializer<Object>) fields.get(offset++)).toString(element));
        }
        sb.append(')');
        return sb.toString();
    }

    @Override
    public Class<TupleType.Tuple> getType()
    {
        return (Class) List.class;
    }

    private int size()
    {
        return fields.size();
    }

    /**
     * Split a tuple value into its component values.
     */
    private ByteBuffer[] split(ByteBuffer value)
    {
        ByteBuffer[] components = new ByteBuffer[size()];
        ByteBuffer input = value.duplicate();
        for (int i = 0; i < size(); i++)
        {
            if (!input.hasRemaining())
                return Arrays.copyOfRange(components, 0, i);

            int size = input.getInt();

            if (input.remaining() < size)
                throw new MarshalException(String.format("Not enough bytes to read %dth component", i));

            // size < 0 means null value
            components[i] = size < 0 ? null : ByteBufferUtil.readBytes(input, size);
        }

        // error out if we got more values in the tuple/UDT than we expected
        if (input.hasRemaining())
        {
            throw new InvalidRequestException(String.format(
            "Expected %s %s for %s column, but got more",
            size(), size() == 1 ? "value" : "values"/*, this.asCQL3Type()*/));
        }

        return components;
    }
}

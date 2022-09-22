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

package org.apache.cassandra.utils.ast;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.serializers.TimestampSerializer;
import org.apache.cassandra.utils.Hex;

public class Literal implements Value
{
    private final Object value;
    private final AbstractType<?> type;

    public Literal(Object value, AbstractType<?> type)
    {
        this.value = value;
        this.type = type;
    }

    @Override
    public void toCQL(StringBuilder sb, int indent)
    {
        toCQL(value, type, sb, indent);
    }

    @Override
    public AbstractType<?> type()
    {
        return type;
    }

    private void toCQL(Object o, AbstractType<?> type, StringBuilder sb, int indent)
    {
        type = type.unwrap();
        if (o instanceof Element)
        {
            ((Element) o).toCQL(sb, indent);
        }
        else if (o instanceof Number || o instanceof UUID || o instanceof Boolean)
        {
            sb.append(o);
        }
        else if (o instanceof InetAddress)
        {
            // convert to string
            sb.append('\'');
            sb.append(((InetAddress) o).getHostAddress());
            sb.append('\'');
        }
        else if (o instanceof String)
        {
            // see https://docs.datastax.com/en/cql-oss/3.3/cql/cql_reference/escape_char_r.html
            String s = (String) o;
            sb.append('\'').append(s.replace("'", "''")).append('\'');
            //TODO support $$ format...
//            boolean normal = s.length() % 2 == 0;
//            if (normal)
//            {
//                sb.append('\'').append(s.replace("'", "''")).append('\'');
//            }
//            else
//            {
//                sb.append("$$").append(s).append("$$");
//            }
        }
        else if (o instanceof List)
        {
            toCQL((List<Object>) o, ((ListType<?>) type).getElementsType(), '[', ']', sb, indent);
        }
        else if (o instanceof Set)
        {
            toCQL((Set<Object>) o, ((SetType<?>) type).getElementsType(), '{', '}', sb, indent);
        }
        else if (o instanceof Map)
        {
            // see https://docs.datastax.com/en/cql-oss/3.3/cql/cql_using/useInsertMap.html
            MapType<Object, Object> mapType = (MapType<Object, Object>) type;
            Map<Object, Object> map = (Map<Object, Object>) o;
            sb.append('{');
            for (Map.Entry<Object, Object> e : map.entrySet())
            {
                toCQL(e.getKey(), mapType.getKeysType(), sb, indent);
                sb.append(": ");
                toCQL(e.getValue(), mapType.getValuesType(), sb, indent);
                sb.append(", ");
            }
            if (!map.isEmpty())
                sb.setLength(sb.length() - 2);
            sb.append('}');
        }
        else if (o instanceof Date && type instanceof TimestampType)
        {
            sb.append('\'');
            // org.apache.cassandra.serializers.TimestampSerializer.dateStringToTimestamp accepts a ton
            // of possible values... but that gets down to testing that type rather than CQL directly,
            // so it is fine to use a single format
            sb.append(TimestampSerializer.getJsonDateFormatter().format((Date) o));
            sb.append('\'');
        }
        else if (o instanceof ByteBuffer)
        {
            ByteBuffer bb = (ByteBuffer) o;
            // what to do depends on the type param
            if (type instanceof UserType)
            {
                // see https://docs.datastax.com/en/cql-oss/3.3/cql/cql_using/useInsertUDT.html
                UserType udt = (UserType) type;
                ByteBuffer[] split = udt.split(ByteBufferAccessor.instance, bb);
                // need to replicate map logic, but can't delegate to map as map assumes single value type
                sb.append('{');
                for (int i = 0; i < udt.size(); i++)
                {
                    // no more found
                    if (i == split.length)
                        break;
                    Symbol name = new Symbol(udt.fieldName(i).toString(), UTF8Type.instance);
                    AbstractType<?> subType = udt.type(i);
                    Object value = subType.compose(split[i]);

                    toCQL(name, UTF8Type.instance, sb, indent); // the type is a lie, but needed for param
                    sb.append(": ");
                    toCQL(value, subType, sb, indent);
                    sb.append(", ");
                }
                if (split != null && split.length > 0)
                    sb.setLength(sb.length() - 2);
                sb.append('}');
            }
            else if (type instanceof TupleType)
            {
                // see https://docs.datastax.com/en/cql-oss/3.3/cql/cql_using/useInsertTuple.html
                TupleType tuple = (TupleType) type;
                ByteBuffer[] split = tuple.split(ByteBufferAccessor.instance, bb);
                sb.append('(');
                for (int i = 0; i < split.length; i++)
                {
                    AbstractType<?> subType = tuple.type(i);
                    Object value = subType.compose(split[i]);

                    toCQL(value, subType, sb, indent);
                    sb.append(", ");
                }
                if (split != null && split.length > 0)
                    sb.setLength(sb.length() - 2);
                sb.append(')');
            }
            else if (type instanceof BytesType)
            {
                sb.append("0X");
                sb.append(Hex.bytesToHex(ByteBufferAccessor.instance.toArray(bb)));
            }
            else
            {
                throw new IllegalArgumentException("Unsupported type: " + o.getClass() + " for type " + type);
            }
        }
        else
        {
            throw new IllegalArgumentException("Unsupported type: " + o.getClass() + " for type " + type);
        }
    }

    private void toCQL(Collection<Object> collection, AbstractType<?> elementType, char prefix, char postfix, StringBuilder sb, int indent)
    {
        sb.append(prefix);
        for (Object element : collection)
        {
            toCQL(element, elementType, sb, indent);
            sb.append(", ");
        }
        if (!collection.isEmpty())
            sb.setLength(sb.length() - 2);
        sb.append(postfix);
    }

    @Override
    public String toString()
    {
        return toCQL();
    }
}

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.ValueAccessor;

public abstract class TypeSerializer<T>
{
    private static final Pattern PATTERN_SINGLE_QUOTE = Pattern.compile("'", Pattern.LITERAL);
    private static final String ESCAPED_SINGLE_QUOTE = Matcher.quoteReplacement("\'");

    public abstract ByteBuffer serialize(T value);

    public abstract <V> T deserialize(V value, ValueAccessor<V> accessor);

    /*
     * Does not modify the position or limit of the buffer even temporarily.
     */
    public final T deserialize(ByteBuffer bytes)
    {
        return deserialize(bytes, ByteBufferAccessor.instance);
    }

    /*
     * Validate that the byte array is a valid sequence for the type this represents.
     * This guarantees deserialize() can be called without errors.
     */
    public abstract <V> void validate(V value, ValueAccessor<V> accessor) throws MarshalException;

    /*
     * Does not modify the position or limit of the buffer even temporarily.
     */
    public final void validate(ByteBuffer bytes) throws MarshalException
    {
        validate(bytes, ByteBufferAccessor.instance);
    }

    public abstract String toString(T value);

    public abstract Class<T> getType();

    protected String toCQLLiteralNonNull(ByteBuffer buffer)
    {
        return toString(deserialize(buffer));
    }

    public boolean isNull(ByteBuffer buffer)
    {
        return buffer == null || !buffer.hasRemaining();
    }

    public final String toCQLLiteral(ByteBuffer buffer)
    {
        return isNull(buffer)
               ? "null"
               :  maybeQuote(toCQLLiteralNonNull(buffer));
    }

    public final String toCQLLiteralNoQuote(ByteBuffer buffer)
    {
        return isNull(buffer)
               ? "null"
               :  toCQLLiteralNonNull(buffer);
    }

    public boolean shouldQuoteCQL()
    {
        return true;
    }

    private String maybeQuote(String value)
    {
        if (shouldQuoteCQL())
            return "'" + PATTERN_SINGLE_QUOTE.matcher(value).replaceAll(ESCAPED_SINGLE_QUOTE) + "'";
        return value;
    }
}


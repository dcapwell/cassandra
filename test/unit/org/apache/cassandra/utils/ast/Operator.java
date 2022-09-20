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

import java.util.stream.Stream;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.EmptyType;

public class Operator implements Expression
{
    public enum Kind
    {
        ADD('+'),
        SUBTRACT('-');

        private final char value;

        Kind(char value)
        {
            this.value = value;
        }
    }

    public final Kind kind;
    public final Expression left;
    public final Expression right;

    public Operator(Kind kind, Expression left, Expression right)
    {
        this.kind = kind;
        this.left = left;
        this.right = right;
    }

    @Override
    public AbstractType<?> type()
    {
        return EmptyType.instance;
    }

    @Override
    public void toCQL(StringBuilder sb, int indent)
    {
        left.toCQL(sb, indent);
        sb.append(' ').append(kind.value).append(' ');
        right.toCQL(sb, indent);
    }

    @Override
    public Stream<? extends Element> stream()
    {
        return Stream.of(left, right);
    }
}

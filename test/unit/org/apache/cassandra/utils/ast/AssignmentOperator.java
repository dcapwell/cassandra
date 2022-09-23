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

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.stream.Stream;

import com.google.common.collect.Iterables;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.SourceDSL;

//TODO this is hacky as Mutation takes Expression then figures out where to put it...
// this can only be applied to UPDATE ... SET
public class AssignmentOperator implements Expression
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
    public final Expression right;

    public AssignmentOperator(Kind kind, Expression right)
    {
        this.kind = kind;
        this.right = right;
    }

    public static EnumSet<Kind> supportsOperators(AbstractType<?> type)
    {
        type = type.unwrap();
        if (type instanceof CollectionType || Operator.supportsOperators(type).stream().anyMatch(k -> k == Operator.Kind.ADD || k == Operator.Kind.SUBTRACT))
            return EnumSet.allOf(Kind.class);
        return EnumSet.noneOf(Kind.class);
    }

    public static Gen<AssignmentOperator> gen(EnumSet<Kind> allowed, Expression right)
    {
        if (allowed.isEmpty())
            throw new IllegalArgumentException("Unable to create a operator gen for empty set of allowed operators");
        if (allowed.size() == 1)
            return SourceDSL.arbitrary().constant(new AssignmentOperator(Iterables.getFirst(allowed, null), right));

        Gen<Kind> kind = SourceDSL.arbitrary().pick(new ArrayList<>(allowed));
        return kind.map(k -> new AssignmentOperator(k, right));
    }

    @Override
    public void toCQL(StringBuilder sb, int indent)
    {
        sb.append(' ').append(kind.value).append("= ");
        right.toCQL(sb, indent);
    }

    @Override
    public Stream<? extends Element> stream()
    {
        return Stream.of(right);
    }

    @Override
    public AbstractType<?> type()
    {
        return right.type();
    }
}

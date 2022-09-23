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
import java.util.Set;
import java.util.stream.Stream;

import com.google.common.collect.Iterables;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.NumberType;
import org.apache.cassandra.db.marshal.StringType;
import org.apache.cassandra.db.marshal.TemporalType;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.SourceDSL;

public class Operator implements Expression
{
    public enum Kind
    {
        ADD('+'),
        SUBTRACT('-'),
        MULTIPLY('*');
        //TODO support avoiding 42 / 0 and 42 % 0 as these will fail
//        DIVIDE('/'),
//        MOD('%');

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
        if (!left.type().equals(right.type()))
            throw new IllegalArgumentException("Types do not match: left=" + left.type() + ", right=" + right.type());
    }

    public static EnumSet<Kind> supportsOperators(AbstractType<?> type)
    {
        type = type.unwrap();
        // see org.apache.cassandra.cql3.functions.OperationFcts.OPERATION
        if (type instanceof NumberType)
            return EnumSet.allOf(Kind.class);
        if (type instanceof TemporalType)
            return EnumSet.of(Kind.ADD, Kind.SUBTRACT);
        if (type instanceof StringType)
            return EnumSet.of(Kind.ADD);
        return EnumSet.noneOf(Kind.class);
    }

    public static Gen<Operator> gen(Set<Kind> allowed, Expression e, Gen<Value> valueGen)
    {
        if (allowed.isEmpty())
            throw new IllegalArgumentException("Unable to create a operator gen for empty set of allowed operators");
        Gen<Operator.Kind> kind = allowed.size() == 1 ?
                                  SourceDSL.arbitrary().constant(Iterables.getFirst(allowed, null))
                                                      : SourceDSL.arbitrary().pick(new ArrayList<>(allowed));
        Gen<Boolean> bool = SourceDSL.booleans().all();
        return rnd -> {
            Expression other = valueGen.generate(rnd);
            Expression left, right;
            if (bool.generate(rnd))
            {
                left = e;
                right = other;
            }
            else
            {
                left = other;
                right = e;
            }
            return new Operator(kind.generate(rnd), TypeHint.maybeApplyTypeHint(left), TypeHint.maybeApplyTypeHint(right));
        };
    }

    @Override
    public AbstractType<?> type()
    {
        return left.type();
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

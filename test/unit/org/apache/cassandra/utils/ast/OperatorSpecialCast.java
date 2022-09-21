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

public class OperatorSpecialCast implements Expression
{
    private final Expression e;
    private final AbstractType<?> type;

    public OperatorSpecialCast(Expression e, AbstractType<?> type)
    {
        this.e = e;
        this.type = type;
    }

    @Override
    public void toCQL(StringBuilder sb, int indent)
    {
        sb.append("(").append(type.asCQL3Type()).append(") ");
        e.toCQL(sb, indent);
    }

    @Override
    public AbstractType<?> type()
    {
        return type;
    }

    @Override
    public Stream<? extends Element> stream()
    {
        return Stream.of(e);
    }
}

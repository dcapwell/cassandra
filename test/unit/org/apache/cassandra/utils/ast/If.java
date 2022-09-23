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

import java.util.List;
import java.util.stream.Stream;

import static org.apache.cassandra.utils.ast.Elements.newLine;

public class If implements Element
{
    private final Conditional conditional;
    private final List<Mutation> mutations;

    public If(Conditional conditional, List<Mutation> mutations)
    {
        this.conditional = conditional;
        this.mutations = mutations;
    }

    @Override
    public void toCQL(StringBuilder sb, int indent)
    {
        sb.append("IF ");
        conditional.toCQL(sb, indent);
        sb.append(" THEN");
        int subIndent = indent + 2;
        for (Mutation mutation : mutations)
        {
            newLine(sb, subIndent);
            mutation.toCQL(sb, subIndent);
            sb.append(';');
        }
        Elements.newLine(sb, indent);
        sb.append("END IF");
    }

    @Override
    public Stream<? extends Element> stream()
    {
        return Stream.concat(Stream.of(conditional), mutations.stream());
    }
}

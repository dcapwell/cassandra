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
import java.util.Objects;

import org.apache.cassandra.cql3.ColumnIdentifier;

public class Reference implements Expression
{
    public final List<String> path;

    public Reference(List<String> path)
    {
        if (path.isEmpty())
            throw new IllegalArgumentException("Reference may not be empty");
        this.path = path;
    }

    @Override
    public void toCQL(StringBuilder sb, int indent)
    {
        path.forEach(p -> sb.append(ColumnIdentifier.maybeQuote(p)).append('.'));
        sb.setLength(sb.length() - 1); // last .
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Reference elements = (Reference) o;
        return Objects.equals(path, elements.path);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(path);
    }
}

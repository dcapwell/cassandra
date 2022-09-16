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

import java.util.Objects;

import org.apache.cassandra.cql3.ColumnIdentifier;

public class Symbol implements Expression
{
    private final String symbol;

    public Symbol(String symbol)
    {
        this.symbol = symbol;
    }

    public Symbol(ColumnIdentifier columnIdentifier)
    {
        this(columnIdentifier.toString());
    }

    public ColumnIdentifier toColumnIdentifier()
    {
        return new ColumnIdentifier(symbol, true);
    }

    @Override
    public void toCQL(StringBuilder sb, int indent)
    {
        sb.append(ColumnIdentifier.maybeQuote(symbol));
    }

    @Override
    public String name()
    {
        return symbol;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Symbol symbol1 = (Symbol) o;
        return Objects.equals(symbol, symbol1.symbol);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(symbol);
    }

    @Override
    public String toString()
    {
        return toCQL();
    }
}

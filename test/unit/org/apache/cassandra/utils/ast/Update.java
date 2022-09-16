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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.collect.Sets;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.utils.ast.Elements.newLine;

public class Update implements Statement
{
    public enum Kind
    {INSERT, UPDATE, DELETE}

    ;
    public final Kind kind;
    public final TableMetadata table;
    public final Map<Symbol, Element> values;
    private final Set<Symbol> primaryColumns, nonPrimaryColumns;
    public final OptionalInt ttl;
    private final List<Element> orderedElements = new ArrayList<>();

    public Update(Kind kind, TableMetadata table, Map<Symbol, Element> values, OptionalInt ttl)
    {
        this.kind = kind;
        this.table = table;
        this.values = values;
        this.ttl = ttl;

        // partition key is always required, so validate
        Set<Symbol> partitionColumns = toSet(table.partitionKeyColumns());
        Set<Symbol> clusteringColumns = toSet(table.clusteringColumns());
        this.primaryColumns = Sets.union(partitionColumns, clusteringColumns);
        Set<Symbol> allColumns = toSet(table.columns());
        nonPrimaryColumns = Sets.difference(allColumns, primaryColumns);

        Set<Symbol> requiredColumns;
        switch (kind)
        {
            case INSERT:
            case UPDATE:
                requiredColumns = primaryColumns;
                break;
            case DELETE:
                requiredColumns = partitionColumns;
                break;
            default:
                throw new IllegalArgumentException("Unknown kind: " + kind);
        }
        if (!values.keySet().containsAll(requiredColumns))
            throw new IllegalArgumentException("Not all required columns present; expected (" + requiredColumns + ") but was (" + values.keySet() + ")");
    }

    @Override
    public void toCQL(StringBuilder sb, int indent)
    {
        if (!orderedElements.isEmpty())
            orderedElements.clear();
        switch (kind)
        {
            case INSERT:
                toCQLInsert(sb, indent);
                break;
            case UPDATE:
                toCQLUpdate(sb, indent);
                break;
            case DELETE:
                toCQLDelete(sb, indent);
                break;
            default:
                throw new IllegalArgumentException("Unsupported kind: " + kind);
        }
    }

    private void toCQLInsert(StringBuilder sb, int indent)
    {
        /*
INSERT INTO [keyspace_name.] table_name (column_list)
VALUES (column_values)
[IF NOT EXISTS]
[USING TTL seconds | TIMESTAMP epoch_in_microseconds]
         */
        sb.append("INSERT INTO ").append(table.toString()).append(" (");
        List<Symbol> columnOrder = new ArrayList<>(values.keySet());
        for (Symbol name : columnOrder)
        {
            name.toCQL(sb, indent);
            orderedElements.add(name);
            sb.append(", ");
        }
        sb.setLength(sb.length() - 2);
        sb.append(")");
        newLine(sb, indent);
        sb.append("VALUES (");
        for (Symbol name : columnOrder)
        {
            Element value = values.get(name);
            orderedElements.add(value);
            value.toCQL(sb, indent);
            sb.append(", ");
        }
        sb.setLength(sb.length() - 2);
        sb.append(")");
        newLine(sb, indent);
        maybeAddTTL(sb, indent);
    }

    private void maybeAddTTL(StringBuilder sb, int indent)
    {
        if (ttl.isPresent())
        {
            sb.append("USING TTL ").append(ttl.getAsInt()).append(" ");
            newLine(sb, indent);
        }
    }

    private void toCQLUpdate(StringBuilder sb, int indent)
    {
        /*
UPDATE [keyspace_name.] table_name
[USING TTL time_value | USING TIMESTAMP timestamp_value]
SET assignment [, assignment] . . .
WHERE row_specification
[IF EXISTS | IF condition [AND condition] . . .] ;
         */
        sb.append("UPDATE ").append(table.toString());
        newLine(sb, indent);
        maybeAddTTL(sb, indent);
        sb.append("SET ");
        for (Symbol name : nonPrimaryColumns)
        {
            Element value = values.get(name);
            name.toCQL(sb, indent);
            orderedElements.add(name);
            sb.append('=');
            value.toCQL(sb, indent);
            orderedElements.add(value);
            sb.append(", ");
        }
        sb.setLength(sb.length() - 2);
        newLine(sb, indent);
        sb.append("WHERE ");
        valuesAnd(sb, indent, primaryColumns);
    }

    private void toCQLDelete(StringBuilder sb, int indent)
    {
        /*
DELETE [column_name (term)][, ...]
FROM [keyspace_name.] table_name
[USING TIMESTAMP timestamp_value]
WHERE PK_column_conditions
[IF EXISTS | IF static_column_conditions]
         */
        sb.append("DELETE ");
        Set<Symbol> toDelete = Sets.intersection(nonPrimaryColumns, values.keySet());
        for (Symbol column : toDelete)
        {
            column.toCQL(sb, indent);
            orderedElements.add(column);
            sb.append(", ");
        }
        if (!toDelete.isEmpty())
            sb.setLength(sb.length() - 2);
        newLine(sb, indent);
        sb.append("FROM ").append(table.toString());
        newLine(sb, indent);
        sb.append("WHERE ");
        // in the case of partition delete, need to exclude clustering
        valuesAnd(sb, indent, Sets.intersection(primaryColumns, values.keySet()));
    }

    private void valuesAnd(StringBuilder sb, int indent, Collection<Symbol> names)
    {
        for (Symbol name : names)
        {
            Element value = values.get(name);
            name.toCQL(sb, indent);
            orderedElements.add(name);
            sb.append('=');
            value.toCQL(sb, indent);
            orderedElements.add(value);
            sb.append(" AND ");
        }
        sb.setLength(sb.length() - 5);
    }

    @Override
    public Stream<? extends Element> stream()
    {
        if (orderedElements.isEmpty())
            toCQL();
        return orderedElements.stream();
    }

    @Override
    public String toString()
    {
        return detailedToString();
    }

    public static Set<Symbol> toSet(Iterable<ColumnMetadata> columns)
    {
        return StreamSupport.stream(columns.spliterator(), false).map(m -> new Symbol(m.name)).collect(Collectors.toSet());
    }
}

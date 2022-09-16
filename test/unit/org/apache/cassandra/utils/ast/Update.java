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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.collect.Sets;

import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.CassandraGenerators;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.SourceDSL;

import static org.apache.cassandra.utils.ast.Elements.newLine;

public class Update implements Statement
{
    public enum Kind
    {INSERT, UPDATE, DELETE}

    ;
    public final Kind kind;
    public final TableMetadata table;
    public final Map<Symbol, Expression> values;
    private final Set<Symbol> primaryColumns, nonPrimaryColumns;
    public final OptionalInt ttl;
    public final OptionalLong timestamp;
    private final List<Element> orderedElements = new ArrayList<>();

    public Update(Kind kind, TableMetadata table, Map<Symbol, Expression> values, OptionalInt ttl, OptionalLong timestamp)
    {
        this.kind = kind;
        this.table = table;
        this.values = values;
        this.ttl = ttl;
        this.timestamp = timestamp;

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
        if (ttl.isPresent() || timestamp.isPresent())
        {
            sb.append("USING ");
            if (ttl.isPresent())
                sb.append("TTL ").append(ttl.getAsInt()).append(" ");
            if (timestamp.isPresent())
                sb.append(ttl.isPresent() ? "AND " : "").append("TIMESTAMP ").append(timestamp.getAsLong());
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
        if (timestamp.isPresent())
        {
            sb.append("USING TIMESTAMP ").append(timestamp.getAsLong());
            newLine(sb, indent);
        }
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

    public static class GenBuilder
    {
        private final TableMetadata metadata;
        private final Set<Symbol> allColumns;
        private final Set<Symbol> primaryColumns;
        private final Set<Symbol> nonPrimaryColumns;
        private Gen<Update.Kind> kindGen = SourceDSL.arbitrary().enumValues(Update.Kind.class);
        private Gen<OptionalInt> ttlGen = SourceDSL.integers().between(1, Math.toIntExact(TimeUnit.DAYS.toSeconds(10))).map(i -> i % 2 == 0 ? OptionalInt.empty() : OptionalInt.of(i));
        private Gen<OptionalLong> timestampGen = SourceDSL.longs().between(1, Long.MAX_VALUE).map(i -> i % 2 == 0 ? OptionalLong.empty() : OptionalLong.of(i));
        private Gen<Map<Symbol, Expression>> valuesGen;

        public GenBuilder(TableMetadata metadata)
        {
            this.metadata = Objects.requireNonNull(metadata);
            this.allColumns = Update.toSet(metadata.columns());
            this.primaryColumns = Update.toSet(metadata.primaryKeyColumns());
            this.nonPrimaryColumns = Sets.difference(allColumns, primaryColumns);

            Map<Symbol, Gen<?>> data = CassandraGenerators.tableDataComposed(metadata)
                                                          .entrySet().stream()
                                                          .collect(Collectors.toMap(e -> new Symbol(e.getKey()), e -> e.getValue()));
            this.valuesGen = rnd -> {
                Map<Symbol, Expression> map = new HashMap<>();
                for (Symbol name : allColumns)
                    map.put(name, new Bind(data.get(name).generate(rnd)));
                return map;
            };
        }

        public GenBuilder withoutTimestamp()
        {
            timestampGen = ignore -> OptionalLong.empty();
            return this;
        }

        public Gen<Update> build()
        {
            return rnd -> {
                Update.Kind kind = kindGen.generate(rnd);
                // when there are not non-primary-columns then can't support UPDATE
                if (nonPrimaryColumns.isEmpty())
                {
                    int i;
                    int maxRetries = 42;
                    for (i = 0; i < maxRetries && kind == Update.Kind.UPDATE; i++)
                        kind = kindGen.generate(rnd);
                    if (i == maxRetries)
                        throw new IllegalArgumentException("Kind gen kept returning UPDATE, but not supported when there are no non-primary columns");
                }

                return new Update(kind, metadata, valuesGen.generate(rnd), ttlGen.generate(rnd), timestampGen.generate(rnd));
            };
        }
    }
}

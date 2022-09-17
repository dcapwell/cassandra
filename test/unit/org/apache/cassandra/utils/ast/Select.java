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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.CassandraGenerators;
import org.quicktheories.core.Gen;
import org.quicktheories.impl.Constraint;

import static org.apache.cassandra.utils.ast.Elements.newLine;

public class Select implements Statement
{
    /*
SELECT * | select_expression | DISTINCT partition //TODO DISTINCT
FROM [keyspace_name.] table_name
[WHERE partition_value
   [AND clustering_filters
   [AND static_filters]]]
[ORDER BY PK_column_name ASC|DESC] //TODO
[LIMIT N]
[ALLOW FILTERING] //TODO
     */
    // select
    public final List<Expression> selections;
    // from
    public final Optional<String> source;
    // where
    public final Optional<Conditional> where;
    public final OptionalInt limit;

    public Select(List<Expression> selections)
    {
        this(selections, Optional.empty(), Optional.empty(), OptionalInt.empty());
    }

    public Select(List<Expression> selections, Optional<String> source, Optional<Conditional> where, OptionalInt limit)
    {
        this.selections = selections;
        this.source = source;
        this.where = where;
        this.limit = limit;
        if (!source.isPresent())
        {
            if (where.isPresent())
                throw new IllegalArgumentException("Can not have a WHERE clause when there isn't a FROM");
            if (limit.isPresent())
                throw new IllegalArgumentException("Can not have a LIMIT clause when there isn't a FROM");
        }
    }

    @Override
    public void toCQL(StringBuilder sb, int indent)
    {
        sb.append("SELECT ");
        if (selections.isEmpty())
        {
            sb.append('*');
        }
        else
        {
            int finalIndent = indent;
            selections.forEach(s -> {
                s.toCQL(sb, finalIndent);
                sb.append(", ");
            });
            sb.setLength(sb.length() - 2); // last ', '
        }
        indent += 2;
        if (source.isPresent())
        {
            newLine(sb, indent);
            sb.append("FROM ").append(source.get());
            if (where.isPresent())
            {
                newLine(sb, indent);
                sb.append("WHERE ");
                where.get().toCQL(sb, indent);
            }
            if (limit.isPresent())
            {
                newLine(sb, indent);
                sb.append("LIMIT ").append(limit.getAsInt());
            }
        }
    }

    @Override
    public Stream<? extends Element> stream()
    {
        List<Element> es = new ArrayList<>(selections.size() + (where.isPresent() ? 1 : 0));
        es.addAll(selections);
        if (where.isPresent())
            es.add(where.get());
        return es.stream();
    }

    @Override
    public String toString()
    {
        return detailedToString();
    }

    public static class GenBuilder
    {
        private final TableMetadata metadata;
        private Gen<List<Expression>> selectGen;
        private Gen<Map<Symbol, Expression>> partitionKeyGen;
        private Gen<OptionalInt> limit;

        public GenBuilder(TableMetadata metadata)
        {
            this.metadata = Objects.requireNonNull(metadata);
            this.selectGen = selectColumns(metadata);
            this.partitionKeyGen = partitionKeyGen(metadata);

            withDefaultLimit();
        }

        public GenBuilder withDefaultLimit()
        {
            Gen<OptionalInt> non = ignore -> OptionalInt.empty();
            Constraint limitLength = Constraint.between(1, 10_000);
            Gen<OptionalInt> positive = rnd -> OptionalInt.of(Math.toIntExact(rnd.next(limitLength)));
            limit = non.mix(positive);
            return this;
        }

        public GenBuilder withLimit1()
        {
            this.limit = ignore -> OptionalInt.of(1);
            return this;
        }

        public GenBuilder withoutLimit()
        {
            this.limit = ignore -> OptionalInt.empty();
            return this;
        }

        public Gen<Select> build()
        {
            return rnd -> {
                List<Expression> select = selectGen.generate(rnd);
                Map<Symbol, Expression> partitionKey = partitionKeyGen.generate(rnd);
                Conditional partitionKeyClause = and(partitionKey);
                return new Select(select, Optional.of(metadata.toString()), Optional.of(partitionKeyClause), limit.generate(rnd));
            };
        }

        private static Conditional and(Map<Symbol, Expression> data)
        {
            Conditional accm = null;
            for (Map.Entry<Symbol, Expression> e : data.entrySet())
            {
                Where where = new Where(Where.Inequalities.EQUAL, e.getKey(), e.getValue());
                accm = accm == null? where : new And(accm, where);
            }
            return accm;
        }

        private static Gen<List<Expression>> selectColumns(TableMetadata metadata)
        {
            List<ColumnMetadata> columns = new ArrayList<>(metadata.columns());
            Constraint between = Constraint.between(0, columns.size() - 1);
            Gen<int[]> indexGen = rnd -> {
                int size = Math.toIntExact(rnd.next(between)) + 1;
                Set<Integer> dedup = new HashSet<>();
                while (dedup.size() < size)
                    dedup.add(Math.toIntExact(rnd.next(between)));
                return dedup.stream().mapToInt(Integer::intValue).toArray();
            };
            return rnd -> {
                int[] indexes = indexGen.generate(rnd);
                List<Expression> es = new ArrayList<>(indexes.length);
                IntStream.of(indexes).mapToObj(columns::get).forEach(c -> es.add(new Symbol(c.name)));
                return es;
            };
        }

        private static Gen<Map<Symbol, Expression>> partitionKeyGen(TableMetadata metadata)
        {
            Map<ColumnIdentifier, Gen<?>> gens = CassandraGenerators.tableDataComposed(metadata);

            Map<Symbol, Gen<Expression>> output = new LinkedHashMap<>();
            for (ColumnMetadata col : metadata.partitionKeyColumns())
                output.put(new Symbol(col.name), gens.get(col.name).map(Bind::new));
            return rnd -> {
                Map<Symbol, Expression> data = new LinkedHashMap<>();
                for (Map.Entry<Symbol, Gen<Expression>> e : output.entrySet())
                    data.put(e.getKey(), e.getValue().generate(rnd));
                return data;
            };
        }
    }
}

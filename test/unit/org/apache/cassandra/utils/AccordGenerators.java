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

package org.apache.cassandra.utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.SourceDSL;
import org.quicktheories.impl.Constraint;

public class AccordGenerators
{
    private enum TxReturn { NONE, TABLE, REF}
    private static final Gen<String> nameGen = Generators.SYMBOL_GEN;

    public static Gen<Txn> txnGen(TableMetadata metadata)
    {
        Gen<Boolean> bool = SourceDSL.booleans().all();
        Constraint letRange = Constraint.between(0, 10);
        Constraint conditionRange = Constraint.between(0, 1);
        Constraint updateRange = Constraint.between(0, 10);
        Gen<Select> selectGen = selectGen(metadata);
        Gen<List<Expression>> returnGen = selectColumns(metadata);
//        Gen<String> nameGen = Generators.IDENTIFIER_GEN;
        // table uses IDENTIFIER_GEN but can't use that here due to lack of "" support
        Gen<TxReturn> txReturnGen = SourceDSL.arbitrary().enumValues(TxReturn.class);
        Gen<TxnUpdate> updateGen = updateGen(metadata);
        return rnd -> {
            TxnBuilder builder = new TxnBuilder();
            do
            {
                int numLets = Math.toIntExact(rnd.next(letRange));
                for (int i = 0; i < numLets; i++)
                {
                    String name;
                    while (builder.lets.containsKey(name = nameGen.generate(rnd))) {}
                    builder.addLet(name, selectGen.generate(rnd));
                }
                switch (txReturnGen.generate(rnd))
                {
                    case REF:
                    {
                        if (!builder.allowedReferences.isEmpty())
                        {
                            Gen<List<Reference>> refsGen = SourceDSL.lists().of(SourceDSL.arbitrary().pick(new ArrayList<>(builder.allowedReferences))).ofSizeBetween(1, Math.max(10, builder.allowedReferences.size()));
                            builder.output = Optional.of(new Select((List<Expression>) (List<?>) refsGen.generate(rnd)));
                        }
                    }
                    break;
                    case TABLE:
                        builder.output = Optional.of(selectGen.generate(rnd));
                        break;
                }
//                int numConditions = Math.toIntExact(rnd.next(conditionRange));
//                for (int i = 0; i < numConditions; i++)
//                {
//                    //TODO
//                }
                int numUpdates = Math.toIntExact(rnd.next(updateRange));
                for (int i = 0; i < numUpdates; i++)
                    builder.addUpdate(updateGen.generate(rnd));
            } while (builder.isEmpty());
            return builder.build();
        };
    }

    private static Set<String> toSet(Iterable<ColumnMetadata> columns)
    {
        return StreamSupport.stream(columns.spliterator(), false).map(m -> m.name.toCQLString()).collect(Collectors.toSet());
    }

    private static Gen<TxnUpdate> updateGen(TableMetadata metadata)
    {
        Set<String> allColumns = toSet(metadata.columns());
        Set<String> primaryColumns = toSet(metadata.primaryKeyColumns());
        Set<String> nonPrimaryColumns = Sets.difference(allColumns, primaryColumns);
        Gen<TxnUpdate.Kind> kindGen = SourceDSL.arbitrary().enumValues(TxnUpdate.Kind.class);
        Map<String, Gen<?>> data = CassandraGenerators.tableDataComposed(metadata);
        Gen<OptionalInt> ttlGen = SourceDSL.integers().between(1, Math.toIntExact(TimeUnit.DAYS.toSeconds(10))).map(i -> i % 2 == 0 ? OptionalInt.empty() : OptionalInt.of(i));
        return rnd -> {
            TxnUpdate.Kind kind = kindGen.generate(rnd);
            // when there are not non-primary-columns then can't support UPDATE
            while (nonPrimaryColumns.isEmpty() && kind == TxnUpdate.Kind.UPDATE)
                kind = kindGen.generate(rnd);
            //TODO don't always include all optional columns
            Map<String, Element> values = new HashMap<>();
            for (String name : allColumns)
            {
                Object value = data.get(name).generate(rnd);
                values.put(name, new Literal<>(value));
            }
            return new TxnUpdate(kind, metadata, values, ttlGen.generate(rnd));
        };
    }

    private static void newLine(StringBuilder sb, int indent)
    {
        sb.append('\n');
        for (int i = 0; i < indent; i++)
            sb.append(' ');
    }

    public static Gen<Select> selectGen(TableMetadata metadata)
    {
        ImmutableList<ColumnMetadata> partitionKeys = metadata.partitionKeyColumns();
        ImmutableList<ColumnMetadata> clusteringColumns = metadata.clusteringColumns();
        Gen<ByteBuffer[]> dataGen = CassandraGenerators.partitionKeyArrayDataGen(metadata);
        Gen<List<Expression>> selectGen = selectColumns(metadata);
        return rnd -> {
            List<Expression> select = selectGen.generate(rnd);
            // accord requires partition lookup, so always include partition keys
            ByteBuffer[] partitionKeyData = dataGen.generate(rnd);
            Conditional partitionKeyClause = and(partitionKeys, partitionKeyData);

            return new Select(select, Optional.of(metadata.toString()), Optional.of(partitionKeyClause), clusteringColumns.isEmpty() ? OptionalInt.empty() : OptionalInt.of(1));
        };
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
            IntStream.of(indexes).mapToObj(columns::get).forEach(c -> es.add(new Symbol(c.name.toCQLString())));
            return es;
        };
    }

    private static Conditional and(ImmutableList<ColumnMetadata> columns, ByteBuffer[] data)
    {
        Conditional accm = whereEq(columns.get(0), data[0]);
        for (int i = 1; i < data.length; i++)
        {
            Conditional where = whereEq(columns.get(i), data[i]);
            accm = new And(accm, where);
        }
        return accm;
    }

    private static Conditional whereEq(ColumnMetadata columnMetadata, ByteBuffer datum)
    {
        return new Where(Where.Inequalities.EQUAL, columnMetadata.name.toCQLString(), literal(columnMetadata.type, datum));
    }

    private static <T> Literal<T> literal(AbstractType<T> type, ByteBuffer datum)
    {
        return new Literal<>(type.compose(datum));
    }

    public interface Element
    {
        void toCQL(StringBuilder sb, int indent);
        default void toCQL(StringBuilder sb)
        {
            toCQL(sb, 0);
        }
        default String toCQL()
        {
            StringBuilder sb = new StringBuilder();
            toCQL(sb, 0);
            return sb.toString();
        }

        default Stream<? extends Element> stream()
        {
            return Stream.empty();
        }

        default Stream<? extends Element> streamRecursive()
        {
            return Stream.concat(stream(), stream().flatMap(Element::stream));
        }
    }

    public static class Txn implements Element
    {
        // lets
        public final List<TxnLet> lets;
        // return
        public final Optional<Select> output;
        public final List<TxnUpdate> updates;

        public Txn(List<TxnLet> lets, Optional<Select> output, List<TxnUpdate> updates)
        {
            this.lets = lets;
            this.output = output;
            this.updates = updates;
        }

        @Override
        public void toCQL(StringBuilder sb, int indent)
        {
            sb.append("BEGIN TRANSACTION");
            int subIndent = indent + 2;

            stream().forEach(e -> {
                newLine(sb, subIndent);
                e.toCQL(sb, subIndent);
                sb.append(';');
            });
            newLine(sb, indent);
            sb.append("COMMIT TRANSACTION");
        }

        @Override
        public Stream<? extends Element> stream()
        {
            Stream<? extends Element> ret = lets.stream();
            if (output.isPresent())
                ret = Stream.concat(ret, Stream.of(output.get()));
            ret = Stream.concat(ret, updates.stream());
            return ret;
        }
    }

    public static class TxnBuilder
    {
        private final Map<String, Select> lets = new HashMap<>();
        // no type system so don't need easy lookup to Expression; just existence check
        private final Set<Reference> allowedReferences = new HashSet<>();
        private Optional<Select> output = Optional.empty();
        private final List<TxnUpdate> updates = new ArrayList<>();

        boolean isEmpty()
        {
            // don't include output as 'BEGIN TRANSACTION SELECT "000000000000000010000"; COMMIT TRANSACTION' isn't valid
//            return lets.isEmpty();
            // TransactionStatement defines empty as no SELECT or updates
            return !output.isPresent() && updates.isEmpty();
        }

        void addLet(String name, Select select)
        {
            if (lets.containsKey(name))
                throw new IllegalArgumentException("Let name " + name + " already exists");
            lets.put(name, select);
            //TODO add support for touple
//            allowedReferences.add(new Reference(Arrays.asList(name)));
            for (Expression e : select.selections)
                //TODO remove " due to current limitation... revert once fixed!
                allowedReferences.add(new Reference(Arrays.asList(name, e.name().replace("\"", ""))));
        }

        void addUpdate(TxnUpdate update)
        {
            this.updates.add(Objects.requireNonNull(update));
        }

        Txn build()
        {
            List<TxnLet> lets = this.lets.entrySet().stream().map(e -> new TxnLet(e.getKey(), e.getValue())).collect(Collectors.toList());
            return new Txn(lets, output, new ArrayList<>(updates));
        }
    }

    public static class TxnUpdate implements Element
    {
        public enum Kind { INSERT, UPDATE, DELETE};
        public final Kind kind;
        public final TableMetadata table;
        public final Map<String, Element> values;
        private final Set<String> primaryColumns, nonPrimaryColumns;
        public final OptionalInt ttl;

        public TxnUpdate(Kind kind, TableMetadata table, Map<String, Element> values, OptionalInt ttl)
        {
            this.kind = kind;
            this.table = table;
            this.values = values;
            this.ttl = ttl;

            // partition key is always required, so validate
            Set<String> partitionColumns = toSet(table.partitionKeyColumns());
            Set<String> clusteringColumns = toSet(table.clusteringColumns());
            this.primaryColumns = Sets.union(partitionColumns, clusteringColumns);
            Set<String> allColumns = toSet(table.columns());
            nonPrimaryColumns = Sets.difference(allColumns, primaryColumns);

            Set<String> requiredColumns;
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
                default: throw new IllegalArgumentException("Unsupported kind: " + kind);
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
            List<String> columnOrder = new ArrayList<>(values.keySet());
            for (String name : columnOrder)
                sb.append(name).append(", ");
            sb.setLength(sb.length() - 2);
            sb.append(")");
            newLine(sb, indent);
            sb.append("VALUES (");
            for (String name : columnOrder)
            {
                Element value = values.get(name);
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
            for (String name : nonPrimaryColumns)
            {
                Element value = values.get(name);
                sb.append(name).append('=');
                value.toCQL(sb, indent);
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
            Sets.SetView<String> toDelete = Sets.intersection(nonPrimaryColumns, values.keySet());
            for (String column : toDelete)
                sb.append(column).append(", ");
            if (!toDelete.isEmpty())
                sb.setLength(sb.length() - 2);
            newLine(sb, indent);
            sb.append("FROM ").append(table.toString());
            newLine(sb, indent);
            sb.append("WHERE ");
            // in the case of partition delete, need to exclude clustering
            valuesAnd(sb, indent, Sets.intersection(primaryColumns, values.keySet()));
        }

        private void valuesAnd(StringBuilder sb, int indent, Collection<String> names)
        {
            for (String name : names)
            {
                Element value = values.get(name);
                sb.append(name).append('=');
                value.toCQL(sb, indent);
                sb.append(" AND ");
            }
            sb.setLength(sb.length() - 5);
        }

        @Override
        public Stream<? extends Element> stream()
        {
            return values.values().stream();
        }
    }

    public static class TxnLet implements Element
    {
        public final String symbol;
        public final Select select;

        public TxnLet(String symbol, Select select)
        {
            this.symbol = symbol;
            this.select = select;
        }

        @Override
        public void toCQL(StringBuilder sb, int indent)
        {
            int offset = sb.length();
            sb.append("LET ").append(symbol).append(" = (");
            select.toCQL(sb, sb.length() - offset + 2);
            sb.append(")");
        }

        @Override
        public Stream<? extends Element> stream()
        {
            return Stream.of(select);
        }
    }

    public static class Select implements Element
    {
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
            selections.forEach(s -> {
                s.toCQL(sb, indent);
                sb.append(", ");
            });
            if (!selections.isEmpty())
                sb.setLength(sb.length() - 2); // last ', '
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
    }
    
    public interface Conditional extends Element
    {

    }

    public static class Where implements Conditional
    {
        public enum Inequalities
        {
            EQUAL("="),
            NOT_EQUAL("!="),
            GREATER_THAN(">"),
            GREATER_THAN_EQ(">="),
            LESS_THAN("<"),
            LESS_THAN_EQ("<=");

            private final String value;

            Inequalities(String value)
            {
                this.value = value;
            }
        }
        public final Inequalities kind;
        public final String symbol;
        public final Expression expression;

        public Where(Inequalities kind, String symbol, Expression expression)
        {
            this.kind = kind;
            this.symbol = symbol;
            this.expression = expression;
        }

        @Override
        public void toCQL(StringBuilder sb, int indent)
        {
            sb.append(symbol).append(' ').append(kind.value).append(' ');
            expression.toCQL(sb, indent);
        }

        @Override
        public Stream<? extends Element> stream()
        {
            return Stream.of(expression);
        }
    }

    public static class And implements Conditional
    {
        private final Conditional left, right;

        public And(Conditional left, Conditional right)
        {
            this.left = left;
            this.right = right;
        }

        @Override
        public void toCQL(StringBuilder sb, int indent)
        {
            left.toCQL(sb, indent);
            sb.append(" AND ");
            right.toCQL(sb, indent);
        }

        @Override
        public Stream<? extends Element> stream()
        {
            return Stream.of(left, right);
        }
    }

    public static class Or implements Conditional
    {
        private final Conditional left, right;

        public Or(Conditional left, Conditional right)
        {
            this.left = left;
            this.right = right;
        }

        @Override
        public void toCQL(StringBuilder sb, int indent)
        {
            left.toCQL(sb, indent);
            sb.append(" AND ");
            right.toCQL(sb, indent);
        }

        @Override
        public Stream<? extends Element> stream()
        {
            return Stream.of(left, right);
        }
    }

    public static class Not implements Conditional
    {
        private final Conditional child;

        public Not(Conditional child)
        {
            this.child = child;
        }

        @Override
        public void toCQL(StringBuilder sb, int indent)
        {
            sb.append("NOT ");
            child.toCQL(sb, indent);
        }

        @Override
        public Stream<? extends Element> stream()
        {
            return Stream.of(child);
        }
    }

    public interface Expression extends Element
    {
        default String name()
        {
            return toCQL();
        }
    }

    public static class Symbol implements Expression
    {
        private final String symbol;

        public Symbol(String symbol)
        {
            this.symbol = symbol;
        }

        @Override
        public void toCQL(StringBuilder sb, int indent)
        {
            sb.append(symbol);
        }

        @Override
        public String name()
        {
            return symbol;
        }
    }

    public static class Literal<T> implements Expression
    {
        private final T value;

        public Literal(T value)
        {
            this.value = value;
        }

        @Override
        public void toCQL(StringBuilder sb, int indent)
        {
            //TODO escape
            sb.append(value);
        }
    }

    public static class Reference implements Expression
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
            path.forEach(p -> sb.append(p).append('.'));
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

    public static class Operator implements Expression
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

    public static class As implements Expression
    {
        private final String symbol;
        private final Expression child;

        public As(String symbol, Expression child)
        {
            this.symbol = symbol;
            this.child = child;
        }

        @Override
        public void toCQL(StringBuilder sb, int indent)
        {
            child.toCQL(sb, indent);
            sb.append(" AS ").append(symbol);
        }

        @Override
        public String name()
        {
            return symbol;
        }

        @Override
        public Stream<? extends Element> stream()
        {
            return Stream.of(child);
        }
    }
}

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
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;
import org.apache.cassandra.cql3.KnownIssue;
import org.apache.cassandra.cql3.ast.AssignmentOperator;
import org.apache.cassandra.cql3.ast.Bind;
import org.apache.cassandra.cql3.ast.CasCondition;
import org.apache.cassandra.cql3.ast.Conditional;
import org.apache.cassandra.cql3.ast.CreateIndexDDL;
import org.apache.cassandra.cql3.ast.Expression;
import org.apache.cassandra.cql3.ast.FunctionCall;
import org.apache.cassandra.cql3.ast.Literal;
import org.apache.cassandra.cql3.ast.Mutation;
import org.apache.cassandra.cql3.ast.Operator;
import org.apache.cassandra.cql3.ast.Reference;
import org.apache.cassandra.cql3.ast.Select;
import org.apache.cassandra.cql3.ast.Symbol;
import org.apache.cassandra.cql3.ast.TableReference;
import org.apache.cassandra.cql3.ast.TypeHint;
import org.apache.cassandra.cql3.ast.Value;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.harry.model.ASTSingleTableModel;
import org.apache.cassandra.harry.model.BytesPartitionState;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.utils.AbstractTypeGenerators.getTypeSupport;
import static org.apache.cassandra.utils.Generators.toGen;

public class ASTGenerators
{
    public static final EnumSet<KnownIssue> IGNORE_ISSUES = KnownIssue.ignoreAll();

    public static Gen<LinkedHashMap<Symbol, Object>> columnValues(List<Symbol> columns)
    {
        List<Gen<?>> gens = new ArrayList<>(columns.size());
        for (int i = 0; i < columns.size(); i++)
            gens.add(toGen(getTypeSupport(columns.get(i).type()).valueGen));
        return rs -> {
            LinkedHashMap<Symbol, Object> vs = new LinkedHashMap<>();
            for (int i = 0; i < columns.size(); i++)
                vs.put(columns.get(i), gens.get(i).next(rs));
            return vs;
        };
    }

    static Gen<Value> valueGen(Object value, AbstractType<?> type)
    {
        return rs -> rs.nextBoolean() ? new Bind(value, type) : new Literal(value, type);
    }

    static Gen<Value> valueGen(AbstractType<?> type)
    {
        Gen<?> v = toGen(AbstractTypeGenerators.getTypeSupport(type).valueGen);
        return rs -> valueGen(v.next(rs), type).next(rs);
    }

    private static <K, V> Map<K, V> assertDeterministic(Map<K, V> map)
    {
        if (map instanceof LinkedHashMap || map instanceof TreeMap || map instanceof EnumMap)
            return map;
        if (map.size() == 1)
            return map;
        throw new AssertionError("Unsupported map type: " + map.getClass());
    }

    public static Gen<AssignmentOperator> assignmentOperatorGen(EnumSet<AssignmentOperator.Kind> allowed, Expression right)
    {
        if (allowed.isEmpty())
            throw new IllegalArgumentException("Unable to create a operator gen for empty set of allowed operators");
        if (allowed.size() == 1)
            return Gens.constant(new AssignmentOperator(Iterables.getFirst(allowed, null), right));

        return rs -> new AssignmentOperator(rs.pickOrderedSet(allowed), right);
    }

    public static Gen<Operator> operatorGen(EnumSet<Operator.Kind> allowed, Expression e, Gen<Value> paramValueGen)
    {
        if (allowed.isEmpty())
            throw new IllegalArgumentException("Unable to create a operator gen for empty set of allowed operators");
        Gen<Operator.Kind> kindGen = rs -> rs.pickOrderedSet(allowed);
        Gen<Boolean> bool = Gens.bools().all();
        return rs -> {
            Gen<Value> valueGen = paramValueGen;
            Operator.Kind kind = kindGen.next(rs);
            if (kind == Operator.Kind.SUBTRACT && e.type() instanceof MapType)
            {
                // `map - set` not `map - map`
                valueGen = valueGen.map(v -> {
                    // since we know E is of type map we know the value is a map
                    Map<?, ?> map = (Map<?, ?>) v.value();
                    Set<?> newValue = map.keySet();
                    SetType<Object> newType = SetType.getInstance(((MapType) e.type()).nameComparator(), false);
                    return v.with(newValue, newType);
                });
            }
            Expression other = valueGen.next(rs);
            Expression left, right;
            if (bool.next(rs))
            {
                left = e;
                right = other;
            }
            else
            {
                left = other;
                right = e;
            }
            //TODO (correctness): "(smallint) ? - 16250" failed, but is this general or is it a small int thing?
            //NOTE: (smallint) of -11843 and 3749 failed as well...
            //NOTE: (long) was found and didn't fail...
            //NOTE: see https://the-asf.slack.com/archives/CK23JSY2K/p1724819303058669 - varint didn't fail but serialized using int32 which causes equality mismatches for pk/ck lookups
            if ((e.type().unwrap() == ShortType.instance
                 || e.type().unwrap() == IntegerType.instance)
                && IGNORE_ISSUES.contains(KnownIssue.SHORT_AND_VARINT_GET_INT_FUNCTIONS)) // seed=7525457176675272023L
            {
                left = new TypeHint(left);
                right = new TypeHint(right);
            }
            return new Operator(kind, TypeHint.maybeApplyTypeHint(left), TypeHint.maybeApplyTypeHint(right));
        };
    }

    public static class ExpressionBuilder<T>
    {
        private final AbstractType<T> type;
        private final EnumSet<Operator.Kind> allowedOperators;
        private Gen<T> valueGen;
        private Gen<Boolean> useOperator = Gens.bools().all();
        private BiFunction<Object, AbstractType<?>, Gen<Value>> literalOrBindGen = ASTGenerators::valueGen;

        public ExpressionBuilder(AbstractType<T> type)
        {
            this.type = type.unwrap();
            this.valueGen = toGen(AbstractTypeGenerators.getTypeSupport(this.type).valueGen);
            this.allowedOperators = Operator.supportsOperators(this.type);
        }

        public ExpressionBuilder withOperators()
        {
            useOperator = i -> true;
            return this;
        }

        public ExpressionBuilder withoutOperators()
        {
            useOperator = i -> false;
            return this;
        }

        public ExpressionBuilder allowOperators()
        {
            useOperator = Gens.bools().all();
            return this;
        }

        public ExpressionBuilder withLiteralOrBindGen(BiFunction<Object, AbstractType<?>, Gen<Value>> literalOrBindGen)
        {
            this.literalOrBindGen = literalOrBindGen;
            return this;
        }

        public Gen<Expression> build()
        {
            //TODO (coverage): rather than single level operators, allow nested (a + b + c + d)
            Gen<Value> leaf = rs -> literalOrBindGen.apply(valueGen.next(rs), type).next(rs);
            return rs -> {
                Expression e = leaf.next(rs);
                if (!allowedOperators.isEmpty() && useOperator.next(rs))
                    e = operatorGen(allowedOperators, e, leaf).next(rs);
                return e;
            };
        }
    }

    public static class SelectGenBuilder
    {
        private final TableMetadata metadata;
        private Gen<List<Expression>> selectGen;
        private Gen<Map<Symbol, Expression>> keyGen;
        private Gen<Optional<Value>> limitGen;
        private BiFunction<Object, AbstractType<?>, Gen<Value>> literalOrBindGen = ASTGenerators::valueGen;

        public SelectGenBuilder(TableMetadata metadata)
        {
            this.metadata = Objects.requireNonNull(metadata);
            this.selectGen = selectColumns(metadata);
            this.keyGen = partitionKeyGen(metadata);

            withDefaultLimit();
        }

        public SelectGenBuilder withLiteralOrBindGen(BiFunction<Object, AbstractType<?>, Gen<Value>> literalOrBindGen)
        {
            this.literalOrBindGen = literalOrBindGen;
            return this;
        }

        public SelectGenBuilder withSelectStar()
        {
            selectGen = ignore -> Collections.emptyList();
            return this;
        }

        public SelectGenBuilder withDefaultLimit()
        {
            Gen<Optional<Value>> non = ignore -> Optional.empty();
            Gen<Optional<Value>> positive = rs -> Optional.of(valueGen(Math.toIntExact(rs.nextInt(1, 10_001)), Int32Type.instance).next(rs));
            limitGen = rs -> rs.nextBoolean() ? non.next(rs) : positive.next(rs);
            return this;
        }

        public SelectGenBuilder withLimit1()
        {
            this.limitGen = rs -> Optional.of(valueGen(1, Int32Type.instance).next(rs));
            return this;
        }

        public SelectGenBuilder withoutLimit()
        {
            this.limitGen = ignore -> Optional.empty();
            return this;
        }

        public SelectGenBuilder withKeys(Gen<Map<Symbol, Object>> partitionKeys, Gen<Map<Symbol, Object>> clusteringKeys)
        {
            keyGen = rs -> {
                Map<Symbol, Expression> keys = new LinkedHashMap<>();
                for (Map.Entry<Symbol, Object> e : assertDeterministic(partitionKeys.next(rs)).entrySet())
                    keys.put(e.getKey(), literalOrBindGen.apply(e.getValue(), e.getKey().type()).next(rs));
                if (!metadata.clusteringColumns().isEmpty())
                {
                    for (Map.Entry<Symbol, Object> e : assertDeterministic(clusteringKeys.next(rs)).entrySet())
                        keys.put(e.getKey(), literalOrBindGen.apply(e.getValue(), e.getKey().type()).next(rs));
                }
                return keys;
            };
            return this;
        }

        public Gen<Select> build()
        {
            Optional<TableReference> ref = Optional.of(TableReference.from(metadata));
            return rs -> {
                List<Expression> select = selectGen.next(rs);
                Conditional keyClause = and(keyGen.next(rs));
                Optional<Value> limit = limitGen.next(rs);
                return new Select(select, ref, Optional.of(keyClause), Optional.empty(), limit);
            };
        }

        private static Conditional and(Map<Symbol, Expression> data)
        {
            Conditional.Builder builder = new Conditional.Builder();
            for (Map.Entry<Symbol, Expression> e : assertDeterministic(data).entrySet())
                builder.where(e.getKey(), Conditional.Where.Inequality.EQUAL, e.getValue());
            return builder.build();
        }

        private static Gen<List<Expression>> selectColumns(TableMetadata metadata)
        {
            List<ColumnMetadata> columns = metadata.columnsInFixedOrder();
            Gen<int[]> indexGen = rs -> {
                int size = Math.toIntExact(rs.nextInt(0, columns.size())) + 1;
                Set<Integer> dedup = new LinkedHashSet<>();
                while (dedup.size() < size)
                    dedup.add(Math.toIntExact(rs.nextInt(0, columns.size())));
                return dedup.stream().mapToInt(Integer::intValue).toArray();
            };
            return rs -> {
                int[] indexes = indexGen.next(rs);
                List<Expression> es = new ArrayList<>(indexes.length);
                IntStream.of(indexes).mapToObj(columns::get).forEach(c -> es.add(new Symbol(c)));
                return es;
            };
        }

        private static Gen<Map<Symbol, Expression>> partitionKeyGen(TableMetadata metadata)
        {
            Map<ColumnMetadata, Gen<?>> gens = new LinkedHashMap<>();
            for (ColumnMetadata col : metadata.columnsInFixedOrder())
                gens.put(col, toGen(AbstractTypeGenerators.getTypeSupport(col.type).valueGen));
            return rs -> {
                Map<Symbol, Expression> output = new LinkedHashMap<>();
                for (ColumnMetadata col : metadata.partitionKeyColumns())
                    output.put(new Symbol(col), gens.get(col)
                                                    .map(o -> valueGen(o, col.type).next(rs))
                                                    .next(rs));
                return output;
            };
        }
    }

    public static class MutationGenBuilder
    {
        public enum DeleteKind { Partition, Row, Column }
        private final TableMetadata metadata;
        private final LinkedHashSet<Symbol> allColumns;
        private final LinkedHashSet<Symbol> partitionColumns, clusteringColumns;
        private final LinkedHashSet<Symbol> primaryColumns;
        private final LinkedHashSet<Symbol> regularColumns, staticColumns, regularAndStaticColumns;
        private Gen<Mutation.Kind> kindGen = Gens.enums().all(Mutation.Kind.class);
        private Gen<OptionalInt> ttlGen = Gens.ints().between(1, Math.toIntExact(TimeUnit.DAYS.toSeconds(10))).map(i -> i % 2 == 0 ? OptionalInt.empty() : OptionalInt.of(i));
        private Gen<OptionalLong> timestampGen = Gens.longs().between(1, Long.MAX_VALUE).map(i -> i % 2 == 0 ? OptionalLong.empty() : OptionalLong.of(i));
        private Collection<Reference> references = Collections.emptyList();
        private Gen<Boolean> withCasGen = Gens.bools().all();
        private Gen<Boolean> useCasIf = Gens.bools().all();
        private BiFunction<RandomSource, List<Symbol>, List<Symbol>> ifConditionFilter = (rs, symbols) -> symbols;
        private Gen<DeleteKind> deleteKindGen = Gens.enums().all(DeleteKind.class);
        private Map<Symbol, ExpressionBuilder<?>> columnExpressions = new LinkedHashMap<>();

        public MutationGenBuilder(TableMetadata metadata)
        {
            this.metadata = Objects.requireNonNull(metadata);
            this.allColumns = Mutation.toSet(metadata::allColumnsInSelectOrder);
            this.partitionColumns = Mutation.toSet(metadata.partitionKeyColumns());
            this.clusteringColumns = Mutation.toSet(metadata.clusteringColumns());
            this.primaryColumns = Mutation.toSet(metadata.primaryKeyColumns());
            this.regularColumns = Mutation.toSet(metadata.regularColumns());
            this.staticColumns = Mutation.toSet(metadata.staticColumns());
            this.regularAndStaticColumns = new LinkedHashSet<>();
            regularAndStaticColumns.addAll(staticColumns);
            regularAndStaticColumns.addAll(regularColumns);

            for (Symbol symbol : allColumns)
                columnExpressions.put(symbol, new ExpressionBuilder<>(symbol.type()));
        }

        public MutationGenBuilder withDeletionKind(Gen<DeleteKind> deleteKindGen)
        {
            this.deleteKindGen = deleteKindGen;
            return this;
        }

        public MutationGenBuilder withDeletionKind(DeleteKind... values)
        {
            return withDeletionKind(Gens.pick(values));
        }

        public MutationGenBuilder withLiteralOrBindGen(BiFunction<Object, AbstractType<?>, Gen<Value>> literalOrBindGen)
        {
            columnExpressions.values().forEach(e -> e.withLiteralOrBindGen(literalOrBindGen));
            return this;
        }

        public MutationGenBuilder withoutTransaction()
        {
            withoutCas();
            return this;
        }

        public MutationGenBuilder withCas()
        {
            withCasGen = Gens.constant(true);
            return this;
        }

        public MutationGenBuilder withoutCas()
        {
            withCasGen = Gens.constant(false);
            return this;
        }

        public MutationGenBuilder withCasGen(Gen<Boolean> withCasGen)
        {
            withCasGen = Objects.requireNonNull(withCasGen);
            return this;
        }

        public MutationGenBuilder withCasIf()
        {
            useCasIf = Gens.constant(true);
            return this;
        }

        public MutationGenBuilder withoutCasIf()
        {
            useCasIf = Gens.constant(false);
            return this;
        }

        public MutationGenBuilder withCasIfGen(Gen<Boolean> gen)
        {
            useCasIf = Objects.requireNonNull(gen);
            return this;
        }

        public MutationGenBuilder withIfColumnFilter(BiFunction<RandomSource, List<Symbol>, List<Symbol>> ifConditionFilter)
        {
            this.ifConditionFilter = Objects.requireNonNull(ifConditionFilter);
            return this;
        }

        public MutationGenBuilder withoutTimestamp()
        {
            timestampGen = ignore -> OptionalLong.empty();
            return this;
        }

        public MutationGenBuilder withoutTtl()
        {
            ttlGen = ignore -> OptionalInt.empty();
            return this;
        }

        public MutationGenBuilder withOperators()
        {
            columnExpressions.values().forEach(e -> e.withOperators());
            return this;
        }

        public MutationGenBuilder withoutOperators()
        {
            columnExpressions.values().forEach(e -> e.withoutOperators());
            return this;
        }

        public MutationGenBuilder withReferences(Collection<Reference> references)
        {
            this.references = references;
            return this;
        }

        private Gen<? extends Map<Symbol, Object>> partitionValueGen = null;
        private Gen<? extends Map<Symbol, Object>> clusteringValueGen = null;

        public MutationGenBuilder withPartitions(Gen<? extends Map<Symbol, Object>> values)
        {
            this.partitionValueGen = values;
            return this;
        }

        public MutationGenBuilder withClusterings(Gen<? extends Map<Symbol, Object>> values)
        {
            this.clusteringValueGen = values;
            return this;
        }

        private static void values(RandomSource rs,
                                   Map<Symbol, ExpressionBuilder<?>> columnExpressions,
                                   Conditional.EqBuilder<?> builder,
                                   LinkedHashSet<Symbol> columns,
                                   @Nullable Gen<? extends Map<Symbol, Object>> gen)
        {
            if (gen != null)
            {
                Map<Symbol, Object> map = gen.next(rs);
                for (Map.Entry<Symbol, ?> e : assertDeterministic(map).entrySet())
                    builder.value(e.getKey(), valueGen(e.getValue(), e.getKey().type()).next(rs));
            }
            else
            {
                //TODO (coverage): support IN rather than just EQ
                for (Symbol s : columns)
                    builder.value(s, columnExpressions.get(s).build().next(rs));
            }
        }

        public Gen<Mutation> build()
        {
            Gen<Boolean> bool = Gens.bools().all();
            Map<? extends AbstractType<?>, List<Reference>> typeToReference = references.stream().collect(Collectors.groupingBy(Reference::type));
            return rs -> {
                Mutation.Kind kind = kindGen.next(rs);
                // when there are not non-primary-columns then can't support UPDATE
                if (kind == Mutation.Kind.UPDATE && regularColumns.isEmpty())
                {
                    int i;
                    int maxRetries = 42;
                    for (i = 0; i < maxRetries && kind == Mutation.Kind.UPDATE; i++)
                        kind = kindGen.next(rs);
                    if (i == maxRetries)
                        throw new IllegalArgumentException("Kind gen kept returning UPDATE, but not supported when there are no non-primary columns");
                }
                boolean isCas = withCasGen.next(rs);
                boolean isTransaction = isCas; //TODO (coverage): add accord support
                switch (kind)
                {
                    case INSERT:
                    {
                        Mutation.InsertBuilder builder = Mutation.insert(metadata);
                        if (isCas)
                            builder.ifNotExists();
                        var ttl = ttlGen.next(rs);
                        if (ttl.isPresent())
                            builder.ttl(valueGen(ttl.getAsInt(), Int32Type.instance).next(rs));
                        var timestamp = timestampGen.next(rs);
                        if (timestamp.isPresent())
                            builder.timestamp(valueGen(timestamp.getAsLong(), LongType.instance).next(rs));
                        values(rs, columnExpressions, builder, partitionColumns, partitionValueGen);
                        values(rs, columnExpressions, builder, clusteringColumns, clusteringValueGen);
                        LinkedHashSet<Symbol> columnsToGenerate;
                        if (regularAndStaticColumns.isEmpty())
                        {
                            columnsToGenerate = new LinkedHashSet<>(0);
                        }
                        else if (regularAndStaticColumns.size() == 1 || bool.next(rs))
                        {
                            // all columns
                            columnsToGenerate = new LinkedHashSet<>(regularAndStaticColumns);
                        }
                        else
                        {
                            // subset
                            columnsToGenerate = new LinkedHashSet<>(subsetRegularAndStaticColumns(rs));
                        }

                        generateRemaining(rs, bool, Mutation.Kind.INSERT, isTransaction, typeToReference, builder, columnsToGenerate);
                        return builder.build();
                    }
                    case UPDATE:
                    {
                        Mutation.UpdateBuilder builder = Mutation.update(metadata);
                        var ttl = ttlGen.next(rs);
                        if (ttl.isPresent())
                            builder.ttl(valueGen(ttl.getAsInt(), Int32Type.instance).next(rs));
                        var timestamp = timestampGen.next(rs);
                        if (timestamp.isPresent())
                            builder.timestamp(valueGen(timestamp.getAsLong(), LongType.instance).next(rs));
                        if (isCas)
                        {
                            if (useCasIf.next(rs))
                            {
                                ifGen(new ArrayList<>(regularAndStaticColumns)).next(rs).ifPresent(c -> builder.ifCondition(c));
                            }
                            else
                            {
                                builder.ifExists();
                            }
                        }
                        values(rs, columnExpressions, builder, partitionColumns, partitionValueGen);
                        values(rs, columnExpressions, builder, clusteringColumns, clusteringValueGen);

                        LinkedHashSet<Symbol> columnsToGenerate;
                        if (regularAndStaticColumns.size() == 1 || bool.next(rs))
                        {
                            // all columns
                            columnsToGenerate = new LinkedHashSet<>(regularAndStaticColumns);
                        }
                        else
                        {
                            // subset must include a regular column
                            columnsToGenerate = new LinkedHashSet<>(subset(rs, regularColumns));
                            if (!staticColumns.isEmpty() && bool.next(rs))
                                columnsToGenerate.addAll(subset(rs, staticColumns));
                        }
                        Conditional.EqBuilder<Mutation.UpdateBuilder> setBuilder = builder::set;
                        generateRemaining(rs, bool, Mutation.Kind.UPDATE, isTransaction, typeToReference, setBuilder, columnsToGenerate);
                        return builder.build();
                    }
                    case DELETE:
                    {
                        Mutation.DeleteBuilder builder = Mutation.delete(metadata);

                        // 3 types of delete: partition, row, columns
                        DeleteKind deleteKind = deleteKindGen.next(rs);
                        // if there are no columns to delete, fallback to row
                        if (deleteKind == DeleteKind.Column && regularAndStaticColumns.isEmpty())
                            deleteKind = DeleteKind.Row;
                        if (deleteKind == DeleteKind.Row && clusteringColumns.isEmpty())
                            deleteKind = DeleteKind.Partition;

                        values(rs, columnExpressions, builder, partitionColumns, partitionValueGen);

                        switch (deleteKind)
                        {
                            case Partition:
                                // nothing to do here, already handled
                                break;
                            case Row:
                                values(rs, columnExpressions, builder, clusteringColumns, clusteringValueGen);
                                break;
                            case Column:
                                if (clusteringColumns.isEmpty())
                                {
                                    subsetRegularAndStaticColumns(rs).forEach(builder::column);
                                }
                                else if (staticColumns.isEmpty())
                                {
                                    subset(rs, regularColumns).forEach(builder::column);
                                    values(rs, columnExpressions, builder, clusteringColumns, clusteringValueGen);
                                }
                                else if (regularColumns.isEmpty())
                                {
                                    subset(rs, staticColumns).forEach(builder::column);
                                }
                                else
                                {
                                    // 2 possible states:
                                    // 1) select a row then delete the columns
                                    // 2) select a partition then select static columns only
                                    if (bool.next(rs))
                                    {
                                        // select static
                                        subset(rs, staticColumns).forEach(builder::column);
                                    }
                                    else
                                    {
                                        // select a row, at least 1 regular, and 0 or more statics
                                        values(rs, columnExpressions, builder, clusteringColumns, clusteringValueGen);
                                        subset(rs, regularColumns).forEach(builder::column);
                                        if (bool.next(rs))
                                            subset(rs, staticColumns).forEach(builder::column);
                                    }
                                }
                                if (!clusteringColumns.isEmpty() && !staticColumns.isEmpty())
                                {
                                    if (bool.next(rs))
                                    {
                                        // static only
                                        subset(rs, staticColumns).forEach(builder::column);
                                    }
                                    else
                                    {
                                        // mixed (piss
                                    }
                                }
                                break;
                            default:
                                throw new UnsupportedOperationException();
                        }

                        var timestamp = timestampGen.next(rs);
                        if (timestamp.isPresent())
                            builder.timestamp(valueGen(timestamp.getAsLong(), LongType.instance).next(rs));
                        if (isCas)
                        {
                            boolean existAllowed = true;
                            List<Symbol> columns;
                            switch (deleteKind)
                            {
                                case Partition:
                                {
                                    // As of this moment delete if partition exists does a full partition read, so its blocked
                                    // due to being too costly... this query is logically correct so we should support as only
                                    // liveness information is needed, but its not supported right now so need to work around
                                    // see ML "[DISCUSS] CASSANDRA-20163 DELETE partition IF static column condition is currently blocked"
                                    // I tried to enable delete partition if static column condition in CASSANDRA-20156, but was
                                    // asked to abandon the patch for consistency reasons.
                                    // Delete partition when there are clustering columns is unsupported, so avoid generating
                                    if (clusteringColumns.isEmpty())
                                    {
                                        // this is the same as delete row
                                        columns = new ArrayList<>(regularAndStaticColumns);
                                        existAllowed = true;
                                    }
                                    else
                                    {
                                        columns = Collections.emptyList();
                                        existAllowed = false;
                                    }
                                }
                                break;
                                case Row:
                                {
                                    columns = new ArrayList<>(regularAndStaticColumns);
                                }
                                break;
                                case Column:
                                {
                                    // some column deletes support without clustering, others dont... to avoid
                                    // relearning this, only allow conditions on the followin columns:
                                    // 1) the columns in the query; only valid columns are present
                                    // 2) static columns; these are always safe to include
                                    LinkedHashSet<Symbol> uniq = new LinkedHashSet<>(builder.columns());
                                    uniq.addAll(staticColumns);
                                    columns = new ArrayList<>(uniq);
                                }
                                break;
                                default:
                                    throw new UnsupportedOperationException(deleteKind.name());
                            }
                            if (!columns.isEmpty() && useCasIf.next(rs))
                            {
                                ifGen(columns).next(rs).ifPresent(builder::ifCondition);
                            }
                            else if (existAllowed)
                            {
                                builder.ifExists();
                            }
                            else
                            {
                                // can't do a CAS query
                            }
                        }
                        return builder.build();
                    }
                    default:
                        throw new UnsupportedOperationException(kind.name());
                }
            };
        }

        private void generateRemaining(RandomSource rs,
                                       Gen<Boolean> bool,
                                       Mutation.Kind kind,
                                       boolean isTransaction,
                                       Map<? extends AbstractType<?>, List<Reference>> typeToReference,
                                       Conditional.EqBuilder<?> builder,
                                       LinkedHashSet<Symbol> columnsToGenerate)
        {
            //TODO (flexability): since expression offers visit to replace things, could also keep the expression in tact but just replace Value with the Reference?
            if (!typeToReference.isEmpty())
            {
                List<Symbol> allowed = new ArrayList<>(columnsToGenerate);
                for (Symbol s : allowed)
                {
                    List<Reference> matches = typeToReference.get(s.type());
                    if (matches == null)
                        continue;
                    if (bool.next(rs))
                    {
                        columnsToGenerate.remove(s);
                        builder.value(s, Gens.pick(matches).next(rs));
                    }
                }
            }
            if (kind == Mutation.Kind.UPDATE && isTransaction)
            {
                for (Symbol c : new ArrayList<>(columnsToGenerate))
                {
                    var useOperator = columnExpressions.get(c).useOperator;
                    EnumSet<AssignmentOperator.Kind> additionOperatorAllowed = AssignmentOperator.supportsOperators(c.type());
                    if (!additionOperatorAllowed.isEmpty() && useOperator.next(rs))
                    {
                        Expression expression = columnExpressions.get(c).build().next(rs);
                        builder.value(c, assignmentOperatorGen(additionOperatorAllowed, expression).next(rs));
                        columnsToGenerate.remove(c);
                    }
                }
            }
            columnsToGenerate.forEach(s -> builder.value(s, columnExpressions.get(s).build().next(rs)));
        }

        private List<Symbol> subsetRegularAndStaticColumns(RandomSource rs)
        {
            return subset(rs, regularAndStaticColumns);
        }

        private static List<Symbol> subset(RandomSource rs, LinkedHashSet<Symbol> columns)
        {
            if (columns.size() == 1)
                return new ArrayList<>(columns);
            int numColumns = rs.nextInt(1, columns.size() + 1);
            List<Symbol> subset = Gens.lists(r -> r.pickOrderedSet(columns)).unique().ofSize(numColumns).next(rs);
            return subset;
        }

        private Gen<Optional<CasCondition.IfCondition>> ifGen(List<Symbol> possibleColumns)
        {
            return rs -> {
                List<Symbol> symbols = ifConditionFilter.apply(rs, possibleColumns);
                if (symbols == null || symbols.isEmpty())
                    return Optional.empty();
                Conditional.Builder builder = new Conditional.Builder();
                for (Symbol symbol : symbols)
                    builder.where(symbol, Conditional.Where.Inequality.EQUAL, columnExpressions.get(symbol).build().next(rs));
                return Optional.of(new CasCondition.IfCondition(builder.build()));
            };
        }
    }

    public static class ModelBasedSelect
    {
        private static final List<Conditional.Where.Inequality> RANGE_INEQUALITY = Stream.of(Conditional.Where.Inequality.values())
                                                                                         .filter(i -> i != Conditional.Where.Inequality.EQUAL && i != Conditional.Where.Inequality.NOT_EQUAL)
                                                                                         .collect(Collectors.toList());
        
        private final ASTSingleTableModel model;
        private final TableMetadata metadata;
        private final LinkedHashMap<Symbol, CreateIndexDDL.IndexedColumn> indexes;
        public final ImmutableList<Symbol> nonPartitionColumns;
        public final ImmutableList<Symbol> nonPartitionIndexedColumns;
        public final ImmutableList<Symbol> searchableColumns;
        // mutable
        private boolean multiNode = true;
        private EnumSet<KnownIssue> ignoredIssues = KnownIssue.ignoreAll();
        private Gen<Conditional.Where.Inequality> rangeInequalityGen = Gens.pick(RANGE_INEQUALITY);
        private Function<AbstractType<?>, Gen<ByteBuffer>> dataGenFor = t -> toGen(getTypeSupport(t).bytesGen());

        public ModelBasedSelect(ASTSingleTableModel model, LinkedHashMap<Symbol, CreateIndexDDL.IndexedColumn> indexes)
        {
            this.model = model;
            this.metadata = model.factory.metadata;
            this.indexes = indexes;

            this.nonPartitionColumns = ImmutableList.<Symbol>builder()
                                               .addAll(model.factory.clusteringColumns)
                                               .addAll(model.factory.staticColumns)
                                               .addAll(model.factory.regularColumns)
                                               .build();
            this.nonPartitionIndexedColumns = ImmutableList.copyOf(nonPartitionColumns.stream()
                                                                                      .filter(indexes::containsKey)
                                                                                      .collect(Collectors.toList()));

            this.searchableColumns = metadata.partitionKeyColumns().size() > 1 ?  ImmutableList.copyOf(model.factory.selectionOrder) : this.nonPartitionColumns;
        }

        public ModelBasedSelect multiNode(boolean value)
        {
            multiNode = value;
            return this;
        }

        public ModelBasedSelect ignoredIssues(EnumSet<KnownIssue> issues)
        {
            ignoredIssues = issues;
            return this;
        }

        public ModelBasedSelect rangeInequalityGen(Gen<Conditional.Where.Inequality> rangeInequalityGen)
        {
            this.rangeInequalityGen = rangeInequalityGen;
            return this;
        }

        public List<Symbol> multiColumnQueryColumns()
        {
            List<Symbol> allowedColumns = searchableColumns;
            if (hasMultiNodeAllowFilteringWithLocalWritesIssue())
                allowedColumns = nonPartitionIndexedColumns;
            return allowedColumns;
        }
        
        public Gen<Annotated> fullTableScan()
        {
            Annotated annotated = new Annotated(Select.builder(metadata).build(), "full table scan");
            return Gens.constant(annotated);
        }
        
        public Gen<Annotated> existing()
        {
            return rs -> {
                NavigableSet<BytesPartitionState.Ref> keys = model.partitionKeys();
                BytesPartitionState.Ref ref = rs.pickOrderedSet(keys);
                Clustering<ByteBuffer> key = ref.key;

                Select.Builder builder = Select.builder().table(metadata);
                ImmutableUniqueList<Symbol> pks = model.factory.partitionColumns;
                ImmutableUniqueList<Symbol> cks = model.factory.clusteringColumns;
                for (Symbol pk : pks)
                    builder.value(pk, key.bufferAt(pks.indexOf(pk)));

                boolean wholePartition = cks.isEmpty() || rs.nextBoolean();
                if (!wholePartition)
                {
                    // find a row to select
                    BytesPartitionState partition = model.get(ref);
                    if (partition.isEmpty())
                    {
                        wholePartition = true;
                    }
                    else
                    {
                        NavigableSet<Clustering<ByteBuffer>> clusteringKeys = partition.clusteringKeys();
                        Clustering<ByteBuffer> clusteringKey = rs.pickOrderedSet(clusteringKeys);
                        for (Symbol ck : cks)
                            builder.value(ck, clusteringKey.bufferAt(cks.indexOf(ck)));
                    }
                }
                return new Annotated(builder.build(), (wholePartition ? "Whole Partition" : "Single Row"));
            };
        }

        public Gen<Annotated> token()
        {
            return rs -> {
                NavigableSet<BytesPartitionState.Ref> keys = model.partitionKeys();
                BytesPartitionState.Ref ref = rs.pickOrderedSet(keys);

                Select.Builder builder = Select.builder().table(metadata);
                builder.where(FunctionCall.tokenByColumns(model.factory.partitionColumns),
                              Conditional.Where.Inequality.EQUAL,
                              token(model.factory.partitionColumns, ref));
                
                return new Annotated(builder.build(), "by token");
            };
        }

        public Gen<Annotated> tokenRange()
        {
            return rs -> {
                NavigableSet<BytesPartitionState.Ref> keys = model.partitionKeys();
                BytesPartitionState.Ref start, end;
                switch (keys.size())
                {
                    case 1:
                        start = end = Iterables.get(keys, 0);
                        break;
                    case 2:
                        start = Iterables.get(keys, 0);
                        end = Iterables.get(keys, 1);
                        break;
                    case 0:
                        throw new IllegalArgumentException("Unable to select token ranges when no partitions exist");
                    default:
                    {
                        int si = rs.nextInt(0, keys.size() - 1);
                        int ei = rs.nextInt(si + 1, keys.size());
                        start = Iterables.get(keys, si);
                        end = Iterables.get(keys, ei);
                    }
                    break;
                }
                Select.Builder builder = Select.builder().table(metadata);
                FunctionCall pkToken = FunctionCall.tokenByColumns(model.factory.partitionColumns);
                boolean startInclusive = rs.nextBoolean();
                boolean endInclusive = rs.nextBoolean();
                if (startInclusive && endInclusive && rs.nextBoolean())
                {
                    // between
                    builder.between(pkToken, token(model.factory.partitionColumns, start), token(model.factory.partitionColumns, end));
                }
                else
                {
                    builder.where(pkToken,
                                  startInclusive ? Conditional.Where.Inequality.GREATER_THAN_EQ : Conditional.Where.Inequality.GREATER_THAN,
                                  token(model.factory.partitionColumns, start));
                    builder.where(pkToken,
                                  endInclusive ? Conditional.Where.Inequality.LESS_THAN_EQ : Conditional.Where.Inequality.LESS_THAN,
                                  token(model.factory.partitionColumns, end));
                }
                return new Annotated(builder.build(), "by token range");
            };
        }

        public Gen<Annotated> multiColumnQuery()
        {
            return rs -> {
                List<Symbol> allowedColumns = multiColumnQueryColumns();

                if (allowedColumns.size() <= 1)
                    throw new IllegalArgumentException("Unable to do multiple column query when there is only a single column");

                int numColumns = rs.nextInt(1, allowedColumns.size()) + 1;

                List<Symbol> cols = Gens.lists(Gens.pick(allowedColumns)).unique().ofSize(numColumns).next(rs);

                Select.Builder builder = Select.builder().table(metadata).allowFiltering();

                for (Symbol symbol : cols)
                {
                    TreeMap<ByteBuffer, List<BytesPartitionState.PrimaryKey>> universe = model.index(symbol);
                    NavigableSet<ByteBuffer> allowed = Sets.filter(universe.navigableKeySet(), b -> !ByteBufferUtil.EMPTY_BYTE_BUFFER.equals(b));
                    ByteBuffer value = value(rs, symbol, allowed);
                    builder.value(symbol, value);
                }

                String annotate = cols.stream().map(symbol -> {
                    var indexed = indexes.get(symbol);
                    return symbol.detailedName() + (indexed == null ? "" : " (indexed with " + indexed.indexDDL.indexer.name() + ")");
                }).collect(Collectors.joining(", "));
                return new Annotated(builder.build(), annotate);
            };
        }

        public Gen<Annotated> nonPartitionQuery()
        {
            return rs -> {
                Symbol symbol;
                if (hasMultiNodeAllowFilteringWithLocalWritesIssue())
                {
                    symbol = rs.pickUnorderedSet(indexes.keySet());
                }
                else
                {
                    symbol = rs.pick(searchableColumns);
                }
                TreeMap<ByteBuffer, List<BytesPartitionState.PrimaryKey>> universe = model.index(symbol);
                // we need to index 'null' so LT works, but we can not directly query it... so filter out when selecting values
                NavigableSet<ByteBuffer> allowed = Sets.filter(universe.navigableKeySet(), b -> !ByteBufferUtil.EMPTY_BYTE_BUFFER.equals(b));
                ByteBuffer value = value(rs, symbol, allowed);
                Select.Builder builder = Select.builder().table(metadata);

                EnumSet<CreateIndexDDL.QueryType> supported = !indexes.containsKey(symbol) ? EnumSet.noneOf(CreateIndexDDL.QueryType.class) : indexes.get(symbol).supportedQueries();
                if (supported.isEmpty() || !supported.contains(CreateIndexDDL.QueryType.Range))
                    builder.allowFiltering();

                // there are known SAI bugs, so need to avoid them to stay stable...
                if (indexes.containsKey(symbol) && indexes.get(symbol).indexDDL.indexer == CreateIndexDDL.SAI)
                {
                    if (symbol.type() == InetAddressType.instance
                        && ignoredIssues.contains(KnownIssue.SAI_INET_MIXED))
                        return eqSearch(rs, symbol, value, builder);
                }

                if (rs.nextBoolean())
                    return simpleRangeSearch(rs, symbol, value, builder);
                //TODO (coverage): define search that has a upper and lower bound: a > and a < | a beteeen ? and ?
                return eqSearch(rs, symbol, value, builder);
            };
        }

        public Gen<Annotated> partitionRestrictedQuery()
        {
            return rs -> {
                //TODO (now): remove duplicate logic
                NavigableSet<BytesPartitionState.Ref> keys = model.partitionKeys();
                BytesPartitionState.Ref ref = rs.pickOrderedSet(keys);
                Clustering<ByteBuffer> key = ref.key;

                Select.Builder builder = Select.builder().table(metadata);
                ImmutableUniqueList<Symbol> pks = model.factory.partitionColumns;
                for (Symbol pk : pks)
                    builder.value(pk, key.bufferAt(pks.indexOf(pk)));


                Symbol symbol;
                List<Symbol> searchableColumns = nonPartitionColumns;
                if (hasMultiNodeAllowFilteringWithLocalWritesIssue())
                {
                    if (nonPartitionIndexedColumns.isEmpty())
                        throw new AssertionError("Ignoring AF_MULTI_NODE_AND_NODE_LOCAL_WRITES is defined, but no non-partition columns are indexed");
                    symbol = rs.pick(nonPartitionIndexedColumns);
                }
                else
                {
                    symbol = rs.pick(searchableColumns);
                }

                TreeMap<ByteBuffer, List<BytesPartitionState.PrimaryKey>> universe = model.index(ref, symbol);
                // we need to index 'null' so LT works, but we can not directly query it... so filter out when selecting values
                NavigableSet<ByteBuffer> allowed = Sets.filter(universe.navigableKeySet(), b -> !ByteBufferUtil.EMPTY_BYTE_BUFFER.equals(b));
                ByteBuffer value = value(rs, symbol, allowed);

                EnumSet<CreateIndexDDL.QueryType> supported = !indexes.containsKey(symbol)
                                                              ? EnumSet.noneOf(CreateIndexDDL.QueryType.class)
                                                              : indexes.get(symbol).supportedQueries();
                if (supported.isEmpty() || !supported.contains(CreateIndexDDL.QueryType.Range))
                    builder.allowFiltering();

                // there are known SAI bugs, so need to avoid them to stay stable...
                if (indexes.containsKey(symbol) && indexes.get(symbol).indexDDL.indexer == CreateIndexDDL.SAI)
                {
                    if (symbol.type() == InetAddressType.instance
                        && ignoredIssues.contains(KnownIssue.SAI_INET_MIXED))
                        return eqSearch(rs, symbol, value, builder);
                }

                if (rs.nextBoolean())
                    return simpleRangeSearch(rs, symbol, value, builder);
                //TODO (coverage): define search that has a upper and lower bound: a > and a < | a beteeen ? and ?
                return eqSearch(rs, symbol, value, builder);
            };
        }

        public Gen<Annotated> all()
        {
            return buildAll().build();
        }

        public Gen<Gen<Annotated>> allWithDynamicWeights()
        {
            return buildAll().buildWithDynamicWeights();
        }

        private Gens.OneOfBuilder<Annotated> buildAll()
        {
            return Gens.<Annotated>oneOf()
                       .add(fullTableScan())
                       .add(existing())
                       .add(token())
                       .add(tokenRange())
                       .add(multiColumnQuery())
                       .add(nonPartitionQuery())
                       .add(partitionRestrictedQuery());
        }

        public static class Annotated
        {
            public final Select select;
            @Nullable
            public final String annotation;

            public Annotated(Select select, @Nullable String annotation)
            {
                this.select = select;
                this.annotation = annotation;
            }
        }

        private boolean hasMultiNodeAllowFilteringWithLocalWritesIssue()
        {
            return multiNode && ignoredIssues.contains(KnownIssue.AF_MULTI_NODE_AND_NODE_LOCAL_WRITES);
        }

        private Annotated simpleRangeSearch(RandomSource rs, Symbol symbol, ByteBuffer value, Select.Builder builder)
        {
            // do a simple search, like > or <
            Conditional.Where.Inequality kind = rangeInequalityGen.next(rs);
            builder.where(symbol, kind, value);
            var indexed = indexes.get(symbol);
            return new Annotated(builder.build(), symbol.detailedName() + (indexed == null ? "" : ", indexed with " + indexed.indexDDL.indexer.name()));
        }

        private Annotated eqSearch(RandomSource rs, Symbol symbol, ByteBuffer value, Select.Builder builder)
        {
            builder.value(symbol, value);
            var indexed = indexes.get(symbol);
            return new Annotated(builder.build(), symbol.detailedName() + (indexed == null ? "" : ", indexed with " + indexed.indexDDL.indexer.name()));
        }

        private ByteBuffer value(RandomSource rs, Symbol symbol, NavigableSet<ByteBuffer> allowed)
        {
            return !allowed.isEmpty() ? rs.pickOrderedSet(allowed) : dataGenFor.apply(symbol.type()).next(rs);
        }

        private static FunctionCall token(ImmutableUniqueList<Symbol> partitionColumns, BytesPartitionState.Ref ref)
        {
            Preconditions.checkNotNull(ref.key);
            List<Value> values = new ArrayList<>(ref.key.size());
            for (int i = 0; i < ref.key.size(); i++)
            {
                ByteBuffer bb = ref.key.bufferAt(i);
                Symbol type = partitionColumns.get(i);
                values.add(new Bind(bb, type.type()));
            }
            return FunctionCall.tokenByValue(values);
        }
    }
}

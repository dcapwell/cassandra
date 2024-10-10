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

package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.Invariants;
import accord.utils.RandomSource;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ast.Conditional;
import org.apache.cassandra.cql3.ast.CreateIndexDDL;
import org.apache.cassandra.cql3.ast.CreateIndexDDL.Indexer;
import org.apache.cassandra.cql3.ast.Expression;
import org.apache.cassandra.cql3.ast.ExpressionEvaluator;
import org.apache.cassandra.cql3.ast.Mutation;
import org.apache.cassandra.cql3.ast.ReferenceExpression;
import org.apache.cassandra.cql3.ast.Select;
import org.apache.cassandra.cql3.ast.Symbol;
import org.apache.cassandra.cql3.ast.TableReference;
import org.apache.cassandra.cql3.ast.Txn;
import org.apache.cassandra.cql3.ast.Where;
import org.apache.cassandra.db.BufferClustering;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.harry.model.DescriptorFactory;
import org.apache.cassandra.harry.model.OpSelectors;
import org.apache.cassandra.harry.model.reconciler.PartitionState;
import org.apache.cassandra.harry.visitors.VisitExecutor;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.AbstractTypeGenerators;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CassandraGenerators;
import org.awaitility.Awaitility;

import static accord.utils.Property.qt;
import static java.lang.String.format;

public class RandomSchemaV2Test extends CQLTester
{
    @BeforeClass
    public static void setUpClass()
    {
        prePrepareServer();

        // When values are large SAI will drop them... soooo... disable that... this test does not care about perf but correctness
        DatabaseDescriptor.getRawConfig().sai_frozen_term_size_warn_threshold = null;
        DatabaseDescriptor.getRawConfig().sai_frozen_term_size_fail_threshold = null;

        // Once per-JVM is enough
        prepareServer();
    }

    enum Mode { AccordEnabled, Default}

    @Test
    public void test()
    {
        requireNetwork();
        Gen<ProtocolVersion> clientVersionGen = rs -> {
            if (rs.decide(.8)) return null;
            return rs.pickOrderedSet(ProtocolVersion.SUPPORTED);
        };
        Gen<Mode> modeGen = Gens.enums().all(Mode.class);
        qt().withSeed(3449272830300698514L).withExamples(100).forAll(Gens.random(), modeGen, clientVersionGen).check((rs, mode, protocolVersion) -> {
            clearState();

            KeyspaceMetadata ks = createKeyspace(rs);
            TableMetadata metadata = createTable(rs, ks.name);
            if (mode == Mode.AccordEnabled)
            {
                // enable accord
                schemaChange(String.format("ALTER TABLE %s WITH " + TransactionalMode.full.asCqlParam(), metadata));
                metadata = Objects.requireNonNull(Schema.instance.getTableMetadata(ks.name, metadata.name));
            }
            Map<ColumnMetadata, CreateIndexDDL> indexedColumns = createIndex(rs, metadata);

            Model model = new Model(metadata);

            Mutation mutation = nonTransactionMutation(rs, metadata);
            if (mode == Mode.AccordEnabled)
                mutation = mutation.withoutTimestamp().withoutTTL(); // Accord doesn't allow custom timestamps or TTL
            if (mutation.kind != Mutation.Kind.DELETE)
                model.update(mutation);
            Select select = select(mutation);
            Object[][] expectedRows = rows(mutation);
            try
            {
                if (protocolVersion != null)
                {
                    //TODO (usability): as of this moment reads don't go through Accord when transaction_mode='full', so need to wrap the select in a txn
                    executeNet(protocolVersion, mode == Mode.AccordEnabled ? Txn.wrap(mutation) : mutation);
                    assertRowsNet(executeNet(protocolVersion, mode == Mode.AccordEnabled ? Txn.wrap(select) : select), expectedRows);
                }
                else
                {
                    execute(mode == Mode.AccordEnabled ? Txn.wrap(mutation) : mutation);
                    assertRows(execute(mode == Mode.AccordEnabled ? Txn.wrap(select) : select), expectedRows);
                }
                if (mutation.kind != Mutation.Kind.DELETE)
                    model.validate(select, expectedRows);
            }
            catch (Throwable t)
            {
                StringBuilder sb = new StringBuilder();
                sb.append("Error writing/reading:");
                sb.append("\nKeyspace:\n ").append(Keyspace.open(metadata.keyspace).getMetadata().toCqlString(false, false, false));
                CassandraGenerators.visitUDTs(metadata, udt -> sb.append("\nUDT:\n").append(udt.toCqlString(false, false, false)));
                sb.append("\nTable:\n").append(metadata.toCqlString(false, false, false));
                sb.append("\nMutation:\n").append(mutation);
                sb.append("\nSelect:\n").append(select);
                throw new AssertionError(sb.toString(), t);
            }

            checkIndexes(metadata, indexedColumns, mutation, mode, protocolVersion);
        });
    }

    private static class OffsetSet<T> extends AbstractSet<T>
    {
        private final BiMap<T, Integer> valueToOffsets = HashBiMap.create();
        private int counter = 0;

        public int offset(T value)
        {
            return valueToOffsets.get(value);
        }

        public T value(int offset)
        {
            return valueToOffsets.inverse().get(offset);
        }

        @Override
        public boolean add(T t)
        {
            if (valueToOffsets.containsKey(t)) return false;
            int offset = counter++;
            valueToOffsets.put(t, offset);
            return true;
        }

        @Override
        public Iterator<T> iterator()
        {
            var all = new ArrayList<>(valueToOffsets.entrySet());
            all.sort(Comparator.comparing(Map.Entry::getValue));
            return Iterators.transform(all.iterator(), Map.Entry::getKey);
        }

        @Override
        public int size()
        {
            return valueToOffsets.size();
        }
    }

    private static class Model
    {
        private final TableMetadata metadata;
        private final OffsetSet<Symbol> partitionColumns = new OffsetSet<>();
        private final OffsetSet<Symbol> clusteringColumns = new OffsetSet<>();
        private final OffsetSet<Symbol> staticColumns = new OffsetSet<>();
        private final OffsetSet<Symbol> regularColumns = new OffsetSet<>();
        private final DescriptorFactory.ValueDescriptorFactory partitionFactory = new DescriptorFactory.ValueDescriptorFactory();
        @Nullable
        private final DescriptorFactory.ValueDescriptorFactory clusteringFactory;
        private final Map<AbstractType<?>, DescriptorFactory.ValueDescriptorFactory> typeFactory = new HashMap<>();
        private final Map<Symbol, Integer> columnOffsets;
        private long time = 0;

        private Model(TableMetadata metadata)
        {
            this.metadata = metadata;
            metadata.partitionKeyColumns().forEach(c -> partitionColumns.add(new Symbol(c)));
            metadata.clusteringColumns().forEach(c -> clusteringColumns.add(new Symbol(c)));
            metadata.staticColumns().forEach(c -> staticColumns.add(new Symbol(c)));
            metadata.regularColumns().forEach(c -> regularColumns.add(new Symbol(c)));
            // its ok to override the factory as its empty...
            staticColumns.forEach(s -> typeFactory.put(s.type(), new DescriptorFactory.ValueDescriptorFactory()));
            regularColumns.forEach(s -> typeFactory.put(s.type(), new DescriptorFactory.ValueDescriptorFactory()));
            this.clusteringFactory = metadata.clusteringColumns().isEmpty() ? null : new DescriptorFactory.ValueDescriptorFactory();

            columnOffsets = new HashMap<>();
            {
                int offset = 0;
                for (ColumnMetadata col : (Iterable<ColumnMetadata>) () -> metadata.allColumnsInSelectOrder())
                    columnOffsets.put(Symbol.from(col), offset++);
            }
        }

        private final Map<Long, List<VisitExecutor.Operation>> pksToOps = new HashMap<>();

        void update(Mutation mutation)
        {
            long lts = time++;
            long opId = lts;
            long pd = partitionFactory.toDescriptor(toPartition(mutation.values));
            long cd = clusteringFactory.toDescriptor(toClustering(mutation.values));
            long[] sds = toDescriptors(staticColumns, mutation.values);
            long[] vds = toDescriptors(regularColumns, mutation.values);
            //TODO (coverage): DeleteRowOp/DeleteOp/DeleteColumnsOp
            VisitExecutor.WriteStaticOp op = new VisitExecutor.WriteStaticOp()
            {
                @Override
                public long pd()
                {
                    return pd;
                }

                @Override
                public long cd()
                {
                    return cd;
                }

                @Override
                public long[] sds()
                {
                    return sds;
                }

                @Override
                public long[] vds()
                {
                    return vds;
                }

                @Override
                public long lts()
                {
                    return lts;
                }

                @Override
                public long opId()
                {
                    return opId;
                }

                @Override
                public OpSelectors.OperationKind kind()
                {
                    return metadata.staticColumns().isEmpty()
                           ? OpSelectors.OperationKind.INSERT
                           : OpSelectors.OperationKind.INSERT_WITH_STATICS;
                }
            };
            pksToOps.computeIfAbsent(pd, i -> new ArrayList<>()).add(op);
        }

        void validate(Select select, Object[][] result)
        {
            Map<Symbol, Expression> values = new HashMap<>();
            select.streamRecursive().forEach(e -> {
                if (!(e instanceof Conditional)) return;

                if (e instanceof Where)
                {
                    Where where = (Where) e;
                    Invariants.checkArgument(where.kind == Where.Inequalities.EQUAL);
                    values.put(where.symbol.streamRecursive(true).filter(s -> s instanceof Symbol).map(s -> (Symbol) s).findFirst().get(), where.expression);
                }
                else if (e instanceof Conditional.In)
                {
                    Conditional.In in = (Conditional.In) e;
                    throw new UnsupportedOperationException("TODO");
                }
            });
            long pd = partitionFactory.toDescriptor(toPartition(values));
            long cd = clusteringFactory.toDescriptor(toClustering(values));
            long[] sds = toDescriptors(staticColumns, result);
            long[] vds = toDescriptors(regularColumns, result);
            List<VisitExecutor.Operation> ops = pksToOps.get(pd);
            if (ops == null) throw new AssertionError("Unknown pd: " + pd);
            //TODO: waiting on Alex
            PartitionState state = null;
//            DataGenerators.UNSET_DESCR;
            // exclude static columns, and do a static column search in the partition
        }

        private long[] toDescriptors(OffsetSet<Symbol> columns, Object[][] result)
        {
            if (result.length == 0) return new long[0];
            Object[] row = result[0];
            long[] ds = new long[columns.size()];
            for (Symbol s : columns)
            {
                Object value = row[columnOffsets.get(s)];
                ByteBuffer bb = value instanceof ByteBuffer ? (ByteBuffer) value: ((AbstractType) s.type()).decompose(value);
                ds[columns.offset(s)] = typeFactory.get(s.type()).toDescriptor(bb);
            }
            return ds;
        }

        private long[] toDescriptors(OffsetSet<Symbol> columns, Map<Symbol, ? extends Expression> values)
        {
            if (columns.isEmpty()) return new long[0];
            long[] ds = new long[columns.size()];
            for (Symbol s : columns)
                ds[columns.offset(s)] = typeFactory.get(s.type()).toDescriptor(ExpressionEvaluator.tryEvalEncoded(values.get(s)).get());
            return ds;
        }

        private ByteBuffer toPartition(Map<Symbol, ? extends Expression> columns)
        {
            return toKey(this.partitionColumns, columns);
        }

        private ByteBuffer toClustering(Map<Symbol, ? extends Expression> columns)
        {
            return toKey(this.clusteringColumns, columns);
        }

        private static ByteBuffer toKey(OffsetSet<Symbol> columns, Map<Symbol, ? extends Expression> keys)
        {
            switch (columns.size())
            {
                case 0: return ByteBufferUtil.EMPTY_BYTE_BUFFER;
                case 1: return ExpressionEvaluator.tryEvalEncoded(keys.get(Iterables.getFirst(columns, null))).get();
            }
            ByteBuffer[] bbs = new ByteBuffer[columns.size()];
            int offset = 0;
            for (Symbol s : columns)
            {
                Optional<ByteBuffer> eval = ExpressionEvaluator.tryEvalEncoded(keys.get(s));
                bbs[offset++] = eval.get();
            }
            return BufferClustering.make(bbs).serializeAsPartitionKey();
        }
    }

    private void checkIndexes(TableMetadata metadata, Map<ColumnMetadata, CreateIndexDDL> indexedColumns, Mutation mutation, Mode mode, ProtocolVersion protocolVersion)
    {
        if (indexedColumns.isEmpty()) return;

        // 2i time!
        // check each column 1 by 1
        for (ColumnMetadata col : indexedColumns.keySet())
        {
            Select select = select(mutation, col, mode);
            try
            {
                if (protocolVersion != null)
                {
                    assertRowsNet(executeNet(protocolVersion, mode == Mode.AccordEnabled ? Txn.wrap(select) : select),
                                  rows(mutation));
                }
                else
                {
                    assertRows(execute(mode == Mode.AccordEnabled ? Txn.wrap(select) : select),
                               rows(mutation));
                }
            }
            catch (Throwable t)
            {
                throw new AssertionError(format("Error reading %s index for col %s: %s:\nKeyspace:\n %s\nTable:\n%s",
                                                indexedColumns.get(col).indexer.name().toCQL(),
                                                col, col.type.unwrap().asCQL3Type(),
                                                Keyspace.open(metadata.keyspace).getMetadata().toCqlString(false, false, false),
                                                metadata.toCqlString(false, false, false)),
                                         t);
            }
        }
        filterKnownIssues(indexedColumns);

        // check what happens when we query all columns within a single indexer
        var groupByIndexer = indexedColumns.entrySet().stream().collect(Collectors.groupingBy(e -> e.getValue().indexer.name(), Collectors.toMap(e -> e.getKey(), e -> e.getValue())));
        for (var e : groupByIndexer.entrySet())
        {
            if (e.getValue().size() == 1) continue; // tested already doing single column queries
            checkMultipleIndexes(metadata, e.getValue(), mutation, mode, protocolVersion);
        }

        // check what happens when we query with all columns
        checkMultipleIndexes(metadata, indexedColumns, mutation, mode, protocolVersion);
    }

    private void filterKnownIssues(Map<ColumnMetadata, CreateIndexDDL> indexedColumns)
    {
        // Remove once CASSANDRA-19891 is fixed
        List<ColumnMetadata> toRemove = new ArrayList<>();
        for (Map.Entry<ColumnMetadata, CreateIndexDDL> e : indexedColumns.entrySet())
        {
            AbstractType<?> type = e.getKey().type.unwrap();
            if (type instanceof CompositeType && AbstractTypeGenerators.contains(type, t -> t instanceof MapType))
                toRemove.add(e.getKey());
        }
        toRemove.forEach(indexedColumns::remove);
    }

    private void checkMultipleIndexes(TableMetadata metadata, Map<ColumnMetadata, CreateIndexDDL> indexedColumns, Mutation mutation, Mode mode, ProtocolVersion protocolVersion)
    {
        Select select = select(mutation, indexedColumns.keySet(), mode);
        if (requiresAllowFiltering(indexedColumns))
            select = select.withAllowFiltering();
        try
        {
            if (protocolVersion != null)
            {
                assertRowsNet(executeNet(protocolVersion, mode == Mode.AccordEnabled ? Txn.wrap(select) : select),
                              rows(mutation));
            }
            else
            {
                assertRows(execute(mode == Mode.AccordEnabled ? Txn.wrap(select) : select),
                           rows(mutation));
            }
        }
        catch (Throwable t)
        {
            throw new AssertionError(format("Error reading all indexes:\nKeyspace:\n %s\nTable:\n%s\nIndexes: [%s]",
                                            Keyspace.open(metadata.keyspace).getMetadata().toCqlString(false, false, false),
                                            metadata.toCqlString(false, false, false),
                                            indexedColumns.values().stream().map(ddl -> ddl.toCQL().replace("org.apache.cassandra.db.marshal.", "")).collect(Collectors.joining(";\n\t"))),
                                     t);
        }
    }

    private boolean requiresAllowFiltering(Map<ColumnMetadata, CreateIndexDDL> indexedColumns)
    {
        return indexedColumns.values().stream().anyMatch(c -> c.indexer.kind() != Indexer.Kind.sai);
    }

    private static Select select(Mutation mutation, Set<ColumnMetadata> columns, Mode mode)
    {
        Select.Builder builder = new Select.Builder().withTable(mutation.table);
        for (ColumnMetadata col : columns)
        {
            Symbol symbol = Symbol.from(col);
            builder.withColumnEquals(symbol, mutation.values.get(symbol));
        }
        if (mode == Mode.AccordEnabled )
        {
            for (ColumnMetadata col : mutation.table.partitionKeyColumns())
            {
                if (columns.contains(col)) continue; // already included
                Symbol symbol = Symbol.from(col);
                builder.withColumnEquals(symbol, mutation.values.get(symbol));
            }
            builder.withLimit(1);
        }
        return builder.build();
    }

    private static Select select(Mutation mutation, ColumnMetadata col, Mode mode)
    {
        return select(mutation, Collections.singleton(col), mode);
    }

    private Map<ColumnMetadata, CreateIndexDDL> createIndex(RandomSource rs, TableMetadata metadata)
    {
        Map<ColumnMetadata, CreateIndexDDL> indexed = new LinkedHashMap<>();
        for (ColumnMetadata col : metadata.columns())
        {
            if (col.type.isReversed()) continue; //TODO (correctness): see https://issues.apache.org/jira/browse/CASSANDRA-19889
            if (col.name.toString().length() >= 48) continue; // TODO (correctness): https://issues.apache.org/jira/browse/CASSANDRA-19897

            AbstractType<?> type = col.type.unwrap();
            if (type.isCollection() && !type.isFrozenCollection()) continue; //TODO (coverage): include non-frozen collections;  the index part works fine, its the select that fails... basic equality isn't allowed for map type... so how do you query?
            List<Indexer> allowed = allowed(metadata, col);
            if (allowed.isEmpty()) continue;
            Indexer indexer = rs.pick(allowed);
            ReferenceExpression colExpression = Symbol.from(col);
            if (type.isFrozenCollection())
                colExpression = new CreateIndexDDL.CollectionReference(CreateIndexDDL.CollectionReference.Kind.FULL, colExpression);

            String name = createIndexName();
            CreateIndexDDL ddl = new CreateIndexDDL(rs.pick(CreateIndexDDL.Version.values()),
                                                    indexer,
                                                    Optional.of(new Symbol(name, UTF8Type.instance)),
                                                    TableReference.from(metadata),
                                                    Collections.singletonList(colExpression),
                                                    Collections.emptyMap());
            String stmt = ddl.toCQL();
            logger.info(stmt);
            schemaChange(stmt);

            SecondaryIndexManager indexManager = Keyspace.open(metadata.keyspace).getColumnFamilyStore(metadata.name).indexManager;
            Index index = indexManager.getIndexByName(name);
            Awaitility.await(stmt)
                      .atMost(1, TimeUnit.MINUTES)
                      .until(() -> indexManager.isIndexQueryable(index));

            indexed.put(col, ddl);
        }
        return indexed;
    }

    private List<Indexer> allowed(TableMetadata metadata, ColumnMetadata col)
    {
        return CreateIndexDDL.supportedIndexers().stream()
                             .filter(i -> i.supported(metadata, col))
                             .collect(Collectors.toList());
    }
}

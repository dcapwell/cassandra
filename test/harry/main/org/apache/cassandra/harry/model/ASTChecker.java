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

package org.apache.cassandra.harry.model;

import java.nio.ByteBuffer;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

import accord.utils.Invariants;
import org.apache.cassandra.cql3.ast.Conditional;
import org.apache.cassandra.cql3.ast.Expression;
import org.apache.cassandra.cql3.ast.ExpressionEvaluator;
import org.apache.cassandra.cql3.ast.Mutation;
import org.apache.cassandra.cql3.ast.Select;
import org.apache.cassandra.cql3.ast.Symbol;
import org.apache.cassandra.cql3.ast.Where;
import org.apache.cassandra.db.BufferClustering;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.harry.data.ResultSetRow;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.dsl.HistoryBuilder;
import org.apache.cassandra.harry.model.reconciler.Reconciler;
import org.apache.cassandra.harry.operations.Query;
import org.apache.cassandra.harry.util.BitSet;
import org.apache.cassandra.harry.visitors.ReplayingVisitor;
import org.apache.cassandra.harry.visitors.VisitExecutor;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.assertj.core.api.Assertions;

public class ASTChecker
{
    public static final long[] EMPTY = new long[0];
    private final TableMetadata metadata;
    private final SchemaSpec schema;
    public final OffsetSet<Symbol> partitionColumns = new OffsetSet<>();
    public final OffsetSet<Symbol> clusteringColumns = new OffsetSet<>();
    public final OffsetSet<Symbol> staticColumns = new OffsetSet<>();
    public final OffsetSet<Symbol> regularColumns = new OffsetSet<>();
    private final DescriptorFactory.ValueDescriptorFactory descriptorFactory = new DescriptorFactory.ValueDescriptorFactory();
    private final Map<Symbol, Integer> columnOffsets;
    private long time = 0;

    public ASTChecker(TableMetadata metadata)
    {
        this.metadata = metadata;
        this.schema = SchemaSpec.fromTableMetadataUnsafe(metadata);
        metadata.partitionKeyColumns().forEach(c -> partitionColumns.add(new Symbol(c)));
        metadata.clusteringColumns().forEach(c -> clusteringColumns.add(new Symbol(c)));
        metadata.staticColumns().forEach(c -> staticColumns.add(new Symbol(c)));
        metadata.regularColumns().forEach(c -> regularColumns.add(new Symbol(c)));

        columnOffsets = new HashMap<>();
        {
            int offset = 0;
            for (ColumnMetadata col : (Iterable<ColumnMetadata>) metadata::allColumnsInSelectOrder)
                columnOffsets.put(Symbol.from(col), offset++);
        }
    }

    private final Map<Long, List<VisitExecutor.Operation>> pksToOps = new HashMap<>();

    public void update(Mutation mutation)
    {
        long lts = time++;
        long pd = descriptorFactory.toDescriptor(toPartition(mutation.values));
        long cd = mutation.values.keySet().containsAll(clusteringColumns) ? descriptorFactory.toDescriptor(toClustering(mutation.values)) : -1;
        VisitExecutor.Operation op;
        if (mutation.kind == Mutation.Kind.DELETE)
        {
            Set<Symbol> columns = mutation.values.keySet();
            Assertions.assertThat(columns).containsAll(partitionColumns);
            columns = Sets.difference(columns, partitionColumns);
            OpSelectors.OperationKind kind = OpSelectors.OperationKind.DELETE_PARTITION;
            if (columns.containsAll(clusteringColumns))
            {
                columns = Sets.difference(columns, clusteringColumns);
                if (columns.isEmpty())
                {
                    kind = OpSelectors.OperationKind.DELETE_ROW;
                }
                else
                {
                    kind = OpSelectors.OperationKind.DELETE_COLUMN;
                }
            }
            switch (kind)
            {
                case DELETE_PARTITION:
                    op = deletePartition(pd, lts);
                    break;
                case DELETE_ROW:
                    op = deleteRow(pd, cd, lts);
                    break;
                case DELETE_COLUMN:
                case DELETE_COLUMN_WITH_STATICS:
                    op = deleteColumn(pd, cd, columns, lts);
                    break;
                default:
                    throw new UnsupportedOperationException();

//                case DELETE_SLICE:
//                case DELETE_RANGE:
            }
        }
        else
        {
            long[] sds = toDescriptors(staticColumns, mutation.values);
            long[] vds = toDescriptors(regularColumns, mutation.values);
            op = new VisitExecutor.WriteStaticOp()
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
                    return lts;
                }

                @Override
                public OpSelectors.OperationKind kind()
                {
                    boolean hasStatics = !metadata.staticColumns().isEmpty();
                    switch (mutation.kind)
                    {
                        case UPDATE:
                            return !hasStatics
                                   ? OpSelectors.OperationKind.UPDATE
                                   : OpSelectors.OperationKind.UPDATE_WITH_STATICS;
                        case INSERT:
                            return !hasStatics
                                   ? OpSelectors.OperationKind.INSERT
                                   : OpSelectors.OperationKind.INSERT_WITH_STATICS;
                        default:
                            throw new UnsupportedOperationException(mutation.kind.name());
                    }
                }
            };
        }
        pksToOps.computeIfAbsent(pd, i -> new ArrayList<>()).add(op);
    }

    private VisitExecutor.DeleteOp deletePartition(long pd, long lts)
    {
        return new VisitExecutor.DeleteOp()
        {

            @Override
            public long pd()
            {
                return pd;
            }

            @Override
            public long lts()
            {
                return lts;
            }

            @Override
            public long opId()
            {
                return lts;
            }

            @Override
            public OpSelectors.OperationKind kind()
            {
                return OpSelectors.OperationKind.DELETE_PARTITION;
            }

            @Override
            public Query relations()
            {
                return new Query.SinglePartitionQuery(Query.QueryKind.SINGLE_PARTITION,
                                                      pd,
                                                      false,
                                                      Collections.emptyList(),
                                                      schema,
                                                      Query.Wildcard.instance);
            }
        };
    }

    private static VisitExecutor.Operation deleteRow(long pd, long cd, long lts)
    {
        return new VisitExecutor.DeleteRowOp()
        {
            @Override
            public long cd()
            {
                return cd;
            }

            @Override
            public long pd()
            {
                return pd;
            }

            @Override
            public long lts()
            {
                return lts;
            }

            @Override
            public long opId()
            {
                return lts;
            }

            @Override
            public OpSelectors.OperationKind kind()
            {
                return OpSelectors.OperationKind.DELETE_ROW;
            }
        };
    }

    private VisitExecutor.Operation deleteColumn(long pd, long cd, Set<Symbol> columns, long lts)
    {
        BitSet bs = new BitSet.BitSet64Bit(columnOffsets.size());
        columns.forEach(s -> bs.set(columnOffsets.get(s)));
        return new VisitExecutor.DeleteColumnsOp()
        {
            @Override
            public long pd()
            {
                return pd;
            }

            @Override
            public long lts()
            {
                return lts;
            }

            @Override
            public long opId()
            {
                return lts;
            }

            @Override
            public OpSelectors.OperationKind kind()
            {
                return Sets.intersection(staticColumns, columns).isEmpty() ? OpSelectors.OperationKind.DELETE_COLUMN : OpSelectors.OperationKind.DELETE_COLUMN_WITH_STATICS;
            }

            @Override
            public long cd()
            {
                return cd;
            }

            @Override
            public BitSet columns()
            {
                return bs;
            }
        };
    }

    public void validate(Select select, Object[][] result)
    {
        Map<Symbol, Expression> values = toValues(select);
        long pd = descriptorFactory.toDescriptor(toPartition(values));
        List<VisitExecutor.Operation> ops = pksToOps.get(pd);
        if (ops == null) throw new AssertionError("Unknown pd: " + pd);

        ReplayingVisitor.Visit[] visits = new ReplayingVisitor.Visit[ops.size()];
        for (int i = 0; i < ops.size(); i++)
            visits[i] = new ReplayingVisitor.Visit(i, pd, new VisitExecutor.Operation[]{ ops.get(i) });
        HistoryBuilder.LongIterator iter = new SequentialLongIterator(0, visits.length);
        Reconciler reconciler = new Reconciler(schema);
        SimplifiedQuiescentChecker.validate(schema,
                                            schema.allColumnsSet,
                                            reconciler.inflatePartitionState(pd, createQuery(select, values, pd, schema), (visitExecutor) -> new ReplayingVisitor(visitExecutor, iter)
                                            {
                                                @Override
                                                public Visit getVisit(long lts)
                                                {
                                                    return visits[(int) lts];
                                                }

                                                @Override
                                                public void replayAll()
                                                {
                                                    while (iter.hasNext())
                                                        visit(iter.getAsLong());
                                                }

                                                @Override
                                                public void replayAll(long pd)
                                                {
                                                    replayAll();
                                                }
                                            }),
                                            rows(result));
    }

    private Query createQuery(Select select, Map<Symbol, Expression> values, long pd, SchemaSpec schema)
    {
        Query query;
        switch (inferKind(select, values))
        {
            case SINGLE_PARTITION:
            {
                query = new Query.SinglePartitionQuery(Query.QueryKind.SINGLE_PARTITION,
                                                       pd,
                                                       false,
                                                       Collections.emptyList(),
                                                       schema,
                                                       Query.Wildcard.instance);
            }
            break;
            case SINGLE_CLUSTERING:
            {
                long cd = descriptorFactory.toDescriptor(toClustering(values));
                query = new Query.SingleClusteringQuery(Query.QueryKind.SINGLE_CLUSTERING,
                                                        pd,
                                                        cd,
                                                        false,
                                                        Collections.emptyList(),
                                                        schema);
            }
            break;
            default:
                throw new UnsupportedOperationException();
        }
        return query;
    }

    private Query.QueryKind inferKind(Select select, Map<Symbol, Expression> values)
    {
        Set<Symbol> symbols = values.keySet();
        if (symbols.containsAll(partitionColumns))
        {
            if (symbols.containsAll(clusteringColumns))
                return Query.QueryKind.SINGLE_CLUSTERING;
            return Query.QueryKind.SINGLE_PARTITION;
        }
        throw new UnsupportedOperationException();
    }

    private static Map<Symbol, Expression> toValues(Select select)
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
                throw new UnsupportedOperationException("TODO");
            }
        });
        return values;
    }

    public List<ResultSetRow> rows(Object[][] rows)
    {
        List<ResultSetRow> rs = new ArrayList<>();

        for (Object[] row : rows)
        {
            long pd = toDescriptor(row, partitionColumns);
            long cd = toDescriptor(row, clusteringColumns);

            long[] vds = new long[regularColumns.size()];
            for (Symbol reg : regularColumns)
                vds[regularColumns.offset(reg)] = descriptorFactory.toDescriptor(toBytes(row, reg));

            long[] sds = new long[staticColumns.size()];
            for (Symbol reg : staticColumns)
                sds[staticColumns.offset(reg)] = descriptorFactory.toDescriptor(toBytes(row, reg));

            rs.add(new ResultSetRow(pd, cd, sds, new long[staticColumns.size()], vds, new long[regularColumns.size()], Collections.emptyList()));
        }

        return rs;
    }

    @SuppressWarnings("unchecked")
    private ByteBuffer toBytes(Object[] row, Symbol symbol)
    {
        Object value = row[columnOffsets.get(symbol)];
        if (value instanceof List && symbol.type().unwrap() instanceof ListType)
        {
            // elements are bbs
            ListType<?> lt = (ListType<?>) symbol.type().unwrap();
            List<?> list = (List<?>) value;
            if (list.isEmpty())
            {
                value = lt.pack(Collections.emptyList());
            }
            else if (list.get(0) instanceof ByteBuffer)
            {
                value = lt.pack(((List<ByteBuffer>) value));
            }
        }
        return value instanceof ByteBuffer
               ? (ByteBuffer) value
               : ((AbstractType) symbol.type()).decompose(value);
    }

    private long toDescriptor(Object[] row, OffsetSet<Symbol> columns)
    {
        if (columns.isEmpty())
            return descriptorFactory.toDescriptor(ByteBufferUtil.EMPTY_BYTE_BUFFER);
        if (columns.size() == 1)
            return descriptorFactory.toDescriptor(toBytes(row, columns.value(0)));
        ByteBuffer[] bbs = new ByteBuffer[columns.size()];
        int offset = 0;
        for (Symbol symbol : columns)
            bbs[offset++] = toBytes(row, symbol);
        return descriptorFactory.toDescriptor(BufferClustering.make(bbs).serializeAsPartitionKey());
    }

    private long[] toDescriptors(OffsetSet<Symbol> columns, Map<Symbol, ? extends Expression> values)
    {
        if (columns.isEmpty()) return EMPTY;
        long[] ds = new long[columns.size()];
        for (Symbol s : columns)
            ds[columns.offset(s)] = descriptorFactory.toDescriptor(ExpressionEvaluator.tryEvalEncoded(values.get(s)).get());
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
            case 0:
                return ByteBufferUtil.EMPTY_BYTE_BUFFER;
            case 1:
                return ExpressionEvaluator.tryEvalEncoded(keys.get(Iterables.getFirst(columns, null))).get();
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
            all.sort(Map.Entry.comparingByValue());
            return Iterators.transform(all.iterator(), Map.Entry::getKey);
        }

        @Override
        public int size()
        {
            return valueToOffsets.size();
        }
    }

    public static class SequentialLongIterator implements HistoryBuilder.LongIterator
    {
        private long current;
        private final long end;

        public SequentialLongIterator(long start, long end)
        {
            this.current = start;
            this.end = end;
        }

        @Override
        public long getAsLong()
        {
            return current++;
        }

        @Override
        public boolean hasNext()
        {
            return current < end;
        }
    }
}

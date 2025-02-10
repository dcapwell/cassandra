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

package org.apache.cassandra.distributed.test.cql3;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.Property;
import accord.utils.RandomSource;
import org.apache.cassandra.cql3.KnownIssue;
import org.apache.cassandra.cql3.ast.CreateIndexDDL;
import org.apache.cassandra.cql3.ast.Mutation;
import org.apache.cassandra.cql3.ast.ReferenceExpression;
import org.apache.cassandra.cql3.ast.Symbol;
import org.apache.cassandra.cql3.ast.TableReference;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.test.sai.SAIUtil;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ASTGenerators;
import org.apache.cassandra.utils.AbstractTypeGenerators;
import org.apache.cassandra.utils.AbstractTypeGenerators.TypeGenBuilder;
import org.apache.cassandra.utils.CassandraGenerators.TableMetadataBuilder;

import static accord.utils.Property.commands;
import static accord.utils.Property.stateful;
import static org.apache.cassandra.utils.Generators.toGen;

//TODO (coverage): add partition restricted clustering range queries: eg. WHERE pk=? and ck BETWEEN ? AND ?
public class SingleNodeTableWalkTest extends StatefulASTBase
{
    private static final Logger logger = LoggerFactory.getLogger(SingleNodeTableWalkTest.class);

    protected void preCheck(Cluster cluster, Property.StatefulBuilder builder)
    {
        // if a failing seed is detected, populate here
        // Example: builder.withSeed(42L);
        // CQL operations may have opertors such as +, -, and / (example 4 + 4), to "apply" them to get a constant value
        // CQL_DEBUG_APPLY_OPERATOR = true;
    }

    protected TypeGenBuilder supportedTypes()
    {
        return AbstractTypeGenerators.withoutUnsafeEquality(AbstractTypeGenerators.builder()
                                                                                  .withTypeKinds(AbstractTypeGenerators.TypeKind.PRIMITIVE));
    }

    protected List<CreateIndexDDL.Indexer> supportedIndexers()
    {
        // since legacy is async it's not clear how the test can wait for the background write to complete...
        return Collections.singletonList(CreateIndexDDL.SAI);
    }

    protected State createState(RandomSource rs, Cluster cluster)
    {
        return new State(rs, cluster);
    }

    protected Cluster createCluster() throws IOException
    {
        return createCluster(1, i -> {});
    }

    @Test
    public void test() throws IOException
    {
        try (Cluster cluster = createCluster())
        {
            Property.StatefulBuilder statefulBuilder = stateful().withExamples(10).withSteps(400);
            preCheck(cluster, statefulBuilder);
            statefulBuilder.check(commands(() -> rs -> createState(rs, cluster))
                                  .add(StatefulASTBase::insert)
                                  .add(StatefulASTBase::fullTableScan)
                                  .addIf(State::hasPartitions, (rs, state) -> state.command(rs, state.selects.existing()))
                                  .addAllIf(State::supportTokens, b -> b.add((rs, state) -> state.command(rs, state.selects.token()))
                                                                        .add((rs, state) -> state.command(rs, state.selects.tokenRange())))
                                  .addIf(State::hasEnoughMemtable, StatefulASTBase::flushTable)
                                  .addIf(State::hasEnoughSSTables, StatefulASTBase::compactTable)
                                  .addIf(State::allowNonPartitionQuery, (rs, state) -> state.command(rs, state.selects.nonPartitionQuery()))
                                  .addIf(State::allowNonPartitionMultiColumnQuery, (rs, state) -> state.command(rs, state.selects.multiColumnQuery()))
                                  .addIf(State::allowPartitionQuery, (rs, state) -> state.command(rs, state.selects.partitionRestrictedQuery()))
                                  .destroyState(State::close)
                                  .onSuccess(onSuccess(logger))
                                  .build());
        }
    }

    protected TableMetadata defineTable(RandomSource rs, String ks)
    {
        //TODO (correctness): the id isn't correct... this is what we use to create the table, so would miss the actual ID
        // Defaults may also be incorrect, but given this is the same version it "shouldn't"
        //TODO (coverage): partition is defined at the cluster level, so have to hard code in this model as the table is changed rather than cluster being recreated... this limits coverage
        return toGen(new TableMetadataBuilder()
                     .withTableKinds(TableMetadata.Kind.REGULAR)
                     .withKnownMemtables()
                     .withKeyspaceName(ks).withTableName("tbl")
                     .withSimpleColumnNames()
                     .withDefaultTypeGen(supportedTypes())
                     .withPartitioner(Murmur3Partitioner.instance)
                     .build())
               .next(rs);
    }

    private List<CreateIndexDDL.Indexer> columnSupportsIndexing(TableMetadata metadata, ColumnMetadata col)
    {
        return supportedIndexers().stream()
                                  .filter(i -> i.supported(metadata, col))
                                  .collect(Collectors.toList());
    }

    public class State extends CommonState
    {
        private final Gen<Mutation> mutationGen;

        public State(RandomSource rs, Cluster cluster)
        {
            super(rs, cluster, defineTable(rs, nextKeyspace()));

            cluster.forEach(i -> i.nodetoolResult("disableautocompaction", metadata.keyspace, this.metadata.name).asserts().success());

            List<LinkedHashMap<Symbol, Object>> uniquePartitions = Gens.lists(ASTGenerators.columnValues(model.factory.partitionColumns))
                                                                       .uniqueBestEffort()
                                                                       .ofSize(rs.nextInt(1, 10))
                                                                       .next(rs);

            this.mutationGen = new ASTGenerators.MutationGenBuilder(metadata)
                               .withoutTransaction()
                               .withoutTtl()
                               .withoutTimestamp()
                               .withPartitions(Gens.pick(uniquePartitions))
                               .build();
        }

        @Override
        protected LinkedHashMap<Symbol, CreateIndexDDL.IndexedColumn> createTable(TableMetadata metadata)
        {
            super.createTable(metadata);
            return createIndexes(rs, metadata);
        }

        @Override
        protected Gen<Mutation> mutationGen()
        {
            return mutationGen;
        }

        private LinkedHashMap<Symbol, CreateIndexDDL.IndexedColumn> createIndexes(RandomSource rs, TableMetadata metadata)
        {
            LinkedHashMap<Symbol, CreateIndexDDL.IndexedColumn> indexed = new LinkedHashMap<>();
            // for some test runs, avoid using indexes
            if (rs.nextBoolean())
                return indexed;
            for (ColumnMetadata col : metadata.columnsInFixedOrder())
            {
                Symbol symbol = Symbol.from(col);
                AbstractType<?> type = symbol.type();

                if (col.name.toString().length() >= 48
                    && IGNORED_ISSUES.contains(KnownIssue.CUSTOM_INDEX_MAX_COLUMN_48))
                    continue;

                if (type.isCollection() && !type.isFrozenCollection()) continue; //TODO (coverage): include non-frozen collections;  the index part works fine, its the select that fails... basic equality isn't allowed for map type... so how do you query?
                List<CreateIndexDDL.Indexer> allowed = columnSupportsIndexing(metadata, col);
                if (allowed.isEmpty()) continue;
                CreateIndexDDL.Indexer indexer = rs.pick(allowed);
                ReferenceExpression colExpression = Symbol.from(col);
                if (type.isFrozenCollection())
                    colExpression = new CreateIndexDDL.CollectionReference(CreateIndexDDL.CollectionReference.Kind.FULL, colExpression);

                String name = "tbl_" + col.name;
                CreateIndexDDL ddl = new CreateIndexDDL(rs.pick(CreateIndexDDL.Version.values()),
                                                        indexer,
                                                        Optional.of(new Symbol(name, UTF8Type.instance)),
                                                        TableReference.from(metadata),
                                                        Collections.singletonList(colExpression),
                                                        Collections.emptyMap());
                String stmt = ddl.toCQL();
                logger.info(stmt);
                cluster.schemaChange(stmt);

                //noinspection OptionalGetWithoutIsPresent
                SAIUtil.waitForIndexQueryable(cluster, metadata.keyspace, ddl.name.get().name());

                indexed.put(symbol, new CreateIndexDDL.IndexedColumn(symbol, ddl));
            }
            return indexed;
        }

        public boolean hasPartitions()
        {
            return !model.isEmpty();
        }

        public boolean supportTokens()
        {
            return hasPartitions();
        }

        public boolean allowNonPartitionQuery()
        {
            boolean result = !model.isEmpty() && !selects.searchableColumns.isEmpty();
            if (hasMultiNodeAllowFilteringWithLocalWritesIssue())
            {
                return hasNonPkIndexedColumns() && result;
            }
            return result;
        }

        public boolean allowNonPartitionMultiColumnQuery()
        {
            return allowNonPartitionQuery() && selects.multiColumnQueryColumns().size() > 1;
        }

        private boolean hasMultiNodeAllowFilteringWithLocalWritesIssue()
        {
            return isMultiNode() && IGNORED_ISSUES.contains(KnownIssue.AF_MULTI_NODE_AND_NODE_LOCAL_WRITES);
        }

        public boolean allowPartitionQuery()
        {
            if (model.isEmpty() || selects.nonPartitionColumns.isEmpty()) return false;
            if (hasMultiNodeAllowFilteringWithLocalWritesIssue())
                return hasNonPkIndexedColumns();
            return true;
        }

        private boolean hasNonPkIndexedColumns()
        {
            return !selects.nonPartitionIndexedColumns.isEmpty();
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("\nSetup:\n");
            toString(sb);
            indexes.values().forEach(c -> sb.append('\n').append(c.indexDDL.toCQL()).append(';'));
            return sb.toString();
        }
    }
}

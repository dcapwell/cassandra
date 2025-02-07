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

package org.apache.cassandra.simulator.test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.junit.Test;

import accord.utils.DefaultRandom;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;
import accord.utils.SeedProvider;
import org.apache.cassandra.cql3.ast.Mutation;
import org.apache.cassandra.cql3.ast.Select;
import org.apache.cassandra.cql3.ast.StandardVisitors;
import org.apache.cassandra.cql3.ast.Statement;
import org.apache.cassandra.cql3.ast.Symbol;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.impl.NodeLocalQuery;
import org.apache.cassandra.distributed.impl.Query;
import org.apache.cassandra.distributed.impl.RowUtil;
import org.apache.cassandra.harry.model.ASTSingleTableModel;
import org.apache.cassandra.harry.model.BytesPartitionState;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.reads.repair.ReadRepairStrategy;
import org.apache.cassandra.simulator.Action;
import org.apache.cassandra.simulator.ActionList;
import org.apache.cassandra.simulator.Actions;
import org.apache.cassandra.simulator.RunnableActionScheduler;
import org.apache.cassandra.simulator.cluster.ClusterActions;
import org.apache.cassandra.simulator.systems.SimulatedActionCallable;
import org.apache.cassandra.simulator.systems.SimulatedSystems;
import org.apache.cassandra.utils.ASTGenerators;
import org.apache.cassandra.utils.AbstractTypeGenerators;
import org.apache.cassandra.utils.CassandraGenerators;
import org.apache.cassandra.utils.FastByteOperations;
import org.apache.cassandra.utils.Generators;
import org.quicktheories.generators.SourceDSL;

import static org.apache.cassandra.simulator.cluster.ClusterActions.InitialConfiguration.initializeAll;
import static org.apache.cassandra.utils.AbstractTypeGenerators.overridePrimitiveTypeSupport;
import static org.apache.cassandra.utils.AbstractTypeGenerators.stringComparator;
import static org.apache.cassandra.utils.Generators.toGen;

/**
 * In order to run these tests in your IDE, you need to first build a simulator jara
 *
 *    ant simulator-jars
 *
 * And then run your test using the following settings (omit add-* if you are running on jdk8):
 *
 -Dstorage-config=$MODULE_DIR$/test/conf
 -Djava.awt.headless=true
 -javaagent:$MODULE_DIR$/lib/jamm-0.4.0.jar
 -ea
 -Dcassandra.debugrefcount=true
 -Xss384k
 -XX:SoftRefLRUPolicyMSPerMB=0
 -XX:ActiveProcessorCount=2
 -XX:HeapDumpPath=build/test
 -Dcassandra.test.driver.connection_timeout_ms=10000
 -Dcassandra.test.driver.read_timeout_ms=24000
 -Dcassandra.memtable_row_overhead_computation_step=100
 -Dcassandra.test.use_prepared=true
 -Dcassandra.test.sstableformatdevelopment=true
 -Djava.security.egd=file:/dev/urandom
 -Dcassandra.testtag=.jdk11
 -Dcassandra.keepBriefBrief=true
 -Dcassandra.allow_simplestrategy=true
 -Dcassandra.strict.runtime.checks=true
 -Dcassandra.reads.thresholds.coordinator.defensive_checks_enabled=true
 -Dcassandra.test.flush_local_schema_changes=false
 -Dcassandra.test.messagingService.nonGracefulShutdown=true
 -Dcassandra.use_nix_recursive_delete=true
 -Dcie-cassandra.disable_schema_drop_log=true
 -Dlogback.configurationFile=file://$MODULE_DIR$/test/conf/logback-simulator.xml
 -Dcassandra.ring_delay_ms=10000
 -Dcassandra.tolerate_sstable_size=true
 -Dcassandra.skip_sync=true
 -Dcassandra.debugrefcount=false
 -Dcassandra.test.simulator.determinismcheck=strict
 -Dcassandra.test.simulator.print_asm=none
 -javaagent:$MODULE_DIR$/build/test/lib/jars/simulator-asm.jar
 -Xbootclasspath/a:$MODULE_DIR$/build/test/lib/jars/simulator-bootstrap.jar
 -XX:ActiveProcessorCount=4
 -XX:-TieredCompilation
 -XX:-BackgroundCompilation
 -XX:CICompilerCount=1
 -XX:Tier4CompileThreshold=1000
 -XX:ReservedCodeCacheSize=256M
 -Xmx16G
 -Xmx4G
 --add-exports java.base/jdk.internal.misc=ALL-UNNAMED
 --add-exports java.base/jdk.internal.ref=ALL-UNNAMED
 --add-exports java.base/sun.nio.ch=ALL-UNNAMED
 --add-exports java.management.rmi/com.sun.jmx.remote.internal.rmi=ALL-UNNAMED
 --add-exports java.rmi/sun.rmi.registry=ALL-UNNAMED
 --add-exports java.rmi/sun.rmi.server=ALL-UNNAMED
 --add-exports java.sql/java.sql=ALL-UNNAMED
 --add-exports java.rmi/sun.rmi.registry=ALL-UNNAMED
 --add-opens java.base/java.lang.module=ALL-UNNAMED
 --add-opens java.base/java.net=ALL-UNNAMED
 --add-opens java.base/jdk.internal.loader=ALL-UNNAMED
 --add-opens java.base/jdk.internal.ref=ALL-UNNAMED
 --add-opens java.base/jdk.internal.reflect=ALL-UNNAMED
 --add-opens java.base/jdk.internal.math=ALL-UNNAMED
 --add-opens java.base/jdk.internal.module=ALL-UNNAMED
 --add-opens java.base/jdk.internal.util.jar=ALL-UNNAMED
 --add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED
 --add-opens jdk.management.jfr/jdk.management.jfr=ALL-UNNAMED
 --add-opens java.desktop/com.sun.beans.introspect=ALL-UNNAMED
 */
public class RenameMeTest extends SimulationTestBase
{
    static
    {
        // limit text/bytes so they are not too big; mostly for debugging than anything
        overridePrimitiveTypeSupport(AsciiType.instance, AbstractTypeGenerators.TypeSupport.of(AsciiType.instance, SourceDSL.strings().ascii().ofLengthBetween(1, 10), stringComparator(AsciiType.instance)));
        overridePrimitiveTypeSupport(UTF8Type.instance, AbstractTypeGenerators.TypeSupport.of(UTF8Type.instance, Generators.utf8(1, 10), stringComparator(UTF8Type.instance)));
        overridePrimitiveTypeSupport(BytesType.instance, AbstractTypeGenerators.TypeSupport.of(BytesType.instance, Generators.bytes(1, 10), FastByteOperations::compareUnsigned));
    }

    @Test
    public void test() throws IOException
    {
        long seed = SeedProvider.instance.nextSeed();
        // To rerun a failed seed
//        seed = SimulationRunner.parseHex("0x2fdbf4dd8925cc9e");

        simulate(seed, ASTSingleTableSimulation::new);
    }

    public static class ASTSingleTableSimulation extends SimpleSimulation
    {
        private final String ks = "ks";
        private final RandomSource rs;
        private final TableMetadata metadata;
        private final ASTSingleTableModel model;
        private int steps = 0;

        protected ASTSingleTableSimulation(SimulatedSystems simulated, RunnableActionScheduler scheduler, Cluster cluster, ClusterActions.Options options)
        {
            super(simulated, scheduler, cluster, options);
            this.rs = new DefaultRandom(simulated.random.uniform(Long.MIN_VALUE, Long.MAX_VALUE)); //TODO (correctness): is "uniform" inclusive with max?
            this.metadata = defineTable(rs, ks);
            this.model = new ASTSingleTableModel(metadata);
        }

        protected AbstractTypeGenerators.TypeGenBuilder supportedTypes()
        {
            return AbstractTypeGenerators.withoutUnsafeEquality(AbstractTypeGenerators.builder()
                                                                                      .withTypeKinds(AbstractTypeGenerators.TypeKind.PRIMITIVE));
        }

        protected TableMetadata defineTable(RandomSource rs, String ks)
        {
            TableMetadata tbl = toGen(new CassandraGenerators.TableMetadataBuilder()
                                      .withTableKinds(TableMetadata.Kind.REGULAR)
                                      .withKnownMemtables()
                                      .withKeyspaceName(ks).withTableName("tbl")
                                      .withSimpleColumnNames()
                                      .withDefaultTypeGen(supportedTypes())
                                      .withPartitioner(Murmur3Partitioner.instance)
                                      .build())
                                .next(rs);
            return tbl.unbuild().params(tbl.params.unbuild().readRepair(ReadRepairStrategy.NONE).build()).build();
        }

        @Override
        protected ActionList initialize()
        {
            return ActionList.of(clusterActions.initializeCluster(initializeAll(cluster.size())),
                                 clusterActions.schemaChange(1, "CREATE KEYSPACE " + ks + " WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor' : "+Math.min(3, cluster.size())+"}"),
                                 clusterActions.schemaChange(1, metadata.toCqlString(false, false, false)));
        }

        @Override
        protected ActionList execute()
        {
            List<LinkedHashMap<Symbol, Object>> uniquePartitions = Gens.lists(toGen(ASTGenerators.columnValues(model.factory.partitionColumns)))
                                                                       .uniqueBestEffort()
                                                                       .ofSize(rs.nextInt(1, 100))
                                                                       .next(rs);

            List<Action> partitions = new ArrayList<>(uniquePartitions.size());
            uniquePartitions.forEach(p -> partitions.add(sequentialPartitionAccess(p)));
            return ActionList.of(partitions);
        }

        private Action sequentialPartitionAccess(LinkedHashMap<Symbol, Object> partition)
        {
            Gen<Mutation> mutationGen = toGen(new ASTGenerators.MutationGenBuilder(metadata)
                                              .withoutTransaction()
                                              .withoutTtl()
                                              .withoutTimestamp()
                                              .withPartitions(i -> partition)
                                              .build());
            Select fullPartitionRead = select(partition).build();
            //TODO (coverage): once SAI and ALLOW FILTERING issues are addressed for single partition queries, add them here
            Gens.OneOfBuilder<Action> commandsBuilder = Gens.<Action>oneOf()
                                                .add(i -> query(mutationGen.next(rs).withTimestamp(steps)))
                                                .add(i -> query(fullPartitionRead));
            if (!model.factory.clusteringColumns.isEmpty())
                commandsBuilder.add(i -> selectRow(partition));
            Gen<Gen<Action>> commands = commandsBuilder.buildWithDynamicWeights();

            return Actions.infiniteStream(1, new Supplier<>()
            {
                Gen<Action> actionGen = null;
                long resetActionsDeadlineNanos = -1;
                @Override
                public Action get()
                {
                    if (actionGen == null || simulated.time.nanoTime() > resetActionsDeadlineNanos)
                        resetActions();
                    steps++;
                    return actionGen.next(rs);
                }

                private void resetActions()
                {
                    actionGen = commands.next(rs);
                    // how long should this distribution be used?
                    //TODO (testing): don't hard code 1m
                    resetActionsDeadlineNanos = simulated.time.nanoTime() + TimeUnit.MINUTES.toNanos(1);
                }
            });
        }

        private Action selectRow(LinkedHashMap<Symbol, Object> partition)
        {

            var builder = select(partition);
            List<Clustering<ByteBuffer>> partitions = model.partitions(builder.build());
            switch (partitions.size())
            {
                case 0: return Actions.empty("No known clustering keys to select");
                case 1:
                    break;
                default:
                    throw new IllegalStateException("Model matched multiple partitions, only 1 is expected");
            }
            BytesPartitionState state = model.get(partitions.get(0));
            if (state == null)
                return Actions.empty("Partition doesn't exist, unable to select a clustering row");
            NavigableSet<Clustering<ByteBuffer>> clusteringKeys = state.clusteringKeys();
            if (clusteringKeys.isEmpty())
                return Actions.empty("Partition is empty, unable to select a clustering row");
            Clustering<ByteBuffer> clusteringKey = rs.pickOrderedSet(clusteringKeys);
            for (Symbol ck : model.factory.clusteringColumns)
                builder.value(ck, clusteringKey.bufferAt(model.factory.clusteringColumns.indexOf(ck)));
            return query(builder.build());
        }

        private Select.Builder select(LinkedHashMap<Symbol, Object> partition)
        {
            Select.Builder builder = Select.builder().table(metadata);
            for (var e : partition.entrySet())
                builder.value(e.getKey(), e.getValue());
            return builder;
        }

        private Action query(Select select)
        {
            return query(select, ConsistencyLevel.ALL, o -> model.validate(RowUtil.toByteBuffer(o), select));
        }

        private Action query(Mutation mutation)
        {
            return query(mutation, ConsistencyLevel.NODE_LOCAL, o -> model.update(mutation));
        }

        private Action query(Statement statement, ConsistencyLevel cl, Consumer<Object[][]> onSuccess)
        {
            int nodeId = cluster.size() == 1 ? 1 : rs.nextInt(0, cluster.size()) + 1;
            return new SimulatedActionCallable<>(statement.getClass().getSimpleName(),
                                                 Action.Modifiers.RELIABLE_NO_TIMEOUTS,
                                                 Action.Modifiers.RELIABLE_NO_TIMEOUTS,
                                                 simulated,
                                                 cluster.get(nodeId),
                                                 query(statement, cl))
            {
                @Override
                public void accept(Object[][] objects, Throwable throwable)
                {
                    if (throwable != null)
                    {
                        simulated.failures.accept(new AssertionError(displaySetup(), throwable));
                        return;
                    }
                    onSuccess.accept(objects);
                }
            };
        }

        private IIsolatedExecutor.SerializableCallable<Object[][]> query(Statement statement, ConsistencyLevel cl)
        {
            // Simulator acts differently than jvm-dtest, so ByteBuffer isn't safe!
            // java.lang.RuntimeException: java.io.NotSerializableException: java.nio.HeapByteBuffer
            // So switch to literals for now
            statement = statement.visit(StandardVisitors.BIND_TO_LITERAL);
            if (cl == ConsistencyLevel.NODE_LOCAL)
                return new NodeLocalQuery(statement.toCQL(), statement.binds());
            return new Query(statement.toCQL(), -1, false, cl, null, statement.binds());
        }

        private String displaySetup()
        {
            return "Setup:\n" +
                   "CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor' : 3};\n"
                   + metadata.toCqlString(false, false, false);
        }
    }
}

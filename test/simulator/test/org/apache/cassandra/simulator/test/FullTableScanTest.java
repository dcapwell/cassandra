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
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.impl.NodeLocalQuery;
import org.apache.cassandra.distributed.impl.Query;
import org.apache.cassandra.harry.model.ASTSingleTableModel;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.reads.repair.ReadRepairStrategy;
import org.apache.cassandra.simulator.Action;
import org.apache.cassandra.simulator.ActionList;
import org.apache.cassandra.simulator.Actions;
import org.apache.cassandra.simulator.Debug;
import org.apache.cassandra.simulator.RunnableActionScheduler;
import org.apache.cassandra.simulator.Simulation;
import org.apache.cassandra.simulator.cluster.ClusterActionListener;
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
import static org.apache.cassandra.simulator.cluster.ClusterActions.Options.noActions;
import static org.apache.cassandra.utils.AbstractTypeGenerators.getTypeSupport;
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
public class FullTableScanTest extends SimulationTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(FullTableScanTest.class);

    static
    {
        overridePrimitiveTypeSupport(AsciiType.instance, AbstractTypeGenerators.TypeSupport.of(AsciiType.instance, SourceDSL.strings().ascii().ofLengthBetween(1, 10), stringComparator(AsciiType.instance)));
        overridePrimitiveTypeSupport(UTF8Type.instance, AbstractTypeGenerators.TypeSupport.of(UTF8Type.instance, Generators.utf8(1, 10), stringComparator(UTF8Type.instance)));
        overridePrimitiveTypeSupport(BytesType.instance, AbstractTypeGenerators.TypeSupport.of(BytesType.instance, Generators.bytes(1, 10), FastByteOperations::compareUnsigned));
    }

    private static final Gen<Gen<Boolean>> WRITE_OR_SCAN_DISTRIBUTION = Gens.bools().mixedDistribution();

    @Test
    public void test() throws IOException
    {
        // To rerun a failed seed
//        testOne(SimulationRunner.parseHex("0x2fdb994d37286ebf"));
        for (int i = 0; i < 100; i++)
            testOne(SeedProvider.instance.nextSeed());
    }

    private static final Gen.IntGen THREAD_COUNT_GEN = Gens.pickInt(10, 100, 1000);

    private void testOne(long seed) throws IOException
    {
        RandomSource rs = new DefaultRandom(seed);
        simulate(seed, new Builder().threadCount(THREAD_COUNT_GEN.nextInt(rs))
                                    .nodes(3, 3)
                                    .dcs(1, 1));
    }
    
    static class Builder extends BasicSimulationBuilder
    {
        protected AbstractTypeGenerators.TypeGenBuilder supportedTypes()
        {
            return AbstractTypeGenerators.withoutUnsafeEquality(AbstractTypeGenerators.builder()
                                                                                      .withTypeKinds(AbstractTypeGenerators.TypeKind.PRIMITIVE));
        }

        protected TableMetadata defineTable(RandomSource rs, String ks)
        {
            //TODO (correctness): the id isn't correct... this is what we use to create the table, so would miss the actual ID
            // Defaults may also be incorrect, but given this is the same version it "shouldn't"
            //TODO (coverage): partition is defined at the cluster level, so have to hard code in this model as the table is changed rather than cluster being recreated... this limits coverage
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
        Simulation create(SimulatedSystems simulated, RunnableActionScheduler scheduler, Cluster cluster, ClusterActions.Options options)
        {
            ClusterActions clusterActions = new ClusterActions(simulated, cluster,
                                                               options, new ClusterActionListener.NoOpListener(), new Debug(new EnumMap<>(Debug.Info.class), new int[0]));
            return new DTestClusterSimulation(simulated, scheduler, cluster)
            {
                private RandomSource rs;
                private TableMetadata metadata;
                private ASTSingleTableModel model;
                private Gen<Mutation> mutationGen;
                private Gen<Boolean> writeOrScan;
                private final List<String> history = new ArrayList<>();
                private int steps = 0;
                private int examples = 0;
                private int writesSinceLastScan = 0;

                @Override
                protected ActionList initialize()
                {
                    rs = new DefaultRandom(simulated.random.uniform(Long.MIN_VALUE, Long.MAX_VALUE)); //TODO (correctness): is "uniform" inclusive with max?
                    writeOrScan = WRITE_OR_SCAN_DISTRIBUTION.next(rs);
                    ClusterActions.Options options = noActions(cluster.size());
                    ClusterActions clusterActions = new ClusterActions(simulated, cluster,
                                                                       options, new ClusterActionListener.NoOpListener(), new Debug(new EnumMap<>(Debug.Info.class), new int[0]));
                    return ActionList.of(clusterActions.initializeCluster(initializeAll(cluster.size())));
                }

                @Override
                protected ActionList teardown()
                {
                    return ActionList.of();
                }

                @Override
                protected ActionList execute()
                {
                    return ActionList.of(Actions.infiniteStream(1, () -> {
                        if (steps++ % 1000 == 0)
                        {
                            history.clear();
                            int example = examples++;
                            String ks = "ks" + example;
                            metadata = defineTable(rs, ks);
                            model = new ASTSingleTableModel(metadata);

                            List<LinkedHashMap<Symbol, Object>> uniquePartitions;
                            {
                                int unique = rs.nextInt(1, 10);
                                List<Symbol> columns = model.factory.partitionColumns;
                                List<Gen<?>> gens = new ArrayList<>(columns.size());
                                for (int i = 0; i < columns.size(); i++)
                                    gens.add(toGen(getTypeSupport(columns.get(i).type()).valueGen));
                                uniquePartitions = Gens.lists(r2 -> {
                                    LinkedHashMap<Symbol, Object> vs = new LinkedHashMap<>();
                                    for (int i = 0; i < columns.size(); i++)
                                        vs.put(columns.get(i), gens.get(i).next(r2));
                                    return vs;
                                }).uniqueBestEffort().ofSize(unique).next(rs);
                            }

                            this.mutationGen = toGen(new ASTGenerators.MutationGenBuilder(metadata)
                                                     .withoutTransaction()
                                                     .withoutTtl()
                                                     .withoutTimestamp()
                                                     .withPartitions(SourceDSL.arbitrary().pick(uniquePartitions))
                                                     .build());

                            return new Actions.ReliableAction("Create schema for example " + example, () -> {
                                List<Action> actions = new ArrayList<>();
                                actions.add(clusterActions.schemaChange(1, "CREATE KEYSPACE " + ks + " WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor' : 3}"));;
                                actions.add(clusterActions.schemaChange(1, metadata.toCqlString(false, false, false)));
                                return ActionList.of(actions).setStrictlySequential();
                            }, true);
                        }

                        int nodeId = cluster.size() == 1 ? 1 : rs.nextInt(0, cluster.size()) + 1;
                        if (writesSinceLastScan == 0 || writeOrScan.next(rs))
                        {
                            // write
                            var mutation = mutationGen.next(rs);
                            history.add(mutation.visit(StandardVisitors.DEBUG).toCQL() + " -- on node" + nodeId);
                            return new SimulatedActionCallable<>("Mutation",
                                                                 Action.Modifiers.RELIABLE_NO_TIMEOUTS,
                                                                 Action.Modifiers.RELIABLE_NO_TIMEOUTS,
                                                                 simulated,
                                                                 cluster.get(nodeId),
                                                                 query(mutation, ConsistencyLevel.NODE_LOCAL))
                            {
                                @Override
                                public void accept(Object[][] objects, Throwable throwable)
                                {
                                    if (throwable != null)
                                    {
                                        failures.accept(decorate(throwable));
                                        return;
                                    }
                                    model.update(mutation);
                                }
                            };
                        }
                        writesSinceLastScan = 0;
                        Select scan = Select.builder(metadata).build();
                        history.add(scan.visit(StandardVisitors.DEBUG).toCQL() + " -- on node " + nodeId);
                        return new SimulatedActionCallable<>("Full Table Scan",
                                                             Action.Modifiers.RELIABLE_NO_TIMEOUTS,
                                                             Action.Modifiers.RELIABLE_NO_TIMEOUTS,
                                                             simulated,
                                                             cluster.get(nodeId),
                                                             query(scan, ConsistencyLevel.ALL))
                        {
                            @Override
                            public void accept(Object[][] objects, Throwable throwable)
                            {
                                if (throwable != null)
                                {
                                    failures.accept(decorate(throwable));
                                    return;
                                }
                                model.validate(toRows(objects), scan);
                            }
                        };
                    }));
                }

                @Override
                public void close()
                {
                    logger.info(displayHistory());
                }

                private IIsolatedExecutor.SerializableCallable<Object[][]> query(Statement statement, ConsistencyLevel cl)
                {
                    if (statement instanceof Mutation)
                    {
                        // Due to simulator's control of time, the observed client behavior is not respected. Here is an example
                        //   Write A
                        //   Write B
                        //   Write C
                        // These writes are all sequential and the client sees them as success, so one would think that
                        // mean that A happens before B happens before C... but this is not true!
                        // Each instance can have a timestamp that drifts from peers, so C can have a smaller timestamp
                        // than B, which has a smaller timestamp than A!
                        // To work around this, make sure all mutations own their timestampss.
                        statement = ((Mutation) statement).withTimestamp(history.size() + 1);
                    }
                    // Simulator acts differently than jvm-dtest, so ByteBuffer isn't safe!
                    // java.lang.RuntimeException: java.io.NotSerializableException: java.nio.HeapByteBuffer
                    // So switch to literals for now
                    statement = statement.visit(StandardVisitors.BIND_TO_LITERAL);
                    if (cl == ConsistencyLevel.NODE_LOCAL)
                        return new NodeLocalQuery(statement.toCQL(), statement.binds());
                    return new Query(statement.toCQL(), -1, cl, null, statement.binds());
                }

                private String displayHistory()
                {
                    StringBuilder sb = new StringBuilder();
                    sb.append("Setup:\n\"CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor' : 3};\"\n").append(metadata.toCqlString(false, false, false));
                    int maxSpaces = spaces(history.size() - 1);
                    sb.append("\nHistory:");
                    for (int i = 0; i < history.size(); i++)
                        sb.append("\n\t").append(padded(i, maxSpaces)).append(": ").append(history.get(i));
                    return sb.toString();
                }

                private AssertionError decorate(Throwable t)
                {
                    return new AssertionError(displayHistory(), t);
                }
            };
        }

    }

    private static final ByteBuffer[][] EMPTY = new ByteBuffer[0][];
    private static ByteBuffer[][] toRows(Object[][] rows)
    {
        if (rows.length == 0) return EMPTY;
        ByteBuffer[][] result = new ByteBuffer[rows.length][];
        for (int i = 0; i < rows.length; i++)
        {
            Object[] in = rows[i];
            ByteBuffer[] out = new ByteBuffer[in.length];
            for (int j = 0; j < in.length; j++)
                out[j] = (ByteBuffer) in[j];
            result[i] = out;
        }
        return result;
    }

    private static int spaces(int value)
    {
        return Integer.toString(value).length();
    }

    private static String padded(int value, int maxSpaces)
    {
        int space = spaces(value);
        int padding = maxSpaces - space;
        return padding > 0
               ? String.format("%0" + maxSpaces + "d", value)
               : Integer.toString(value);
    }
}

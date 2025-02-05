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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.Consumer;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.utils.DefaultRandom;
import accord.utils.RandomSource;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.harry.execution.DataTracker;
import org.apache.cassandra.harry.gen.OperationsGenerators;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.reads.repair.ReadRepairStrategy;
import org.apache.cassandra.simulator.ActionSchedule;
import org.apache.cassandra.simulator.ActionSchedule.Work;
import org.apache.cassandra.simulator.ClusterSimulation;
import org.apache.cassandra.simulator.RunnableActionScheduler;
import org.apache.cassandra.simulator.Simulation;
import org.apache.cassandra.simulator.SimulationRunner;
import org.apache.cassandra.simulator.systems.SimulatedSystems;
import org.apache.cassandra.utils.AbstractTypeGenerators;
import org.apache.cassandra.utils.CassandraGenerators;
import org.apache.cassandra.utils.CloseableIterator;

import static accord.utils.Property.qt;
import static org.apache.cassandra.utils.Generators.regexWord;
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
public class FullTableScanTest extends SimulationRunner
{
    private static final Logger logger = LoggerFactory.getLogger(FullTableScanTest.class);
    public static final String KS = "ks";

    @Test
    public void test()
    {
        qt().withExamples(10).check(FullTableScanTest::test);
    }

    private static void test(RandomSource rs)
    {
//        logger.info("Seed 0x{}", Long.toHexString(seed));
    }

    static class BaseSimulationBuilder extends ClusterSimulation.Builder<BaseSimulation>
    {
        protected final Consumer<IInstanceConfig> configUpdater;

        BaseSimulationBuilder(Consumer<IInstanceConfig> configUpdater)
        {
            this.configUpdater = configUpdater;
        }

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
        public ClusterSimulation<BaseSimulation> create(long seed) throws IOException
        {
            org.apache.cassandra.simulator.RandomSource random = new org.apache.cassandra.simulator.RandomSource.Default();
            random.reset(seed);
            return new ClusterSimulation<>(random, seed, 1, this, configUpdater,
                                           (simulated, scheduler, cluster, options) -> {
                                               DefaultRandom rs = new DefaultRandom(seed);
                                               return new TestSimulation(simulated, scheduler, rs, defineTable(rs, KS));
                                           });
        }
    }

    static class TestSimulation extends BaseSimulation
    {
        private final RandomSource rs;
        private final TableMetadata metadata;

        protected TestSimulation(SimulatedSystems simulated, RunnableActionScheduler scheduler, RandomSource rs, TableMetadata metadata)
        {
            super(simulated, scheduler);
            this.rs = rs;
            this.metadata = metadata;
        }

        @Override
        Work[] work()
        {
            List<Work> work = new ArrayList<>();
            return work.toArray(Work[]::new);
        }
    }

    static abstract class BaseSimulation implements Simulation
    {
        protected final SimulatedSystems simulated;
        protected final RunnableActionScheduler scheduler;

        protected BaseSimulation(SimulatedSystems simulated, RunnableActionScheduler scheduler)
        {
            this.simulated = simulated;
            this.scheduler = scheduler;
        }

        abstract Work[] work();

        @Override
        public CloseableIterator<?> iterator()
        {
            return new ActionSchedule(simulated.time, simulated.futureScheduler, () -> 0L, scheduler, work());
        }

        @Override
        public void run()
        {
            try (CloseableIterator<?> iter = iterator())
            {
                while (iter.hasNext())
                {
                    checkForErrors();
                    iter.next();
                }
                checkForErrors();
            }
        }

        private void checkForErrors()
        {
            if (simulated.failures.hasFailure())
            {
                AssertionError error = new AssertionError("Errors detected during simulation");
                // don't care about the stack trace... the issue is the errors found and not what part of the scheduler we stopped
                error.setStackTrace(new StackTraceElement[0]);
                simulated.failures.get().forEach(error::addSuppressed);
                throw error;
            }
        }

        @Override
        public void close() throws Exception
        {

        }
    }
}

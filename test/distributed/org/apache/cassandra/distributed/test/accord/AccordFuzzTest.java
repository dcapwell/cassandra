///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.cassandra.distributed.test.accord;
//
//import java.io.IOException;
//import java.util.List;
//import java.util.concurrent.TimeUnit;
//
//import org.junit.Ignore;
//import org.junit.Test;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import accord.coordinate.Preempted;
//import org.apache.cassandra.cql3.ColumnIdentifier;
//import org.apache.cassandra.db.marshal.AbstractType;
//import org.apache.cassandra.db.marshal.ReversedType;
//import org.apache.cassandra.db.marshal.UTF8Type;
//import org.apache.cassandra.db.marshal.UserType;
//import org.apache.cassandra.distributed.Cluster;
//import org.apache.cassandra.distributed.api.ConsistencyLevel;
//import org.apache.cassandra.distributed.api.Feature;
//import org.apache.cassandra.distributed.shared.ClusterUtils;
//import org.apache.cassandra.distributed.test.TestBaseImpl;
//import org.apache.cassandra.schema.ColumnMetadata;
//import org.apache.cassandra.schema.TableMetadata;
//import org.apache.cassandra.service.accord.AccordService;
//import org.apache.cassandra.utils.AssertionUtils;
//import org.apache.cassandra.utils.CassandraGenerators;
//import org.apache.cassandra.utils.FailingConsumer;
//import org.apache.cassandra.utils.Generators;
//import org.apache.cassandra.utils.ast.Txn;
//import org.quicktheories.core.Gen;
//import org.quicktheories.generators.SourceDSL;
//
//import static org.quicktheories.QuickTheory.qt;
//
//public class AccordFuzzTest extends TestBaseImpl
//{
//    private static final Logger logger = LoggerFactory.getLogger(AccordFuzzTest.class);
//
//    private static final Gen<String> nameGen = Generators.SYMBOL_GEN;
//
//    @Test
//    @Ignore("Reported already")
//    public void repo() throws IOException
//    {
//        String createStatement = "CREATE TABLE distributed_test_keyspace.c (\n" +
//                                 "    \"A\" smallint PRIMARY KEY\n" +
//                                 ") WITH additional_write_policy = '99p'\n" +
//                                 "    AND bloom_filter_fp_chance = 0.01\n" +
//                                 "    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}\n" +
//                                 "    AND cdc = false\n" +
//                                 "    AND comment = ''\n" +
//                                 "    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}\n" +
//                                 "    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}\n" +
//                                 "    AND memtable = 'default'\n" +
//                                 "    AND crc_check_chance = 1.0\n" +
//                                 "    AND default_time_to_live = 0\n" +
//                                 "    AND extensions = {}\n" +
//                                 "    AND gc_grace_seconds = 864000\n" +
//                                 "    AND max_index_interval = 2048\n" +
//                                 "    AND memtable_flush_period_in_ms = 0\n" +
//                                 "    AND min_index_interval = 128\n" +
//                                 "    AND read_repair = 'BLOCKING'\n" +
//                                 "    AND speculative_retry = '99p';";
//        String cql = "BEGIN TRANSACTION\n" +
//                     "  LET ojWb3zq6tLR8szov3pe12ZZYOcMMvvrVGZJuwYcU = (SELECT \"A\"\n" +
//                     "                                                  FROM distributed_test_keyspace.c\n" +
//                     "                                                  WHERE \"A\" = -31546);\n" +
//                     "  LET gzbA53L = (SELECT \"A\"\n" +
//                     "                 FROM distributed_test_keyspace.c\n" +
//                     "                 WHERE \"A\" = -10448);\n" +
//                     "  LET t_Wb0OpWXtV9s6CxqM4RW2ira_WcrRz5Foy2baYllokAef = (SELECT \"A\"\n" +
//                     "                                                        FROM distributed_test_keyspace.c\n" +
//                     "                                                        WHERE \"A\" = 11926);\n" +
//                     "  LET pgsw9G = (SELECT \"A\"\n" +
//                     "                FROM distributed_test_keyspace.c\n" +
//                     "                WHERE \"A\" = 3868);\n" +
//                     "  SELECT \"A\"\n" +
//                     "  FROM distributed_test_keyspace.c\n" +
//                     "  WHERE \"A\" = 23219;\n" +
//                     "COMMIT TRANSACTION";
//        try (Cluster cluster = createCluster())
//        {
//            cluster.schemaChange(createStatement);
//            ClusterUtils.awaitGossipSchemaMatch(cluster);
//            cluster.forEach(node -> node.runOnInstance(() -> AccordService.instance.createEpochFromConfigUnsafe()));
//
//            cluster.coordinator(1).execute(cql, ConsistencyLevel.ANY);
//        }
//    }
//
//    @Test
//    public void test() throws IOException
//    {
//        class C
//        {
//            final TableMetadata metadata;
//            final List<Txn> transactions;
//
//            C(TableMetadata metadata, List<Txn> selects)
//            {
//                this.metadata = metadata;
//                this.transactions = selects;
//            }
//
//            @Override
//            public String toString()
//            {
//                StringBuilder sb = new StringBuilder();
//                sb.append("Table:\n").append(metadata.toCqlString(false, false));
//                sb.append("Transactions:\n");
//                for (int i = 0; i < transactions.size(); i++)
//                {
//                    Txn t = transactions.get(i);
//                    sb.append("Txn@").append(i).append("\n");
//                    sb.append(t.toCQL()).append('\n');
//                }
//                return sb.toString();
//            }
//        }
//        Gen<String> uniqTableNames = Generators.uniqueSymbolGen();
//        Gen<TableMetadata> metadataGen = CassandraGenerators.tableMetadataGenBuilder()
//                                                            .withKind(TableMetadata.Kind.REGULAR)
//                                                            .withKeyspace(KEYSPACE).withName(uniqTableNames).withColumnName(nameGen)
//                                                            .build();
//        Gen<C> gen = rnd -> {
//            TableMetadata metadata = metadataGen.generate(rnd);
//            List<Txn> selects = SourceDSL.lists().of(AccordGenerators.txnGen(metadata)).ofSize(10).generate(rnd);
//            return new C(metadata, selects);
//        };
//        try (Cluster cluster = createCluster())
//        {
//            qt().withFixedSeed(800226806560166L).withExamples(10).withShrinkCycles(0).forAll(gen).checkAssert(FailingConsumer.orFail(c -> {
//                // create UDTs if present
//                for (ColumnMetadata column : c.metadata.columns())
//                    maybeCreateUDT(cluster, column.type);
//                String createStatement = c.metadata.toCqlString(false, false);
//                logger.info("Creating table\n{}", createStatement);
//                cluster.schemaChange(createStatement);
//                ClusterUtils.awaitGossipSchemaMatch(cluster);
//                cluster.forEach(node -> node.runOnInstance(() -> AccordService.instance.createEpochFromConfigUnsafe()));
//                TimeUnit.SECONDS.sleep(10);
//
//                for (Txn t : c.transactions)
//                {
//                    logger.info("Trying Transaction\n{}", t.toCQL());
//                    while (true)
//                    {
//                        try
//                        {
//                            cluster.coordinator(1).execute(t.toCQL(), ConsistencyLevel.ANY, t.binds());
//                            break;
//                        }
//                        catch (Exception e)
//                        {
//                            //TODO this is a bad error to give to users... shouldn't this be retried automatically?
//                            if (AssertionUtils.rootCauseIs(Preempted.class).matches(e))
//                            {
//                                logger.info("Preempted, attempt retry...");
//                                continue;
//                            }
////                            Throwables.getRootCause(e).getClass().getdeclared
//                            throw new RuntimeException("Table:\n" + createStatement + "\nCQL:\n" + t, e);
//                        }
//                    }
//                }
//
//                AccordIntegrationTest.awaitAsyncApply(cluster);
//            }));
//            logger.info("Done");
//        }
//    }
//
//    private static void maybeCreateUDT(Cluster cluster, AbstractType<?> type)
//    {
//        for (AbstractType<?> subtype : type.subTypes())
//            maybeCreateUDT(cluster, subtype);
//        if (type.isUDT())
//        {
//            if (type.isReversed())
//                type = ((ReversedType) type).baseType;
//            UserType udt = (UserType) type;
//            cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + ColumnIdentifier.maybeQuote(udt.keyspace) + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + Math.min(3, cluster.size()) + "};");
//            logger.info("Creating UDT {}.{}", ColumnIdentifier.maybeQuote(udt.keyspace), ColumnIdentifier.maybeQuote(UTF8Type.instance.compose(udt.name)));
//            cluster.schemaChange(udt.toCqlString(false, true));
//        }
//    }
//
//    private static Cluster createCluster() throws IOException
//    {
//        return init(Cluster.build(2).withConfig(c -> c.with(Feature.NETWORK, Feature.GOSSIP).set("write_request_timeout", "30s")).start());
//    }
//}

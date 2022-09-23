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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.coordinate.Preempted;
import ch.qos.logback.classic.Level;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestTimeoutException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.AccordMessageSink;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.AccordVerbHandler;
import org.apache.cassandra.utils.AssertionUtils;
import org.apache.cassandra.utils.CassandraGenerators;
import org.apache.cassandra.utils.FailingConsumer;
import org.apache.cassandra.utils.Generators;
import org.apache.cassandra.utils.ast.Select;
import org.apache.cassandra.utils.ast.Statement;
import org.apache.cassandra.utils.ast.Txn;
import org.apache.cassandra.utils.ast.Mutation;
import org.quicktheories.core.Gen;

import static org.quicktheories.QuickTheory.qt;

//TODO
// Build a in-memory model of what the DB should look like and Limit to N partitions
// Make all Select/Mutation expressions go through a logic that makes them trippy... SELECT CAST((int) a AS int)...
// a += b support rather than rely on a = a + b
//Confirm fixed
// references in WHERE clause (blocked due to needing to know partition accessed, but clustering/regular clusters may also be involved)
// "InvalidRequestException: value references can't be used with primary key columns" should be more informative; which statement?
public class CqlFuzzTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(CqlFuzzTest.class);
    private static final Gen<TableMetadata> metadataGen = CassandraGenerators.tableMetadataGenBuilder()
                                                                             .withKind(TableMetadata.Kind.REGULAR)
                                                                             .withKeyspace(KEYSPACE).withName(Generators.uniqueSymbolGen())
                                                                             .withoutReversed()
                                                                             .build();
    private static Cluster cluster;

    @BeforeClass
    public static void setup() throws IOException
    {
        cluster = Cluster.build(2).start();
        init(cluster);
    }

    @AfterClass
    public static void teardown()
    {
        if (cluster != null)
            cluster.close();
    }

    @Test
    public void cql()
    {
        TableMetadata metadata = createTable();
        Gen<Statement> select = (Gen<Statement>) (Gen<?>) new Select.GenBuilder(metadata).build();
        // not doing CAS so can't support operators
        Gen<Statement> mutation = (Gen<Statement>) (Gen<?>) new Mutation.GenBuilder(metadata).withoutOperators().build();
        int weight = 100 / 4;
        Gen<Statement> statements = Generators.mix(ImmutableMap.of(select, weight, mutation, weight * 3));
        new Fuzz(metadata, statements).run();
    }

    @Test
    public void accord()
    {
        TableMetadata metadata = createTable();
        cluster.forEach(node -> node.runOnInstance(() -> AccordService.instance.createEpochFromConfigUnsafe()));
        // txn can be large causing issues when debug logging triggers
        cluster.forEach(i -> i.runOnInstance(() -> {
            setLoggerInfo("accord");
            setLoggerInfo(AccordMessageSink.class.getCanonicalName());
            setLoggerInfo(AccordVerbHandler.class.getCanonicalName());
            setLoggerInfo("org.apache.cassandra.service.accord.AccordCallback");
        }));

        Gen<Statement> statements = (Gen<Statement>) (Gen<?>) new Txn.GenBuilder(metadata).build();
        new Fuzz(metadata, statements).run();
    }

    private static class Fuzz
    {
        private final TableMetadata metadata;
        private final Gen<Statement> statements;

        private Fuzz(TableMetadata metadata, Gen<Statement> statements)
        {
            this.metadata = metadata;
            this.statements = statements;
        }

        protected void before(Statement stmt)
        {
        }

        protected void after(Statement stmt)
        {
        }

        protected void error(Statement stmt, Throwable t)
        {

        }

        public void run()
        {
            qt().withFixedSeed(32533285503833L).withShrinkCycles(0).forAll(statements).checkAssert(FailingConsumer.orFail(stmt -> {
                before(stmt);
                try
                {
                    cluster.coordinator(1).execute(stmt.toCQL(), ConsistencyLevel.QUORUM, stmt.binds());
                    after(stmt);
                    return;
                }
                catch (Exception e)
                {
                    error(stmt, e);
                    if (Throwables.getRootCause(e) instanceof InterruptedException)
                    {
                        // this was seen from SlabPoolCleaner but I am not even sure how it made it up here or what
                        // interrupted the thread...
                        logger.warn("Coordinator returned a InterruptedException, not sure how this happpened but it did...");
                        return;
                    }
                    if (AssertionUtils.rootCauseIsInstanceof(RequestTimeoutException.class).matches(e))
                    {
                        logger.info("Timeout seen", e);
                        after(stmt);
                        return;
                    }
                    if (AssertionUtils.rootCauseIsInstanceof(Preempted.class).matches(e))
                    {
                        logger.info("Preempted seen", e);
                        after(stmt);
                        return;
                    }
                    // sometimes the generator produces a schema where the partition key can be "too big" and gets
                    // rejected... rather than failing we just say "success"...
                    //TODO fix...
                    if (AssertionUtils.rootCauseIsInstanceof(InvalidRequestException.class).matches(e))
                    {
                        Throwable cause = Throwables.getRootCause(e);
                        if (cause.getMessage().matches("Key length of %d is longer than maximum of %d"))
                        {
                            logger.warn("Issue with generator; key length is too large", cause);
                            after(stmt);
                            return;
                        }
                    }
                    throw new RuntimeException(debugString(metadata, stmt), e);
                }
            }));
        }
    }

    private static void setLoggerInfo(String name)
    {
        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(name)).setLevel(Level.INFO);
    }

    private static final List<String> UDTS = new ArrayList<>();
    private static TableMetadata createTable()
    {
        TableMetadata metadata = Generators.get(metadataGen);

        // create UDTs if present
        for (ColumnMetadata column : metadata.columns())
            maybeCreateUDT(cluster, column.type);
        String createStatement = metadata.toCqlString(false, false);
        logger.info("Creating table\n{}", createStatement);
        cluster.schemaChange(createStatement);
        ClusterUtils.awaitGossipSchemaMatch(cluster);
        return metadata;
    }

    private static String debugString(TableMetadata metadata, Statement statement)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("CQL:\n").append(metadata.toCqlString(false, false)).append('\n');
        if (!UDTS.isEmpty())
        {
            sb.append("User Defined Types:\n");
            for (String s : UDTS)
                sb.append(s).append('\n');
        }
        sb.append("Statement:\n");
        statement.toCQL(sb, 0);
        return sb.toString();
    }

    private static void maybeCreateUDT(Cluster cluster, AbstractType<?> type)
    {
        type = type.unwrap();
        for (AbstractType<?> subtype : type.subTypes())
            maybeCreateUDT(cluster, subtype);
        if (type.isUDT())
        {
            UserType udt = (UserType) type;
            cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + ColumnIdentifier.maybeQuote(udt.keyspace) + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + Math.min(3, cluster.size()) + "};");
            String cql = udt.toCqlString(false, true);
            logger.info("Creating UDT {}.{} with CQL:\n{}", ColumnIdentifier.maybeQuote(udt.keyspace), ColumnIdentifier.maybeQuote(UTF8Type.instance.compose(udt.name)), cql);
            UDTS.add(cql);
            cluster.schemaChange(cql);
        }
    }
}

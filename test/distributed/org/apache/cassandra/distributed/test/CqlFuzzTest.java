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
import java.util.regex.Pattern;

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
import org.apache.cassandra.db.marshal.ReversedType;
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
import org.apache.cassandra.utils.ast.Update;
import org.quicktheories.core.Gen;

import static org.quicktheories.QuickTheory.qt;

public class CqlFuzzTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(CqlFuzzTest.class);
    private static final Gen<TableMetadata> metadataGen = CassandraGenerators.tableMetadataGenBuilder()
                                                                             .withKind(TableMetadata.Kind.REGULAR)
                                                                             .withKeyspace(KEYSPACE).withName(Generators.uniqueSymbolGen())
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
        Gen<Statement> update = (Gen<Statement>) (Gen<?>) new Update.GenBuilder(metadata).build();
        int weight = 100 / 4;
        Gen<Statement> statements = Generators.mix(ImmutableMap.of(select, weight, update, weight * 3));
        fuzz(metadata, statements);
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
        }));

        Gen<Statement> statements = (Gen<Statement>) (Gen<?>) new Txn.GenBuilder(metadata).build();
        fuzz(metadata, statements);
    }

    private static void fuzz(TableMetadata metadata, Gen<Statement> statements)
    {
        qt().withFixedSeed(32533285503833L).withShrinkCycles(0).forAll(statements).checkAssert(FailingConsumer.orFail(stmt -> {
            logger.info("Trying Statement\n{}", stmt.detailedToString());
            int i;
            Exception exception = null;
            for (i = 0; i < 10; i++)
            {
                try
                {
                    cluster.coordinator(1).execute(stmt.toCQL(), ConsistencyLevel.QUORUM, stmt.binds());
                    return;
                }
                catch (Exception e)
                {
                    if (exception != null)
                    {
                        if (!exception.getClass().equals(e.getClass()))
                            exception.addSuppressed(e);
                    }
                    else
                    {
                        exception = e;
                    }
                    if (AssertionUtils.rootCauseIsInstanceof(RequestTimeoutException.class).matches(e))
                    {
                        logger.info("Timeout seen, attempting retry {}", i);
                        continue;
                    }
                    if (AssertionUtils.rootCauseIsInstanceof(Preempted.class).matches(e))
                    {
                        logger.info("Preempted, attempt retry...");
                        continue;
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
                            return;
                        }
                    }
                    throw new RuntimeException(debugString(metadata, stmt), e);
                }
            }
            throw new RuntimeException("Too many retries " + i + ":\n" + debugString(metadata, stmt), exception);
        }));
    }

    private static void setLoggerInfo(String name)
    {
        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(name)).setLevel(Level.INFO);
    }

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
        sb.append("Statement:\n");
        statement.toCQL(sb, 0);
        return sb.toString();
    }

    private static void maybeCreateUDT(Cluster cluster, AbstractType<?> type)
    {
        if (type.isReversed())
            type = ((ReversedType) type).baseType;
        for (AbstractType<?> subtype : type.subTypes())
            maybeCreateUDT(cluster, subtype);
        if (type.isUDT())
        {
            if (type.isReversed())
                type = ((ReversedType) type).baseType;
            UserType udt = (UserType) type;
            cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + ColumnIdentifier.maybeQuote(udt.keyspace) + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + Math.min(3, cluster.size()) + "};");
            String cql = udt.toCqlString(false, true);
            logger.info("Creating UDT {}.{} with CQL:\n{}", ColumnIdentifier.maybeQuote(udt.keyspace), ColumnIdentifier.maybeQuote(UTF8Type.instance.compose(udt.name)), cql);
            cluster.schemaChange(cql);
        }
    }
}

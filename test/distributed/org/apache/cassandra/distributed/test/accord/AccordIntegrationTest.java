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

package org.apache.cassandra.distributed.test.accord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.google.common.base.Splitter;
import com.google.common.util.concurrent.Uninterruptibles;

import accord.coordinate.Preempted;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Throwables;
import org.awaitility.Awaitility;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.local.Status;
import accord.messages.Commit;
import accord.primitives.Keys;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.api.QueryResults;
import org.apache.cassandra.distributed.shared.AssertUtils;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.impl.Instance;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.distributed.util.QueryResultUtil;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.txn.TxnBuilder;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FailingConsumer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

//TODO there are too many new clusters, this will cause Metaspace issues.  Once Schema and topology are integrated, can switch
// to a shared cluster with isolated tables
@SuppressWarnings("Convert2MethodRef")
public class AccordIntegrationTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(AccordIntegrationTest.class);

    private static final String keyspace = "ks";

    private static void assertRow(Cluster cluster, String query, int k, int c, int v)
    {
        Object[][] result = cluster.coordinator(1).execute(query, ConsistencyLevel.QUORUM);
        assertArrayEquals(new Object[]{new Object[] {k, c, v}}, result);
    }

    private static void test(FailingConsumer<Cluster> fn) throws Exception
    {
        try (Cluster cluster = createCluster())
        {
            cluster.schemaChange("CREATE KEYSPACE " + keyspace + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 2}");
            cluster.schemaChange("CREATE TABLE " + keyspace + ".tbl (k int, c int, v int, primary key (k, c))");
            cluster.forEach(node -> node.runOnInstance(() -> AccordService.instance.createEpochFromConfigUnsafe()));
            
            // Evict commands from the cache immediately to expose problems loading from disk.
            cluster.forEach(node -> node.runOnInstance(() -> AccordService.instance.setCacheSize(0)));

            fn.accept(cluster);

            // Make sure transaction state settles.
            awaitAsyncApply(cluster);
        }
    }

    private static Cluster createCluster() throws IOException
    {
        // need to up the timeout else tests get flaky
        return init(Cluster.build(2).withConfig(c -> c.with(Feature.NETWORK).set("write_request_timeout_in_ms", TimeUnit.SECONDS.toMillis(10))).start());
    }

    @Test
    public void testQuery() throws Throwable
    {
        test(cluster -> {
            cluster.coordinator(1).execute("INSERT INTO " + keyspace + ".tbl (k, c, v) VALUES (1, 0, 3);", ConsistencyLevel.ALL);

            String query = "BEGIN TRANSACTION\n" +
                           "  LET row1 = (SELECT v FROM " + keyspace + ".tbl WHERE k=0 AND c=0);\n" +
                           "  LET row2 = (SELECT v FROM " + keyspace + ".tbl WHERE k=1 AND c=0);\n" +
                           "  SELECT v FROM " + keyspace + ".tbl WHERE k=1 AND c=0;\n" +
                           "  IF row1 IS NULL AND row2.v = 3 THEN\n" +
                           "    INSERT INTO " + keyspace + ".tbl (k, c, v) VALUES (0, 0, 1);\n" +
                           "  END IF\n" +
                           "COMMIT TRANSACTION";
            Object[][] result = cluster.coordinator(1).execute(query, ConsistencyLevel.ANY);
            assertEquals(3, result[0][0]);

            String check = "BEGIN TRANSACTION\n" +
                           "  SELECT * FROM " + keyspace + ".tbl WHERE k=0 AND c=0;\n" +
                           "COMMIT TRANSACTION";

            // TODO: Retry on preemption may become unnecessary after the Unified Log is integrated.
            assertRowEqualsWithPreemptedRetry(cluster, check, new Object[] { 0, 0, 1 });
        });
    }

    // TODO: This fails sporadically, sometimes w/ timeouts and sometimes w/ Preempted issues.
    @Test
    public void testRecovery() throws Exception
    {
        test(cluster -> {
            IMessageFilters.Filter lostApply = cluster.filters().verbs(Verb.ACCORD_APPLY_REQ.id).drop();
            IMessageFilters.Filter lostCommit = cluster.filters().verbs(Verb.ACCORD_COMMIT_REQ.id).to(2).drop();

            String query = "BEGIN TRANSACTION\n" +
                           "  LET row1 = (SELECT v FROM " + keyspace + ".tbl WHERE k=0 AND c=0);\n" +
                           "  SELECT row1.v;\n" +
                           "  IF row1 IS NULL THEN\n" +
                           "    INSERT INTO " + keyspace + ".tbl (k, c, v) VALUES (0, 0, 1);\n" +
                           "  END IF\n" +
                           "COMMIT TRANSACTION";
            Object[][] result = cluster.coordinator(1).execute(query, ConsistencyLevel.ANY);
            assertNull(result[0][0]); // row1.v shouldn't have existed when the txn's SELECT was executed

            lostApply.off();
            lostCommit.off();

            awaitAsyncApply(cluster);

            // TODO: We should be able to just perform this txn without waiting for APPLY explicitly.
            // Querying again should trigger recovery...
            query = "BEGIN TRANSACTION\n" +
                    "  LET row1 = (SELECT v FROM " + keyspace + ".tbl WHERE k=0 AND c=0);\n" +
                    "  SELECT row1.v;\n" +
                    "  IF row1.v = 1 THEN\n" +
                    "    UPDATE " + keyspace + ".tbl SET v=2 WHERE k = 0 AND c = 0;\n" +
                    "  END IF\n" +
                    "COMMIT TRANSACTION";
            result = cluster.coordinator(1).execute(query, ConsistencyLevel.ANY);
            assertEquals(1, result[0][0]); // The following assertion should fail if this does, but check it anyway.

            // TODO: This shouldn't be necessary if a read-only transaction follows...
            awaitAsyncApply(cluster);

            assertRow(cluster, "SELECT * FROM " + keyspace + ".tbl WHERE k=0 AND c=0", 0, 0, 2);

            query = "BEGIN TRANSACTION\n" +
                    "  LET row1 = (SELECT v FROM " + keyspace + ".tbl WHERE k=0 AND c=0);\n" +
                    "  SELECT row1.v;\n" +
                    "  IF row1 IS NULL THEN\n" +
                    "    INSERT INTO " + keyspace + ".tbl (k, c, v) VALUES (0, 0, 3);\n" +
                    "  END IF\n" +
                    "COMMIT TRANSACTION";
            result = cluster.coordinator(1).execute(query, ConsistencyLevel.ANY);
            assertEquals(2, result[0][0]);
            assertRow(cluster, "SELECT * FROM " + keyspace + ".tbl WHERE k=0 AND c=0", 0, 0, 2);
        });
    }

    @Test
    public void multipleShards() throws IOException, TimeoutException
    {
        // can't reuse test() due to it using "int" for pk; this test needs "blob"
        try (Cluster cluster = createCluster())
        {
            cluster.schemaChange("CREATE KEYSPACE " + keyspace + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 1}");
            cluster.schemaChange("CREATE TABLE " + keyspace + ".tbl (k blob, c int, v int, primary key (k, c))");
            cluster.forEach(node -> node.runOnInstance(() -> AccordService.instance.createEpochFromConfigUnsafe()));
            cluster.forEach(node -> node.runOnInstance(() -> AccordService.instance.setCacheSize(0)));

            List<String> tokens = cluster.stream()
                                         .flatMap(i -> StreamSupport.stream(Splitter.on(",").split(i.config().getString("initial_token")).spliterator(), false))
                                         .collect(Collectors.toList());

            List<ByteBuffer> keys = tokens.stream()
                                          .map(t -> (Murmur3Partitioner.LongToken) Murmur3Partitioner.instance.getTokenFactory().fromString(t))
                                          .map(Murmur3Partitioner.LongToken::keyForToken)
                                          .collect(Collectors.toList());

            List<String> keyStrings = keys.stream().map(bb -> "0x" + ByteBufferUtil.bytesToHex(bb)).collect(Collectors.toList());
            StringBuilder query = new StringBuilder("BEGIN TRANSACTION\n");
            
            for (int i = 0; i < keys.size(); i++)
                query.append("  LET row" + i + " = (SELECT * FROM " + keyspace + ".tbl WHERE k=" + keyStrings.get(i) + " AND c=0);\n");

            query.append("  SELECT row0.v;\n")
                 .append("  IF ");

            for (int i = 0; i < keys.size(); i++)
                query.append((i > 0 ? " AND row" : "row") + i + " IS NULL");

            query.append(" THEN\n");

            for (int i = 0; i < keys.size(); i++)
                query.append("    INSERT INTO " + keyspace + ".tbl (k, c, v) VALUES (" + keyStrings.get(i) + ", 0, " + i +");\n");
            
            query.append("  END IF\n");
            query.append("COMMIT TRANSACTION");
            Object[][] txnResult = cluster.coordinator(1).execute(query.toString(), ConsistencyLevel.ANY);
            assertNull(txnResult[0][0]); // row0.v shouldn't have existed when the txn's SELECT was executed

            cluster.get(1).runOnInstance(() -> {
                TxnBuilder txn = new TxnBuilder();

                for (int i = 0; i < keyStrings.size(); i++)
                    txn.withRead("row" + i, "SELECT * FROM " + keyspace + ".tbl WHERE k=" + keyStrings.get(i) + " and c=0");

                Keys keySet = txn.build().keys();
                Topologies topology = AccordService.instance.node.topology().withUnsyncedEpochs(keySet, 1);
                // we don't detect out-of-bounds read/write yet, so use this to validate we reach different shards
                Assertions.assertThat(topology.totalShards()).isEqualTo(2);
            });

            awaitAsyncApply(cluster);

            // TODO: We should be able to just perform a read-only txn without waiting for APPLY explicitly.
            SimpleQueryResult result = cluster.coordinator(1).executeWithResult("SELECT * FROM " + keyspace + ".tbl", ConsistencyLevel.ALL);
            QueryResults.Builder expected = QueryResults.builder().columns("k", "c", "v");
            for (int i = 0; i < keys.size(); i++)
                expected.row(keys.get(i), 0, i);
            AssertUtils.assertRows(result, expected.build());
        }
    }

    @Test
    public void testLostCommitReadTriggersFallbackRead() throws Exception
    {
        test(cluster -> {
            // It's expected that the required Read will happen regardless of whether this fails to return a read
            cluster.filters().verbs(Verb.ACCORD_COMMIT_REQ.id).messagesMatching((from, to, iMessage) -> cluster.get(from).callOnInstance(() -> {
                Message<?> msg = Instance.deserializeMessage(iMessage);
                if (msg.payload instanceof Commit)
                    return ((Commit) msg.payload).read;
                return false;
            })).drop();

            String query = "BEGIN TRANSACTION\n" +
                           "  LET row1 = (SELECT * FROM " + keyspace + ".tbl WHERE k=0 AND c=0);\n" +
                           "  SELECT row1.v;\n" +
                           "  IF row1 IS NULL THEN\n" +
                           "    INSERT INTO " + keyspace + ".tbl (k, c, v) VALUES (0, 0, 1);\n" +
                           "  END IF\n" +
                           "COMMIT TRANSACTION";
            cluster.coordinator(1).executeWithResult(query, ConsistencyLevel.ANY);

            // TODO: Is 10 seconds going to be flaky here?
            Awaitility.await("For recovery to occur")
                      .atMost(Duration.ofSeconds(10))
                      .pollInterval(1, TimeUnit.SECONDS)
                      .untilAsserted(() -> assertRow(cluster, "SELECT * FROM " + keyspace + ".tbl WHERE k=0 AND c=0", 0, 0, 1));
        });
    }

    @Test
    public void testReadOnlyTx() throws Exception
    {
        test(cluster -> {
            String query = "BEGIN TRANSACTION\n" +
                           "  SELECT * FROM " + keyspace + ".tbl WHERE k=0 AND c=0;\n" +
                           "COMMIT TRANSACTION";
            SimpleQueryResult result = cluster.coordinator(1).executeWithResult(query, ConsistencyLevel.ANY);
            assertFalse(result.hasNext());
        });
    }

    @Test
    public void testWriteOnlyTx() throws Exception
    {
        test(cluster -> {
            String query = "BEGIN TRANSACTION\n" +
                           "  INSERT INTO " + keyspace + ".tbl (k, c, v) VALUES (0, 0, 1);\n" +
                           "COMMIT TRANSACTION";
            SimpleQueryResult result = cluster.coordinator(1).executeWithResult(query, ConsistencyLevel.ANY);
            assertFalse(result.hasNext());

            String check = "BEGIN TRANSACTION\n" +
                           "  SELECT * FROM " + keyspace + ".tbl WHERE k=0 AND c=0;\n" +
                           "COMMIT TRANSACTION";

            // TODO: Retry on preemption may become unnecessary after the Unified Log is integrated.
            assertRowEqualsWithPreemptedRetry(cluster, check, new Object[] {0, 0, 1});
        });
    }

    @Test
    public void testReturningLetReferences() throws Throwable
    {
        test(cluster -> {
            cluster.coordinator(1).execute("INSERT INTO " + keyspace + ".tbl (k, c, v) VALUES (1, 0, 3);", ConsistencyLevel.ALL);

            String query = "BEGIN TRANSACTION\n" +
                    "  LET row1 = (SELECT * FROM " + keyspace + ".tbl WHERE k=0 AND c=0);\n" +
                    "  LET row2 = (SELECT * FROM " + keyspace + ".tbl WHERE k=1 AND c=0);\n" +
                    "  SELECT row2.v;\n" +
                    "  IF row1 IS NULL AND row2.v = 3 THEN\n" +
                    "    INSERT INTO " + keyspace + ".tbl (k, c, v) VALUES (0, 0, 1);\n" +
                    "  END IF\n" +
                    "COMMIT TRANSACTION";
            Object[][] result = cluster.coordinator(1).execute(query, ConsistencyLevel.ANY);
            assertEquals(3, result[0][0]);

            awaitAsyncApply(cluster);

            // TODO: We should be able to just perform a read-only txn without waiting for APPLY explicitly.
            assertRow(cluster, "SELECT * FROM " + keyspace + ".tbl WHERE k=0 AND c=0", 0, 0, 1);
        });
    }

    @Test
    public void variableSubstitution() throws Throwable
    {
        String keyspace = "ks" + System.currentTimeMillis();
        try (Cluster cluster = createCluster())
        {
            cluster.schemaChange("CREATE KEYSPACE " + keyspace + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 2}");
            cluster.schemaChange("CREATE TABLE " + keyspace + ".tbl1 (k int, c int, v int, primary key (k, c))");
            cluster.schemaChange("CREATE TABLE " + keyspace + ".tbl2 (k int, c int, v int, primary key (k, c))");
            cluster.forEach(node -> node.runOnInstance(() -> AccordService.instance.createEpochFromConfigUnsafe()));
            cluster.forEach(node -> node.runOnInstance(() -> AccordService.instance.setCacheSize(0)));
            cluster.coordinator(1).execute("INSERT INTO " + keyspace + ".tbl1 (k, c, v) VALUES (1, 2, 3);", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("INSERT INTO " + keyspace + ".tbl2 (k, c, v) VALUES (2, 2, 4);", ConsistencyLevel.ALL);

            String query = "BEGIN TRANSACTION\n" +
                           "  LET row1 = (SELECT * FROM " + keyspace + ".tbl1 WHERE k=1 AND c=2);\n" +
                           "  LET row2 = (SELECT * FROM " + keyspace + ".tbl2 WHERE k=2 AND c=2);\n" +
                           "  SELECT v FROM " + keyspace + ".tbl1 WHERE k=1 AND c=2;\n" +
                           "  IF row1.v = 3 AND row2.v = 4 THEN\n" +
                           "    UPDATE " + keyspace + ".tbl1 SET v=row2.v WHERE k=1 AND c=2;\n" +
                           "  END IF\n" +
                           "COMMIT TRANSACTION";
            Object[][] result = cluster.coordinator(1).execute(query, ConsistencyLevel.ANY);
            assertEquals(3, result[0][0]);

            awaitAsyncApply(cluster);

            // TODO: We should be able to just perform a read-only txn without waiting for APPLY explicitly.
            assertRow(cluster, "SELECT * FROM " + keyspace + ".tbl1 WHERE k=1 AND c=2", 1, 2, 4);
        }
    }

    @Test
    public void additionAssignment() throws Throwable
    {
        String keyspace = "ks" + System.currentTimeMillis();
        try (Cluster cluster = createCluster())
        {
            cluster.schemaChange("CREATE KEYSPACE " + keyspace + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 2}");
            cluster.schemaChange("CREATE TABLE " + keyspace + ".tbl1 (k int, c int, v int, primary key (k, c))");
            cluster.schemaChange("CREATE TABLE " + keyspace + ".tbl2 (k int, c int, v int, primary key (k, c))");
            cluster.forEach(node -> node.runOnInstance(() -> AccordService.instance.createEpochFromConfigUnsafe()));
            cluster.forEach(node -> node.runOnInstance(() -> AccordService.instance.setCacheSize(0)));
            cluster.coordinator(1).execute("INSERT INTO " + keyspace + ".tbl1 (k, c, v) VALUES (1, 2, 3);", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("INSERT INTO " + keyspace + ".tbl2 (k, c, v) VALUES (2, 2, 4);", ConsistencyLevel.ALL);

            String query = "BEGIN TRANSACTION\n" +
                           "  SELECT v FROM " + keyspace + ".tbl1 WHERE k=1 AND c=2;\n" +
                           "  UPDATE " + keyspace + ".tbl1 SET v += 5 WHERE k=1 AND c=2;\n" +
                           "  UPDATE " + keyspace + ".tbl2 SET v -= 2 WHERE k=2 AND c=2;\n" +
                           "COMMIT TRANSACTION";
            Object[][] result = cluster.coordinator(1).execute(query, ConsistencyLevel.ANY);
            assertEquals(3, result[0][0]);

            awaitAsyncApply(cluster);

            // TODO: We should be able to just perform a read-only txn without waiting for APPLY explicitly.
            assertRow(cluster, "SELECT * FROM " + keyspace + ".tbl1 WHERE k=1 AND c=2", 1, 2, 8);
            assertRow(cluster, "SELECT * FROM " + keyspace + ".tbl2 WHERE k=2 AND c=2", 2, 2, 2);
        }
    }

    @Test
    public void multiKeyMultiQuery() throws Throwable
    {
        String keyspace = "ks" + System.currentTimeMillis();

        try (Cluster cluster = createCluster())
        {
            cluster.schemaChange("CREATE KEYSPACE " + keyspace + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 2}");
            cluster.schemaChange("CREATE TABLE " + keyspace + ".tbl (k int, c int, v int, primary key (k, c))");
            cluster.forEach(node -> node.runOnInstance(() -> AccordService.instance.createEpochFromConfigUnsafe()));
            cluster.forEach(node -> node.runOnInstance(() -> AccordService.instance.setCacheSize(0)));

            String query1 = "BEGIN TRANSACTION\n" +
                            "  LET select1 = (SELECT * FROM " + keyspace + ".tbl WHERE k=0 AND c=0);\n" +
                            "  LET select2 = (SELECT * FROM " + keyspace + ".tbl WHERE k=1 AND c=0);\n" +
                            "  SELECT v FROM " + keyspace + ".tbl WHERE k=0 AND c=0;\n" +
                            "  IF select1 IS NULL THEN\n" +
                            "    INSERT INTO " + keyspace + ".tbl (k, c, v) VALUES (0, 0, 0);\n" +
                            "    INSERT INTO " + keyspace + ".tbl (k, c, v) VALUES (1, 0, 0);\n" +
                            "  END IF\n" +
                            "COMMIT TRANSACTION";
            Object[][] result1 = cluster.coordinator(1).execute(query1, ConsistencyLevel.ANY);
            assertEquals(0, result1.length);

            awaitAsyncApply(cluster);

            // TODO: We should be able to just perform a read-only txn without waiting for APPLY explicitly.
            assertRow(cluster, "SELECT * FROM " + keyspace + ".tbl WHERE k=0 AND c=0", 0, 0, 0);
            assertRow(cluster, "SELECT * FROM " + keyspace + ".tbl WHERE k=1 AND c=0", 1, 0, 0);

            String query2 = "BEGIN TRANSACTION\n" +
                            "  LET select1 = (SELECT * FROM " + keyspace + ".tbl WHERE k=1 AND c=0);\n" +
                            "  LET select2 = (SELECT * FROM " + keyspace + ".tbl WHERE k=2 AND c=0);\n" +
                            "  SELECT v FROM " + keyspace + ".tbl WHERE k=1 AND c=0;\n" +
                            "  IF select1.v = 0 THEN\n" +
                            "    INSERT INTO " + keyspace + ".tbl (k, c, v) VALUES (1, 0, 1);\n" +
                            "    INSERT INTO " + keyspace + ".tbl (k, c, v) VALUES (2, 0, 1);\n" +
                            "  END IF\n" +
                            "COMMIT TRANSACTION";
            Object[][] result2 = cluster.coordinator(1).execute(query2, ConsistencyLevel.ANY);
            assertEquals(0, result2[0][0]);

            awaitAsyncApply(cluster);

            // TODO: We should be able to just perform a read-only txn without waiting for APPLY explicitly.
            assertRow(cluster, "SELECT * FROM " + keyspace + ".tbl WHERE k=0 AND c=0", 0, 0, 0);
            assertRow(cluster, "SELECT * FROM " + keyspace + ".tbl WHERE k=1 AND c=0", 1, 0, 1);
            assertRow(cluster, "SELECT * FROM " + keyspace + ".tbl WHERE k=2 AND c=0", 2, 0, 1);
        }
    }

    @Test
    public void demoTest() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(3).withConfig(c -> c.set("write_request_timeout_in_ms", TimeUnit.SECONDS.toMillis(10))).start()))
        {
            cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor':3};");
            cluster.schemaChange("CREATE TABLE IF NOT EXISTS ks.org_docs ( org_name text, doc_id int, contents_version int static, title text, permissions int, PRIMARY KEY (org_name, doc_id) );");
            cluster.schemaChange("CREATE TABLE IF NOT EXISTS ks.org_users ( org_name text, user text, members_version int static, permissions int, PRIMARY KEY (org_name, user) );");
            cluster.schemaChange("CREATE TABLE IF NOT EXISTS ks.user_docs ( user text, doc_id int, title text, org_name text, permissions int, PRIMARY KEY (user, doc_id) );");

            cluster.forEach(node -> node.runOnInstance(() -> AccordService.instance.createEpochFromConfigUnsafe()));
            cluster.forEach(node -> node.runOnInstance(() -> AccordService.instance.setCacheSize(0)));

            cluster.coordinator(1).execute("INSERT INTO ks.org_users (org_name, user, members_version, permissions) VALUES ('demo', 'blake', 5, 777);\n", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("INSERT INTO ks.org_users (org_name, user, members_version, permissions) VALUES ('demo', 'scott', 5, 777);\n", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("INSERT INTO ks.org_docs (org_name, doc_id, contents_version, title, permissions) VALUES ('demo', 100, 5, 'README', 644);\n", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("INSERT INTO ks.user_docs (user, doc_id, title, org_name, permissions) VALUES ('blake', 1, 'recipes', NULL, 777);\n", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("INSERT INTO ks.user_docs (user, doc_id, title, org_name, permissions) VALUES ('blake', 100, 'README', 'demo', 644);\n", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("INSERT INTO ks.user_docs (user, doc_id, title, org_name, permissions) VALUES ('scott', 2, 'to do list', NULL, 777);\n", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("INSERT INTO ks.user_docs (user, doc_id, title, org_name, permissions) VALUES ('scott', 100, 'README', 'demo', 644);\n", ConsistencyLevel.ALL);

            String addDoc =  "BEGIN TRANSACTION\n" +
                             "  LET demo_user = (SELECT * FROM ks.org_users WHERE org_name='demo' LIMIT 1);\n" +
                             "  LET existing = (SELECT * FROM ks.org_docs WHERE org_name='demo' AND doc_id=101);\n" +
                             "  SELECT members_version FROM ks.org_users WHERE org_name='demo';\n" +
                             "  IF demo_user.members_version = 5 AND existing IS NULL THEN\n" +
                             "    UPDATE ks.org_docs SET title='slides.key', permissions=777, contents_version += 1 WHERE org_name='demo' AND doc_id=101;\n" +
                             "    UPDATE ks.user_docs SET title='slides.key', permissions=777 WHERE user='blake' AND doc_id=101;\n" +
                             "    UPDATE ks.user_docs SET title='slides.key', permissions=777 WHERE user='scott' AND doc_id=101;\n" +
                             "  END IF\n" +
                             "COMMIT TRANSACTION";
            Object[][] result1 = cluster.coordinator(1).execute(addDoc, ConsistencyLevel.ANY);
            assertEquals(5, result1[0][0]);

            awaitAsyncApply(cluster);

            // TODO: We should be able to just perform this txn without waiting for APPLY explicitly.
            String addUser = "BEGIN TRANSACTION\n" +
                             "  LET demo_doc = (SELECT * FROM ks.org_docs WHERE org_name='demo' LIMIT 1);\n" +
                             "  LET existing = (SELECT * FROM ks.org_users WHERE org_name='demo' AND user='benedict');\n" +
                             "  SELECT contents_version FROM ks.org_docs WHERE org_name='demo';\n" +
                             "  IF demo_doc.contents_version = 6 AND existing IS NULL THEN\n" +
                             "    UPDATE ks.org_users SET permissions=777, members_version += 1 WHERE org_name='demo' AND user='benedict';\n" +
                             "    UPDATE ks.user_docs SET title='README', permissions=644 WHERE user='benedict' AND doc_id=100;\n" +
                             "    UPDATE ks.user_docs SET title='slides.key', permissions=777 WHERE user='benedict' AND doc_id=101;\n" +
                             "  END IF\n" +
                             "COMMIT TRANSACTION";
            Object[][] result2 = cluster.coordinator(1).execute(addUser, ConsistencyLevel.ANY);
            assertEquals(6, result2[0][0]);
        }
    }

    private static void assertRowEqualsWithPreemptedRetry(Cluster cluster, String check, Object[] row)
    {
        try
        {
            Object[][] checkResult = cluster.coordinator(1).execute(check, ConsistencyLevel.ANY);
            assertArrayEquals(new Object[]{ row }, checkResult);
        }
        catch (Throwable t)
        {
            if (Throwables.getRootCause(t).toString().contains(Preempted.class.getName()))
            {
                Object[][] checkResult = cluster.coordinator(1).execute(check, ConsistencyLevel.ANY);
                assertArrayEquals(new Object[]{ row }, checkResult);
            }
            else
            {
                throw t;
            }
        }
    }

    @SuppressWarnings("UnstableApiUsage")
    public static void awaitAsyncApply(Cluster cluster) throws TimeoutException
    {
        Duration deadline = Duration.ofSeconds(30);
        long deadlineNanos = nanoTime() + deadline.toNanos();
        AtomicReference<TimeoutException> timeout = new AtomicReference<>(null);
        cluster.stream().filter(i -> !i.isShutdown()).forEach(inst -> {
            while (timeout.get() == null)
            {
                SimpleQueryResult pending = inst.executeInternalWithResult("SELECT store_generation, store_index, txn_id, status FROM system_accord.commands WHERE status < ? ALLOW FILTERING", Status.Executed.ordinal());
                pending = QueryResultUtil.map(pending, Map.of(
                        "txn_id", (ByteBuffer bb) -> AccordKeyspace.deserializeTimestampOrNull(bb, TxnId::new),
                        "status", (Integer ordinal) -> Status.values()[ordinal]
                ));
                logger.info("[node{}] Pending:\n{}", inst.config().num(), QueryResultUtil.expand(pending));
                pending.reset();
                if (!pending.hasNext())
                    break;
                if (nanoTime() > deadlineNanos)
                {
                    pending.reset();
                    timeout.set(new TimeoutException("Timeout waiting on Accord Txn to complete after " + deadline + "; node" + inst.config().num() + " Pending:\n" + QueryResultUtil.expand(pending)));
                    break;
                }
                Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
            }
        });
        if (timeout.get() != null)
            throw timeout.get();
    }

//    @Test
//    public void acceptInvalidationTest()
//    {
//
//    }
//
//    @Test
//    public void applyAndCheckTest()
//    {
//
//    }
//
//    @Test
//    public void beginInvalidationTest()
//    {
//
//    }
//
//    @Test
//    public void checkStatusTest()
//    {
//
//    }
}

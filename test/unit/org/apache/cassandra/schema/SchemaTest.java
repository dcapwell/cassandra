/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.schema;

import java.io.IOException;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.service.ClientWarn;
import org.assertj.core.api.Assertions;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

//@RunWith(BMUnitRunner.class)
public class SchemaTest extends CQLTester
{
    @BeforeClass
    public static void setupDatabaseDescriptor()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testTransKsMigration() throws IOException
    {
        CommitLog.instance.start();
        SchemaLoader.cleanupAndLeaveDirs();
        Schema.instance.loadFromDisk();
        assertEquals(0, Schema.instance.getNonSystemKeyspaces().size());

        Gossiper.instance.start((int) (System.currentTimeMillis() / 1000));
        Keyspace.setInitialized();

        try
        {
            // add a few.
            MigrationManager.announceNewKeyspace(KeyspaceMetadata.create("ks0", KeyspaceParams.simple(3)));
            MigrationManager.announceNewKeyspace(KeyspaceMetadata.create("ks1", KeyspaceParams.simple(3)));

            assertNotNull(Schema.instance.getKeyspaceMetadata("ks0"));
            assertNotNull(Schema.instance.getKeyspaceMetadata("ks1"));

            Schema.instance.unload(Schema.instance.getKeyspaceMetadata("ks0"));
            Schema.instance.unload(Schema.instance.getKeyspaceMetadata("ks1"));

            assertNull(Schema.instance.getKeyspaceMetadata("ks0"));
            assertNull(Schema.instance.getKeyspaceMetadata("ks1"));

            Schema.instance.loadFromDisk();

            assertNotNull(Schema.instance.getKeyspaceMetadata("ks0"));
            assertNotNull(Schema.instance.getKeyspaceMetadata("ks1"));
        }
        finally
        {
            Gossiper.instance.stop();
        }
    }

    @Test
    @BMRules(rules = { @BMRule(name = "client warning 1",
                               targetClass = "CreateKeyspaceStatement",
                               targetMethod = "apply",
                               targetLocation = "AT INVOKE KeyspaceParams.validate",
                               action = "org.apache.cassandra.schema.SchemaTest.addWarn()"),
                       @BMRule(name = "client warning 2",
                               targetClass = "CreateKeyspaceStatement",
                               targetMethod = "clientWarnings",
                               targetLocation = "AT EXIT",
                               action = "return org.apache.cassandra.schema.SchemaTest.addWarnToList($!)"),
                       @BMRule(name = "client warning 3",
                               targetClass = "AlterSchemaStatement",
                               targetMethod = "clientWarnings",
                               targetLocation = "AT EXIT",
                               action = "return org.apache.cassandra.schema.SchemaTest.addWarnToList($!)")})
    public void testClientWarningsOnCreateKeyspace() throws Throwable
    {
        ClientWarn.instance.captureWarnings();
        injectedWarnings.clear();
        createKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");

        Assertions.assertThat(injectedWarnings).contains(msg1, msg2); // failure here means the bm rules need to be updated
        Assertions.assertThat(ClientWarn.instance.getWarnings()).containsExactlyInAnyOrder(msg1, msg2);
    }

    @Test
    @BMRules(rules = { @BMRule(name = "client warning 1",
                               targetClass = "CreateTableStatement",
                               targetMethod = "apply",
                               targetLocation = "AT INVOKE TableMetadata.validate",
                               action = "org.apache.cassandra.schema.SchemaTest.addWarn()"),
                       @BMRule(name = "client warning 2",
                               targetClass = "CreateTableStatement",
                               targetMethod = "clientWarnings",
                               targetLocation = "AT EXIT",
                               action = "return org.apache.cassandra.schema.SchemaTest.addWarnToList($!)"),
                       @BMRule(name = "client warning 3",
                               targetClass = "AlterSchemaStatement",
                               targetMethod = "clientWarnings",
                               targetLocation = "AT EXIT",
                               action = "return org.apache.cassandra.schema.SchemaTest.addWarnToList($!)"),
    })
    public void testClientWarningsOnCreateTable() throws Throwable
    {
        ClientWarn.instance.captureWarnings();
        injectedWarnings.clear();
        createTable("CREATE TABLE %s (k int primary key, v int)");

        Assertions.assertThat(injectedWarnings).contains(msg1, msg2); // failure here means the bm rules need to be updated
        Assertions.assertThat(ClientWarn.instance.getWarnings()).containsExactlyInAnyOrder(msg1, msg2);
    }

    static volatile String msg1, msg2;

    @Before
    public void before()
    {
        msg1 = UUID.randomUUID().toString();
        msg2 = UUID.randomUUID().toString();
    }

    static Queue<String> injectedWarnings = new ConcurrentLinkedQueue<String>();

    public static void addWarn()
    {
        ClientWarn.instance.warn(msg1);
        injectedWarnings.add(msg1);
    }

    public static Set<String> addWarnToList(Set<String> warns)
    {
        warns = new HashSet<>(warns);
        warns.add(msg2);
        injectedWarnings.add(msg2);
        return warns;
    }
}

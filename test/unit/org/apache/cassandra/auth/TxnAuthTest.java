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

package org.apache.cassandra.auth;

import java.net.InetSocketAddress;
import java.util.Collections;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.TransactionStatement;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.transport.messages.ResultMessage;

import static org.junit.Assert.assertEquals;

import static org.apache.cassandra.db.ConsistencyLevel.NODE_LOCAL;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

// TODO: Resolve the hack that makes node reuse possible through schema changes in AccordConfigurationService
public class TxnAuthTest extends CQLTester
{
    @BeforeClass
    public static void setUpAuthAndAccord() throws Exception
    {
        CassandraRelevantProperties.ENABLE_NODELOCAL_QUERIES.setBoolean(true);

        SchemaLoader.prepareServer();
        IRoleManager roleManager = new AuthTestUtils.LocalCassandraRoleManager();
        SchemaLoader.setupAuth(roleManager,
                               new AuthTestUtils.LocalPasswordAuthenticator(),
                               new AuthTestUtils.LocalCassandraAuthorizer(),
                               new AuthTestUtils.LocalCassandraNetworkAuthorizer());
        roleManager.setup();
        AuthCacheService.initializeAndRegisterCaches();
        AuthTestUtils.setupSuperUser();

        SchemaLoader.startGossiper();
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        tmd.clearUnsafe();
        StorageService.instance.setTokens(Collections.singleton(tmd.partitioner.getRandomToken()));
    }

    @Before
    public void setUpTest()
    {
        createTable("CREATE TABLE %s (k int, v int, PRIMARY KEY(k))");
        AccordService.instance.createEpochFromConfigUnsafe();
    }

    @Test
    public void canSelectInTxnWithPermissions()
    {
        QueryProcessor.process(formatQuery("INSERT INTO %s (k, v) VALUES (0, 0)"), NODE_LOCAL);

        ClientState clientState = createUserAndLogin();
        String query = formatQuery("BEGIN TRANSACTION\n" +
                                   "  SELECT * FROM %s WHERE k = 0;\n" +
                                   "COMMIT TRANSACTION");

        assertUnauthorized(query, clientState);

        grantTo(clientState, Permission.SELECT);
        ResultMessage.Rows message = (ResultMessage.Rows) execute(query, clientState);
        assertEquals(1, message.result.size());
    }

    @Test
    public void canSelectRefsInTxnWithPermissions()
    {
        QueryProcessor.process(formatQuery("INSERT INTO %s (k, v) VALUES (0, 0)"), NODE_LOCAL);

        ClientState clientState = createUserAndLogin();
        String query = formatQuery("BEGIN TRANSACTION\n" +
                                   "  LET row0 = (SELECT * FROM %s WHERE k = 0);\n" +
                                   "  SELECT row0.v;\n" +
                                   "COMMIT TRANSACTION");

        assertUnauthorized(query, clientState);

        grantTo(clientState, Permission.SELECT);
        ResultMessage.Rows message = (ResultMessage.Rows) execute(query, clientState);
        assertEquals(1, message.result.size());
    }

    @Test
    public void canInsertOnlyInTxnWithPermissions()
    {
        ClientState clientState = createUserAndLogin();
        String insert = formatQuery("BEGIN TRANSACTION\n" +
                                    "    INSERT INTO %s (k, v) VALUES (0, 0);\n" +
                                    "COMMIT TRANSACTION");

        assertUnauthorized(insert, clientState);

        grantTo(clientState, Permission.MODIFY);
        execute(insert, clientState);
    }

    @Test
    public void canExecuteTxnWithAutoGeneratedRead()
    {
        QueryProcessor.process(formatQuery("INSERT INTO %s (k, v) VALUES (0, 0)"), NODE_LOCAL);
        
        ClientState clientState = createUserAndLogin();
        String update = formatQuery("BEGIN TRANSACTION\n" +
                                    "    UPDATE %s SET v += 1 WHERE k = 0 ;\n" +
                                    "COMMIT TRANSACTION");

        assertUnauthorized(update, clientState);

        // We should still fail here, given we need permisions to SELECT for the generated reads.
        grantTo(clientState, Permission.MODIFY);
        assertUnauthorized(update, clientState);

        grantTo(clientState, Permission.SELECT);
        execute(update, clientState);
    }
    
    private void assertUnauthorized(String query, ClientState clientState)
    {
        Assertions.assertThatThrownBy(() -> execute(query, clientState))
                  .isInstanceOf(UnauthorizedException.class)
                  .hasMessageContaining(clientState.getUser().getName());
    }

    private void grantTo(ClientState clientState, Permission permission)
    {
        AuthTestUtils.authorize(formatQuery("GRANT " + permission + " ON TABLE %s TO " + clientState.getUser().getName()));
    }

    private ClientState createUserAndLogin()
    {
        String username = AuthTestUtils.createName();
        AuthTestUtils.authenticate("CREATE ROLE %s WITH password = 'password' AND LOGIN = true", username);
        ClientState clientState = ClientState.forExternalCalls(InetSocketAddress.createUnresolved("127.0.0.1", 123));
        clientState.login(new AuthenticatedUser(username));
        return clientState;
    }

    private ResultMessage execute(String query, ClientState clientState)
    {
        TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(query);
        TransactionStatement statement = (TransactionStatement) parsed.prepare(clientState);
        QueryOptions options = QueryOptions.forInternalCalls(NODE_LOCAL, Collections.emptyList());
        QueryState queryState = new QueryState(clientState);
        return QueryProcessor.instance.process(statement, queryState, options, nanoTime());
    }
}

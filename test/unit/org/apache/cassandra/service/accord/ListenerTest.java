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

package org.apache.cassandra.service.accord;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.local.CommandListener;
import accord.local.PreLoadContext;
import accord.local.Status;
import accord.primitives.TxnId;
import accord.utils.async.AsyncResults;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;
import org.mockito.Mockito;

import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;
import static org.apache.cassandra.service.accord.AccordTestUtils.clearCache;
import static org.apache.cassandra.service.accord.AccordTestUtils.createAccordCommandStore;
import static org.apache.cassandra.service.accord.AccordTestUtils.txnId;
import static org.apache.cassandra.service.accord.AccordTestUtils.updateCommandUsingLifeCycle;

public class ListenerTest
{
    private static final AtomicLong clock = new AtomicLong(0);

    //TODO avoid copy/paste with AsyncOperationTest
    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1),
                                    parse("CREATE TABLE tbl (k int, c int, v int, primary key (k, c))", "ks"));
        StorageService.instance.initServer();
    }

    @Before
    public void before()
    {
        QueryProcessor.executeInternal("TRUNCATE system_accord.commands");
        QueryProcessor.executeInternal("TRUNCATE system_accord.commands_for_key");
    }

    @Test
    public void safeTransient()
    {
        AccordCommandStore commandStore = createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");
        TxnId id = txnId(1, clock.incrementAndGet(), 1);

        updateCommandUsingLifeCycle(commandStore, id, Status.PreAccepted);

        CommandListener listener = Mockito.mock(CommandListener.class);
        Mockito.when(listener.isTransient()).thenReturn(true);
        Mockito.when(listener.listenerPreLoadContext(id)).thenReturn(PreLoadContext.contextFor(id));

        AsyncResults.getUninterruptibly(commandStore.execute(PreLoadContext.contextFor(id), safe -> safe.command(id).addListener(listener)).beginAsResult());

        clearCache(commandStore);

        updateCommandUsingLifeCycle(commandStore, id, Status.Accepted);

        // did we see it?
        Mockito.verify(listener, Mockito.times(1)).onChange(Mockito.any(), Mockito.eq(id));
    }
}

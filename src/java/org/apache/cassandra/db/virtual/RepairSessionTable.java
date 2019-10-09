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

package org.apache.cassandra.db.virtual;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.consistent.ConsistentSession;
import org.apache.cassandra.repair.consistent.CoordinatorSession;
import org.apache.cassandra.repair.consistent.LocalSession;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ActiveRepairService;

public class RepairSessionTable extends AbstractVirtualTable
{
    protected RepairSessionTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "repair_session")
                           .comment("table snapshots")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .addPartitionKeyColumn("id", UTF8Type.instance)
                           .addRegularColumn("type", UTF8Type.instance)
                           .addRegularColumn("state", UTF8Type.instance)
                           .addRegularColumn("coordinator", UTF8Type.instance)
                           .addRegularColumn("table_ids", ListType.getInstance(UTF8Type.instance, false))
                           .addRegularColumn("repaired_at", LongType.instance)
                           .addRegularColumn("ranges", ListType.getInstance(UTF8Type.instance, false))
                           .addRegularColumn("participants", ListType.getInstance(UTF8Type.instance, false))
                           // local
                           .addRegularColumn("started_at", LongType.instance)
                           .addRegularColumn("last_update", LongType.instance)
                           // coordinated
                           .addRegularColumn("session_start", LongType.instance)
                           .addRegularColumn("repair_start", LongType.instance)
                           .addRegularColumn("finalize_start", LongType.instance)
                           .addRegularColumn("participant_states", MapType.getInstance(UTF8Type.instance, UTF8Type.instance, false))
                           .build());
    }

    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());
        ActiveRepairService.instance.consistent.local.getAllSessions().forEach(s -> addLocal(result, s));
        ActiveRepairService.instance.consistent.coordinated.getAllSessions().forEach(s -> addCoordinated(result, s));
        return result;
    }

    private static void addLocal(SimpleDataSet result, LocalSession session)
    {
        add(result, session);
        result.column("type", "local");
        // both are seconds
        result.column("started_at", session.startedAt);
        result.column("last_update", session.getLastUpdate());
    }


    private static void addCoordinated(SimpleDataSet result, CoordinatorSession session)
    {
        add(result, session);
        result.column("type", "coordinated");
        result.column("session_start", session.getSessionStart());
        result.column("repair_start", session.getRepairStart());
        result.column("finalize_start", session.getFinalizeStart());
        Map<String, String> participantStates = new HashMap<>();
        for (Map.Entry<InetAddressAndPort, ConsistentSession.State> e : session.getParticipantStates().entrySet())
            participantStates.put(e.getKey().toString(), e.getValue().name());
        result.column("participant_states", participantStates);
    }

    private static void add(SimpleDataSet result, ConsistentSession session)
    {
        result.row(session.sessionID.toString());
        result.column("state", session.getState().name());
        result.column("coordinator", session.coordinator.toString());
        result.column("table_ids", session.tableIds.stream().map(TableId::toString).collect(Collectors.toList()));
        result.column("repaired_at", session.repairedAt);
        result.column("ranges", session.ranges.stream().map(Range::toString).collect(Collectors.toList()));
        result.column("participants", session.participants.stream().map(InetAddressAndPort::toString).collect(Collectors.toList()));
    }
}

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

import java.util.List;

import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.streaming.SessionInfo;
import org.apache.cassandra.streaming.StreamManager;
import org.apache.cassandra.streaming.StreamState;

public class StreamStateTable extends AbstractVirtualTable
{
    protected StreamStateTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "stream_state")
                           .comment("active stream states")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .addPartitionKeyColumn("planId", UTF8Type.instance)
                           .addRegularColumn("direction", UTF8Type.instance)
                           .addRegularColumn("description", UTF8Type.instance)
                           .addRegularColumn("currentRxBytes", LongType.instance)
                           .addRegularColumn("totalRxBytes", LongType.instance)
                           .addRegularColumn("rxPercentage", DoubleType.instance)
                           .addRegularColumn("currentTxBytes", LongType.instance)
                           .addRegularColumn("totalTxBytes", LongType.instance)
                           .addRegularColumn("txPercentage", DoubleType.instance)
                           .build());
    }

    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());
        add(result, "initiated", StreamManager.instance.getInitiatedStreamsState());
        add(result, "receiving", StreamManager.instance.getReceivingStreamsState());
        return result;
    }

    private static void add(SimpleDataSet result, String type, List<StreamState> states)
    {
        for (StreamState state : states)
        {
            add(result, type, state);
        }
    }

    private static void add(SimpleDataSet result, String type, StreamState state)
    {
        long currentRxBytes = 0;
        long totalRxBytes = 0;
        long currentTxBytes = 0;
        long totalTxBytes = 0;
        for (SessionInfo sessInfo : state.sessions)
        {
            currentRxBytes += sessInfo.getTotalSizeReceived();
            totalRxBytes += sessInfo.getTotalSizeToReceive();
            currentTxBytes += sessInfo.getTotalSizeSent();
            totalTxBytes += sessInfo.getTotalSizeToSend();
        }
        double rxPercentage = (totalRxBytes == 0 ? 100L : currentRxBytes * 100L / totalRxBytes);
        double txPercentage = (totalTxBytes == 0 ? 100L : currentTxBytes * 100L / totalTxBytes);

        result.row(state.planId.toString())
              .column("direction", type)
              .column("description", state.streamOperation.getDescription())
              .column("currentRxBytes", currentRxBytes)
              .column("totalRxBytes", totalRxBytes)
              .column("rxPercentage", rxPercentage)
              .column("currentTxBytes", currentTxBytes)
              .column("totalTxBytes", totalTxBytes)
              .column("txPercentage", txPercentage);
    }
}

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

import java.util.Map;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Directories.SnapshotSizeDetails;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.schema.TableMetadata;

public class SnapshotDetailsTable extends AbstractVirtualTable
{
    protected SnapshotDetailsTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "table_snapshots")
                           .comment("table snapshots")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .addPartitionKeyColumn("keyspace", UTF8Type.instance)
                           .addPartitionKeyColumn("column_family", UTF8Type.instance)
                           .addClusteringColumn("name", UTF8Type.instance)
                           .addRegularColumn("size_on_disk_bytes", LongType.instance)
                           .addRegularColumn("data_size_bytes", LongType.instance)
                           .build());
    }

    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());
        for (Keyspace ks : Keyspace.all())
        {
            for (ColumnFamilyStore cfs : ks.getColumnFamilyStores())
            {
                for (Map.Entry<String, SnapshotSizeDetails> e : cfs.getSnapshotDetails().entrySet())
                {
                    SnapshotSizeDetails details = e.getValue();
                    result.row(ks.getName(), cfs.getTableName(), e.getKey());
                    result.column("size_on_disk_bytes", details.getSizeOnDiskBytes());
                    result.column("data_size_bytes", details.getDataSizeBytes());
                }
            }
        }
        return result;
    }
}

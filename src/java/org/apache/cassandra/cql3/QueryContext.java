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

package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.service.ClientState;

//TODO if you have a static multi-cell row what does Lists.Discarder due; accesses is always on the current clustering
public interface QueryContext
{
    QueryOptions options();
    ClientState clientState();

    default ByteBuffer bindAndGet(Term term)
    {
        return term.bindAndGet(options());
    }

    default Term.Terminal bind(Term term) throws InvalidRequestException
    {
        return term.bind(options());
    }

    Cell<?> getCell(DecoratedKey key, ColumnMetadata metadata);
    ComplexColumnData getComplexColumnData(DecoratedKey key, ColumnMetadata metadata);

    // mutation data

    void addTombstone(ColumnMetadata column) throws InvalidRequestException;
    void addTombstone(ColumnMetadata column, CellPath path) throws InvalidRequestException;
    Cell<?> addCell(ColumnMetadata column, ByteBuffer value) throws InvalidRequestException;
    Cell<?> addCell(ColumnMetadata column, CellPath path, ByteBuffer value) throws InvalidRequestException;
    void addCounter(ColumnMetadata column, long increment) throws InvalidRequestException;
    void setComplexDeletionTime(ColumnMetadata column);
    void setComplexDeletionTimeForOverwrite(ColumnMetadata column);
}

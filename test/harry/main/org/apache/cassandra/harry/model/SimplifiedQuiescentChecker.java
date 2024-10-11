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

package org.apache.cassandra.harry.model;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

import org.apache.cassandra.harry.data.ResultSetRow;
import org.apache.cassandra.harry.ddl.ColumnSpec;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.model.reconciler.PartitionState;
import org.apache.cassandra.harry.model.reconciler.Reconciler;

import static org.apache.cassandra.harry.gen.DataGenerators.NIL_DESCR;
import static org.apache.cassandra.harry.gen.DataGenerators.UNSET_DESCR;
import static org.apache.cassandra.harry.model.Model.NO_TIMESTAMP;

public class SimplifiedQuiescentChecker
{
    public static Reconciler.RowState adjustForSelection(Reconciler.RowState row, SchemaSpec schema, Set<ColumnSpec<?>> selection, boolean isStatic)
    {
        if (selection.size() == schema.allColumns.size())
            return row;

        List<ColumnSpec<?>> columns = isStatic ? schema.staticColumns : schema.regularColumns;
        Reconciler.RowState newRowState = row.clone();
        assert newRowState.vds.length == columns.size();
        for (int i = 0; i < columns.size(); i++)
        {
            if (!selection.contains(columns.get(i)))
            {
                newRowState.vds[i] = UNSET_DESCR;
                newRowState.lts[i] = NO_TIMESTAMP;
            }
        }
        return newRowState;
    }

    public static void validate(SchemaSpec schema, @Nullable Set<ColumnSpec<?>> selection, PartitionState partitionState, List<ResultSetRow> actualRows)
    {
        boolean isWildcardQuery = selection == null;
        if (isWildcardQuery)
            selection = new HashSet<>(schema.allColumns);

        Iterator<ResultSetRow> actual = actualRows.iterator();
        Collection<Reconciler.RowState> expectedRows = partitionState.rows(false);

        Iterator<Reconciler.RowState> expected = expectedRows.iterator();

        // It is possible that we only get a single row in response, and it is equal to static row
        if (partitionState.isEmpty() && partitionState.staticRow() != null && actual.hasNext())
        {
            ResultSetRow actualRowState = actual.next();
            if (actualRowState.cd != UNSET_DESCR && actualRowState.cd != partitionState.staticRow().cd)
            {
                throw new LocalValidationException(partitionState.toString(schema),
                                                   toString(actualRows),
                                                   "Found a row while model predicts statics only:" +
                                                   "\nExpected: %s" +
                                                   "\nActual: %s",
                                                   partitionState.staticRow(),
                                                   actualRowState);
            }

            for (int i = 0; i < actualRowState.vds.length; i++)
            {
                if (actualRowState.vds[i] != NIL_DESCR || actualRowState.lts[i] != NO_TIMESTAMP)
                    throw new LocalValidationException(partitionState.toString(schema),
                                                       toString(actualRows),
                                                       "Found a row while model predicts statics only:" +
                                                       "\nActual: %s",
                                                       actualRowState);
            }

            assertStaticRow(partitionState, actualRows,
                            adjustForSelection(partitionState.staticRow(), schema, selection, true),
                            actualRowState, schema);
        }

        while (actual.hasNext() && expected.hasNext())
        {
            ResultSetRow actualRowState = actual.next();
            Reconciler.RowState originalExpectedRowState = expected.next();
            Reconciler.RowState expectedRowState = adjustForSelection(originalExpectedRowState, schema, selection, false);

            if (schema.trackLts)
                partitionState.compareVisitedLts(actualRowState.visited_lts);

            // TODO: this is not necessarily true. It can also be that ordering is incorrect.
            if (actualRowState.cd != UNSET_DESCR && actualRowState.cd != expectedRowState.cd)
            {
                throw new LocalValidationException(partitionState.toString(schema),
                                                   toString(actualRows),
                                                   "Found a row in the model that is not present in the resultset:" +
                                                   "\nExpected: %s" +
                                                   "\nActual: %s",
                                                   expectedRowState.toString(schema),
                                                   actualRowState);
            }

            if (!Arrays.equals(expectedRowState.vds, actualRowState.vds))
                throw new LocalValidationException(partitionState.toString(schema),
                                                   toString(actualRows),
                                                   "Returned row state doesn't match the one predicted by the model:" +
                                                   "\nExpected: %s (%s)" +
                                                   "\nActual:   %s (%s).",
                                                   descriptorsToString(expectedRowState.vds), expectedRowState.toString(schema),
                                                   descriptorsToString(actualRowState.vds), actualRowState);

            if (!ltsEqual(expectedRowState.lts, actualRowState.lts))
                throw new LocalValidationException(partitionState.toString(schema),
                                                   toString(actualRows),
                                                   "Timestamps in the row state don't match ones predicted by the model:" +
                                                   "\nExpected: %s (%s)" +
                                                   "\nActual:   %s (%s).",
                                                   Arrays.toString(expectedRowState.lts), expectedRowState.toString(schema),
                                                   Arrays.toString(actualRowState.lts), actualRowState);

            if (partitionState.staticRow() != null || actualRowState.hasStaticColumns())
            {
                Reconciler.RowState expectedStaticRowState = adjustForSelection(partitionState.staticRow(), schema, selection, true);
                assertStaticRow(partitionState, actualRows, expectedStaticRowState, actualRowState, schema);
            }
        }

        if (actual.hasNext() || expected.hasNext())
        {
            throw new LocalValidationException(partitionState.toString(schema),
                                               toString(actualRows),
                                               "Expected results to have the same number of results, but %s result iterator has more results." +
                                               "\nExpected: %s" +
                                               "\nActual:   %s" +
                                               "\nQuery: %s",
                                               actual.hasNext() ? "actual" : "expected",
                                               expectedRows,
                                               actualRows);
        }
    }

    public static boolean ltsEqual(long[] expected, long[] actual)
    {
        if (actual == expected)
            return true;
        if (actual == null || expected == null)
            return false;

        int length = actual.length;
        if (expected.length != length)
            return false;

        for (int i = 0; i < actual.length; i++)
        {
            if (actual[i] == NO_TIMESTAMP)
                continue;
            if (actual[i] != expected[i])
                return false;
        }
        return true;
    }

    public static void assertStaticRow(PartitionState partitionState,
                                       List<ResultSetRow> actualRows,
                                       Reconciler.RowState staticRow,
                                       ResultSetRow actualRowState,
                                       SchemaSpec schemaSpec)
    {
        if (!Arrays.equals(staticRow.vds, actualRowState.sds))
            throw new LocalValidationException(partitionState.toString(schemaSpec),
                                               toString(actualRows),
                                               "Returned static row state doesn't match the one predicted by the model:" +
                                               "\nExpected: %s (%s)" +
                                               "\nActual:   %s (%s).",
                                               descriptorsToString(staticRow.vds), staticRow.toString(schemaSpec),
                                               descriptorsToString(actualRowState.sds), actualRowState);

        if (!ltsEqual(staticRow.lts, actualRowState.slts))
            throw new LocalValidationException(partitionState.toString(schemaSpec),
                                               toString(actualRows),
                                               "Timestamps in the static row state don't match ones predicted by the model:" +
                                               "\nExpected: %s (%s)" +
                                               "\nActual:   %s (%s).",
                                               Arrays.toString(staticRow.lts), staticRow.toString(schemaSpec),
                                               Arrays.toString(actualRowState.slts), actualRowState);
    }

    public static String descriptorsToString(long[] descriptors)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < descriptors.length; i++)
        {
            if (descriptors[i] == NIL_DESCR)
                sb.append("NIL");
            if (descriptors[i] == UNSET_DESCR)
                sb.append("UNSET");
            else
                sb.append(descriptors[i]);
            if (i > 0)
                sb.append(", ");
        }
        return sb.toString();
    }

    public static String toString(Collection<Reconciler.RowState> collection, SchemaSpec schema)
    {
        StringBuilder builder = new StringBuilder();

        for (Reconciler.RowState rowState : collection)
            builder.append(rowState.toString(schema)).append("\n");
        return builder.toString();
    }

    public static String toString(List<ResultSetRow> collection)
    {
        StringBuilder builder = new StringBuilder();

        for (ResultSetRow rowState : collection)
            builder.append(rowState.toString()).append("\n");
        return builder.toString();
    }

    public static class LocalValidationException extends RuntimeException
    {
        public LocalValidationException(String partitionState, String observedState, String format, Object... objects)
        {
            super(String.format(format, objects) +
                  "\nPartition state:\n" + partitionState +
                  "\nObserved state:\n" + observedState);
        }
    }
}

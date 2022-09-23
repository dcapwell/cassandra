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

package org.apache.cassandra.cql3.transactions;

import com.google.common.base.Preconditions;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnReference;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.txn.TxnReferenceValue;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;

public abstract class ReferenceValue
{
    public abstract TxnReferenceValue bindAndGet(QueryOptions options);

    public static abstract class Raw
    {
        public abstract ReferenceValue prepare(ColumnMetadata receiver, VariableSpecifications bindVariables);

        /**
         * used by queries that implicitly require a read without explicitly defining a reference (ie: v+=3)
         */
        public abstract boolean hasSelfReference();

        public abstract void setSelfSourceName(String name);

        public abstract void setTableMetadata(TableMetadata metadata);
    }

    public static class Constant extends ReferenceValue
    {
        private final Term term;

        public Constant(Term term)
        {
            this.term = term;
        }

        @Override
        public TxnReferenceValue bindAndGet(QueryOptions options)
        {
            return new TxnReferenceValue.Constant(term.bindAndGet(options));
        }

        public static class Raw extends ReferenceValue.Raw
        {
            private final Term.Raw term;

            public Raw(Term.Raw term)
            {
                this.term = term;
            }

            @Override
            public boolean hasSelfReference()
            {
                return false;
            }

            @Override
            public void setSelfSourceName(String name)
            {

            }

            @Override
            public void setTableMetadata(TableMetadata metadata)
            {

            }

            @Override
            public ReferenceValue prepare(ColumnMetadata receiver, VariableSpecifications bindVariables)
            {
                return new Constant(term.prepare(receiver.ksName, receiver));
            }
        }
    }

    public static class Substitution extends ReferenceValue
    {
        private final ColumnReference reference;

        public Substitution(ColumnReference reference)
        {
            this.reference = reference;
        }

        @Override
        public TxnReferenceValue bindAndGet(QueryOptions options)
        {
            return new TxnReferenceValue.Substitution(reference.toValueReference(options));
        }

        public static class Raw extends ReferenceValue.Raw
        {
            private final ColumnReference.Raw reference;

            public Raw(ColumnReference.Raw reference)
            {
                this.reference = reference;
            }

            @Override
            public boolean hasSelfReference()
            {
                return false;
            }

            @Override
            public void setSelfSourceName(String name)
            {

            }

            @Override
            public void setTableMetadata(TableMetadata metadata)
            {

            }

            @Override
            public ReferenceValue prepare(ColumnMetadata receiver, VariableSpecifications bindVariables)
            {
                reference.checkResolved();
                checkTrue(reference.column() != null, "substitution references must reference a column (%s)", reference);
                return new Substitution((ColumnReference) reference.prepare("", receiver));
            }
        }
    }

    public static class SelfReference extends Raw
    {
        private final ColumnIdentifier column;
        private String sourceName = null;
        private TableMetadata metadata = null;

        public SelfReference(ColumnIdentifier column)
        {
            this.column = column;
        }

        @Override
        public ReferenceValue prepare(ColumnMetadata receiver, VariableSpecifications bindVariables)
        {
            Preconditions.checkState(sourceName != null);
            Preconditions.checkState(metadata != null);
            ColumnMetadata columnMetadata = metadata.getColumn(column);
            checkNotNull(column, "%s doesn't reference a valid column", column);
            return new Substitution(ColumnReference.Raw.prepare("", receiver, sourceName, 0, columnMetadata, null));
        }

        @Override
        public boolean hasSelfReference()
        {
            return true;
        }

        @Override
        public void setSelfSourceName(String sourceName)
        {
            this.sourceName = sourceName;
        }

        @Override
        public void setTableMetadata(TableMetadata metadata)
        {
            this.metadata = metadata;
        }
    }

    public static class Addition extends ReferenceValue
    {
        private final ReferenceValue left;
        private final ReferenceValue right;

        public Addition(ReferenceValue left, ReferenceValue right)
        {
            this.left = left;
            this.right = right;
        }

        @Override
        public TxnReferenceValue bindAndGet(QueryOptions options)
        {
            return new TxnReferenceValue.Sum(left.bindAndGet(options), right.bindAndGet(options));
        }

        public static class Raw extends ReferenceValue.Raw
        {
            private final ReferenceValue.Raw left;
            private final ReferenceValue.Raw right;

            public Raw(ReferenceValue.Raw left, ReferenceValue.Raw right)
            {
                this.left = left;
                this.right = right;
            }

            @Override
            public boolean hasSelfReference()
            {
                return left.hasSelfReference() || right.hasSelfReference();
            }

            @Override
            public void setSelfSourceName(String name)
            {
                left.setSelfSourceName(name);
                right.setSelfSourceName(name);
            }

            @Override
            public void setTableMetadata(TableMetadata metadata)
            {
                left.setTableMetadata(metadata);
                right.setTableMetadata(metadata);
            }

            @Override
            public ReferenceValue prepare(ColumnMetadata receiver, VariableSpecifications bindVariables)
            {
                return new Addition(left.prepare(receiver, bindVariables), right.prepare(receiver, bindVariables));
            }
        }
    }

    public static class Subtraction extends ReferenceValue
    {
        private final ReferenceValue left;
        private final ReferenceValue right;

        public Subtraction(ReferenceValue left, ReferenceValue right)
        {
            this.left = left;
            this.right = right;
        }

        @Override
        public TxnReferenceValue bindAndGet(QueryOptions options)
        {
            return new TxnReferenceValue.Difference(left.bindAndGet(options), right.bindAndGet(options));
        }

        public static class Raw extends ReferenceValue.Raw
        {
            private final ReferenceValue.Raw left;
            private final ReferenceValue.Raw right;

            public Raw(ReferenceValue.Raw left, ReferenceValue.Raw right)
            {
                this.left = left;
                this.right = right;
            }

            @Override
            public boolean hasSelfReference()
            {
                return left.hasSelfReference() || right.hasSelfReference();
            }

            @Override
            public void setSelfSourceName(String name)
            {
                left.setSelfSourceName(name);
                right.setSelfSourceName(name);
            }

            @Override
            public void setTableMetadata(TableMetadata metadata)
            {
                left.setTableMetadata(metadata);
                right.setTableMetadata(metadata);
            }

            @Override
            public ReferenceValue prepare(ColumnMetadata receiver, VariableSpecifications bindVariables)
            {
                return new Subtraction(left.prepare(receiver, bindVariables), right.prepare(receiver, bindVariables));
            }
        }
    }
}

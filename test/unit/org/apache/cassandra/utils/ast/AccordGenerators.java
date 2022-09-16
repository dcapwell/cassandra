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

package org.apache.cassandra.utils.ast;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Sets;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.CassandraGenerators;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.SourceDSL;
import org.quicktheories.impl.Constraint;

import static org.apache.cassandra.utils.Generators.SYMBOL_GEN;
import static org.apache.cassandra.utils.ast.Elements.newLine;

public class AccordGenerators
{
    private enum TxReturn { NONE, TABLE, REF}

    public static Gen<Txn> txnGen(TableMetadata metadata)
    {
        Gen<Boolean> bool = SourceDSL.booleans().all();
        Constraint letRange = Constraint.between(0, 10);
        Constraint conditionRange = Constraint.between(0, 1);
        Constraint updateRange = Constraint.between(0, 10);
        Gen<Select> selectGen = new Select.GenBuilder(metadata)
                                .withLimit1()
                                .build();
//        Gen<String> nameGen = Generators.IDENTIFIER_GEN;
        // table uses IDENTIFIER_GEN but can't use that here due to lack of "" support
        Gen<TxReturn> txReturnGen = SourceDSL.arbitrary().enumValues(TxReturn.class);
        Gen<Update> updateGen = new Update.GenBuilder(metadata)
                                .withoutTimestamp()
                                .build();
        return rnd -> {
            TxnBuilder builder = new TxnBuilder();
            do
            {
                int numLets = Math.toIntExact(rnd.next(letRange));
                for (int i = 0; i < numLets; i++)
                {
                    String name;
                    while (builder.lets.containsKey(name = SYMBOL_GEN.generate(rnd))) {}
                    builder.addLet(name, selectGen.generate(rnd));
                }
                switch (txReturnGen.generate(rnd))
                {
                    case REF:
                    {
                        if (!builder.allowedReferences.isEmpty())
                        {
                            Gen<List<Reference>> refsGen = SourceDSL.lists().of(SourceDSL.arbitrary().pick(new ArrayList<>(builder.allowedReferences))).ofSizeBetween(1, Math.max(10, builder.allowedReferences.size()));
                            builder.addReturn(new Select((List<Expression>) (List<?>) refsGen.generate(rnd)));
                        }
                    }
                    break;
                    case TABLE:
                        builder.addReturn(selectGen.generate(rnd));
                        break;
                }
//                int numConditions = Math.toIntExact(rnd.next(conditionRange));
//                for (int i = 0; i < numConditions; i++)
//                {
//                    //TODO
//                }
                int numUpdates = Math.toIntExact(rnd.next(updateRange));
                for (int i = 0; i < numUpdates; i++)
                    builder.addUpdate(updateGen.generate(rnd));
            } while (builder.isEmpty());
            return builder.build();
        };
    }

    public static class Txn implements Statement
    {
        // lets
        public final List<Let> lets;
        // return
        public final Optional<Select> output;
        public final List<Update> updates;

        public Txn(List<Let> lets, Optional<Select> output, List<Update> updates)
        {
            this.lets = lets;
            this.output = output;
            this.updates = updates;
        }

        @Override
        public void toCQL(StringBuilder sb, int indent)
        {
            sb.append("BEGIN TRANSACTION");
            int subIndent = indent + 2;

            stream().forEach(e -> {
                newLine(sb, subIndent);
                e.toCQL(sb, subIndent);
                sb.append(';');
            });
            newLine(sb, indent);
            sb.append("COMMIT TRANSACTION");
        }

        @Override
        public Stream<? extends Element> stream()
        {
            Stream<? extends Element> ret = lets.stream();
            if (output.isPresent())
                ret = Stream.concat(ret, Stream.of(output.get()));
            ret = Stream.concat(ret, updates.stream());
            return ret;
        }

        @Override
        public String toString()
        {
            return detailedToString();
        }
    }

    public static class TxnBuilder
    {
        private final Map<String, Select> lets = new HashMap<>();
        // no type system so don't need easy lookup to Expression; just existence check
        private final Set<Reference> allowedReferences = new HashSet<>();
        private Optional<Select> output = Optional.empty();
        private final List<Update> updates = new ArrayList<>();

        boolean isEmpty()
        {
            // don't include output as 'BEGIN TRANSACTION SELECT "000000000000000010000"; COMMIT TRANSACTION' isn't valid
//            return lets.isEmpty();
            // TransactionStatement defines empty as no SELECT or updates
            return !output.isPresent() && updates.isEmpty();
        }

        void addLet(String name, Select select)
        {
            if (lets.containsKey(name))
                throw new IllegalArgumentException("Let name " + name + " already exists");
            lets.put(name, select);
            //TODO add support for touple
//            allowedReferences.add(new Reference(Arrays.asList(name)));
            for (Expression e : select.selections)
                //TODO remove " due to current limitation... revert once fixed!
                allowedReferences.add(new Reference(Arrays.asList(name, e.name().replace("\"", ""))));
        }

        void addReturn(Select select)
        {
            output = Optional.of(select);
        }

        void addUpdate(Update update)
        {
            this.updates.add(Objects.requireNonNull(update));
        }

        Txn build()
        {
            List<Let> lets = this.lets.entrySet().stream().map(e -> new Let(e.getKey(), e.getValue())).collect(Collectors.toList());
            return new Txn(lets, output, new ArrayList<>(updates));
        }
    }

    public static class Let implements Element
    {
        public final String symbol;
        public final Select select;

        public Let(String symbol, Select select)
        {
            this.symbol = symbol;
            this.select = select;
        }

        @Override
        public void toCQL(StringBuilder sb, int indent)
        {
            int offset = sb.length();
            sb.append("LET ").append(symbol).append(" = (");
            select.toCQL(sb, sb.length() - offset + 2);
            sb.append(")");
        }

        @Override
        public Stream<? extends Element> stream()
        {
            return Stream.of(select);
        }
    }

    public static class Reference implements Expression
    {
        public final List<String> path;

        public Reference(List<String> path)
        {
            if (path.isEmpty())
                throw new IllegalArgumentException("Reference may not be empty");
            this.path = path;
        }

        @Override
        public void toCQL(StringBuilder sb, int indent)
        {
            path.forEach(p -> sb.append(p).append('.'));
            sb.setLength(sb.length() - 1); // last .
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Reference elements = (Reference) o;
            return Objects.equals(path, elements.path);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(path);
        }
    }
}

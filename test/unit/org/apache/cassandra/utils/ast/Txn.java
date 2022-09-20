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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractTypeGenerators;
import org.apache.cassandra.utils.Generators;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.SourceDSL;
import org.quicktheories.impl.Constraint;

import static org.apache.cassandra.utils.Generators.SYMBOL_GEN;
import static org.apache.cassandra.utils.ast.Elements.newLine;

public class Txn implements Statement
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

    public static class GenBuilder
    {
        public enum TxReturn { NONE, TABLE, REF}
        private final TableMetadata metadata;
        private Constraint letRange = Constraint.between(0, 10);
        private Constraint updateRange = Constraint.between(0, 10);
        private Gen<Select> selectGen;
        private Gen<TxReturn> txReturnGen = SourceDSL.arbitrary().enumValues(TxReturn.class);
        private Gen<Update> updateGen;

        public GenBuilder(TableMetadata metadata)
        {
            this.metadata = metadata;
            this.selectGen = new Select.GenBuilder(metadata)
                             .withLimit1()
                             .build();
            this.updateGen = new Update.GenBuilder(metadata)
                             .withoutTimestamp()
                             .build();
        }

        public Gen<Txn> build()
        {
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
                    int numUpdates = Math.toIntExact(rnd.next(updateRange));
                    for (int i = 0; i < numUpdates; i++)
                        builder.addUpdate(updateGen.generate(rnd));
                } while (builder.isEmpty());
                return builder.build();
            };
        }
    }

    private static class TxnBuilder
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

            Reference ref = Reference.of(new Symbol(name, toNamedTuple(select)));
            for (Expression e : select.selections)
                addAllowedReference(ref.add(e.name(), e.type()));
        }

        private AbstractType<?> toNamedTuple(Select select)
        {
            //TODO don't rely on UserType...
            List<FieldIdentifier> fieldNames = new ArrayList<>(select.selections.size());
            List<AbstractType<?>> fieldTypes = new ArrayList<>(select.selections.size());
            for (Expression e : select.selections)
            {
                fieldNames.add(FieldIdentifier.forQuoted(e.name()));
                fieldTypes.add(e.type());
            }
            return new UserType(null, null, fieldNames, fieldTypes, false);
        }

        private void maybeAddRecursiveReferences(Reference ref)
        {
            AbstractType<?> type = ref.type();
            if (type.isReversed())
                type = ((ReversedType) type).baseType;
            if (type.isCollection())
            {
                if (type instanceof SetType)
                {
                    // [value] syntax
                    SetType set = (SetType) type;
                    AbstractType subType = set.getElementsType();
                    Object value = Generators.get(AbstractTypeGenerators.getTypeSupport(subType).valueGen);
                    addAllowedReference(ref.lastAsCollection(l -> new CollectionAccess(l, new Bind(value, subType), subType)));


                }
                else if (type instanceof MapType)
                {
                    // [key] syntax
                    MapType map = (MapType) type;
                    AbstractType keyType = map.getKeysType();
                    AbstractType valueType = map.getValuesType();

                    Object v = Generators.get(AbstractTypeGenerators.getTypeSupport(keyType).valueGen);
                    addAllowedReference(ref.lastAsCollection(l -> new CollectionAccess(l, new Bind(v, keyType), valueType)));
                }
                // see Selectable.specForElementOrSlice; ListType is not supported
//                if (type instanceof ListType)
//                {
//                    // supports index
//                    AbstractType<?> subType = ((ListType<?>) type).getElementsType();
//                    for (int index : Arrays.asList(0, Integer.MAX_VALUE))
//                    {
//                        List<String> path = new ArrayList<>(ref.path.size());
//                        for (int i = 0; i < ref.path.size() - 1; i++)
//                            path.add(ref.path.get(i));
//                        path.add(ref.path.get(ref.path.size() - 1) + "[" + index + "]");
//                        addAllowedReference(new Reference(path, subType));
//                    }
//                }
            }
            else if (type.isUDT())
            {
                UserType udt = (UserType) type;
                for (int i = 0; i < udt.size(); i++)
                    addAllowedReference(ref.add(udt.fieldName(i).toString(), udt.type(i)));
            }
        }

        private void addAllowedReference(Reference ref)
        {
            allowedReferences.add(ref);
            maybeAddRecursiveReferences(ref);
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
}

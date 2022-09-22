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
import java.util.Collection;
import java.util.Collections;
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
    public final Optional<If> ifBlock;
    public final List<Update> updates;

    public Txn(List<Let> lets, Optional<Select> output, Optional<If> ifBlock, List<Update> updates)
    {
        this.lets = lets;
        this.output = output;
        this.ifBlock = ifBlock;
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
            if (!(e instanceof If))
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
        if (ifBlock.isPresent())
            ret = Stream.concat(ret, Stream.of(ifBlock.get()));
        ret = Stream.concat(ret, updates.stream());
        return ret;
    }

    @Override
    public String toString()
    {
        return toCQL();
    }

    public static class GenBuilder
    {
        public enum TxReturn { NONE, TABLE, REF}
        private final TableMetadata metadata;
        private Constraint letRange = Constraint.between(0, 10);
        private Constraint ifUpdateRange = Constraint.between(1, 3);
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
            Gen<Boolean> bool = SourceDSL.booleans().all();
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
                    Gen<Reference> refGen = SourceDSL.arbitrary().pick(new ArrayList<>(builder.allowedReferences));
                    switch (txReturnGen.generate(rnd))
                    {
                        case REF:
                        {
                            if (!builder.allowedReferences.isEmpty())
                            {
                                Gen<List<Reference>> refsGen = SourceDSL.lists().of(refGen).ofSizeBetween(1, Math.max(10, builder.allowedReferences.size()));
                                builder.addReturn(new Select((List<Expression>) (List<?>) refsGen.generate(rnd)));
                            }
                        }
                        break;
                        case TABLE:
                            builder.addReturn(selectGen.generate(rnd));
                            break;
                    }
                    if (!builder.lets.isEmpty() && bool.generate(rnd))
                    {
                        Gen<Conditional> conditionalGen = conditionalGen(refGen);
                        int numUpdates = Math.toIntExact(rnd.next(ifUpdateRange));
                        List<Update> updates = new ArrayList<>(numUpdates);
                        for (int i = 0; i < numUpdates; i++)
                            updates.add(updateGen.generate(rnd));
                        builder.addIf(new If(conditionalGen.generate(rnd), updates));
                    }
                    else
                    {
                        // Current limitation is that mutations are tied to the condition if present; can't have
                        // a condition and mutations that don't belong to it in v1... once multiple conditions are
                        // supported then can always attempt to add updates
                        int numUpdates = Math.toIntExact(rnd.next(updateRange));
                        for (int i = 0; i < numUpdates; i++)
                            builder.addUpdate(updateGen.generate(rnd));
                    }
                } while (builder.isEmpty());
                return builder.build();
            };
        }

        private static Gen<Conditional> conditionalGen(Gen<Reference> refGen)
        {
            Constraint numConditionsConstraint = Constraint.between(1, 10);
            return rnd -> {
                //TODO support OR
                Gen<Where> whereGen = whereGen(refGen.generate(rnd));
                int size = Math.toIntExact(rnd.next(numConditionsConstraint));
                Conditional accum = whereGen.generate(rnd);
                for (int i = 1; i < size; i++)
                    accum = new And(accum, whereGen.generate(rnd));
                return accum;
            };
        }

        private static Gen<Where> whereGen(Reference ref)
        {
            Gen<Where.Inequalities> kindGen = SourceDSL.arbitrary().enumValues(Where.Inequalities.class);
            Gen<?> dataGen = AbstractTypeGenerators.getTypeSupport(ref.type()).valueGen;
            return rnd -> {
                Where.Inequalities kind = kindGen.generate(rnd);
                return new Where(kind, ref, Value.gen(dataGen.generate(rnd), ref.type()).generate(rnd));
            };
        }
    }

    public static List<Reference> recursiveReferences(Reference ref)
    {
        List<Reference> accum = new ArrayList<>();
        recursiveReferences(accum, ref);
        return accum.isEmpty() ? Collections.emptyList() : accum;
    }

    private static void recursiveReferences(Collection<Reference> accum, Reference ref)
    {
        AbstractType<?> type = ref.type().unwrap();
        if (type.isCollection())
        {
            //TODO Caleb to add support for [] like normal read/write supports
//            if (type instanceof SetType)
//            {
//                // [value] syntax
//                SetType set = (SetType) type;
//                AbstractType subType = set.getElementsType();
//                Object value = Generators.get(AbstractTypeGenerators.getTypeSupport(subType).valueGen);
//                Reference subRef = ref.lastAsCollection(l -> new CollectionAccess(l, new Bind(value, subType), subType));
//                accum.add(subRef);
//                recursiveReferences(accum, subRef);
//            }
//            else if (type instanceof MapType)
//            {
//                // [key] syntax
//                MapType map = (MapType) type;
//                AbstractType keyType = map.getKeysType();
//                AbstractType valueType = map.getValuesType();
//
//                Object v = Generators.get(AbstractTypeGenerators.getTypeSupport(keyType).valueGen);
//                Reference subRef = ref.lastAsCollection(l -> new CollectionAccess(l, new Bind(v, keyType), valueType));
//                accum.add(subRef);
//                recursiveReferences(accum, subRef);
//            }
            // see Selectable.specForElementOrSlice; ListType is not supported
        }
        else if (type.isUDT())
        {
            //TODO Caleb to support multiple nesting
//            UserType udt = (UserType) type;
//            for (int i = 0; i < udt.size(); i++)
//            {
//                Reference subRef = ref.add(udt.fieldName(i).toString(), udt.type(i));
//                accum.add(subRef);
//                recursiveReferences(accum, subRef);
//            }
        }
    }

    private static class TxnBuilder
    {
        private final Map<String, Select> lets = new HashMap<>();
        // no type system so don't need easy lookup to Expression; just existence check
        private final Set<Reference> allowedReferences = new HashSet<>();
        private Optional<Select> output = Optional.empty();
        private Optional<If> ifBlock = Optional.empty();
        private final List<Update> updates = new ArrayList<>();

        boolean isEmpty()
        {
            // don't include output as 'BEGIN TRANSACTION SELECT "000000000000000010000"; COMMIT TRANSACTION' isn't valid
//            return lets.isEmpty();
            // TransactionStatement defines empty as no SELECT or updates
            return !output.isPresent() && !ifBlock.isPresent() && updates.isEmpty();
        }

        void addLet(String name, Select select)
        {
            if (lets.containsKey(name))
                throw new IllegalArgumentException("Let name " + name + " already exists");
            lets.put(name, select);

            Reference ref = Reference.of(new Symbol(name, toNamedTuple(select)));
            for (Expression e : select.selections)
                addAllowedReference(ref.add(e));
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

        private void addAllowedReference(Reference ref)
        {
            allowedReferences.add(ref);
            recursiveReferences(allowedReferences, ref);
        }

        void addReturn(Select select)
        {
            output = Optional.of(select);
        }

        void addIf(If block)
        {
            ifBlock = Optional.of(block);
        }

        void addUpdate(Update update)
        {
            this.updates.add(Objects.requireNonNull(update));
        }

        Txn build()
        {
            List<Let> lets = this.lets.entrySet().stream().map(e -> new Let(e.getKey(), e.getValue())).collect(Collectors.toList());
            return new Txn(lets, output, ifBlock, new ArrayList<>(updates));
        }
    }
}

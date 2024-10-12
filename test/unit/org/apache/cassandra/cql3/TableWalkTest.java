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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Test;

import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.Property;
import accord.utils.Property.SimpleCommand;
import accord.utils.RandomSource;
import org.apache.cassandra.cql3.ast.Mutation;
import org.apache.cassandra.cql3.ast.Select;
import org.apache.cassandra.cql3.ast.Symbol;
import org.apache.cassandra.harry.model.ASTChecker;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ASTGenerators.MutationGenBuilder;
import org.quicktheories.generators.SourceDSL;

import static accord.utils.Property.commands;
import static accord.utils.Property.multistep;
import static accord.utils.Property.stateful;
import static org.apache.cassandra.utils.AbstractTypeGenerators.getTypeSupport;
import static org.apache.cassandra.utils.Generators.toGen;

public class TableWalkTest extends CQLTester
{
    public Property.Command<State, Void, ?> insert(RandomSource rs, State state)
    {
        Mutation mutation = state.mutationGen.next(rs);
        Select select = select(mutation);
        long pd = state.checker.pd(mutation);
        return multistep(new SimpleCommand<>(mutation.kind.name() + " " + pd, s2 -> {
                             execute(mutation);
                             s2.checker.update(mutation);
                         }),
                         new SimpleCommand<>("SELECT " + pd, s2 -> s2.checker.validate(select, getRows(execute(select)))));
    }

    @Test
    public void test()
    {
        stateful().withSeed(3449305727684885768L).withExamples(1).withSteps(20).check(commands(() -> State::new)
                         .add(this::insert)
                         .build());
    }

    private class State
    {
        private final TableMetadata metadata;
        private final ASTChecker checker;
        private final Gen<Mutation> mutationGen;

        public State(RandomSource rs)
        {
            KeyspaceMetadata ks = createKeyspace(rs);
            this.metadata = createTable(rs, ks.name);
            List<Map<Symbol, ByteBuffer>> uniqueValues;
            {
                int unique = rs.nextInt(1, 10);
                if (metadata.partitionKeyColumns().size() == 1)
                {
                    Symbol col = Symbol.from(metadata.partitionKeyColumns().get(0));
                    var bbs = Gens.lists(toGen(getTypeSupport(col.type()).bytesGen()))
                        .uniqueBestEffort()
                        .ofSize(unique)
                    .next(rs);
                    uniqueValues = bbs.stream().map(b -> Map.of(col, b)).collect(Collectors.toList());
                }
                else
                {
                    List<Symbol> columns = metadata.partitionKeyColumns().stream().map(Symbol::from).collect(Collectors.toList());
                    uniqueValues = Gens.lists(r2 -> {
                        Map<Symbol, ByteBuffer> vs = new HashMap<>();
                        for (Symbol column : columns)
                            vs.put(column, toGen(getTypeSupport(column.type()).bytesGen()).next(rs));
                        return vs;
                    }).uniqueBestEffort().ofSize(unique).next(rs);
                }
            }
            this.checker = new ASTChecker(metadata);
            this.mutationGen = toGen(new MutationGenBuilder(metadata)
                                                .withoutTransaction()
                                                .withPartitions(SourceDSL.arbitrary().pick(uniqueValues))
                                                .build());
        }
    }
}

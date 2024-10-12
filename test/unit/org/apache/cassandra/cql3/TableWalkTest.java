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
import org.apache.cassandra.cql3.ast.Bind;
import org.apache.cassandra.cql3.ast.Mutation;
import org.apache.cassandra.cql3.ast.Select;
import org.apache.cassandra.cql3.ast.Symbol;
import org.apache.cassandra.harry.model.ASTChecker;
import org.apache.cassandra.harry.model.OpSelectors;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ASTGenerators.MutationGenBuilder;
import org.quicktheories.generators.SourceDSL;

import static accord.utils.Property.commands;
import static accord.utils.Property.stateful;
import static org.apache.cassandra.utils.AbstractTypeGenerators.getTypeSupport;
import static org.apache.cassandra.utils.Generators.toGen;

public class TableWalkTest extends CQLTester
{
    public Property.Command<State, Void, ?> insert(RandomSource rs, State state)
    {
        Mutation mutation = state.mutationGen.next(rs).withoutTTL().withoutTimestamp();
        OpSelectors.OperationKind kind = state.checker.kind(mutation);
        Select select = select(mutation);
        long pd = state.checker.pd(mutation);
//        String name = kind.name() + " pd" + pd;
        String name = mutation.toString();
        return new SimpleCommand<>(name, s2 -> {
            execute(mutation);
            s2.checker.update(mutation);
            s2.checker.validate(select, getRows(execute(select)));
        });
    }

    @Test
    public void test()
    {
//        3449305727684885768L
        stateful().withSeed(-7576457307656450664L).withExamples(100).withSteps(20).check(commands(() -> State::new)
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
            List<Map<Symbol, Object>> uniqueValues;
            {
                int unique = rs.nextInt(1, 10);
                if (metadata.partitionKeyColumns().size() == 1)
                {
                    Symbol col = Symbol.from(metadata.partitionKeyColumns().get(0));
                    List<Object> bbs = Gens.lists(toGen(getTypeSupport(col.type()).valueGen).map(v -> (Object) v))
                                      .uniqueBestEffort()
                                      .ofSize(unique)
                                      .next(rs);
                    uniqueValues = bbs.stream().map(b -> Map.of(col, b)).collect(Collectors.toList());
                }
                else
                {
                    List<Symbol> columns = metadata.partitionKeyColumns().stream().map(Symbol::from).collect(Collectors.toList());
                    uniqueValues = Gens.lists(r2 -> {
                        Map<Symbol, Object> vs = new HashMap<>();
                        for (Symbol column : columns)
                        {
                            Object value = toGen(getTypeSupport(column.type()).valueGen).next(rs);
                            vs.put(column, value);
                        }
                        return vs;
                    }).uniqueBestEffort().ofSize(unique).next(rs);
                }
            }
            this.checker = new ASTChecker(metadata);
            this.mutationGen = toGen(new MutationGenBuilder(metadata)
                                     .withoutTransaction()
                                     .withPartitions(SourceDSL.arbitrary().pick(uniqueValues))
                                     //TODO (coverage): there are known issues with literals, so this is to make the tests more stable
                                     .withLiteralOrBindGen((value, type) -> i -> new Bind(value, type))
                                     .build());
        }

        @Override
        public String toString()
        {
            return metadata.toCqlString(false, false, false);
        }
    }
}

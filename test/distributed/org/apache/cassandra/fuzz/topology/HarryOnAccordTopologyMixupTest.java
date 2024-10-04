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

package org.apache.cassandra.fuzz.topology;

import accord.utils.Property;
import org.apache.cassandra.service.consensus.TransactionalMode;

public class HarryOnAccordTopologyMixupTest extends HarryTopologyMixupTest
{
    public HarryOnAccordTopologyMixupTest()
    {
        super(new AccordMode(AccordMode.Kind.Direct, TransactionalMode.full));
    }

    @Override
    protected void preCheck(Property.StatefulBuilder builder)
    {
        // if a failing seed is detected, populate here
        // Example: builder.withSeed(42L);
//        builder.withSeed(3449456650920152085L); // Hitting issue with CMS fetching epoch ranges, looks like Snapshot bug?
//        builder.withSeed(3449446918502811623L); // 1 down node causes Accord to timeout requests... which is matching what we see in Benchmarks
//        builder.withSeed(3449445670822319918L); // Host replacement of 2 (node5 start) timeout... logs spam errors in accord.impl.progresslog.DefaultProgressLog$RunInvoker.accept(DefaultProgressLog.java:484)
//        builder.withSeed(3449445250539969843L); // looks to be the same as above...
    }
}

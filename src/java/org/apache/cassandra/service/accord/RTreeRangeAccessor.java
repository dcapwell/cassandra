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

package org.apache.cassandra.service.accord;

import accord.api.RoutingKey;
import accord.primitives.Range;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.utils.RTree;

public enum RTreeRangeAccessor implements RTree.Accessor<RoutingKey, Range>
{
    instance;

    @Override
    public RoutingKey start(Range range)
    {
        return range.start();
    }

    @Override
    public RoutingKey end(Range range)
    {
        return range.end();
    }

    @Override
    public boolean contains(Range range, RoutingKey routingKey)
    {
        return range.contains(routingKey);
    }

    @Override
    public boolean intersects(Range range, RoutingKey start, RoutingKey end)
    {
        return intersects(range, new TokenRange((AccordRoutingKey) start, (AccordRoutingKey) end));
    }

    @Override
    public boolean intersects(Range left, Range right)
    {
        return left.compareIntersecting(right) == 0;
    }
}
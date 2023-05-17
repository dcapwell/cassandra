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

package org.apache.cassandra.config;

import org.yaml.snakeyaml.introspector.Property;

public class ListenableProperty<A, B> extends ForwardingProperty
{
    private final CompositeConfigListener<A, B> listeners = new CompositeConfigListener();

    public ListenableProperty(Property property)
    {
        super(property);
    }

    public boolean add(String name, ConfigListener<A, B> listener)
    {
        return listeners.add(name, listener);
    }

    public boolean remove(String name)
    {
        return listeners.remove(name);
    }

    @Override
    public void set(Object o, Object o1) throws Exception
    {
        super.set(o, listeners.visit((A) o, getName(), (B) o1));
    }
}

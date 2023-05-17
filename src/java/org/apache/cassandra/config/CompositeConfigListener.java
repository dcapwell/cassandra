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

import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

public class CompositeConfigListener<A, B> implements ConfigListener<A, B>
{
    private final CopyOnWriteArrayList<Key<A, B>> listeners = new CopyOnWriteArrayList();

    @Override
    public B visit(A object, String name, B value)
    {
        for (Key<A, B> key : listeners)
            value = key.listener.visit(object, name, value);
        return value;
    }

    public boolean add(String name, ConfigListener<A, B> listener)
    {
        return listeners.addIfAbsent(new Key<>(name, listener));
    }

    public boolean remove(String name)
    {
        return listeners.removeIf(k -> k.name.equals(name));
    }

    @Override
    public String toString()
    {
        return "CompositeConfigListener{" +
               "listeners=" + listeners +
               '}';
    }

    private static final class Key<A, B>
    {
        final String name;
        final ConfigListener<A, B> listener;

        private Key(String name, ConfigListener<A, B> listener)
        {
            this.name = name;
            this.listener = listener;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Key<?, ?> key = (Key<?, ?>) o;
            return name.equals(key.name);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name);
        }

        @Override
        public String toString()
        {
            return name;
        }
    }
}

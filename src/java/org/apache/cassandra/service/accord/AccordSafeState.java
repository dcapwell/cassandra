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

import java.util.function.Function;

import accord.local.SafeState;
import accord.utils.async.AsyncChain;
import org.apache.cassandra.service.accord.AccordLoadingState.LoadingState;

public interface AccordSafeState<K, V> extends SafeState<V>
{
    void set(V update);
    V original();
    long estimatedSizeOnHeap();

    default boolean hasUpdate()
    {
        return original() != current();
    }

    default void revert()
    {
        set(original());
    }

    LoadingState loadingState();
    Runnable load(Function<K, V> loadFunction);
    AsyncChain<?> listen();
    Throwable failure();


}

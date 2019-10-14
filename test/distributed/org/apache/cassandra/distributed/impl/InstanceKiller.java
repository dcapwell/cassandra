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

package org.apache.cassandra.distributed.impl;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.JVMStabilityInspector;

class InstanceKiller
{
    private static final AtomicBoolean KILLING = new AtomicBoolean();

    public static boolean isKilled()
    {
        return KILLING.get();
    }

    public static JVMStabilityInspector.Killer createKiller(Runnable shutdown)
    {
        // the bad part is that System.exit kills the JVM, so all code which calls kill won't hit the
        // next line; yet in in-JVM dtests System.exit is not desirable, so need to rely on a runtime exception
        // as a means to try to stop execution
        // there is a expected limitation that the exception only blocks the execution of the current thread, the
        // provided shutdown hook is expected to also stop all other threads.
        return new JVMStabilityInspector.Killer() {
            @Override
            protected void killCurrentJVM(Throwable t, boolean quiet)
            {
                if (KILLING.compareAndSet(false, true))
                {
                    StorageService.instance.removeShutdownHook();
                    shutdown.run();
                }
                throw new InstanceShutdown();
            }
        };
    }

    public static final class InstanceShutdown extends RuntimeException { }
}

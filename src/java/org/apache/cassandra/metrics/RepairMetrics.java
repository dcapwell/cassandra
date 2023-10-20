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

package org.apache.cassandra.metrics;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import org.apache.cassandra.net.Verb;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class RepairMetrics
{
    public static final String TYPE_NAME = "Repair";
    public static final Counter previewFailures = Metrics.counter(DefaultNameFactory.createMetricName(TYPE_NAME, "PreviewFailures", null));
    private static final Histogram retries = Metrics.histogram(DefaultNameFactory.createMetricName(TYPE_NAME, "Retries", null), false);
    private static final Map<Verb, Histogram> retriesByVerb = Collections.synchronizedMap(new EnumMap<>(Verb.class));
    private static final Counter retryTimeout = Metrics.counter(DefaultNameFactory.createMetricName(TYPE_NAME, "RetryTimeout", null));
    private static final Map<Verb, Counter> retryTimeoutByVerb = Collections.synchronizedMap(new EnumMap<>(Verb.class));

    public static void init()
    {
        // noop
    }

    public static void retry(Verb verb, int attempt)
    {
        retries.update(attempt);
        Histogram histo = retriesByVerb.computeIfAbsent(verb, ignore -> Metrics.histogram(DefaultNameFactory.createMetricName(TYPE_NAME, "Retries-" + verb.name(), null), false));
        histo.update(attempt);
    }

    public static void retryTimeout(Verb verb)
    {
        retryTimeout.inc();
        Counter counter = retryTimeoutByVerb.computeIfAbsent(verb, ignore -> Metrics.counter(DefaultNameFactory.createMetricName(TYPE_NAME, "RetryTimeout-" + verb.name(), null)));
        counter.inc();
    }
}

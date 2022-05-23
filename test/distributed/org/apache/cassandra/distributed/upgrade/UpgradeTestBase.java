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

package org.apache.cassandra.distributed.upgrade;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.collect.Ordering;

import com.vdurmont.semver4j.Semver;
import com.vdurmont.semver4j.Semver.SemverType;

import org.junit.After;
import org.junit.BeforeClass;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.shared.DistributedTestBase;
import org.apache.cassandra.distributed.shared.ThrowingRunnable;
import org.apache.cassandra.distributed.shared.Versions;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.SimpleGraph;

import static org.apache.cassandra.distributed.shared.Versions.Version;
import static org.apache.cassandra.distributed.shared.Versions.find;

public class UpgradeTestBase extends DistributedTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(UpgradeTestBase.class);

    @After
    public void afterEach()
    {
        triggerGC();
    }

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        ICluster.setup();
    }


    public UpgradeableCluster.Builder builder()
    {
        return UpgradeableCluster.build();
    }

    public static interface RunOnCluster
    {
        public void run(UpgradeableCluster cluster) throws Throwable;
    }

    public static interface RunOnClusterAndNode
    {
        public void run(UpgradeableCluster cluster, int node) throws Throwable;
    }

    public static final Semver v30 = new Semver("3.0.0-alpha1", SemverType.LOOSE);
    public static final Semver v3X = new Semver("3.11.0", SemverType.LOOSE);
    public static final Semver v40 = new Semver("4.0-alpha1", SemverType.LOOSE);
    public static final Semver v41 = new Semver("4.1-alpha1", SemverType.LOOSE);
    public static final Semver v42 = new Semver("4.2-alpha1", SemverType.LOOSE);

    protected static final SimpleGraph<Semver> SUPPORTED_UPGRADE_PATHS = new SimpleGraph.Builder()
                                                                         .addEdge(v30, v3X)
                                                                         .addEdge(v30, v40)
                                                                         .addEdge(v30, v41)
                                                                         .addEdge(v30, v42)
                                                                         .addEdge(v3X, v40)
                                                                         .addEdge(v3X, v41)
                                                                         .addEdge(v3X, v42)
                                                                         .addEdge(v40, v41)
                                                                         .addEdge(v40, v42)
                                                                         .addEdge(v41, v42)
                                                                         .build();

    // the last is always the current
    public static final Semver CURRENT = Ordering.natural().max(SUPPORTED_UPGRADE_PATHS.vertices());
    public static final Semver OLDEST = Ordering.natural().min(SUPPORTED_UPGRADE_PATHS.vertices());

    public static class TestVersions
    {
        final Version initial;
        final List<Version> upgrade;
        final List<Semver> upgradeVersions;

        public TestVersions(Version initial, List<Version> upgrade)
        {
            this.initial = initial;
            this.upgrade = upgrade;
            this.upgradeVersions = upgrade.stream().map(v -> v.version).collect(Collectors.toList());
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TestVersions that = (TestVersions) o;
            return Objects.equals(initial.version, that.initial.version) && Objects.equals(upgradeVersions, that.upgradeVersions);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(initial.version, upgradeVersions);
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append(initial.version).append(" -> ");
            sb.append(upgradeVersions);
            return sb.toString();
        }
    }

    public static class TestCase implements ThrowingRunnable
    {
        private final Versions versions;
        private final List<TestVersions> upgrade = new ArrayList<>();
        private int nodeCount = 3;
        private RunOnCluster setup;
        private RunOnClusterAndNode runBeforeNodeRestart;
        private RunOnClusterAndNode runAfterNodeUpgrade;
        private RunOnCluster runAfterClusterUpgrade;
        private final Set<Integer> nodesToUpgrade = new LinkedHashSet<>();
        private Consumer<IInstanceConfig> configConsumer;
        private Consumer<UpgradeableCluster.Builder> builderConsumer;

        public TestCase()
        {
            this(find());
        }

        public TestCase(Versions versions)
        {
            this.versions = versions;
        }

        public TestCase nodes(int nodeCount)
        {
            this.nodeCount = nodeCount;
            return this;
        }

        public TestCase defaultUpgrades()
        {
            return upgrades(OLDEST, CURRENT);
        }

        /** performs all supported upgrade paths that exist in between from and CURRENT (inclusive) **/
        public TestCase upgradesFrom(Semver from)
        {
            return upgrades(from, CURRENT);
        }

        /** performs all supported upgrade paths that exist in between from and to (inclusive) **/
        public TestCase upgrades(Semver from, Semver to)
        {
            List<TestVersions> upgrades = findUpgradePaths(from, to);
            // order by most direct to largest path
            Collections.sort(upgrades, Comparator.comparingInt(a -> a.upgrade.size()));
            logger.info("Adding upgrades of\n{}", upgrades.stream().map(TestVersions::toString).collect(Collectors.joining("\n")));
            this.upgrade.addAll(upgrades);
            return this;
        }

        private List<TestVersions> findUpgradePaths(Semver from, Semver to)
        {
            return SUPPORTED_UPGRADE_PATHS.findPaths(from, to).stream()
                                          .map(m -> new TestVersions(versions.getLatest(m.get(0)), m.stream().skip(1).map(versions::getLatest).collect(Collectors.toList())))
                                          .collect(Collectors.toList());
        }

        /** Will test this specific upgrade path **/
        public TestCase singleUpgrade(Semver from)
        {
            TestVersions target = new TestVersions(versions.getLatest(from), Arrays.asList(versions.getLatest(CURRENT)));
            Set<TestVersions> supported = new HashSet<>(findUpgradePaths(from, CURRENT));
            if (!supported.contains(target))
                throw new AssertionError("Upgrading from " + from + " to " + CURRENT + " isn't directly supported and must go through other versions first; supported paths: " + supported);
            this.upgrade.add(target);
            return this;
        }

        public TestCase setup(RunOnCluster setup)
        {
            this.setup = setup;
            return this;
        }

        public TestCase runBeforeNodeRestart(RunOnClusterAndNode runBeforeNodeRestart)
        {
            this.runBeforeNodeRestart = runBeforeNodeRestart;
            return this;
        }

        public TestCase runAfterNodeUpgrade(RunOnClusterAndNode runAfterNodeUpgrade)
        {
            this.runAfterNodeUpgrade = runAfterNodeUpgrade;
            return this;
        }

        public TestCase runAfterClusterUpgrade(RunOnCluster runAfterClusterUpgrade)
        {
            this.runAfterClusterUpgrade = runAfterClusterUpgrade;
            return this;
        }

        public TestCase withConfig(Consumer<IInstanceConfig> config)
        {
            this.configConsumer = config;
            return this;
        }

        public TestCase withBuilder(Consumer<UpgradeableCluster.Builder> builder)
        {
            this.builderConsumer = builder;
            return this;
        }

        public void run() throws Throwable
        {
            if (setup == null)
                throw new AssertionError();
            if (upgrade.isEmpty())
                throw new AssertionError("no upgrade paths have been specified (or exist)");
            if (runAfterClusterUpgrade == null && runAfterNodeUpgrade == null)
                throw new AssertionError();
            if (runBeforeNodeRestart == null)
                runBeforeNodeRestart = (c, n) -> {};
            if (runAfterClusterUpgrade == null)
                runAfterClusterUpgrade = (c) -> {};
            if (runAfterNodeUpgrade == null)
                runAfterNodeUpgrade = (c, n) -> {};
            if (nodesToUpgrade.isEmpty())
                for (int n = 1; n <= nodeCount; n++)
                    nodesToUpgrade.add(n);

            for (TestVersions upgrade : this.upgrade)
            {
                logger.info("testing upgrade from {} to {}", upgrade.initial.version, upgrade.upgradeVersions);
                try (UpgradeableCluster cluster = init(UpgradeableCluster.create(nodeCount, upgrade.initial, configConsumer, builderConsumer)))
                {
                    setup.run(cluster);



                    for (Version nextVersion : upgrade.upgrade)
                    {
                        try
                        {
                            for (int n : nodesToUpgrade)
                            {
                                cluster.get(n).shutdown().get();
                                triggerGC();
                                cluster.get(n).setVersion(nextVersion);
                                runBeforeNodeRestart.run(cluster, n);
                                cluster.get(n).startup();
                                runAfterNodeUpgrade.run(cluster, n);
                            }

                            runAfterClusterUpgrade.run(cluster);

                            cluster.checkAndResetUncaughtExceptions();
                        }
                        catch (Throwable t)
                        {
                            throw new AssertionError(String.format("Error in test '%s' while upgrading to '%s'", upgrade, nextVersion.version), t);
                        }
                    }
                }
            }
        }

        public TestCase nodesToUpgrade(int ... nodes)
        {
            Set<Integer> set = new HashSet<>(nodes.length);
            for (int n : nodes)
            {
                set.add(n);
            }
            nodesToUpgrade.addAll(set);
            return this;
        }

        public TestCase nodesToUpgradeOrdered(int ... nodes)
        {
            for (int n : nodes)
            {
                nodesToUpgrade.add(n);
            }
            return this;
        }
     }

    private static void triggerGC()
    {
        System.runFinalization();
        System.gc();
    }

    protected TestCase allUpgrades(int nodes, int... toUpgrade)
    {
        return new TestCase().nodes(nodes)
                             .defaultUpgrades()
                             .nodesToUpgrade(toUpgrade);
    }

    protected static int primaryReplica(List<Long> initialTokens, Long token)
    {
        int primary = 1;

        for (Long initialToken : initialTokens)
        {
            if (token <= initialToken)
            {
                break;
            }

            primary++;
        }

        return primary;
    }

    protected static Long tokenFrom(int key)
    {
        DecoratedKey dk = Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes(key));
        return (Long) dk.getToken().getTokenValue();
    }

    protected static int nextNode(int current, int numNodes)
    {
        return current == numNodes ? 1 : current + 1;
    }
}

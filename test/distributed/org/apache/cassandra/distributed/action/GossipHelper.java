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

package org.apache.cassandra.distributed.action;


import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Stream;

import net.openhft.chronicle.core.util.SerializableBiFunction;
import net.openhft.chronicle.core.util.SerializableConsumer;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.impl.Instance;
import org.apache.cassandra.distributed.shared.VersionedApplicationState;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.service.PendingRangeCalculatorService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.distributed.impl.DistributedTestSnitch.toCassandraInetAddressAndPort;


public class GossipHelper
{

    public static ClusterAction clusterAction(InstanceAction action)
    {
        return cluster -> cluster.stream().forEach(instance -> {
            action.apply(cluster, instance);
        });
    }

    public static ClusterAction clusterAction(InstanceAction action, Predicate<IInvokableInstance> filter)
    {
        return cluster -> cluster.stream().forEach(instance -> {
            if (filter.test(instance))
                action.apply(cluster, instance);
        });
    }

    public static ClusterAction clusterAction(InstanceAction action, int... instanceIds)
    {
        return cluster -> {
            for (int idx : instanceIds)
                action.apply(cluster, cluster.get(idx));
        };

    }

    public static InstanceAction statusToBootstrap(IInvokableInstance newNode)
    {
        return new StatusToBootstrap(newNode);
    }

    public static ClusterAction statusToNormal()
    {
        return new ClusterAction()
        {
            public void apply(ICluster<IInvokableInstance> cluster)
            {
                cluster.stream().forEach((peer) -> {
                    clusterAction(statusToNormal(peer)).apply(cluster);
                });
            }
        };
    }

    public static InstanceAction statusToNormal(IInvokableInstance newNode)
    {
        return new StatusToBootstrap(newNode);
    }

    public static InstanceAction statusToLeaving(IInvokableInstance newNode)
    {
        return new StatusToLeaving(newNode);
    }

    public static InstanceAction bootstrap()
    {
        return new BootstrapAction();
    }

    public static InstanceAction bootstrap(boolean joinRing, Duration waitForBootstrap, Duration waitForSchema)
    {
        return new BootstrapAction(joinRing, waitForBootstrap, waitForSchema);
    }

    public static InstanceAction disseminateGossipState(IInvokableInstance newNode)
    {
        return new DisseminateGossipState(newNode);
    }

    public static InstanceAction pullSchemaFrom(IInvokableInstance pullFrom)
    {
        return new PullSchemaFrom(pullFrom);
    }

    private static class StatusToBootstrap implements InstanceAction
    {
        final IInvokableInstance newNode;

        public StatusToBootstrap(IInvokableInstance newNode)
        {
            this.newNode = newNode;
        }

        public void apply(ICluster<IInvokableInstance> cluster, IInvokableInstance instance)
        {
            changeGossipState(instance,
                              newNode,
                              Arrays.asList(tokens(newNode),
                                            statusBootstrapping(newNode),
                                            statusWithPortBootstrapping(newNode)));
        }
    }

    private static class StatusToLeaving implements InstanceAction
    {
        final IInvokableInstance newNode;

        public StatusToLeaving(IInvokableInstance newNode)
        {
            this.newNode = newNode;
        }

        public void apply(ICluster<IInvokableInstance> cluster, IInvokableInstance instance)
        {
            changeGossipState(instance,
                              newNode,
                              Arrays.asList(tokens(newNode),
                                            statusLeaving(newNode),
                                            statusWithPortLeaving(newNode)));
        }
    }

    private static InstanceAction disableBinary = (cluster, instance) -> {
        instance.runOnInstance(() -> {
            StorageService.instance.stopNativeTransport();
        });
    };

    public static InstanceAction disableBinary()
    {
        return disableBinary;
    }

    private static class DisseminateGossipState implements InstanceAction
    {
        final IInvokableInstance[] from;

        public DisseminateGossipState(IInvokableInstance... from)
        {
            this.from = from;
        }

        public void apply(ICluster<IInvokableInstance> cluster, IInvokableInstance instance)
        {
            Map<InetSocketAddress, byte[]> m = new HashMap<>();
            for (IInvokableInstance node : from)
            {
                byte[] epBytes = node.callsOnInstance(() -> {
                    EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(FBUtilities.getBroadcastAddressAndPort());
                    return toBytes(epState);
                }).call();
                m.put(node.broadcastAddress(), epBytes);
            }

            instance.appliesOnInstance((IIsolatedExecutor.SerializableFunction<Map<InetSocketAddress, byte[]>, Void>)
                                       (map) -> {
                                           Map<InetAddressAndPort, EndpointState> newState = new HashMap<>();
                                           for (Map.Entry<InetSocketAddress, byte[]> e : map.entrySet())
                                               newState.put(toCassandraInetAddressAndPort(e.getKey()), fromBytes(e.getValue()));

                                           Gossiper.runInGossipStageBlocking(() -> {
                                               Gossiper.instance.applyStateLocally(newState);
                                           });
                                           return null;
                                       }).apply(m);
        }
    }

    public static byte[] toBytes(EndpointState epState)
    {
        try (DataOutputBuffer out = new DataOutputBuffer(1024))
        {
            EndpointState.serializer.serialize(epState, out, MessagingService.current_version);
            return out.toByteArray();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static EndpointState fromBytes(byte[] bytes)
    {
        try (DataInputBuffer in = new DataInputBuffer(bytes))
        {
            return EndpointState.serializer.deserialize(in, MessagingService.current_version);
        }
        catch (Throwable t)
        {
            throw new RuntimeException(t);
        }
    }

    private static class PullSchemaFrom implements InstanceAction
    {
        final IInvokableInstance pullFrom;

        public PullSchemaFrom(IInvokableInstance pullFrom)
        {
            this.pullFrom = pullFrom;
        }

        public void apply(ICluster<IInvokableInstance> cluster, IInvokableInstance pullTo)
        {
            InetSocketAddress addr = pullFrom.broadcastAddress();

            pullTo.runOnInstance(() -> {
                InetAddressAndPort endpoint = toCassandraInetAddressAndPort(addr);
                EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
                MigrationManager.scheduleSchemaPull(endpoint, state);
                MigrationManager.waitUntilReadyForBootstrap();
            });
        }
    }

    public static class BootstrapAction implements InstanceAction
    {
        private final boolean joinRing;
        private final Duration waitForBootstrap;
        private final Duration waitForSchema;

        public BootstrapAction()
        {
            this(true, Duration.ofMinutes(10), Duration.ofSeconds(10));
        }

        public BootstrapAction(boolean joinRing, Duration waitForBootstrap, Duration waitForSchema)
        {
            this.joinRing = joinRing;
            this.waitForBootstrap = waitForBootstrap;
            this.waitForSchema = waitForSchema;
        }

        public void apply(ICluster<IInvokableInstance> cluster, IInvokableInstance instance)
        {
            instance.appliesOnInstance((String partitionerString, String tokenString) -> {
                IPartitioner partitioner = FBUtilities.newPartitioner(partitionerString);
                List<Token> tokens = Collections.singletonList(partitioner.getTokenFactory().fromString(tokenString));
                try
                {
                    StorageService.instance.waitForSchema((int) waitForSchema.toMillis());
                    PendingRangeCalculatorService.instance.blockUntilFinished();
                    StorageService.instance.startBootstrap(tokens).get(waitForBootstrap.toMillis(), TimeUnit.MILLISECONDS);
                    StorageService.instance.setUpDistributedSystemKeyspaces();
                    if (joinRing)
                        StorageService.instance.finishJoiningRing(true, tokens);
                }
                catch (Throwable t)
                {
                    throw new RuntimeException(t);
                }

                return null;
            }).apply(instance.config().getString("partitioner"), instance.config().getString("initial_token"));
        }
    }

    public static class DecomissionAction implements InstanceAction
    {
        public void apply(ICluster<IInvokableInstance> cluster, IInvokableInstance target)
        {
            target.runOnInstance(() -> {
                try
                {
                    StorageService.instance.decommission(false);
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException();
                }
            });
        }
    }

    private static class StatusToNormal implements InstanceAction
    {
        final IInvokableInstance peer;

        public StatusToNormal(IInvokableInstance peer)
        {
            this.peer = peer;
        }

        public void apply(ICluster<IInvokableInstance> cluster, IInvokableInstance target)
        {
            changeGossipState(target,
                              peer,
                              Arrays.asList(tokens(peer),
                                            statusNormal(peer),
                                            statusWithPortNormal(peer)));
        }
    }

    public static VersionedApplicationState tokens(IInvokableInstance instance)
    {
        return versionedToken(instance, ApplicationState.TOKENS, (partitioner, tokens) -> new VersionedValue.VersionedValueFactory(partitioner).tokens(tokens));
    }

    public static VersionedApplicationState statusNormal(IInvokableInstance instance)
    {
        return versionedToken(instance, ApplicationState.STATUS, (partitioner, tokens) -> new VersionedValue.VersionedValueFactory(partitioner).normal(tokens));
    }

    public static VersionedApplicationState statusWithPortNormal(IInvokableInstance instance)
    {
        return versionedToken(instance, ApplicationState.STATUS_WITH_PORT, (partitioner, tokens) -> new VersionedValue.VersionedValueFactory(partitioner).normal(tokens));
    }

    public static VersionedApplicationState statusBootstrapping(IInvokableInstance instance)
    {
        return versionedToken(instance, ApplicationState.STATUS, (partitioner, tokens) -> new VersionedValue.VersionedValueFactory(partitioner).bootstrapping(tokens));
    }

    public static VersionedApplicationState statusWithPortBootstrapping(IInvokableInstance instance)
    {
        return versionedToken(instance, ApplicationState.STATUS_WITH_PORT, (partitioner, tokens) -> new VersionedValue.VersionedValueFactory(partitioner).bootstrapping(tokens));
    }

    public static VersionedApplicationState statusLeaving(IInvokableInstance instance)
    {
        return versionedToken(instance, ApplicationState.STATUS, (partitioner, tokens) -> new VersionedValue.VersionedValueFactory(partitioner).leaving(tokens));
    }

    public static VersionedApplicationState statusWithPortLeaving(IInvokableInstance instance)
    {
        return versionedToken(instance, ApplicationState.STATUS_WITH_PORT, (partitioner, tokens) -> new VersionedValue.VersionedValueFactory(partitioner).leaving(tokens));
    }

    public static VersionedValue toVersionedValue(VersionedApplicationState vv)
    {
        return VersionedValue.unsafeMakeVersionedValue(vv.value, vv.version);
    }

    public static ApplicationState toApplicationState(VersionedApplicationState vv)
    {
        return ApplicationState.values()[vv.applicationState];
    }


    public static VersionedApplicationState versionedToken(IInvokableInstance instance, ApplicationState applicationState, SerializableBiFunction<IPartitioner, Collection<Token>, VersionedValue> supplier)
    {
        return instance.appliesOnInstance((String partitionerString, String tokenString) -> {
            IPartitioner partitioner = FBUtilities.newPartitioner(partitionerString);
            Token token = partitioner.getTokenFactory().fromString(tokenString);

            VersionedValue versionedValue = supplier.apply(partitioner, Collections.singleton(token));
            return new VersionedApplicationState(applicationState.ordinal(), versionedValue.value, versionedValue.version);
        }).apply(instance.config().getString("partitioner"), instance.config().getString("initial_token"));
    }

    public void removeFromRing(IInstance peer)
    {
        InetAddressAndPort endpoint = toCassandraInetAddressAndPort(peer.broadcastAddress());

        TokenMetadata tokenMetadata = StorageService.instance.getTokenMetadata();
        tokenMetadata.removeEndpoint(endpoint);
        Collection<Token> tokens = tokenMetadata.getTokens(endpoint);
        if (!tokens.isEmpty())
            tokenMetadata.removeBootstrapTokens(tokens);

        Gossiper.runInGossipStageBlocking(() -> Gossiper.instance.unsafeAnulEndpoint(endpoint));

        SystemKeyspace.removeEndpoint(endpoint);
        PendingRangeCalculatorService.instance.update();
        PendingRangeCalculatorService.instance.blockUntilFinished();
    }


    public static void changeGossipState(Stream<IInvokableInstance> targets, Instance peer, List<VersionedApplicationState> newState)
    {
        targets.forEach((instance) -> {
            changeGossipState(instance, peer, newState);
        });
    }

    /**
     * Changes gossip state of the `peer` on `target`
     */
    public static void changeGossipState(IInvokableInstance target, IInstance peer, List<VersionedApplicationState> newState)
    {
        int version = peer.isShutdown() ? MessagingService.current_version : peer.getMessagingVersion();

        target.appliesOnInstance((InetSocketAddress addr, UUID hostId, Integer messagingVersion) -> {
            InetAddressAndPort endpoint = toCassandraInetAddressAndPort(addr);
            StorageService storageService = StorageService.instance;

            Gossiper.runInGossipStageBlocking(() -> {
                EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
                if (state == null)
                {
                    Gossiper.instance.initializeNodeUnsafe(endpoint, hostId, 1);
                    state = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
                    if (state.isAlive() && !Gossiper.instance.isDeadState(state))
                        Gossiper.instance.realMarkAlive(endpoint, state);
                }

                for (VersionedApplicationState value : newState)
                {
                    ApplicationState as = toApplicationState(value);
                    VersionedValue vv = toVersionedValue(value);
                    state.addApplicationState(as, vv);
                    storageService.onChange(endpoint, as, vv);
                }
            });

            MessagingService.instance().versions.set(endpoint, messagingVersion);
            return null;
        }).apply(peer.broadcastAddress(), peer.config().hostId(), version);
    }
}

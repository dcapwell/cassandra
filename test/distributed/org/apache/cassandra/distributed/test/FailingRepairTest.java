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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableConsumer;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableRunnable;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.ForwardingSSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.messages.RepairMessage;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ActiveRepairService.ParentRepairStatus;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamManager;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

@RunWith(Parameterized.class)
public class FailingRepairTest extends DistributedTestBase implements Serializable
{
    private static final Object[][] EMPTY_ROWS = new Object[0][0];
    private static Cluster CLUSTER;

    private final Verb messageType;
    private final RepairParallelism parallelism;
    private final SerializableRunnable setup;
    private final SerializableConsumer<Message<RepairMessage>> action;

    public FailingRepairTest(Verb messageType, RepairParallelism parallelism, SerializableRunnable setup, SerializableConsumer<Message<RepairMessage>> action)
    {
        this.messageType = messageType;
        this.parallelism = parallelism;
        this.setup = setup;
        this.action = action;
    }

    @Parameters(name = "{0}/{1}")
    public static Collection<Object[]> messages()
    {
        class MessageOverride
        {
            private final Verb messageType;
            private final SerializableRunnable setup;
            private final SerializableConsumer<Message<RepairMessage>> injectFailure;

            MessageOverride(Verb messageType, SerializableRunnable setup, SerializableConsumer<Message<RepairMessage>> injectFailure)
            {
                this.messageType = messageType;
                this.setup = setup;
                this.injectFailure = injectFailure;
            }
        }
        return Stream.of(RepairParallelism.values())
                     .flatMap(p -> {
                         List<MessageOverride> os = new ArrayList<>(4);
                         os.add(new MessageOverride(Verb.PREPARE_MSG, () -> {
                         }, ignore -> {
                             throw new IllegalArgumentException("Test would like you to fail, so do as I command");
                         }));
                         os.add(new MessageOverride(Verb.SNAPSHOT_MSG, () -> {
                         }, ignore -> {
                             throw new IllegalArgumentException("Test would like you to fail, so do as I command");
                         }));
                         os.add(new MessageOverride(Verb.VALIDATION_REQ, failingReaders(Verb.VALIDATION_REQ, p), ignore -> {
                         }));
                         os.add(new MessageOverride(Verb.SYNC_REQ, failingReaders(Verb.SYNC_REQ, p), ignore -> {
                         }));
                         return os.stream()
                                  // when doing parallel, snapshot is not done, so exclude from checking (since this repair SHOULD pass)
                                  .filter(mo -> !(p == RepairParallelism.PARALLEL && mo.messageType == Verb.SNAPSHOT_MSG))
                                  .map(tc -> new Object[]{ tc.messageType, p, tc.setup, tc.injectFailure });
                     }).collect(Collectors.toList());
    }

    private static SerializableRunnable failingReaders(Verb type, RepairParallelism parallelism)
    {
        return () -> {
            String cfName = getCfName(type, parallelism);
            ColumnFamilyStore cf = Keyspace.open(KEYSPACE).getColumnFamilyStore(cfName);
            cf.forceBlockingFlush();
            Set<SSTableReader> remove = cf.getLiveSSTables();
            Set<SSTableReader> replace = new HashSet<>();
            if (type == Verb.VALIDATION_REQ)
            {
                for (SSTableReader r : remove)
                    replace.add(new FailingSSTableReader(r));
            }
            else if (type == Verb.SYNC_REQ)
            {
                for (SSTableReader r : remove)
                    replace.add(new FailingDataChannelSSTableReader(r));
            }
            else
            {
                throw new UnsupportedOperationException("verb: " + type);
            }
            cf.getTracker().removeUnsafe(remove);
            cf.addSSTables(replace);
        };
    }

    private static String getCfName(Verb type, RepairParallelism parallelism)
    {
        return type.name().toLowerCase() + "_" + parallelism.name().toLowerCase();
    }

    @BeforeClass
    public static void createCluster() throws IOException
    {
        // streaming requires networking ATM
        // streaming also requires gossip or isn't setup properly
        CLUSTER = init(Cluster.build(3).withConfig(c -> c.with(Feature.NETWORK).with(Feature.GOSSIP)).start());
    }

    @AfterClass
    public static void teardown()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    @Before
    public void cleanupState()
    {
        // since the cluster is shared between tests, make sure to clear the state
        for (int i = 1; i <= CLUSTER.size(); i++)
        {
            CLUSTER.get(i).runOnInstance(() -> {
                MessagingService.instance().inboundSink.clear();

                for (ColumnFamilyStore cf : Keyspace.open(KEYSPACE).getColumnFamilyStores())
                {
                    for (String snapshots : cf.getSnapshotDetails().keySet())
                    {
                        cf.clearSnapshot(snapshots);
                    }
                }

                StreamManager.instance.clear();
            });
        }
        CLUSTER.clearErrors();
    }

    @Test(timeout = 1 * 60 * 1000)
    public void testFailingMessage() throws IOException
    {
        String tableName = getCfName(messageType, parallelism);
        String fqtn = KEYSPACE + "." + tableName;

        CLUSTER.schemaChange("CREATE TABLE " + fqtn + " (k INT, PRIMARY KEY (k))");

        // create data which will NOT conflict
        int lhsOffset = 10;
        int rhsOffset = 20;
        int limit = rhsOffset + (rhsOffset - lhsOffset);
        for (int i = 0; i < lhsOffset; i++)
            CLUSTER.coordinator(1)
                   .execute("INSERT INTO " + fqtn + " (k) VALUES (?)", ConsistencyLevel.ALL, i);

        // create data on LHS which does NOT exist in RHS
        for (int i = lhsOffset; i < rhsOffset; i++)
            CLUSTER.get(1).executeInternal("INSERT INTO " + fqtn + " (k) VALUES (?)", i);

        // create data on RHS which does NOT exist in LHS
        for (int i = rhsOffset; i < limit; i++)
            CLUSTER.get(2).executeInternal("INSERT INTO " + fqtn + " (k) VALUES (?)", i);

        // at this point, the two nodes should be out of sync, so confirm missing data
        // node 1
        Object[][] node1Records = toRows(IntStream.range(0, rhsOffset));
        Object[][] node1Actuals = toNaturalOrder(CLUSTER.get(1).executeInternal("SELECT k FROM " + fqtn));
        Assert.assertArrayEquals(node1Records, node1Actuals);

        // node 2
        Object[][] node2Records = toRows(IntStream.concat(IntStream.range(0, lhsOffset), IntStream.range(rhsOffset, limit)));
        Object[][] node2Actuals = toNaturalOrder(CLUSTER.get(2).executeInternal("SELECT k FROM " + fqtn));
        Assert.assertArrayEquals(node2Records, node2Actuals);

        // Inject a failure to the specific messaging type
        //TODO why does cluster.forEach NOT work but .get.runOnInstance does?
        for (int i = 1; i <= CLUSTER.size(); i++)
        {
            CLUSTER.get(i).runOnInstance(() -> {
                setup.run();
                MessagingService.instance().inboundSink.add(m -> {
                    if (m.verb() != messageType)
                        return true;

                    Message<RepairMessage> message = (Message<RepairMessage>) m;
                    action.accept(message);
                    return true;
                });
            });
        }

        // run a repair which is expdcted to fail
        List<String> repairStatus = CLUSTER.get(2).callsOnInstance(() -> {
            // need all ranges on the host
            // could get rid of this and do two repairs, primary range on each node.
            String ranges = StorageService.instance.getLocalAndPendingRanges(KEYSPACE).stream()
                                                   .map(r -> r.left + ":" + r.right)
                                                   .collect(Collectors.joining(","));
            Map<String, String> args = new HashMap<String, String>()
            {{
                put(RepairOption.PARALLELISM_KEY, parallelism.getName());
                put(RepairOption.PRIMARY_RANGE_KEY, "false");
                put(RepairOption.INCREMENTAL_KEY, "false");
                put(RepairOption.TRACE_KEY, "true"); // why not?
                put(RepairOption.PULL_REPAIR_KEY, "false");
                put(RepairOption.FORCE_REPAIR_KEY, "false");
                put(RepairOption.RANGES_KEY, ranges);
            }};
            int cmd = StorageService.instance.repairAsync(KEYSPACE, args);
            Assert.assertFalse("repair command was 0, which means no-op", cmd == 0);
            List<String> status;
            do
            {
                Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
                status = StorageService.instance.getParentRepairStatus(cmd);
            } while (status == null || status.get(0).equals(ParentRepairStatus.IN_PROGRESS.name()));

            return status;
        }).call();
        Assert.assertEquals(repairStatus.toString(), ParentRepairStatus.FAILED, ParentRepairStatus.valueOf(repairStatus.get(0)));

        List<Throwable> errors = CLUSTER.getErrors();
        // check errors
        switch (messageType)
        {
            case VALIDATION_REQ:
                Assert.assertTrue(errors.toString(), errors.stream().anyMatch(FailingRepairTest::hasCorruptSSTableException));
                Assert.assertFalse(errors.toString(), errors.stream().anyMatch(FailingRepairTest::hasFSReadError));
                break;
            case SYNC_REQ:
                Assert.assertFalse(errors.toString(), errors.stream().anyMatch(FailingRepairTest::hasCorruptSSTableException));
                Assert.assertTrue(errors.toString(), errors.stream().anyMatch(FailingRepairTest::hasFSReadError));
                break;
            default:
                Assert.assertFalse(errors.toString(), errors.stream().anyMatch(FailingRepairTest::hasCorruptSSTableException));
                Assert.assertFalse(errors.toString(), errors.stream().anyMatch(FailingRepairTest::hasFSReadError));
        }

        // make sure local state gets cleaned up
        for (int i = 1; i <= CLUSTER.size(); i++)
        {
            // when running non-parallel, snapshots are taken, so make sure they are cleaned up
            Object[][] rows = CLUSTER.get(i).executeInternal("SELECT * FROM system_views.table_snapshots");
            Assert.assertArrayEquals("Expected no rows but given " + Arrays.deepToString(rows), EMPTY_ROWS, rows);

            // make sure no stream sessions are still open
            rows = CLUSTER.get(i).executeInternal("SELECT * FROM system_views.stream_state");
            Assert.assertArrayEquals(EMPTY_ROWS, rows);
        }
    }

    private static boolean hasError(Throwable e, Class<? extends Throwable> klass)
    {
        final String klassName = klass.getName();
        for (Throwable e2 = e; e2 != null; e2 = e2.getCause())
        {
            // if this is called in a different ClassLoader then equals checks will fail
            // so rely on name checks...
            if (e2.getClass().getName().equals(klassName))
                return true;
        }
        return false;
    }

    private static boolean hasFSReadError(Throwable e)
    {
        return hasError(e, FSReadError.class);
    }

    private static boolean hasCorruptSSTableException(Throwable e)
    {
        return hasError(e, CorruptSSTableException.class);
    }

    private static Object[][] toNaturalOrder(Object[][] actuals)
    {
        // data is returned in token order, so rather than try to be fancy and order expected in token order
        // convert it to natural
        int[] values = new int[actuals.length];
        for (int i = 0; i < values.length; i++)
            values[i] = (Integer) actuals[i][0];
        Arrays.sort(values);
        return toRows(IntStream.of(values));
    }

    private static Object[][] toRows(IntStream values)
    {
        return values
               .mapToObj(v -> new Object[]{ v })
               .toArray(Object[][]::new);
    }

    private static final class FailingSSTableReader extends ForwardingSSTableReader
    {

        private FailingSSTableReader(SSTableReader delegate)
        {
            super(delegate);
        }

        public ISSTableScanner getScanner()
        {
            return new FailingISSTableScanner();
        }

        public ISSTableScanner getScanner(Collection<Range<Token>> ranges)
        {
            return new FailingISSTableScanner();
        }

        public ISSTableScanner getScanner(Iterator<AbstractBounds<PartitionPosition>> rangeIterator)
        {
            return new FailingISSTableScanner();
        }

        public ISSTableScanner getScanner(ColumnFilter columns, DataRange dataRange, SSTableReadsListener listener)
        {
            return new FailingISSTableScanner();
        }

        public ChannelProxy getDataChannel()
        {
            throw new RuntimeException();
        }

        public String toString()
        {
            return "FailingSSTableReader[" + super.toString() + "]";
        }
    }

    private static final class FailingDataChannelSSTableReader extends ForwardingSSTableReader
    {

        private final ChannelProxy dataChannel;

        private FailingDataChannelSSTableReader(SSTableReader delegate)
        {
            super(delegate);
            FileChannel mock = Mockito.mock(FileChannel.class);
            try
            {
                Mockito.doAnswer(throwingAnswer(() -> new IOException("Failure to read"))).when(mock).read(Mockito.any(), Mockito.anyLong());
                Mockito.doAnswer(throwingAnswer(() -> new IOException("Failure to transferTo"))).when(mock).transferTo(Mockito.anyLong(), Mockito.anyLong(), Mockito.any());
                Mockito.doAnswer(throwingAnswer(() -> new IOException("Failure to map"))).when(mock).map(Mockito.any(), Mockito.anyLong(), Mockito.anyLong());
                Mockito.doReturn(delegate.onDiskLength()).when(mock).size();
            }
            catch (IOException e)
            {
                throw new AssertionError(e);
            }
            this.dataChannel = new ChannelProxy(delegate.getFilename(), mock);
        }

        public ChannelProxy getDataChannel()
        {
            return dataChannel;
        }
    }

    private static <T> Answer<T> throwingAnswer(Supplier<Throwable> fn)
    {
        return ignore -> {
            throw fn.get();
        };
    }

    private static final class FailingISSTableScanner implements ISSTableScanner
    {
        public long getLengthInBytes()
        {
            return 0;
        }

        public long getCompressedLengthInBytes()
        {
            return 0;
        }

        public long getCurrentPosition()
        {
            return 0;
        }

        public long getBytesScanned()
        {
            return 0;
        }

        public Set<SSTableReader> getBackingSSTables()
        {
            return Collections.emptySet();
        }

        public TableMetadata metadata()
        {
            return null;
        }

        public void close()
        {

        }

        public boolean hasNext()
        {
            throw new CorruptSSTableException(new IOException("Test commands it"), "mahahahaha!");
        }

        public UnfilteredRowIterator next()
        {
            throw new CorruptSSTableException(new IOException("Test commands it"), "mahahahaha!");
        }
    }
}

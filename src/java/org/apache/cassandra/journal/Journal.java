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
package org.apache.cassandra.journal;

import java.io.IOException;
import java.nio.file.FileStore;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.zip.CRC32;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer.Context;
import org.apache.cassandra.concurrent.Interruptible.TerminateException;
import org.apache.cassandra.concurrent.Interruptible;
import org.apache.cassandra.concurrent.SequentialExecutorPlus;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.PathUtils;
import org.apache.cassandra.journal.Segments.ReferencedSegments;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Crc;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.concurrent.WaitQueue;

import static java.lang.String.format;
import static java.util.Comparator.comparing;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.Daemon.NON_DAEMON;
import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.Interrupts.SYNCHRONIZED;
import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.SimulatorSafe.SAFE;
import static org.apache.cassandra.concurrent.Interruptible.State.NORMAL;
import static org.apache.cassandra.concurrent.Interruptible.State.SHUTTING_DOWN;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.apache.cassandra.utils.concurrent.WaitQueue.newWaitQueue;

/**
 * A generic append-only journal with some special features:
 * <p><ul>
 * <li>Records can be looked up by key
 * <li>Records can be tagged with multiple owner node ids
 * <li>Records can be invalidated by their owner ids
 * <li>Fully invalidated records get purged during segment compaction
 * </ul><p>
 *
 * Type parameters:
 * @param <V> the type of records stored in the journal
 * @param <K> the type of keys used to address the records;
              must be fixed-size and byte-order comparable
 */
public class Journal<K, V>
{
    private static final Logger logger = LoggerFactory.getLogger(Journal.class);

    final String name;
    final File directory;
    final Params params;

    final KeySupport<K> keySupport;
    final ValueSerializer<K, V> valueSerializer;

    final Metrics<K, V> metrics;
    final Flusher<K, V> flusher;
    //final Invalidator<K, V> invalidator;
    //final Compactor<K, V> compactor;

    volatile long replayLimit;
    final AtomicLong nextSegmentId = new AtomicLong();

    private volatile ActiveSegment<K> currentSegment = null;

    // segment that is ready to be used; allocator thread fills this and blocks until consumed
    private volatile ActiveSegment<K> availableSegment = null;

    private final AtomicReference<Segments<K>> segments = new AtomicReference<>();

    Interruptible allocator;
    private final WaitQueue segmentPrepared = newWaitQueue();
    private final WaitQueue allocatorThreadWaitQueue = newWaitQueue();
    private final BooleanSupplier allocatorThreadWaitCondition = () -> (availableSegment == null);

    SequentialExecutorPlus closer;
    //private final Set<Descriptor> invalidations = Collections.newSetFromMap(new ConcurrentHashMap<>());

    public Journal(String name,
                   File directory,
                   Params params,
                   KeySupport<K> keySupport,
                   ValueSerializer<K, V> valueSerializer)
    {
        this.name = name;
        this.directory = directory;
        this.params = params;

        this.keySupport = keySupport;
        this.valueSerializer = valueSerializer;

        this.metrics = new Metrics<>(name);
        this.flusher = new Flusher<>(this);
        //this.invalidator = new Invalidator<>(this);
        //this.compactor = new Compactor<>(this);
    }

    public void start()
    {
        metrics.register(flusher);

        deleteTmpFiles();

        List<Descriptor> descriptors = Descriptor.list(directory);
        // find the largest existing timestamp
        descriptors.sort(null);
        long maxTimestamp = descriptors.isEmpty()
                          ? Long.MIN_VALUE
                          : descriptors.get(descriptors.size() - 1).timestamp;
        nextSegmentId.set(replayLimit = Math.max(currentTimeMillis(), maxTimestamp + 1));

        segments.set(Segments.ofStatic(StaticSegment.open(descriptors, keySupport)));
        closer = executorFactory().sequential(name + "-closer");
        allocator = executorFactory().infiniteLoop(name + "-allocator", new AllocateRunnable(), SAFE, NON_DAEMON, SYNCHRONIZED);
        advanceSegment(null);
        flusher.start();
        //invalidator.start();
        //compactor.start();
    }

    /**
     * Cleans up unfinished component files from previous run (metadata and index)
     */
    private void deleteTmpFiles()
    {
        for (File tmpFile : directory.listUnchecked(Descriptor::isTmpFile))
            tmpFile.delete();
    }

    public void shutdown()
    {
        allocator.shutdown();
        //compactor.stop();
        //invalidator.stop();
        flusher.shutdown();
        closer.shutdown();
        closeAllSegments();
        metrics.deregister();
    }

    /**
     * Looks up a record by the provided id.
     * <p/>
     * Looking up an invalidated record may or may not return a record, depending on
     * compaction progress.
     * <p/>
     * In case multiple copies of the record exist in the log (e.g. because of user retries),
     * only the first found record will be consumed.
     *
     * @param id user-provided record id, expected to roughly correlate with time and go up
     * @param consumer function to consume the raw record (bytes and invalidation set) if found
     * @return true if the record was found, false otherwise
     */
    public boolean read(K id, RecordConsumer<K> consumer)
    {
        try (ReferencedSegments<K> segments = selectAndReference(id))
        {
            for (Segment<K> segment : segments.all())
                if (segment.read(id, consumer))
                    return true;
        }

        return false;
    }

    /**
     * Looks up a record by the provided id.
     * <p/>
     * Looking up an invalidated record may or may not return a record, depending on
     * compaction progress.
     * <p/>
     * In case multiple copies of the record exist in the log (e.g. because of user retries),
     * the first one found will be returned.
     *
     * @param id user-provided record id, expected to roughly correlate with time and go up
     * @return deserialized record if found, null otherwise
     */
    public V read(K id)
    {
        EntrySerializer.EntryHolder<K> holder = new EntrySerializer.EntryHolder<>();

        try (ReferencedSegments<K> segments = selectAndReference(id))
        {
            for (Segment<K> segment : segments.all())
            {
                if (segment.read(id, holder))
                {
                    try
                    {
                        return valueSerializer.deserialize(holder.key, new DataInputBuffer(holder.value, false), segment.descriptor.userVersion);
                    }
                    catch (IOException e)
                    {
                        // can only throw if serializer is buggy
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        return null;
    }

    /**
     * Synchronously write a record to the journal.
     * <p/>
     * Blocks until the record has been deemed durable according to the journal flush mode.
     *
     * @param id user-provided record id, expected to roughly correlate with time and go up
     * @param record the record to store
     * @param hosts hosts expected to invalidate the record
     */
    public void write(K id, V record, Set<Integer> hosts)
    {
        try (DataOutputBuffer dob = DataOutputBuffer.scratchBuffer.get())
        {
            valueSerializer.serialize(record, dob, MessagingService.current_version);
            ActiveSegment<K>.Allocation alloc = allocate(dob.getLength(), hosts);
            alloc.write(id, dob.unsafeGetBufferAndFlip(), hosts);
            flusher.waitForFlush(alloc);
        }
        catch (IOException e)
        {
            // exception during record serialization into the scratch buffer
            throw new RuntimeException(e);
        }
    }

    /**
     * Asynchronously write a record to the journal. Writes to the journal in the calling thread,
     * but doesn't wait for flush.
     * <p/>
     * Executes the supplied callback on the executor provided,
     * once the record has been deemed durable according to the journal flush mode.
     *
     * @param id user-provided record id, expected to roughly correlate with time and go up
     * @param record the record to store
     * @param hosts hosts expected to invalidate the record
     */
    public void asyncWrite(K id, V record, Set<Integer> hosts, @Nonnull Executor executor, @Nonnull Runnable onDurable)
    {
        try (DataOutputBuffer dob = DataOutputBuffer.scratchBuffer.get())
        {
            valueSerializer.serialize(record, dob, MessagingService.current_version);
            ActiveSegment<K>.Allocation alloc = allocate(dob.getLength(), hosts);
            alloc.write(id, dob.unsafeGetBufferAndFlip(), hosts);
            flusher.asyncFlush(alloc, executor, onDurable);
        }
        catch (IOException e)
        {
            // exception during record serialization into the scratch buffer
            throw new RuntimeException(e);
        }
    }

    private ActiveSegment<K>.Allocation allocate(int entrySize, Set<Integer> hosts)
    {
        ActiveSegment<K> segment = currentSegment;

        ActiveSegment<K>.Allocation alloc;
        while (null == (alloc = segment.allocate(entrySize, hosts)))
        {
            // failed to allocate; move to a new segment with enough room
            advanceSegment(segment);
            segment = currentSegment;
        }
        return alloc;
    }

    /*
     * Segment allocation logic.
     */

    private void advanceSegment(ActiveSegment<K> oldSegment)
    {
        while (true)
        {
            synchronized (this)
            {
                // do this in a critical section, so we can maintain the order of
                // segment construction when moving to allocatingFrom/activeSegments
                if (currentSegment != oldSegment)
                    return;

                // if a segment is ready, take it now, otherwise wait for the allocator thread to construct it
                if (availableSegment != null)
                {
                    // success - change allocatingFrom and activeSegments (which must be kept in order) before leaving the critical section
                    addNewActiveSegment(currentSegment = availableSegment);
                    availableSegment = null;
                    break;
                }
            }

            awaitAvailableSegment(oldSegment);
        }

        // signal the allocator thread to prepare a new segment
        wakeAllocator();

        if (null != oldSegment)
            closeActiveSegmentAndOpenAsStatic(oldSegment);

        // request that the journal be flushed out-of-band, as we've finished a segment
        flusher.requestExtraFlush();
    }

    private void awaitAvailableSegment(ActiveSegment<K> currentActiveSegment)
    {
        do
        {
            WaitQueue.Signal prepared = segmentPrepared.register(metrics.waitingOnSegmentAllocation.time(), Context::stop);
            if (availableSegment == null && currentSegment == currentActiveSegment)
                prepared.awaitUninterruptibly();
            else
                prepared.cancel();
        }
        while (availableSegment == null && currentSegment == currentActiveSegment);
    }

    private void wakeAllocator()
    {
        allocatorThreadWaitQueue.signalAll();
    }

    private void discardAvailableSegment()
    {
        ActiveSegment<K> next;
        synchronized (this)
        {
            next = availableSegment;
            availableSegment = null;
        }
        if (next != null)
            next.closeAndDiscard();
    }

    private class AllocateRunnable implements Interruptible.Task
    {
        @Override
        public void run(Interruptible.State state) throws InterruptedException
        {
            if (state == NORMAL)
                runNormal();
            else if (state == SHUTTING_DOWN)
                shutDown();
        }

        private void runNormal() throws InterruptedException
        {
            boolean interrupted = false;
            try
            {
                if (availableSegment != null)
                    throw new IllegalStateException("availableSegment is not null");

                // synchronized to prevent thread interrupts while performing IO operations and also
                // clear interrupted status to prevent ClosedByInterruptException in createSegment()
                synchronized (this)
                {
                    interrupted = Thread.interrupted();
                    availableSegment = createSegment();

                    segmentPrepared.signalAll();
                    Thread.yield();
                }
            }
            catch (Throwable t)
            {
                if (!handleError("Failed allocating journal segments", t))
                {
                    discardAvailableSegment();
                    throw new TerminateException();
                }
                TimeUnit.SECONDS.sleep(1L); // sleep for a second to avoid log spam
            }

            interrupted = interrupted || Thread.interrupted();
            if (!interrupted)
            {
                try
                {
                    // If we offered a segment, wait for it to be taken before reentering the loop.
                    // There could be a new segment in next not offered, but only on failure to discard it while
                    // shutting down-- nothing more can or needs to be done in that case.
                    WaitQueue.waitOnCondition(allocatorThreadWaitCondition, allocatorThreadWaitQueue);
                }
                catch (InterruptedException e)
                {
                    interrupted = true;
                }
            }

            if (interrupted)
            {
                discardAvailableSegment();
                throw new InterruptedException();
            }
        }

        private void shutDown() throws InterruptedException
        {
            try
            {
                // if shutdown() started and finished during segment creation, we'll be left with a
                // segment that no one will consume; discard it
                discardAvailableSegment();
            }
            catch (Throwable t)
            {
                handleError("Failed shutting down segment allocator", t);
                throw new TerminateException();
            }
        }
    }

    private ActiveSegment<K> createSegment()
    {
        Descriptor descriptor = Descriptor.create(directory, nextSegmentId.getAndIncrement(), params.userVersion());
        return ActiveSegment.create(descriptor, params, keySupport);
    }

    /**
     * Request that journal segment files flush themselves, if needed. Blocks.
     */
    void flush()
    {
        ActiveSegment<K> current = currentSegment;
        for (ActiveSegment<K> segment : segments().onlyActive())
        {
            // do not sync segments that became active after flush started
            if (segment.descriptor.timestamp > current.descriptor.timestamp)
                return;
            segment.flush();
        }
    }

    private void closeAllSegments()
    {
        Segments<K> allSegments = segments();
        for (ActiveSegment<K> segment : allSegments.onlyActive())
            segment.closeAndIfEmptyDiscard();
        for (StaticSegment<K> segment : allSegments.onlyStatic())
            segment.close();
    }

    /**
     * Select segments that could potentially have an entry with the specified id and
     * attempt to grab references to them all.
     *
     * @return a subset of segments with references to them
     */
    ReferencedSegments<K> selectAndReference(K id)
    {
        while (true)
        {
            ReferencedSegments<K> referenced = segments().selectAndReference(id);
            if (null != referenced)
                return referenced;
        }
    }

    private Segments<K> segments()
    {
        return segments.get();
    }

    private void addNewActiveSegment(ActiveSegment<K> activeSegment)
    {
        Segments<K> currentSegments, newSegments;
        do
        {
            currentSegments = segments();
            newSegments = currentSegments.withNewActiveSegment(activeSegment);
        }
        while (!segments.compareAndSet(currentSegments, newSegments));
    }

    private void replaceCompletedSegment(ActiveSegment<K> activeSegment, StaticSegment<K> staticSegment)
    {
        Segments<K> currentSegments, newSegments;
        do
        {
            currentSegments = segments();
            newSegments = currentSegments.withCompletedSegment(activeSegment, staticSegment);
        }
        while (!segments.compareAndSet(currentSegments, newSegments));
    }

    private void replaceCompactedSegment(StaticSegment<K> oldSegment, StaticSegment<K> newSegment)
    {
        Segments<K> currentSegments, newSegments;
        do
        {
            currentSegments = segments();
            newSegments = currentSegments.withCompactedSegment(oldSegment, newSegment);
        }
        while (!segments.compareAndSet(currentSegments, newSegments));
    }

    /**
     * Take care of a finished active segment:
     * 1. discard tail
     * 2. flush to disk
     * 3. persist index and metadata
     * 4. open the segment as static
     * 5. replace the finished active segment with the opened static one in Segments view
     * 6. release the Ref so the active segment will be cleaned up by its Tidy instance
     */
    private class CloseActiveSegmentRunnable implements Runnable
    {
        private final ActiveSegment<K> activeSegment;

        CloseActiveSegmentRunnable(ActiveSegment<K> activeSegment)
        {
            this.activeSegment = activeSegment;
        }

        @Override
        public void run()
        {
            activeSegment.discardUnusedTail();
            activeSegment.flush();
            activeSegment.persistComponents();
            replaceCompletedSegment(activeSegment, StaticSegment.open(activeSegment.descriptor, keySupport));
            activeSegment.release();
        }
    }

    void closeActiveSegmentAndOpenAsStatic(ActiveSegment<K> activeSegment)
    {
        closer.execute(new CloseActiveSegmentRunnable(activeSegment));
    }

    /*
     * Replay logic
     */

    /**
     * Iterate over and invoke the supplied callback on every record,
     * with segments iterated in segment timestamp order. Only visits
     * finished, on-disk segments.
     */
    public void replayStaticSegments(RecordConsumer<K> consumer)
    {
        List<StaticSegment<K>> staticSegments = new ArrayList<>(segments().onlyStatic());
        staticSegments.sort(comparing(segment -> segment.descriptor));

        for (StaticSegment<K> segment : staticSegments)
            segment.forEachRecord(consumer);
    }

    /*
     * Static helper methods used by journal components
     */

    static void validateCRC(CRC32 crc, int readCRC) throws Crc.InvalidCrc
    {
        if (readCRC != (int) crc.getValue())
            throw new Crc.InvalidCrc(readCRC, (int) crc.getValue());
    }

    /*
     * Error handling
     */

    /**
     * @return true if the invoking thread should continue, or false if it should terminate itself
     */
    boolean handleError(String message, Throwable t)
    {
        Params.FailurePolicy policy = params.failurePolicy();
        JVMStabilityInspector.inspectJournalThrowable(t, name, policy);

        switch (policy)
        {
            default:
                throw new AssertionError(policy);
            case DIE:
            case STOP:
                StorageService.instance.stopTransports();
                //$FALL-THROUGH$
            case STOP_JOURNAL:
                message = format("%s. Journal %s failure policy is %s; terminating thread.", message, name, policy);
                logger.error(maybeAddDiskSpaceContext(message), t);
                return false;
            case IGNORE:
                message = format("%s. Journal %s failure policy is %s; ignoring excepton.", message, name, policy);
                logger.error(maybeAddDiskSpaceContext(message), t);
                return true;
        }
    }

    /**
     * Add additional information to the error message if the journal directory does not have enough free space.
     *
     * @param message the original error message
     * @return the message with additional information if possible
     */
    private String maybeAddDiskSpaceContext(String message)
    {
        long availableDiskSpace = PathUtils.tryGetSpace(directory.toPath(), FileStore::getTotalSpace);
        int segmentSize = params.segmentSize();

        if (availableDiskSpace >= segmentSize)
            return message;

        return format("%s. %d bytes required for next journal segment but only %d bytes available. " +
                      "Check %s to see if not enough free space is the reason for this error.",
                      message, segmentSize, availableDiskSpace, directory);
    }
}
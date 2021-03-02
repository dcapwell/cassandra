package org.apache.cassandra.io.util;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config;

public class DataOutputBufferTest
{
    @BeforeClass
    public static void before()
    {
        System.setProperty(Config.PROPERTY_PREFIX + "dob_max_recycle_large_bytes", Integer.toString(1 * 1024 * 1024)); // set to 1mb so tests are simpler
    }

    @Test
    public void largeBufferMetrics() throws Exception
    {
        try (DataOutputBuffer buffer = DataOutputBuffer.largeBuffer()) {
            Assert.assertEquals(1, DataOutputBuffer.LARGE_BUFFER_THREADS_ALLOCATED.getCount());
            Assert.assertEquals(DataOutputBuffer.DEFAULT_INITIAL_BUFFER_SIZE, DataOutputBuffer.LARGE_BUFFER_ALLOCATED.getCount());

            // write larger than max buffer size
            for (int i = 0; i < 1024 * 1024; i++) // we are 1mb buffer but writing 4 bytes, so actually allocating 4mb
                buffer.writeInt(i);

            // this isn't the normal expected results, but for this test 4mb happens to perfectly line up
            Assert.assertEquals(4 * 1024 * 1024, DataOutputBuffer.LARGE_BUFFER_ALLOCATED.getCount());
        }

        // on clear the thread is still ref it, but the buffer gets reset to the max value
        Assert.assertEquals(1, DataOutputBuffer.LARGE_BUFFER_THREADS_ALLOCATED.getCount());
        Assert.assertEquals(1 * 1024 * 1024, DataOutputBuffer.LARGE_BUFFER_ALLOCATED.getCount());

        // can't simulate GC, so call finalize directly
        DataOutputBuffer.largeScratchBuffer.get().getClass().getDeclaredMethod("finalize").invoke(DataOutputBuffer.largeScratchBuffer.get());
        DataOutputBuffer.largeScratchBuffer.remove();

        Assert.assertEquals(0, DataOutputBuffer.LARGE_BUFFER_THREADS_ALLOCATED.getCount());
        Assert.assertEquals(0, DataOutputBuffer.LARGE_BUFFER_ALLOCATED.getCount());
    }
}
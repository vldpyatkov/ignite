package org.apache.ignite.internal.processors.datastreamer;

import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.TimeoutException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.stream.StreamReceiver;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Test timeout for Data streamer.
 */
public class DataStreamerTimeoutTest extends GridCommonAbstractTest {

    /** Cache name. */
    public static final String CACHE_NAME = "cacheName";

    /** Timeout. */
    public static final int TIMEOUT = 1_000;

    /** Amount of entries. */
    public static final int ENTRY_AMOUNT = 100;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCacheConfiguration(cacheConfiguration());

        return cfg;
    }

    /**
     * Gets cache configuration.
     *
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration() {
        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setBackups(1);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setName(CACHE_NAME);

        return cacheCfg;
    }

    /**
     * Test timeout on {@code DataStreamer.addData()} method
     * @throws Exception If fail.
     */
    public void testTimeoutOnCloseMethod() throws Exception {
        Ignite ignite = startGrid(1);

        boolean thrown = false;

        try (IgniteDataStreamer ldr = ignite.dataStreamer(CACHE_NAME)) {
            ldr.timeout(TIMEOUT);
            ldr.receiver(new TestDataReceiver());
            ldr.perNodeBufferSize(ENTRY_AMOUNT);

            for (int i=0; i < ENTRY_AMOUNT; i++)
                ldr.addData(i, i);

        }
        catch (TimeoutException e) {
            assertEquals(e.getMessage(), "Data streamer exceeded timeout when flushing.");
            thrown = true;
        }
        finally {
            stopAllGrids();
        }

        assertTrue(thrown);
    }

    /**
     * Test timeout on {@code DataStreamer.close()} method
     * @throws Exception If fail.
     */
    public void testTimeoutOnAddDataMethod() throws Exception {
        Ignite ignite = startGrid(1);

        boolean thrown = false;

        IgniteDataStreamer ldr = ignite.dataStreamer(CACHE_NAME);

        try {
            ldr.timeout(TIMEOUT);
            ldr.receiver(new TestDataReceiver());
            ldr.perNodeBufferSize(ENTRY_AMOUNT/2);
            ldr.perNodeParallelOperations(1);

            try {
                for (int i=0; i < ENTRY_AMOUNT; i++)
                    ldr.addData(i, i);
            }
            catch (TimeoutException e) {
                assertEquals(e.getMessage(), "Data streamer exceeded timeout when starts parallel operation.");

                thrown = true;
            }

        }
        finally {
            if (thrown)
                ldr.close(true);

            stopAllGrids();
        }

        assertTrue(thrown);
    }

    /**
     * Test receiver for timeout expiration emulation.
     */
    private static class TestDataReceiver implements StreamReceiver {

        /** Is first. */
        boolean isFirst = true;

        /** {@inheritDoc} */
        @Override public void receive(IgniteCache cache, Collection collection) throws IgniteException {
            try {
                if (isFirst)
                    U.sleep(2 * TIMEOUT);

                isFirst = false;
            }
            catch (IgniteInterruptedCheckedException e) {
                throw new IgniteException(e);
            }
        }
    }

}

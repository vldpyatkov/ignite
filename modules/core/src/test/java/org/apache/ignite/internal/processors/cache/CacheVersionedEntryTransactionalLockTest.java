/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import java.util.Collection;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Tests transactional locks acquired only for unchanged cache entry versions.
 */
@RunWith(Parameterized.class)
public class CacheVersionedEntryTransactionalLockTest extends GridCommonAbstractTest {
    /** */
    private static Ignite ignite0;

    /** */
    private static Ignite ignite1;

    /** */
    private static Ignite ignite2;

    /** */
    private static Ignite ignite3;

    /** */
    private static Ignite client;

    /** */
    @Parameterized.Parameter(0)
    public boolean useNearCache;

    /** */
    @Parameterized.Parameter(1)
    public int backups;

    /** */
    @Parameterized.Parameter(2)
    public boolean replicated;

    /**
     * Returns data for test.
     * @return Test parameters.
     */
    @Parameterized.Parameters(name = "useNearCache={0}, backups={1}, replicated={2}")
    public static Collection<Object[]> testData() {
        return List.of(new Object[][] {
            {false, 0, false},
            {false, 0, true},
            {false, 1, false},
            {false, 1, true},
            {false, 2, false},
            {false, 2, true},

            {true, 0, false},
            {true, 0, true},
            {true, 1, false},
            {true, 1, true},
            {true, 2, false},
            {true, 2, true}
        });
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        ignite0 = startGrid(0);
        ignite1 = startGrid(1);
        ignite2 = startGrid(2);
        ignite3 = startGrid(3);
        client = startClientGrid();

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        ignite0.destroyCache(DEFAULT_CACHE_NAME);

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi());
    }

    /**
     * Creates transactional cache.
     *
     * @param ignite Node.
     * @return Transactional cache.
     */
    private IgniteCache<Integer, Integer> transactionalCache(Ignite ignite) {
        CacheConfiguration<?, ?> ccfg =
            new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setNearConfiguration(useNearCache ? new NearCacheConfiguration<>() : null)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setCacheMode(replicated ? CacheMode.REPLICATED : CacheMode.PARTITIONED)
                .setBackups(backups);

        return (IgniteCache<Integer, Integer>)ignite.createCache(ccfg);
    }

    @Test
    public void testLockBeforePutLocalKeyTest() throws Exception {
        IgniteCache<Integer, Integer> cache = transactionalCache(ignite0);

        int key = primaryKey(cache);

        checkLockBeforePut(cache, key, READ_COMMITTED);
    }

    @Test
    public void testLockBeforePutRemoteKeyTest() throws Exception {
        IgniteCache<Integer, Integer> cache = transactionalCache(ignite0);

        int key = primaryKey(ignite1.cache(DEFAULT_CACHE_NAME));

        checkLockBeforePut(cache, key, REPEATABLE_READ);
    }

    @Test
    public void testLockBeforePutLocalKeyRepeatableReadTest() throws Exception {
        IgniteCache<Integer, Integer> cache = transactionalCache(ignite0);

        int key = primaryKey(cache);

        checkLockBeforePut(cache, key, REPEATABLE_READ);
    }

    @Test
    public void testLockBeforePutRemoteKeyRepeatableReadTest() throws Exception {
        IgniteCache<Integer, Integer> cache = transactionalCache(ignite0);

        int key = primaryKey(ignite1.cache(DEFAULT_CACHE_NAME));

        checkLockBeforePut(cache, key, REPEATABLE_READ);
    }

    @Test
    public void testEntryVersionDoesNotChangeWhenEntryIsNotUpdated() throws Exception {
        IgniteCache<Integer, Integer> cache = transactionalCache(ignite0);

        int localKey = primaryKey(cache);
        int remoteKey = primaryKey(ignite1.cache(DEFAULT_CACHE_NAME));

        cache.put(localKey, 0);
        cache.put(remoteKey, 0);

        CacheEntry<Integer, Integer> localEntry = cache.getEntry(localKey);
        CacheEntry<Integer, Integer> remoteEntry = cache.getEntry(remoteKey);

        try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            assertTrue(acquireLockForEntry(cache, localEntry, 0));
            assertTrue(acquireLockForEntry(cache, remoteEntry, 0));

            tx.commit();
        }

        assertEquals(localEntry.version(), cache.getEntry(localKey).version());
        assertEquals(remoteEntry.version(), cache.getEntry(remoteKey).version());

        try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            assertTrue(acquireLockForEntry(cache, localEntry, 0));
            assertTrue(acquireLockForEntry(cache, remoteEntry, 0));

            tx.rollback();
        }

        assertEquals(localEntry.version(), cache.getEntry(localKey).version());
        assertEquals(remoteEntry.version(), cache.getEntry(remoteKey).version());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @Ignore
    public void testVersionedEntryLockReturnsFalseWhenEntryIsLockedByAnotherTransaction() throws Exception {
        Ignite holder = grid(0);
        Ignite initiator = grid(1);

        IgniteCache<Integer, Integer> holderCache = transactionalCache(holder);
        IgniteCache<Integer, Integer> cache = initiator.cache(DEFAULT_CACHE_NAME);

        int primaryKey = primaryKey(holderCache);

        holderCache.put(primaryKey, 0);

        CacheEntry<Integer, Integer> entry = holderCache.getEntry(primaryKey);

        try (Transaction holderTx = holder.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            holderCache.put(primaryKey, 42);

            try (Transaction tx = initiator.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
                assertFalse(acquireLockForEntry(cache, entry, 10));

                tx.rollback();
            }

            holderTx.rollback();
        }

        assertEquals(0, holderCache.get(primaryKey).intValue());
        assertEquals(0, cache.get(primaryKey).intValue());
    }

    private void checkLockBeforePut(IgniteCache<Integer, Integer> cache, int key, TransactionIsolation txIsolation) throws IgniteCheckedException {
        cache.put(key, 0);

        TestRecordingCommunicationSpi.spi(ignite0).record((node, message) -> {
            info("PVD:: Message sent [from=" + ignite0.cluster().localNode().consistentId() +
                ", to=" + node.consistentId() +
                ", msg=" + message.getClass().getSimpleName() + ']');

            return false;
        });

        CacheEntry<Integer, Integer> entry = cache.getEntry(key);

        assertNotNull(entry);
        assertNotNull(entry.version());

        try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, txIsolation)) {
            boolean locked = acquireLockForEntry(cache, entry, 0);

            assertTrue(locked);

            assertEquals(0, cache.get(key).intValue());

            cache.put(key, 1);

            assertEquals(1, cache.get(key).intValue());

            tx.commit();
        }

        assertEquals(1, cache.get(key).intValue());
        assertTrue(cache.getEntry(key).version().compareTo(entry.version()) > 0);
    }

    @SuppressWarnings("unchecked")
    private static boolean acquireLockForEntry(
        IgniteCache<Integer, Integer> cache,
        CacheEntry<Integer, Integer> entry,
        long timeout
    ) throws IgniteCheckedException {
        return cache.unwrap(IgniteCacheProxy.class).internalProxy().lock(entry, timeout);
    }
}

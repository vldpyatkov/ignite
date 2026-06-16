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
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionTimeoutException;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Tests transactional entry locking through internal cache API.
 */
@RunWith(Parameterized.class)
public class LockTxEntryOneNodeTest extends GridCommonAbstractTest {
    @ClassRule
    public static Timeout globalTimeout = new Timeout(30, TimeUnit.SECONDS);

    /** */
    private static final int KEY = 1;

    /** */
    private static final int INIT_VAL = 1;

    /** */
    private static Ignite ignite;

    /** */
    @Parameterized.Parameter(0)
    public boolean commit;

    /** */
    @Parameterized.Parameter(1)
    public boolean useNearCache;

    /** */
    @Parameterized.Parameter(2)
    public CacheMode cacheMode;

    /** Cache. */
    private IgniteCache<Integer, Integer> cache;

    /**
     * Returns data for test.
     * @return Test parameters.
     */
    @Parameterized.Parameters(name = "commit={0}, useNearCache={1}, cacheMode={2}")
    public static Collection<Object[]> testData() {
        return List.of(new Object[][] {
            {false, false, PARTITIONED},
            {false, false, REPLICATED},
            {true, false, PARTITIONED},
            {true, false, REPLICATED},
            {false, true, PARTITIONED},
            {false, true, REPLICATED},
            {false, false, PARTITIONED},
            {false, false, REPLICATED},
        });
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        ignite = startGrid(0);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cache = ignite.createCache(new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setNearConfiguration(useNearCache ? new NearCacheConfiguration<>() : null)
            .setCacheMode(cacheMode));

        cache.put(KEY, INIT_VAL);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        ignite.destroyCache(DEFAULT_CACHE_NAME);

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLockTxEntryInPessimisticTransaction() throws Exception {
        CacheEntry<Integer, Integer> entry = cache.getEntry(KEY);

        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            assertTrue(acquireLockForEntry(entry, 0));

            assertEquals(entry.getValue(), cache.get(KEY));

            checkInaccessInOtherTx(cache);

            if (commit)
                tx.commit();
        }

        assertEquals(INIT_VAL, cache.get(KEY).intValue());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLockAlreadyLockedTxEntry() throws Exception {
        CacheEntry<Integer, Integer> entry = cache.getEntry(KEY);

        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            cache.put(KEY, 2);

            assertTrue(acquireLockForEntry(entry, 0));

            assertEquals(2, cache.get(KEY).intValue());

            checkInaccessInOtherTx(cache);

            if (commit)
                tx.commit();
        }

        assertEquals(commit ? 2 : INIT_VAL, cache.get(KEY).intValue());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFailToLockTxEntryInPessimisticTransaction() throws Exception {
        CacheEntry<Integer, Integer> entry = cache.getEntry(KEY);

        cache.put(KEY, 2);

        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            assertFalse(acquireLockForEntry(entry, 0));

            assertEquals(2, cache.get(KEY).intValue());

            checkAccessInOtherTx(cache);

            if (commit)
                tx.commit();
        }

        assertEquals(2, cache.get(KEY).intValue());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLockTxEntryReturnsFalseOnTimeoutWhenLockedInOtherTransaction() throws Exception {
        CacheEntry<Integer, Integer> entry = cache.getEntry(KEY);

        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            assertTrue(acquireLockForEntry(entry, 0));

            IgniteInternalFuture<Boolean> lockFut = GridTestUtils.runAsync(new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
                        boolean locked = acquireLockForEntry(entry, 100);

                        assertFalse(locked);

                        tx.commit();

                        return locked;
                    }
                }
            });

            assertFalse(lockFut.get(10_000));

            if (commit)
                tx.commit();
        }

        assertEquals(INIT_VAL, cache.get(KEY).intValue());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUpdateAfterLock() throws Exception {
        CacheEntry<Integer, Integer> entry = cache.getEntry(KEY);

        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            assertTrue(acquireLockForEntry(entry, 0));

            checkInaccessInOtherTx(cache);

            assertEquals(entry.getValue(), cache.get(KEY));

            cache.put(KEY, 2);

            assertEquals(2, cache.get(KEY).intValue());

            if (commit)
                tx.commit();
        }

        assertEquals(commit ? 2 : INIT_VAL, cache.get(KEY).intValue());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLockTxEntryFailsWithoutTransaction() throws Exception {
        CacheEntry<Integer, Integer> entry = cache.getEntry(KEY);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                acquireLockForEntry(entry, 0);

                return null;
            }
        }, IgniteCheckedException.class, "without transaction");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLockTxEntryAsyncFailsInOptimisticTransaction() throws Exception {
        CacheEntry<Integer, Integer> entry = cache.getEntry(KEY);

        try (Transaction tx = ignite.transactions().txStart(OPTIMISTIC, REPEATABLE_READ)) {
            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    return acquireLockForEntry(entry, 0);
                }
            }, IgniteCheckedException.class, "optimistic transaction");
        }
    }

    private void checkAccessInOtherTx(IgniteCache<Integer, Integer> cache) throws IgniteCheckedException {
        IgniteInternalFuture<Void> accessFut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() {
                try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 100, 1)) {
                    cache.get(KEY);

                    tx.commit();
                }

                return null;
            }
        });

        accessFut.get(10_000);
    }

    private void checkInaccessInOtherTx(IgniteCache<Integer, Integer> cache) throws IgniteCheckedException {
        IgniteInternalFuture<Void> accessFut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() {
                try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 100, 1)) {
                    cache.get(KEY);

                    tx.commit();
                }

                return null;
            }
        });

        GridTestUtils.assertThrowsWithCause(new Callable<Object>() {
            @Override public Object call() throws Exception {
                return accessFut.get();
            }
        }, TransactionTimeoutException.class);
    }

    @SuppressWarnings("unchecked")
    private boolean acquireLockForEntry(
        CacheEntry<Integer, Integer> entry, 
        long timeout
    ) throws IgniteCheckedException {
        return cache.unwrap(IgniteCacheProxy.class).internalProxy().lockTxEntry(entry, timeout);
    }   
}

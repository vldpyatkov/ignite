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

import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionTimeoutException;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Tests transactional entry locking through internal cache API.
 */
public class GridCacheAdapterLockTxEntryTest extends GridCommonAbstractTest {
    /** */
    private static final int KEY = 1;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLockTxEntryInPessimisticTransaction() throws Exception {
        IgniteCache<Integer, Integer> cache = startCache();
        IgniteInternalCache<Integer, Integer> internalCache = internalCache(0, DEFAULT_CACHE_NAME);
        CacheEntry<Integer, Integer> entry = cache.getEntry(KEY);

        try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            assertTrue(internalCache.lockTxEntry(entry, 0));

            assertEquals(entry.getValue(), cache.get(KEY));

            checkAccessInOtherTx(cache);

            tx.commit();
        }

        assertEquals(KEY, cache.get(KEY).intValue());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFailToLockTxEntryInPessimisticTransaction() throws Exception {
        IgniteCache<Integer, Integer> cache = startCache();

        IgniteInternalCache<Integer, Integer> internalCache = internalCache(0, DEFAULT_CACHE_NAME);
        CacheEntry<Integer, Integer> entry = cache.getEntry(KEY);

        cache.put(KEY, 2);

        try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            assertFalse(internalCache.lockTxEntry(entry, 0));

            assertEquals(entry.getValue(), cache.get(KEY));

            checkAccessInOtherTx(cache);

            tx.commit();
        }

        assertEquals(2, cache.get(KEY).intValue());
    }

    private void checkAccessInOtherTx(IgniteCache<Integer, Integer> cache) {
        IgniteInternalFuture<Void> writeFut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() {
                try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 100, 1)) {
                    cache.get(KEY);

                    tx.commit();
                }

                return null;
            }
        });

        GridTestUtils.assertThrowsWithCause(new Callable<Object>() {
            @Override public Object call() throws Exception {
                return writeFut.get();
            }
        }, TransactionTimeoutException.class);
    }

//    /**
//     * @throws Exception If failed.
//     */
//    @Test
//    public void testLockTxEntryAsyncInPessimisticTransaction() throws Exception {
//        IgniteCache<Integer, Integer> cache = startCache();
//        IgniteInternalCache<Integer, Integer> internalCache = internalCache(0, DEFAULT_CACHE_NAME);
//        CacheEntry<Integer, Integer> entry = cache.getEntry(KEY);
//
//        try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
//            IgniteInternalFuture<Boolean> fut = internalCache.lockTxEntryAsync(entry, 0);
//
//            assertTrue(fut.get());
//            assertTrue(internalCache.isLockedByThread(KEY));
//
//            tx.commit();
//        }
//    }
//
//    /**
//     * @throws Exception If failed.
//     */
//    @Test
//    public void testLockTxEntryFailsWithoutTransaction() throws Exception {
//        IgniteCache<Integer, Integer> cache = startCache();
//        IgniteInternalCache<Integer, Integer> internalCache = internalCache(0, DEFAULT_CACHE_NAME);
//        CacheEntry<Integer, Integer> entry = cache.getEntry(KEY);
//
//        GridTestUtils.assertThrows(log, new Callable<Object>() {
//            @Override public Object call() throws Exception {
//                internalCache.lockTxEntry(entry, 0);
//
//                return null;
//            }
//        }, IgniteCheckedException.class, "without transaction");
//    }
//
//    /**
//     * @throws Exception If failed.
//     */
//    @Test
//    public void testLockTxEntryAsyncFailsInOptimisticTransaction() throws Exception {
//        IgniteCache<Integer, Integer> cache = startCache();
//        IgniteInternalCache<Integer, Integer> internalCache = internalCache(0, DEFAULT_CACHE_NAME);
//        CacheEntry<Integer, Integer> entry = cache.getEntry(KEY);
//
//        try (Transaction tx = grid(0).transactions().txStart(OPTIMISTIC, REPEATABLE_READ)) {
//            GridTestUtils.assertThrows(log, new Callable<Object>() {
//                @Override public Object call() throws Exception {
//                    return internalCache.lockTxEntryAsync(entry, 0).get();
//                }
//            }, IgniteCheckedException.class, "optimistic transaction");
//        }
//    }

    /**
     * @return Started cache.
     * @throws Exception If failed.
     */
    private IgniteCache<Integer, Integer> startCache() throws Exception {
        startGrid(0);

        IgniteCache<Integer, Integer> cache = grid(0).createCache(new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setCacheMode(PARTITIONED));

        cache.put(KEY, KEY);

        return cache;
    }
}

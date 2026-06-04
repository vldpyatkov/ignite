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

import java.util.Random;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

public class EntryLockTest extends GridCommonAbstractTest {
    /**
     *
     */
    public boolean useNearCache = false;

    /**
     *
     */
    public int backups = 1;

    /**
     *
     */
    public boolean replicated = true;

    /**
     * {@inheritDoc}
     */
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
                .setAffinity(new RendezvousAffinityFunction(false, 32))
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setNearConfiguration(useNearCache ? new NearCacheConfiguration<>() : null)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setCacheMode(replicated ? CacheMode.REPLICATED : CacheMode.PARTITIONED)
                .setBackups(backups);

        return (IgniteCache<Integer, Integer>)ignite.createCache(ccfg);
    }

    /**
     * TODO: Delete it in a PR.
     * @throws Exception
     */
    @Test
    public void testStartCaheLockDestry() throws Exception {
        IgniteEx ignite0 = startGrids(3);

        for (int i = 0; i < 100; i++) {
            info("Iteration: " + i);

            IgniteCache<Integer, Integer> cache = transactionalCache(ignite0);

            int key = new Random().nextInt();

            cache.put(key, i);

            try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
                CacheEntry<Integer, Integer> entry = cache.getEntry(key);

                assertNotNull(entry);
                assertNotNull(entry.version());

                boolean locked = acquireLockForEntry(cache, entry, 0);

                assertTrue(locked);

                for (int j = 10_000; j < 10_100; j++)
                    cache.put(j, j);

                tx.commit();
            }

            cache.destroy();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testVersionedEntryLockFailsWithoutExplicitTransaction() throws Exception {
        IgniteEx ignite0 = startGrid(0);

        IgniteCache<Integer, Integer> cache = transactionalCache(ignite0);
        int key = new Random().nextInt();

        cache.put(key, 0);

        CacheEntry<Integer, Integer> entry = cache.getEntry(key);

        GridTestUtils.assertThrows(log,
            () -> acquireLockForEntry(cache, entry, 0),
            IgniteCheckedException.class,
            "active pessimistic transaction");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testVersionedEntryLockFailsInOptimisticTransaction() throws Exception {
        IgniteEx ignite0 = startGrid(0);

        IgniteCache<Integer, Integer> cache = transactionalCache(ignite0);
        int key = new Random().nextInt();

        cache.put(key, 0);

        CacheEntry<Integer, Integer> entry = cache.getEntry(key);

        try (Transaction tx = ignite0.transactions().txStart(OPTIMISTIC, READ_COMMITTED)) {
            GridTestUtils.assertThrows(log,
                () -> acquireLockForEntry(cache, entry, 0),
                IgniteCheckedException.class,
                "active pessimistic transaction");

            tx.rollback();
        }
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

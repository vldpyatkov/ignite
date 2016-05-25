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

package org.apache.ignite.internal.processors.cache.local;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.swapspace.file.FileSwapSpaceSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class CacheOffHeapMetricsTest extends GridCommonAbstractTest {
    /** Grid count. */
    private static final int GRID_CNT = 1;

    /** Keys count. */
    private static final int KEYS_CNT = 1000;

    /** Entry size. */
    private static final int ENTRY_SIZE = 86; // Calculated as allocated size divided on entries count.

    /** Offheap max count. */
    private static final int OFFHEAP_MAX_CNT = KEYS_CNT / 2;

    /** Offheap max size. */
    private static final int OFFHEAP_MAX_SIZE = ENTRY_SIZE * OFFHEAP_MAX_CNT;

    /** Cache. */
    private IgniteCache<Integer, Integer> cache;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setSwapSpaceSpi(new FileSwapSpaceSpi());

        return cfg;
    }

    /**
     * @param offHeapSize Max off-heap size.
     * @param swapEnabled Swap enabled.
     */
    private void createCache(int offHeapSize, boolean swapEnabled) {
        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setStatisticsEnabled(true);

        ccfg.setCacheMode(CacheMode.LOCAL);
        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg.setMemoryMode(CacheMemoryMode.OFFHEAP_TIERED);

        ccfg.setOffHeapMaxMemory(offHeapSize);
        ccfg.setSwapEnabled(swapEnabled);

        cache = grid(0).getOrCreateCache(ccfg);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        if (cache != null)
            cache.destroy();
    }

    /**
     * @throws Exception if failed.
     */
    public void testOffHeapMetrics() throws Exception {
        createCache(0, false);

        for (int i = 0; i < KEYS_CNT; i++)
            cache.put(i, i);

        printStat();

        assertEquals(cache.localMetrics().getCacheEvictions(), cache.localMetrics().getOffHeapPuts());
        assertEquals(KEYS_CNT, cache.localMetrics().getOffHeapGets());
        assertEquals(0, cache.localMetrics().getOffHeapHits());
        assertEquals(0f, cache.localMetrics().getOffHeapHitPercentage());
        assertEquals(KEYS_CNT, cache.localMetrics().getOffHeapMisses());
        assertEquals(100f, cache.localMetrics().getOffHeapMissPercentage());
        assertEquals(0, cache.localMetrics().getOffHeapRemovals());

        assertEquals(0, cache.localMetrics().getOffHeapEvictions());
        assertEquals(cache.localMetrics().getCacheEvictions(), cache.localMetrics().getOffHeapEntriesCount());
        assertEquals(cache.localMetrics().getCacheEvictions(), cache.localMetrics().getOffHeapPrimaryEntriesCount());
        assertEquals(0, cache.localMetrics().getOffHeapBackupEntriesCount());

        for (int i = 0; i < KEYS_CNT; i++)
            cache.get(i);

        printStat();

        assertEquals(cache.localMetrics().getCacheEvictions(), cache.localMetrics().getOffHeapGets());
        assertEquals(KEYS_CNT * 2, cache.localMetrics().getOffHeapGets());
        assertEquals(KEYS_CNT, cache.localMetrics().getOffHeapHits());
        assertEquals(100 * KEYS_CNT / (KEYS_CNT * 2.0), cache.localMetrics().getOffHeapHitPercentage(), 0.1);
        assertEquals(KEYS_CNT, cache.localMetrics().getOffHeapMisses());
        assertEquals(100 * KEYS_CNT / (KEYS_CNT * 2.0), cache.localMetrics().getOffHeapMissPercentage(), 0.1);
        assertEquals(0, cache.localMetrics().getOffHeapRemovals());

        assertEquals(0, cache.localMetrics().getOffHeapEvictions());
        assertEquals(KEYS_CNT, cache.localMetrics().getOffHeapEntriesCount());
        assertEquals(KEYS_CNT, cache.localMetrics().getOffHeapPrimaryEntriesCount());
        assertEquals(0, cache.localMetrics().getOffHeapBackupEntriesCount());

        for (int i = KEYS_CNT; i < KEYS_CNT * 2; i++)
            cache.get(i);

        printStat();

        assertEquals(cache.localMetrics().getCacheEvictions(), cache.localMetrics().getOffHeapGets());
        assertEquals(KEYS_CNT * 3, cache.localMetrics().getOffHeapGets());
        assertEquals(KEYS_CNT, cache.localMetrics().getOffHeapHits());
        assertEquals(100 / 3.0, cache.localMetrics().getOffHeapHitPercentage(), 0.1);
        assertEquals(KEYS_CNT * 2, cache.localMetrics().getOffHeapMisses());
        assertEquals(100 - (100 / 3.0), cache.localMetrics().getOffHeapMissPercentage(), 0.1);
        assertEquals(0, cache.localMetrics().getOffHeapRemovals());

        assertEquals(0, cache.localMetrics().getOffHeapEvictions());
        assertEquals(KEYS_CNT, cache.localMetrics().getOffHeapEntriesCount());
        assertEquals(KEYS_CNT, cache.localMetrics().getOffHeapPrimaryEntriesCount());
        assertEquals(0, cache.localMetrics().getOffHeapBackupEntriesCount());

        for (int i = 0; i < KEYS_CNT; i++)
            cache.remove(i);

        printStat();

        assertEquals(cache.localMetrics().getCacheEvictions(), cache.localMetrics().getOffHeapGets());
        assertEquals(KEYS_CNT * 4, cache.localMetrics().getOffHeapGets());
        assertEquals(KEYS_CNT * 2, cache.localMetrics().getOffHeapHits());
        assertEquals(100 * (KEYS_CNT * 2.0) / (KEYS_CNT * 4.0),
            cache.localMetrics().getOffHeapHitPercentage(), 0.1);
        assertEquals(KEYS_CNT * 2, cache.localMetrics().getOffHeapMisses());
        assertEquals(100 * KEYS_CNT * 2.0 / (KEYS_CNT * 4.0),
            cache.localMetrics().getOffHeapMissPercentage(), 0.1);
        assertEquals(KEYS_CNT, cache.localMetrics().getOffHeapRemovals());

        assertEquals(0, cache.localMetrics().getOffHeapEvictions());
        assertEquals(0, cache.localMetrics().getOffHeapEntriesCount());
        assertEquals(0, cache.localMetrics().getOffHeapPrimaryEntriesCount());
        assertEquals(0, cache.localMetrics().getOffHeapBackupEntriesCount());
    }

    /**
     * @throws Exception if failed.
     */
    public void testOffHeapAndSwapMetrics() throws Exception {
        createCache(OFFHEAP_MAX_SIZE, true);

        for (int i = 0; i < KEYS_CNT; i++)
            cache.put(i, i);

        printStat();

        assertEquals(cache.localMetrics().getCacheEvictions(), cache.localMetrics().getOffHeapGets());
        assertEquals(KEYS_CNT, cache.localMetrics().getOffHeapGets());
        assertEquals(0, cache.localMetrics().getOffHeapHits());
        assertEquals(0f, cache.localMetrics().getOffHeapHitPercentage());
        assertEquals(KEYS_CNT, cache.localMetrics().getOffHeapMisses());
        assertEquals(100f, cache.localMetrics().getOffHeapMissPercentage());
        assertEquals(0, cache.localMetrics().getOffHeapRemovals());

        assertEquals(KEYS_CNT - OFFHEAP_MAX_CNT, cache.localMetrics().getOffHeapEvictions());
        assertEquals(OFFHEAP_MAX_CNT, cache.localMetrics().getOffHeapEntriesCount());
        assertEquals(OFFHEAP_MAX_CNT, cache.localMetrics().getOffHeapPrimaryEntriesCount());
        assertEquals(0, cache.localMetrics().getOffHeapBackupEntriesCount());

        assertEquals(cache.localMetrics().getOffHeapEvictions(), cache.localMetrics().getSwapPuts());
        assertEquals(KEYS_CNT, cache.localMetrics().getSwapGets());
        assertEquals(0, cache.localMetrics().getSwapHits());
        assertEquals(0f, cache.localMetrics().getSwapHitPercentage());
        assertEquals(KEYS_CNT, cache.localMetrics().getSwapMisses());
        assertEquals(100f, cache.localMetrics().getSwapMissPercentage());
        assertEquals(0, cache.localMetrics().getSwapRemovals());

        assertEquals(cache.localMetrics().getOffHeapEvictions(), cache.localMetrics().getSwapEntriesCount());

        for (int i = 0; i < KEYS_CNT; i++)
            cache.get(i);

        printStat();

        assertEquals(cache.localMetrics().getCacheEvictions(), cache.localMetrics().getOffHeapPuts());
        assertEquals(KEYS_CNT * 2, cache.localMetrics().getOffHeapGets());
        assertEquals(0, cache.localMetrics().getOffHeapHits());
        assertEquals(0.0, cache.localMetrics().getOffHeapHitPercentage(), 0.1);
        assertEquals(KEYS_CNT * 2, cache.localMetrics().getOffHeapMisses());
        assertEquals(100.0, cache.localMetrics().getOffHeapMissPercentage(), 0.1);
        assertEquals(0, cache.localMetrics().getOffHeapRemovals());

        assertEquals(cache.localMetrics().getCacheEvictions() - OFFHEAP_MAX_CNT, cache.localMetrics().getOffHeapEvictions());
        assertEquals(OFFHEAP_MAX_CNT, cache.localMetrics().getOffHeapEntriesCount());
        assertEquals(OFFHEAP_MAX_CNT, cache.localMetrics().getOffHeapPrimaryEntriesCount());
        assertEquals(0, cache.localMetrics().getOffHeapBackupEntriesCount());

        assertEquals(cache.localMetrics().getOffHeapEvictions(), cache.localMetrics().getSwapPuts());
        assertEquals(KEYS_CNT * 2, cache.localMetrics().getSwapGets());
        assertEquals(KEYS_CNT, cache.localMetrics().getSwapHits());
        assertEquals(100 * KEYS_CNT / (KEYS_CNT * 2.0), cache.localMetrics().getSwapHitPercentage(), 0.1);
        assertEquals(KEYS_CNT, cache.localMetrics().getSwapMisses());
        assertEquals(100 * KEYS_CNT / (KEYS_CNT * 2.0), cache.localMetrics().getSwapMissPercentage(), 0.1);
        assertEquals(KEYS_CNT, cache.localMetrics().getSwapRemovals());

        assertEquals(KEYS_CNT - OFFHEAP_MAX_CNT, cache.localMetrics().getSwapEntriesCount());

        for (int i = KEYS_CNT; i < KEYS_CNT * 2; i++)
            cache.get(i);

        printStat();

        assertEquals(cache.localMetrics().getCacheEvictions(), cache.localMetrics().getOffHeapGets());
        assertEquals(KEYS_CNT * 3, cache.localMetrics().getOffHeapGets());
        assertEquals(0, cache.localMetrics().getOffHeapHits());
        assertEquals(0.0, cache.localMetrics().getOffHeapHitPercentage(), 0.1);
        assertEquals(KEYS_CNT * 3, cache.localMetrics().getOffHeapMisses());
        assertEquals(100.0, cache.localMetrics().getOffHeapMissPercentage(), 0.1);
        assertEquals(0, cache.localMetrics().getOffHeapRemovals());

        assertEquals(cache.localMetrics().getCacheEvictions() - OFFHEAP_MAX_CNT - KEYS_CNT,
            cache.localMetrics().getOffHeapEvictions());
        assertEquals(OFFHEAP_MAX_CNT, cache.localMetrics().getOffHeapEntriesCount());
        assertEquals(OFFHEAP_MAX_CNT, cache.localMetrics().getOffHeapPrimaryEntriesCount());
        assertEquals(0, cache.localMetrics().getOffHeapBackupEntriesCount());

        assertEquals(cache.localMetrics().getOffHeapEvictions(), cache.localMetrics().getSwapPuts());
        assertEquals(KEYS_CNT * 3, cache.localMetrics().getSwapGets());
        assertEquals(KEYS_CNT, cache.localMetrics().getSwapHits());
        assertEquals(100 / 3.0, cache.localMetrics().getSwapHitPercentage(), 0.1);
        assertEquals(KEYS_CNT * 2, cache.localMetrics().getSwapMisses());
        assertEquals(100 - (100 / 3.0), cache.localMetrics().getSwapMissPercentage(), 0.1);
        assertEquals(KEYS_CNT, cache.localMetrics().getSwapRemovals());

        assertEquals(KEYS_CNT - OFFHEAP_MAX_CNT, cache.localMetrics().getSwapEntriesCount());

        for (int i = 0; i < KEYS_CNT; i++)
            cache.remove(i);

        printStat();

        assertEquals(cache.localMetrics().getCacheEvictions(), cache.localMetrics().getOffHeapGets());
        assertEquals(KEYS_CNT * 4, cache.localMetrics().getOffHeapGets());
        assertEquals(OFFHEAP_MAX_CNT, cache.localMetrics().getOffHeapHits());
        assertEquals(100 * OFFHEAP_MAX_CNT / (KEYS_CNT * 4.0),
            cache.localMetrics().getOffHeapHitPercentage(), 0.1);
        assertEquals(KEYS_CNT * 4 - OFFHEAP_MAX_CNT, cache.localMetrics().getOffHeapMisses());
        assertEquals(100 * (KEYS_CNT * 4 - OFFHEAP_MAX_CNT) / (KEYS_CNT * 4.0),
            cache.localMetrics().getOffHeapMissPercentage(), 0.1);
        assertEquals(OFFHEAP_MAX_CNT, cache.localMetrics().getOffHeapRemovals());

        assertEquals(cache.localMetrics().getCacheEvictions() - OFFHEAP_MAX_CNT - 2 * KEYS_CNT, cache.localMetrics().getOffHeapEvictions());
        assertEquals(0, cache.localMetrics().getOffHeapEntriesCount());
        assertEquals(0, cache.localMetrics().getOffHeapPrimaryEntriesCount());
        assertEquals(0, cache.localMetrics().getOffHeapBackupEntriesCount());

        assertEquals(cache.localMetrics().getOffHeapEvictions(), cache.localMetrics().getSwapPuts());
        assertEquals(KEYS_CNT * 4 - OFFHEAP_MAX_CNT, cache.localMetrics().getSwapGets());
        assertEquals(KEYS_CNT * 2 - OFFHEAP_MAX_CNT, cache.localMetrics().getSwapHits());
        assertEquals(100 * (KEYS_CNT * 2.0 - OFFHEAP_MAX_CNT) / (KEYS_CNT * 4.0 - OFFHEAP_MAX_CNT),
            cache.localMetrics().getSwapHitPercentage(), 0.1);
        assertEquals(KEYS_CNT * 2, cache.localMetrics().getSwapMisses());
        assertEquals(100 * KEYS_CNT * 2.0 / (KEYS_CNT * 4.0 - OFFHEAP_MAX_CNT),
            cache.localMetrics().getSwapMissPercentage(), 0.1);
        assertEquals(KEYS_CNT * 2 - OFFHEAP_MAX_CNT, cache.localMetrics().getSwapRemovals());

        assertEquals(0, cache.localMetrics().getSwapEntriesCount());
    }

    /**
     * Prints stats.
     */
    protected void printStat() {
        System.out.println("!!! -------------------------------------------------------");
        System.out.println("!!! Puts: cache = " + cache.localMetrics().getCachePuts() +
            ", offheap = " + cache.localMetrics().getOffHeapPuts() +
            ", swap = " + cache.localMetrics().getSwapPuts());
        System.out.println("!!! Gets: cache = " + cache.localMetrics().getCacheGets() +
            ", offheap = " + cache.localMetrics().getOffHeapGets() +
            ", swap = " + cache.localMetrics().getSwapGets());
        System.out.println("!!! Removes: cache = " + cache.localMetrics().getCacheRemovals() +
            ", offheap = " + cache.localMetrics().getOffHeapRemovals() +
            ", swap = " + cache.localMetrics().getSwapRemovals());
        System.out.println("!!! Evictions: cache = " + cache.localMetrics().getCacheEvictions() +
            ", offheap = " + cache.localMetrics().getOffHeapEvictions() +
            ", swap = none" );
        System.out.println("!!! Hits: cache = " + cache.localMetrics().getCacheHits() +
            ", offheap = " + cache.localMetrics().getOffHeapHits() +
            ", swap = " + cache.localMetrics().getSwapHits());
        System.out.println("!!! Hit(%): cache = " + cache.localMetrics().getCacheHitPercentage() +
            ", offheap = " + cache.localMetrics().getOffHeapHitPercentage() +
            ", swap = " + cache.localMetrics().getSwapHitPercentage());
        System.out.println("!!! Misses: cache = " + cache.localMetrics().getCacheMisses() +
            ", offheap = " + cache.localMetrics().getOffHeapMisses() +
            ", swap = " + cache.localMetrics().getSwapMisses());
        System.out.println("!!! Miss(%): cache = " + cache.localMetrics().getCacheMissPercentage() +
            ", offheap = " + cache.localMetrics().getOffHeapMissPercentage() +
            ", swap = " + cache.localMetrics().getSwapMissPercentage());
        System.out.println("!!! Entries: cache = " + cache.localMetrics().getSize() +
            ", offheap = " + cache.localMetrics().getOffHeapEntriesCount() +
            ", swap = " + cache.localMetrics().getSwapEntriesCount());
        System.out.println("!!! Size: cache = none" +
            ", offheap = " + cache.localMetrics().getOffHeapAllocatedSize() +
            ", swap = " + cache.localMetrics().getSwapSize());
        System.out.println();
    }

}

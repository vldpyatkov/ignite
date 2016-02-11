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

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.PRIMARY;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_TIERED;
import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_VALUES;
import static org.apache.ignite.cache.CacheMemoryMode.ONHEAP_TIERED;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class CacheContinuousQueryRandomOperationsTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int NODES = 5;

    /** */
    private static final int KEYS = 10;

    /** */
    private static final int VALS = 10;

    /** */
    public static final int ITERATION_CNT = 1000;

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        cfg.setClientMode(client);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(NODES - 1);

        client = true;

        startGrid(NODES - 1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicClient() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            ATOMIC,
            ONHEAP_TIERED,
            false);

        testContinuousQuery(ccfg, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomic() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            ATOMIC,
            ONHEAP_TIERED,
            false);

        testContinuousQuery(ccfg, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicAllNodes() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            ATOMIC,
            ONHEAP_TIERED,
            false);

        testContinuousQuery(ccfg, null, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicReplicated() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(REPLICATED,
            0,
            ATOMIC,
            ONHEAP_TIERED,
            false);

        testContinuousQuery(ccfg, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicReplicatedAllNodes() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(REPLICATED,
            0,
            ATOMIC,
            ONHEAP_TIERED,
            false);

        testContinuousQuery(ccfg, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicReplicatedClient() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(REPLICATED,
            0,
            ATOMIC,
            ONHEAP_TIERED,
            false);

        testContinuousQuery(ccfg, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicOffheapValues() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            ATOMIC,
            OFFHEAP_VALUES,
            false);

        testContinuousQuery(ccfg, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicOffheapValuesAllNodes() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            ATOMIC,
            OFFHEAP_VALUES,
            false);

        testContinuousQuery(ccfg, null, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicOffheapValuesClient() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            ATOMIC,
            OFFHEAP_VALUES,
            false);

        testContinuousQuery(ccfg, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicOffheapTiered() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            ATOMIC,
            OFFHEAP_TIERED,
            false);

        testContinuousQuery(ccfg, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicOffheapTieredAllNodes() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            ATOMIC,
            OFFHEAP_TIERED,
            false);

        testContinuousQuery(ccfg, null, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicOffheapTieredClient() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            ATOMIC,
            OFFHEAP_TIERED,
            false);

        testContinuousQuery(ccfg, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicNoBackups() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            0,
            ATOMIC,
            ONHEAP_TIERED,
            false);

        testContinuousQuery(ccfg, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicNoBackupsAllNodes() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            0,
            ATOMIC,
            ONHEAP_TIERED,
            false);

        testContinuousQuery(ccfg, null, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicNoBackupsClient() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            0,
            ATOMIC,
            ONHEAP_TIERED,
            false);

        testContinuousQuery(ccfg, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTx() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            TRANSACTIONAL,
            ONHEAP_TIERED,
            false);

        testContinuousQuery(ccfg, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxAllNodes() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            TRANSACTIONAL,
            ONHEAP_TIERED,
            false);

        testContinuousQuery(ccfg, null, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxExplicit() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            TRANSACTIONAL,
            ONHEAP_TIERED,
            false);

        testContinuousQuery(ccfg, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxClient() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            TRANSACTIONAL,
            ONHEAP_TIERED,
            false);

        testContinuousQuery(ccfg, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxClientExplicit() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            TRANSACTIONAL,
            ONHEAP_TIERED,
            false);

        testContinuousQuery(ccfg, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxReplicated() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(REPLICATED,
            0,
            TRANSACTIONAL,
            ONHEAP_TIERED,
            false);

        testContinuousQuery(ccfg, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxReplicatedClient() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(REPLICATED,
            0,
            TRANSACTIONAL,
            ONHEAP_TIERED,
            false);

        testContinuousQuery(ccfg, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxOffheapValues() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            TRANSACTIONAL,
            OFFHEAP_VALUES,
            false);

        testContinuousQuery(ccfg, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxOffheapValuesAllNodes() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            TRANSACTIONAL,
            OFFHEAP_VALUES,
            false);

        testContinuousQuery(ccfg, null, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxOffheapValuesExplicit() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            TRANSACTIONAL,
            OFFHEAP_VALUES,
            false);

        testContinuousQuery(ccfg, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxOffheapValuesClient() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            TRANSACTIONAL,
            OFFHEAP_VALUES,
            false);

        testContinuousQuery(ccfg, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxOffheapTiered() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            TRANSACTIONAL,
            OFFHEAP_TIERED,
            false);

        testContinuousQuery(ccfg, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxOffheapTieredAllNodes() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            TRANSACTIONAL,
            OFFHEAP_TIERED,
            false);

        testContinuousQuery(ccfg, null, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxOffheapTieredClient() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            TRANSACTIONAL,
            OFFHEAP_TIERED,
            false);

        testContinuousQuery(ccfg, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxOffheapTieredClientExplicit() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            TRANSACTIONAL,
            OFFHEAP_TIERED,
            false);

        testContinuousQuery(ccfg, true, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxNoBackups() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            0,
            TRANSACTIONAL,
            ONHEAP_TIERED,
            false);

        testContinuousQuery(ccfg, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxNoBackupsAllNodes() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            0,
            TRANSACTIONAL,
            ONHEAP_TIERED,
            false);

        testContinuousQuery(ccfg, null, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxNoBackupsExplicit() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            0,
            TRANSACTIONAL,
            ONHEAP_TIERED,
            false);

        testContinuousQuery(ccfg, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxNoBackupsClient() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            0,
            TRANSACTIONAL,
            ONHEAP_TIERED,
            false);

        testContinuousQuery(ccfg, true, false);
    }

    /**
     * @param ccfg Cache configuration.
     * @param client Client. If {@code null} then listener will be registered on all nodes.
     * @param expTx Explicit tx.
     * @throws Exception If failed.
     */
    private void testContinuousQuery(CacheConfiguration<Object, Object> ccfg, Boolean client, boolean expTx)
        throws Exception {
        ignite(0).createCache(ccfg);

        try {
            IgniteCache<Object, Object> cache = null;

            if (client != null) {
                if (client)
                    cache = ignite(NODES - 1).cache(ccfg.getName());
                else
                    cache = ignite(0).cache(ccfg.getName());
            }

            long seed = System.currentTimeMillis();

            Random rnd = new Random(seed);

            log.info("Random seed: " + seed);

            final BlockingQueue<CacheEntryEvent<?, ?>> evtsQueue =
                new ArrayBlockingQueue<>(50_000);

            Collection<QueryCursor<?>> curs = new ArrayList<>();

            if (cache != null) {
                ContinuousQuery<Object, Object> qry = new ContinuousQuery<>();

                qry.setLocalListener(new CacheEntryUpdatedListener<Object, Object>() {
                    @Override public void onUpdated(Iterable<CacheEntryEvent<?, ?>> evts) {
                        for (CacheEntryEvent<?, ?> evt : evts)
                            evtsQueue.add(evt);
                    }
                });

                QueryCursor<?> cur = cache.query(qry);

                curs.add(cur);
            }
            else {
                for (int i = 0; i < NODES - 1; i++) {
                    ContinuousQuery<Object, Object> qry = new ContinuousQuery<>();

                    qry.setLocalListener(new CacheEntryUpdatedListener<Object, Object>() {
                        @Override public void onUpdated(Iterable<CacheEntryEvent<?, ?>> evts) {
                            for (CacheEntryEvent<?, ?> evt : evts)
                                evtsQueue.add(evt);
                        }
                    });

                    QueryCursor<?> cur = ignite(i).cache(ccfg.getName()).query(qry);

                    curs.add(cur);
                }

                cache = ignite(ThreadLocalRandom.current().nextInt(NODES - 1)).cache(ccfg.getName());
            }

            ConcurrentMap<Object, Object> expData = new ConcurrentHashMap<>();

            Map<Integer, Long> partCntr = new ConcurrentHashMap<>();

            try {
                for (int i = 0; i < ITERATION_CNT; i++) {
                    if (i % 100 == 0)
                        log.info("Iteration: " + i);

                    randomUpdate(rnd, evtsQueue, expData, partCntr, cache, expTx, curs.size());
                }
            }
            finally {
                for (QueryCursor<?> cur : curs)
                    cur.close();
            }
        }
        finally {
            ignite(0).destroyCache(ccfg.getName());
        }
    }

    /**
     * @param rnd Random generator.
     * @param evtsQueue Events queue.
     * @param expData Expected cache data.
     * @param partCntr Partition counter.
     * @param cache Cache.
     * @param expTx Explicit TX.
     * @param qryCnt Query count.
     * @throws Exception If failed.
     */
    private void randomUpdate(
        Random rnd,
        BlockingQueue<CacheEntryEvent<?, ?>> evtsQueue,
        ConcurrentMap<Object, Object> expData,
        Map<Integer, Long> partCntr,
        IgniteCache<Object, Object> cache,
        boolean expTx,
        int qryCnt)
        throws Exception {
        Object key = new QueryTestKey(rnd.nextInt(KEYS));
        Object newVal = value(rnd);
        Object oldVal = expData.get(key);

        int op = rnd.nextInt(11);

        Ignite ignite = cache.unwrap(Ignite.class);

        Transaction tx = null;

        if (expTx && cache.getConfiguration(CacheConfiguration.class).getAtomicityMode() == TRANSACTIONAL)
            tx = ignite.transactions().txStart();

        try {
            // log.info("Random operation [key=" + key + ", op=" + op + ']');

            switch (op) {
                case 0: {
                    cache.put(key, newVal);

                    if (tx != null)
                        tx.commit();

                    updatePartitionCounter(cache, key, partCntr);

                    waitAndCheckEvent(evtsQueue, partCntr, affinity(cache), key, newVal, oldVal, qryCnt);

                    expData.put(key, newVal);

                    break;
                }

                case 1: {
                    cache.getAndPut(key, newVal);

                    if (tx != null)
                        tx.commit();

                    updatePartitionCounter(cache, key, partCntr);

                    waitAndCheckEvent(evtsQueue, partCntr, affinity(cache), key, newVal, oldVal, qryCnt);

                    expData.put(key, newVal);

                    break;
                }

                case 2: {
                    cache.remove(key);

                    if (tx != null)
                        tx.commit();

                    updatePartitionCounter(cache, key, partCntr);

                    waitAndCheckEvent(evtsQueue, partCntr, affinity(cache), key, null, oldVal, qryCnt);

                    expData.remove(key);

                    break;
                }

                case 3: {
                    cache.getAndRemove(key);

                    if (tx != null)
                        tx.commit();

                    updatePartitionCounter(cache, key, partCntr);

                    waitAndCheckEvent(evtsQueue, partCntr, affinity(cache), key, null, oldVal, qryCnt);

                    expData.remove(key);

                    break;
                }

                case 4: {
                    cache.invoke(key, new EntrySetValueProcessor(newVal, rnd.nextBoolean()));

                    if (tx != null)
                        tx.commit();

                    updatePartitionCounter(cache, key, partCntr);

                    waitAndCheckEvent(evtsQueue, partCntr, affinity(cache), key, newVal, oldVal, qryCnt);

                    expData.put(key, newVal);

                    break;
                }

                case 5: {
                    cache.invoke(key, new EntrySetValueProcessor(null, rnd.nextBoolean()));

                    if (tx != null)
                        tx.commit();

                    updatePartitionCounter(cache, key, partCntr);

                    waitAndCheckEvent(evtsQueue, partCntr, affinity(cache), key, null, oldVal, qryCnt);

                    expData.remove(key);

                    break;
                }

                case 6: {
                    cache.putIfAbsent(key, newVal);

                    if (tx != null)
                        tx.commit();

                    if (oldVal == null) {
                        updatePartitionCounter(cache, key, partCntr);

                        waitAndCheckEvent(evtsQueue, partCntr, affinity(cache), key, newVal, null, qryCnt);

                        expData.put(key, newVal);
                    }
                    else
                        checkNoEvent(evtsQueue);

                    break;
                }

                case 7: {
                    cache.getAndPutIfAbsent(key, newVal);

                    if (tx != null)
                        tx.commit();

                    if (oldVal == null) {
                        updatePartitionCounter(cache, key, partCntr);

                        waitAndCheckEvent(evtsQueue, partCntr, affinity(cache), key, newVal, null, qryCnt);

                        expData.put(key, newVal);
                    }
                    else
                        checkNoEvent(evtsQueue);

                    break;
                }

                case 8: {
                    cache.replace(key, newVal);

                    if (tx != null)
                        tx.commit();

                    if (oldVal != null) {
                        updatePartitionCounter(cache, key, partCntr);

                        waitAndCheckEvent(evtsQueue, partCntr, affinity(cache), key, newVal, oldVal, qryCnt);

                        expData.put(key, newVal);
                    }
                    else
                        checkNoEvent(evtsQueue);

                    break;
                }

                case 9: {
                    cache.getAndReplace(key, newVal);

                    if (tx != null)
                        tx.commit();

                    if (oldVal != null) {
                        updatePartitionCounter(cache, key, partCntr);

                        waitAndCheckEvent(evtsQueue, partCntr, affinity(cache), key, newVal, oldVal, qryCnt);

                        expData.put(key, newVal);
                    }
                    else
                        checkNoEvent(evtsQueue);

                    break;
                }

                case 10: {
                    if (oldVal != null) {
                        Object replaceVal = value(rnd);

                        boolean success = replaceVal.equals(oldVal);

                        if (success) {
                            cache.replace(key, replaceVal, newVal);

                            if (tx != null)
                                tx.commit();

                            updatePartitionCounter(cache, key, partCntr);

                            waitAndCheckEvent(evtsQueue, partCntr, affinity(cache), key, newVal, oldVal, qryCnt);

                            expData.put(key, newVal);
                        }
                        else {
                            cache.replace(key, replaceVal, newVal);

                            if (tx != null)
                                tx.commit();

                            checkNoEvent(evtsQueue);
                        }
                    }
                    else {
                        cache.replace(key, value(rnd), newVal);

                        if (tx != null)
                            tx.commit();

                        checkNoEvent(evtsQueue);
                    }

                    break;
                }

                default:
                    fail();
            }
        } finally {
            if (tx != null)
                tx.close();
        }
    }

    /**
     * @param cache Cache.
     * @param key Key
     * @param cntrs Partition counters.
     */
    private void updatePartitionCounter(IgniteCache<Object, Object> cache, Object key, Map<Integer, Long> cntrs) {
        Affinity<Object> aff = cache.unwrap(Ignite.class).affinity(cache.getName());

        int part = aff.partition(key);

        Long partCntr = cntrs.get(part);

        if (partCntr == null)
            partCntr = 0L;

        cntrs.put(part, ++partCntr);
    }

    /**
     * @param rnd Random generator.
     * @return Cache value.
     */
    private static Object value(Random rnd) {
        return new QueryTestValue(rnd.nextInt(VALS));
    }

    /**
     * @param evtsQueue Event queue.
     * @param partCntrs Partition counters.
     * @param aff Affinity function.
     * @param key Key.
     * @param val Value.
     * @param oldVal Old value.
     * @param qryCnt Query count.
     * @throws Exception If failed.
     */
    private void waitAndCheckEvent(BlockingQueue<CacheEntryEvent<?, ?>> evtsQueue,
        Map<Integer, Long> partCntrs,
        Affinity<Object> aff,
        Object key,
        Object val,
        Object oldVal, int qryCnt) throws Exception {
        if (val == null && oldVal == null) {
            checkNoEvent(evtsQueue);

            return;
        }

        for (int i = 0; i < qryCnt; i++) {
            CacheEntryEvent<?, ?> evt = evtsQueue.poll(5, SECONDS);

            assertNotNull("Failed to wait for event [key=" + key + ", val=" + val + ", oldVal=" + oldVal + ']', evt);
            assertEquals(key, evt.getKey());
            assertEquals(val, evt.getValue());
            assertEquals(oldVal, evt.getOldValue());

            Long cntr = partCntrs.get(aff.partition(key));

            assertNotNull(cntr);
            assertEquals(cntr, evt.unwrap(Long.class));
        }
    }

    /**
     * @param evtsQueue Event queue.
     * @throws Exception If failed.
     */
    private void checkNoEvent(BlockingQueue<CacheEntryEvent<?, ?>> evtsQueue) throws Exception {
        CacheEntryEvent<?, ?> evt = evtsQueue.poll(50, MILLISECONDS);

        assertNull(evt);
    }

    /**
     *
     * @param cacheMode Cache mode.
     * @param backups Number of backups.
     * @param atomicityMode Cache atomicity mode.
     * @param memoryMode Cache memory mode.
     * @param store If {@code true} configures dummy cache store.
     * @return Cache configuration.
     */
    private CacheConfiguration<Object, Object> cacheConfiguration(
        CacheMode cacheMode,
        int backups,
        CacheAtomicityMode atomicityMode,
        CacheMemoryMode memoryMode,
        boolean store) {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>();

        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setCacheMode(cacheMode);
        ccfg.setMemoryMode(memoryMode);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setAtomicWriteOrderMode(PRIMARY);

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(backups);

        if (store) {
            ccfg.setCacheStoreFactory(new TestStoreFactory());
            ccfg.setReadThrough(true);
            ccfg.setWriteThrough(true);
        }

        return ccfg;
    }

    /**
     *
     */
    private static class TestStoreFactory implements Factory<CacheStore<Object, Object>> {
        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public CacheStore<Object, Object> create() {
            return new CacheStoreAdapter() {
                @Override public Object load(Object key) throws CacheLoaderException {
                    return null;
                }

                @Override public void write(Cache.Entry entry) throws CacheWriterException {
                    // No-op.
                }

                @Override public void delete(Object key) throws CacheWriterException {
                    // No-op.
                }
            };
        }
    }

    /**
     *
     */
    static class QueryTestKey implements Serializable {
        /** */
        private final Integer key;

        /**
         * @param key Key.
         */
        public QueryTestKey(Integer key) {
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            QueryTestKey that = (QueryTestKey)o;

            return key.equals(that.key);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return key.hashCode();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(QueryTestKey.class, this);
        }
    }

    /**
     *
     */
    static class QueryTestValue implements Serializable {
        /** */
        private final Integer val1;

        /** */
        private final String val2;

        /**
         * @param val Value.
         */
        public QueryTestValue(Integer val) {
            this.val1 = val;
            this.val2 = String.valueOf(val);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            QueryTestValue that = (QueryTestValue) o;

            return val1.equals(that.val1) && val2.equals(that.val2);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = val1.hashCode();

            res = 31 * res + val2.hashCode();

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(QueryTestValue.class, this);
        }
    }

    /**
     *
     */
    protected static class EntrySetValueProcessor implements EntryProcessor<Object, Object, Object> {
        /** */
        private Object val;

        /** */
        private boolean retOld;

        /**
         * @param val Value to set.
         * @param retOld Return old value flag.
         */
        public EntrySetValueProcessor(Object val, boolean retOld) {
            this.val = val;
            this.retOld = retOld;
        }

        /** {@inheritDoc} */
        @Override public Object process(MutableEntry<Object, Object> e, Object... args) {
            Object old = retOld ? e.getValue() : null;

            if (val != null)
                e.setValue(val);
            else
                e.remove();

            return old;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(EntrySetValueProcessor.class, this);
        }
    }
}

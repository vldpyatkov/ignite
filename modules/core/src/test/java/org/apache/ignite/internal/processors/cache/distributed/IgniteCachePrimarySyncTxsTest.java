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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_ASYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;

/**
 *
 */
public class IgniteCachePrimarySyncTxsTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int SRVS = 4;

    /** */
    private static final int NODES = SRVS + 1;

    /** */
    private boolean clientMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        cfg.setClientMode(clientMode);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(SRVS);

        clientMode = true;

        Ignite client = startGrid(SRVS);

        assertTrue(client.configuration().isClientMode());
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxSyncMode() throws Exception {
        Ignite ignite = ignite(0);

        List<IgniteCache<Object, Object>> caches = new ArrayList<>();

        try {
            caches.add(ignite.createCache(cacheConfiguration("fullSync1", FULL_SYNC, 1)));
            caches.add(ignite.createCache(cacheConfiguration("fullSync2", FULL_SYNC, 1)));
            caches.add(ignite.createCache(cacheConfiguration("fullAsync1", FULL_ASYNC, 1)));
            caches.add(ignite.createCache(cacheConfiguration("fullAsync2", FULL_ASYNC, 1)));
            caches.add(ignite.createCache(cacheConfiguration("primarySync1", PRIMARY_SYNC, 1)));
            caches.add(ignite.createCache(cacheConfiguration("primarySync2", PRIMARY_SYNC, 1)));

            for (int i = 0; i < NODES; i++)
                checkTxSyncMode(ignite(i));
        }
        finally {
            for (IgniteCache<Object, Object> cache : caches)
                ignite.destroyCache(cache.getName());
        }
    }

    private void checkTxSyncMode(Ignite ignite) {

    }

    /**
     * @param name Cache name.
     * @param syncMode Write synchronization mode.
     * @param backups Number of backups.
     * @return Cache configuration.
     */
    private CacheConfiguration<Object, Object> cacheConfiguration(String name,
        CacheWriteSynchronizationMode syncMode,
        int backups) {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>();

        ccfg.setName(name);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(syncMode);
        ccfg.setBackups(backups);

        return ccfg;
    }

    public void testPrimarySyncMessages() throws Exception {

    }
}

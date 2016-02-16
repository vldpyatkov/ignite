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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * TODO IGNITE-1232.
 */
public class IgniteCacheJoinQueryTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int NODES = 1;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        CacheKeyConfiguration keyCfg = new CacheKeyConfiguration();

        keyCfg.setTypeName(TestKey1.class.getName());
        keyCfg.setAffinityKeyFieldName("affKey");

        cfg.setCacheKeyConfiguration(keyCfg);

        cfg.setMarshaller(null);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(NODES);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinityKey() throws Exception {
        CacheConfiguration ccfg = new CacheConfiguration();

        QueryEntity qryEntity = new QueryEntity();
        qryEntity.setKeyType(TestKey1.class.getName());
        qryEntity.setValueType(Integer.class.getName());

        ccfg.setQueryEntities(F.asList(qryEntity));

        Ignite ignite = ignite(0);

        IgniteCache cache = ignite.createCache(ccfg);

        try {

        }
        finally {
            ignite.destroyCache(ccfg.getName());
        }
    }

    /**
     *
     */
    public static class TestKey1 {
        /** */
        private int key;

        /** */
        private int affKey;

        /**
         * @param key Key.
         */
        public TestKey1(int key) {
            this.key = key;

            affKey = key + 1;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestKey1 testKey1 = (TestKey1)o;

            return key == testKey1.key;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return key;
        }
    }
}

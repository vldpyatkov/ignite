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

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
@SuppressWarnings("unchecked")
public class IgniteCacheJoinQueryTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int NODES = 4;

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        CacheKeyConfiguration keyCfg = new CacheKeyConfiguration();

        keyCfg.setTypeName(PersonKeyWithAffinity.class.getName());
        keyCfg.setAffinityKeyFieldName("affKey");

        cfg.setCacheKeyConfiguration(keyCfg);

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
    public void testAffinityKeyNotQueryField() throws Exception {
        CacheConfiguration ccfg = cacheConfiguration(PARTITIONED, 1, true, false);

        IgniteCache cache = ignite(0).createCache(ccfg);

        try {
            putData(cache, true);

            for (int i = 0; i < NODES; i++) {
                final IgniteCache cache0 = ignite(i).cache(ccfg.getName());

                final String QRY = "select o.name, p.name " +
                    "from Organization o, Person p " +
                    "where p.orgId = o._key";

                SqlFieldsQuery qry = new SqlFieldsQuery(QRY);

                cache0.query(qry).getAll();

                GridTestUtils.assertThrows(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        SqlFieldsQuery qry = new SqlFieldsQuery(QRY);

                        qry.setDistributedJoins(true);

                        cache0.query(qry).getAll();

                        return null;
                    }
                }, CacheException.class, null);

                cache0.query(qry).getAll();
            }
        }
        finally {
            ignite(0).destroyCache(ccfg.getName());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testJoinQuery() throws Exception {
        testJoinQuery(PARTITIONED, 0, false);

        testJoinQuery(PARTITIONED, 1, false);

        testJoinQuery(REPLICATED, 0, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testJoinQueryWithAffinityKey() throws Exception {
        testJoinQuery(PARTITIONED, 0, true);

        testJoinQuery(PARTITIONED, 1, true);

        testJoinQuery(REPLICATED, 0, true);
    }

    /**
     * @param cacheMode Cache mode.
     * @param backups Number of backups.
     * @param affKey If {@code true} uses key with affinity key field.
     */
    public void testJoinQuery(CacheMode cacheMode, int backups, boolean affKey) {
        CacheConfiguration ccfg = cacheConfiguration(cacheMode, backups, affKey, affKey);

        IgniteCache cache = ignite(0).createCache(ccfg);

        try {
            Map<Integer, Integer> cnts = putData(cache, affKey);

            for (int i = 0; i < NODES; i++)
                checkJoin(ignite(i).cache(ccfg.getName()), cnts);
        }
        finally {
            ignite(0).destroyCache(ccfg.getName());
        }
    }

    /**
     * @param cache Cache.
     * @param personCnt Count of persons per organization.
     */
    private void checkJoin(IgniteCache cache, Map<Integer, Integer> personCnt) {
        SqlFieldsQuery qry = new SqlFieldsQuery("select o.name, p.name " +
            "from Organization o, Person p " +
            "where p.orgId = o._key and o._key=?");

        qry.setDistributedJoins(true);

        for (int i = 0; i < personCnt.size(); i++) {
            qry.setArgs(i);

            List<List<Object>> res = cache.query(qry).getAll();

            assertEquals((int)personCnt.get(i), res.size());
        }
    }

    /**
     * @param cacheMode Cache mode.
     * @param backups Number of backups.
     * @param affKey If {@code true} uses key with affinity key field.
     * @param includeAffKey If {@code true} includes affinity key field in query fields.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(CacheMode cacheMode,
        int backups,
        boolean affKey,
        boolean includeAffKey) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setCacheMode(cacheMode);

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(backups);

        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        QueryEntity person = new QueryEntity();
        person.setKeyType(affKey ? PersonKeyWithAffinity.class.getName() : PersonKey.class.getName());
        person.setValueType(Person.class.getName());
        person.addQueryField("orgId", Integer.class.getName(), null);
        person.addQueryField("id", Integer.class.getName(), null);
        person.addQueryField("name", String.class.getName(), null);

        if (includeAffKey)
            person.addQueryField("affKey", Integer.class.getName(), null);

        QueryEntity org = new QueryEntity();
        org.setKeyType(Integer.class.getName());
        org.setValueType(Organization.class.getName());
        org.addQueryField("name", String.class.getName(), null);

        ccfg.setQueryEntities(F.asList(person, org));

        return ccfg;
    }

    /**
     * @param cache Cache.
     * @param affKey If {@code true} uses key with affinity key field.
     * @return Count of persons per organization.
     */
    private Map<Integer, Integer> putData(IgniteCache cache, boolean affKey) {
        Map<Integer, Integer> personCnt = new HashMap<>();

        final int ORG_CNT = 10;

        for (int i = 0; i < ORG_CNT; i++)
            cache.put(i, new Organization("org-" + i));

        Set<Integer> ids = new HashSet<>();

        for (int i = 0; i < ORG_CNT; i++) {
            int cnt = ThreadLocalRandom.current().nextInt(100);

            for (int j = 0; j < cnt; j++) {
                int personId = ThreadLocalRandom.current().nextInt();

                while (!ids.add(personId))
                    personId = ThreadLocalRandom.current().nextInt();

                Object key = affKey ? new PersonKeyWithAffinity(personId) : new PersonKey(personId);

                String name = "person-" + personId;

                cache.put(key, new Person(i, name));

                assertEquals(name, ((Person)cache.get(key)).name);
            }

            personCnt.put(i, cnt);
        }

        return personCnt;
    }

    /**
     *
     */
    public static class PersonKeyWithAffinity {
        /** */
        private int id;

        /** */
        private int affKey;

        /**
         * @param id Key.
         */
        public PersonKeyWithAffinity(int id) {
            this.id = id;

            affKey = id + 1;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            PersonKeyWithAffinity other = (PersonKeyWithAffinity)o;

            return id == other.id;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
        }
    }

    /**
     *
     */
    public static class PersonKey {
        /** */
        private int id;

        /**
         * @param id Key.
         */
        public PersonKey(int id) {
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            PersonKey other = (PersonKey)o;

            return id == other.id;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
        }
    }

    /**
     *
     */
    private static class Person implements Serializable {
        /** */
        @QuerySqlField
        int orgId;

        /** */
        @QuerySqlField
        String name;

        /**
         * @param orgId Organization ID.
         * @param name Name.
         */
        public Person(int orgId, String name) {
            this.orgId = orgId;
            this.name = name;
        }
    }

    /**
     *
     */
    private static class Organization implements Serializable {
        /** */
        @QuerySqlField
        String name;

        /**
         * @param name Name.
         */
        public Organization(String name) {
            this.name = name;
        }
    }
}

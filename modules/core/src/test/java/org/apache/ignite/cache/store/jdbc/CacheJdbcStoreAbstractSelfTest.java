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

package org.apache.ignite.cache.store.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.store.jdbc.dialect.H2Dialect;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.h2.jdbcx.JdbcConnectionPool;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Class for {@code PojoCacheStore} tests.
 */
public abstract class CacheJdbcStoreAbstractSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    protected static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** DB connection URL. */
    protected static final String DFLT_CONN_URL = "jdbc:h2:mem:TestDatabase;DB_CLOSE_DELAY=-1";

    /** Organization count. */
    protected static final int ORGANIZATION_CNT = 1000;

    /** Person count. */
    protected static final int PERSON_CNT = 100000;

    /** Flag indicating that tests should use primitive classes like java.lang.Integer for keys. */
    protected static boolean primitiveKeys = false;

    /**
     * @return Connection to test in-memory H2 database.
     * @throws SQLException
     */
    protected Connection getConnection() throws SQLException {
        return DriverManager.getConnection(DFLT_CONN_URL, "sa", "");
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        Connection conn = getConnection();

        Statement stmt = conn.createStatement();

        stmt.executeUpdate("DROP TABLE IF EXISTS Organization");
        stmt.executeUpdate("DROP TABLE IF EXISTS Person");

        stmt.executeUpdate("CREATE TABLE Organization (id integer PRIMARY KEY, name varchar(50), city varchar(50))");
        stmt.executeUpdate("CREATE TABLE Person (id integer PRIMARY KEY, org_id integer, name varchar(50))");

        conn.commit();

        U.closeQuiet(stmt);

        fillSampleDatabase(conn);

        U.closeQuiet(conn);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setCacheConfiguration(cacheConfiguration());

        cfg.setMarshaller(marshaller());

        ConnectorConfiguration connCfg = new ConnectorConfiguration();
        cfg.setConnectorConfiguration(connCfg);

        return cfg;
    }

    /**
     * @return Marshaller to be used in test.
     */
    protected abstract Marshaller marshaller();

    /** */
    protected CacheJdbcPojoStoreConfiguration storeConfiguration() {
        CacheJdbcPojoStoreConfiguration storeCfg = new CacheJdbcPojoStoreConfiguration();

        storeCfg.setDialect(new H2Dialect());

        storeCfg.setTypes(storeTypes());

        return storeCfg;
    }

    /**
     * @return Types to be used in test.
     */
    protected abstract JdbcType[] storeTypes();

    /** */
    protected CacheConfiguration cacheConfiguration() throws Exception {
        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);
        cc.setAtomicityMode(ATOMIC);
        cc.setSwapEnabled(false);
        cc.setWriteBehindEnabled(false);

        CacheJdbcPojoStoreFactory<Object, Object> storeFactory = new CacheJdbcPojoStoreFactory<>();
        storeFactory.setConfiguration(storeConfiguration());
        storeFactory.setDataSource(JdbcConnectionPool.create(DFLT_CONN_URL, "sa", "")); // H2 DataSource

        cc.setCacheStoreFactory(storeFactory);
        cc.setReadThrough(true);
        cc.setWriteThrough(true);
        cc.setLoadPreviousValue(true);

        return cc;
    }

    /**
     * Fill in-memory database with sample data.
     *
     * @param conn Connection to database.
     * @throws SQLException In case of filling database with sample data failed.
     */
    protected void fillSampleDatabase(Connection conn) throws SQLException {
        info("Start to fill sample database...");

        PreparedStatement orgStmt = conn.prepareStatement("INSERT INTO Organization(id, name, city) VALUES (?, ?, ?)");

        for (int i = 0; i < ORGANIZATION_CNT; i++) {
            orgStmt.setInt(1, i);
            orgStmt.setString(2, "name" + i);
            orgStmt.setString(3, "city" + i % 10);

            orgStmt.addBatch();
        }

        orgStmt.executeBatch();

        U.closeQuiet(orgStmt);

        conn.commit();

        PreparedStatement prnStmt = conn.prepareStatement("INSERT INTO Person(id, org_id, name) VALUES (?, ?, ?)");

        for (int i = 0; i < PERSON_CNT; i++) {
            prnStmt.setInt(1, i);
            prnStmt.setInt(2, i % 100);
            prnStmt.setString(3, "name" + i);

            prnStmt.addBatch();
        }

        prnStmt.executeBatch();

        conn.commit();

        U.closeQuiet(prnStmt);

        info("Sample database prepared.");
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadCache() throws Exception {
        primitiveKeys = false;

        startGrid();

        info("Execute testLoadCache...");

        IgniteCache<Object, Object> c1 = grid().cache(null);

        c1.loadCache(null);

        assertEquals(ORGANIZATION_CNT + PERSON_CNT, c1.size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadCachePrimitiveKeys() throws Exception {
        primitiveKeys = true;

        startGrid();

        info("Execute testLoadCachePrimitiveKeys...");

        IgniteCache<Object, Object> c1 = grid().cache(null);

        c1.loadCache(null);

        assertEquals(ORGANIZATION_CNT + PERSON_CNT, c1.size());
    }
}

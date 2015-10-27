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
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.store.jdbc.dialect.H2Dialect;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.portable.PortableMarshaller;
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
public class CacheJdbcPortableStoreSelfTest extends CacheJdbcStoreAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected Marshaller marshaller(){
        PortableMarshaller marsh = new PortableMarshaller();

        Collection<String> clsNames = new ArrayList<>();
        clsNames.add("org.apache.ignite.cache.store.jdbc.model.OrganizationKey");
        clsNames.add("org.apache.ignite.cache.store.jdbc.model.Organization");
        clsNames.add("org.apache.ignite.cache.store.jdbc.model.PersonKey");
        clsNames.add("org.apache.ignite.cache.store.jdbc.model.Person");

        marsh.setClassNames(clsNames);

        return marsh;
    }

    /** {@inheritDoc} */
    @Override protected CacheJdbcPojoStoreType[] storeTypes() {
        CacheJdbcPojoStoreType[] storeTypes = new CacheJdbcPojoStoreType[2];

        storeTypes[0] = new CacheJdbcPojoStoreType();
        storeTypes[0].setKeepSerialized(true);
        storeTypes[0].setDatabaseSchema("PUBLIC");
        storeTypes[0].setDatabaseTable("ORGANIZATION");
        storeTypes[0].setKeyType("org.apache.ignite.cache.store.jdbc.model.OrganizationKey");
        storeTypes[0].setKeyFields(new CacheJdbcPojoStoreTypeField(Types.INTEGER, "ID", Integer.class, "id"));
        storeTypes[0].setValueType("org.apache.ignite.cache.store.jdbc.model.Organization");
        storeTypes[0].setValueFields(
            new CacheJdbcPojoStoreTypeField(Types.INTEGER, "ID", Integer.class, "id"),
            new CacheJdbcPojoStoreTypeField(Types.VARCHAR, "NAME", String.class, "name"),
            new CacheJdbcPojoStoreTypeField(Types.VARCHAR, "CITY", String.class, "city"));

        storeTypes[1] = new CacheJdbcPojoStoreType();
        storeTypes[1].setKeepSerialized(true);
        storeTypes[1].setDatabaseSchema("PUBLIC");
        storeTypes[1].setDatabaseTable("PERSON");
        storeTypes[1].setKeyType("org.apache.ignite.cache.store.jdbc.model.PersonKey");
        storeTypes[1].setKeyFields(new CacheJdbcPojoStoreTypeField(Types.INTEGER, "ID", Integer.class, "id"));
        storeTypes[1].setValueType("org.apache.ignite.cache.store.jdbc.model.Person");
        storeTypes[1].setValueFields(
            new CacheJdbcPojoStoreTypeField(Types.INTEGER, "ID", Integer.class, "id"),
            new CacheJdbcPojoStoreTypeField(Types.INTEGER, "ORG_ID", Integer.class, "orgId"),
            new CacheJdbcPojoStoreTypeField(Types.VARCHAR, "NAME", String.class, "name"));

        return storeTypes;
    }
}

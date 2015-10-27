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

import java.sql.Types;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.marshaller.portable.PortableMarshaller;

/**
 * Class for {@code PojoCacheStore} tests.
 */
public class CacheJdbcPojoStoreSelfTest extends CacheJdbcStoreAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected Marshaller marshaller(){
        OptimizedMarshaller marsh = new OptimizedMarshaller();

        return marsh;
    }

    /** {@inheritDoc} */
    @Override protected CacheJdbcPojoStoreType[] storeTypes() {
        CacheJdbcPojoStoreType[] storeTypes = new CacheJdbcPojoStoreType[2];

        storeTypes[0] = new CacheJdbcPojoStoreType();
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

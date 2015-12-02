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

package org.apache.ignite.examples.datagrid.store.auto;

import java.sql.Types;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStore;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreFactory;
import org.apache.ignite.cache.store.jdbc.JdbcType;
import org.apache.ignite.cache.store.jdbc.JdbcTypeField;
import org.apache.ignite.cache.store.jdbc.dialect.H2Dialect;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.examples.model.Person;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 * Predefined configuration for examples with {@link CacheJdbcPojoStore}.
 */
public class CacheConfig {
    /** Cache name. */
    public static final String CACHE_NAME = "CacheAutoStore";

    /**
     * Configure cache with store.
     */
    public static CacheConfiguration<Long, Person> jdbcPojoStoreCache() {
        CacheJdbcPojoStoreFactory<Long, Person> storeFactory = new CacheJdbcPojoStoreFactory<>();

        storeFactory.setDataSourceBean("h2-example-db");
        storeFactory.setDialect(new H2Dialect());

        JdbcType jdbcType = new JdbcType();

        jdbcType.setCacheName(CACHE_NAME);
        jdbcType.setDatabaseSchema("PUBLIC");
        jdbcType.setDatabaseTable("PERSON");

        jdbcType.setKeyType("java.lang.Long");
        jdbcType.setValueType("org.apache.ignite.examples.model.Person");

        jdbcType.setKeyFields(new JdbcTypeField(Types.BIGINT, "ID", Long.class, "id"));
        jdbcType.setValueFields(
            new JdbcTypeField(Types.BIGINT, "ID", long.class, "id"),
            new JdbcTypeField(Types.VARCHAR, "FIRST_NAME", String.class, "firstName"),
            new JdbcTypeField(Types.VARCHAR, "LAST_NAME", String.class, "lastName")
        );

        storeFactory.setTypes(jdbcType);

        CacheConfiguration<Long, Person> cfg = new CacheConfiguration<>(CACHE_NAME);

        cfg.setCacheStoreFactory(storeFactory);

        // Set atomicity as transaction, since we are showing transactions in the example.
        cfg.setAtomicityMode(TRANSACTIONAL);

        cfg.setKeepBinaryInStore(true);

        cfg.setWriteBehindEnabled(true);

        cfg.setReadThrough(true);
        cfg.setWriteThrough(true);

        return cfg;
    }
}

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

import java.io.Serializable;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;

/**
 * Description for type that could be stored into database by store.
 */
public class CacheJdbcPojoStoreType implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Schema name in database. */
    private String dbSchema;

    /** Table name in database. */
    private String dbTbl;

    /** Key class used to store key in cache. */
    private String keyType;

    /** List of fields descriptors for key object. */
    @GridToStringInclude
    private CacheJdbcPojoStoreTypeField[] keyFields;

    /** Value class used to store value in cache. */
    private String valType;

    /** List of fields descriptors for value object. */
    @GridToStringInclude
    private CacheJdbcPojoStoreTypeField[] valFields;

    /** If {@code true} object is stored as IgniteObject. */
    private boolean keepSerialized;

    /**
     * Empty constructor (all values are initialized to their defaults).
     */
    public CacheJdbcPojoStoreType() {
        /* No-op. */
    }

    /**
     * Copy constructor.
     *
     * @param type Type to copy.
     */
    public CacheJdbcPojoStoreType(CacheJdbcPojoStoreType type) {
        dbSchema = type.getDatabaseSchema();
        dbTbl = type.getDatabaseTable();

        keyType = type.getKeyType();
        keyFields = type.getKeyFields();

        valType = type.getValueType();
        valFields = type.getValueFields();

        keepSerialized = type.isKeepSerialized();
    }

    /**
     * Gets database schema name.
     *
     * @return Schema name.
     */
    public String getDatabaseSchema() {
        return dbSchema;
    }

    /**
     * Sets database schema name.
     *
     * @param dbSchema Schema name.
     */
    public CacheJdbcPojoStoreType setDatabaseSchema(String dbSchema) {
        this.dbSchema = dbSchema;

        return this;
    }

    /**
     * Gets table name in database.
     *
     * @return Table name in database.
     */
    public String getDatabaseTable() {
        return dbTbl;
    }

    /**
     * Table name in database.
     *
     * @param dbTbl Table name in database.
     * @return {@code this} for chaining.
     */
    public CacheJdbcPojoStoreType setDatabaseTable(String dbTbl) {
        this.dbTbl = dbTbl;

        return this;
    }

    /**
     * Gets key type.
     *
     * @return Key type.
     */
    public String getKeyType() {
        return keyType;
    }

    /**
     * Sets key type.
     *
     * @param keyType Key type.
     * @return {@code this} for chaining.
     */
    public CacheJdbcPojoStoreType setKeyType(String keyType) {
        this.keyType = keyType;

        return this;
    }

    /**
     * Sets key type.
     *
     * @param cls Key type class.
     * @return {@code this} for chaining.
     */
    public CacheJdbcPojoStoreType setKeyType(Class<?> cls) {
        setKeyType(cls.getName());

        return this;
    }

    /**
     * Gets value type.
     *
     * @return Key type.
     */
    public String getValueType() {
        return valType;
    }

    /**
     * Sets value type.
     *
     * @param valType Value type.
     * @return {@code this} for chaining.
     */
    public CacheJdbcPojoStoreType setValueType(String valType) {
        this.valType = valType;

        return this;
    }

    /**
     * Sets value type.
     *
     * @param cls Value type class.
     * @return {@code this} for chaining.
     */
    public CacheJdbcPojoStoreType setValueType(Class<?> cls) {
        setValueType(cls.getName());

        return this;
    }

    /**
     * Gets optional persistent key fields (needed only if {@link CacheJdbcPojoStore} is used).
     *
     * @return Persistent key fields.
     */
    public CacheJdbcPojoStoreTypeField[] getKeyFields() {
        return keyFields;
    }

    /**
     * Sets optional persistent key fields (needed only if {@link CacheJdbcPojoStore} is used).
     *
     * @param keyFields Persistent key fields.
     * @return {@code this} for chaining.
     */
    public CacheJdbcPojoStoreType setKeyFields(CacheJdbcPojoStoreTypeField... keyFields) {
        this.keyFields = keyFields;

        return this;
    }

    /**
     * Gets optional persistent value fields (needed only if {@link CacheJdbcPojoStore} is used).
     *
     * @return Persistent value fields.
     */
    public CacheJdbcPojoStoreTypeField[] getValueFields() {
        return valFields;
    }

    /**
     * Sets optional persistent value fields (needed only if {@link CacheJdbcPojoStore} is used).
     *
     * @param valFields Persistent value fields.
     * @return {@code this} for chaining.
     */
    public CacheJdbcPojoStoreType setValueFields(CacheJdbcPojoStoreTypeField... valFields) {
        this.valFields = valFields;

        return this;
    }

    /**
     * Gets how value stored in cache.
     *
     * @return {@code true} if object is stored as IgniteObject.
     */
    public boolean isKeepSerialized() {
        return keepSerialized;
    }

    /**
     * Sets how value stored in cache.
     *
     * @param keepSerialized {@code true} if object is stored as IgniteObject.
     * @return {@code this} for chaining.
     */
    public CacheJdbcPojoStoreType setKeepSerialized(boolean keepSerialized) {
        this.keepSerialized = keepSerialized;

        return this;
    }
}

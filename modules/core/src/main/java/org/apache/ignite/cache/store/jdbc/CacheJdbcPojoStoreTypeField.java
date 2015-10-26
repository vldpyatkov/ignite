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
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Description of how field declared in database and in cache.
 */
public class CacheJdbcPojoStoreTypeField implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Field JDBC type in database. */
    private int dbFieldType;

    /** Field name in database. */
    private String dbFieldName;

    /** Field java type. */
    private Class<?> javaFieldType;

    /** Field name in java object. */
    private String javaFieldName;

    /**
     * Default constructor.
     */
    public CacheJdbcPojoStoreTypeField() {
        // No-op.
    }

    /**
     * Full constructor.
     *
     * @param dbFieldType Field JDBC type in database.
     * @param dbFieldName Field name in database.
     * @param javaFieldType Field java type.
     * @param javaFieldName Field name in java object.
     */
    public CacheJdbcPojoStoreTypeField(int dbFieldType, String dbFieldName, Class<?> javaFieldType, String javaFieldName) {
        this.dbFieldType = dbFieldType;
        this.dbFieldName = dbFieldName;
        this.javaFieldType = javaFieldType;
        this.javaFieldName = javaFieldName;
    }

    /**
     * Copy constructor.
     *
     * @param field Field to copy.
     */
    public CacheJdbcPojoStoreTypeField(CacheJdbcPojoStoreTypeField field) {
        this(field.getDatabaseFieldType(), field.getDatabaseFieldName(),
            field.getJavaFieldType(), field.getJavaFieldName());
    }

    /**
     * @return Column JDBC type in database.
     */
    public int getDatabaseFieldType() {
        return dbFieldType;
    }

    /**
     * @param dbType Column JDBC type in database.
     */
    public void setDatabaseFieldType(int dbType) {
        this.dbFieldType = dbType;
    }


    /**
     * @return Column name in database.
     */
    public String getDatabaseFieldName() {
        return dbFieldName;
    }

    /**
     * @param dbName Column name in database.
     */
    public void setDatabaseFieldName(String dbName) {
        this.dbFieldName = dbName;
    }

    /**
     * @return Field java type.
     */
    public Class<?> getJavaFieldType() {
        return javaFieldType;
    }

    /**
     * @param javaType Corresponding java type.
     */
    public void setJavaFieldType(Class<?> javaType) {
        this.javaFieldType = javaType;
    }

    /**
     * @return Field name in java object.
     */
    public String getJavaFieldName() {
        return javaFieldName;
    }

    /**
     * @param javaName Field name in java object.
     */
    public void setJavaFieldName(String javaName) {
        this.javaFieldName = javaName;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof CacheJdbcPojoStoreTypeField))
            return false;

        CacheJdbcPojoStoreTypeField that = (CacheJdbcPojoStoreTypeField)o;

        return dbFieldType == that.dbFieldType && dbFieldName.equals(that.dbFieldName) &&
            javaFieldType == that.javaFieldType && javaFieldName.equals(that.javaFieldName);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = dbFieldType;
        res = 31 * res + dbFieldName.hashCode();

        res = 31 * res + javaFieldType.hashCode();
        res = 31 * res + javaFieldName.hashCode();

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheJdbcPojoStoreTypeField.class, this);
    }
}

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

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.cache.CacheException;
import javax.cache.integration.CacheLoaderException;
import org.apache.ignite.IgnitePortables;
import org.apache.ignite.cache.IgniteObject;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.portable.PortableBuilder;
import org.apache.ignite.portable.PortableObject;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link CacheStore} backed by JDBC and POJO via reflection.
 *
 * This implementation stores objects in underlying database using java beans mapping description via reflection. <p>
 * Use {@link CacheJdbcPojoStoreFactory} factory to pass {@link CacheJdbcPojoStore} to {@link CacheConfiguration}.
 */
public class CacheJdbcPojoStore<K, V> extends CacheAbstractJdbcStore<K, V> {
    /** POJO methods cache. */
    private volatile Map<String, Map<String, PojoMethodsCache>> pojosMethods = Collections.emptyMap();

    /** Portables builders cache. */
    private volatile Map<String, Map<String, Integer>> portablesTypeIds = Collections.emptyMap();

    /**
     * Get field value from object for use as query parameter.
     *
     * @param cacheName Cache name.
     * @param typeName Type name.
     * @param fieldName Field name.
     * @param obj Cache object.
     * @return Field value from object.
     * @throws CacheException in case of error.
     */
    @Override @Nullable protected Object extractParameter(@Nullable String cacheName, String typeName, String fieldName,
        Object obj) throws CacheException {
        switch (typeKind(cacheName, typeName)) {
            case SIMPLE:
                return obj;
            case POJO:
                return extractPojoParameter(cacheName, typeName, fieldName, obj);
            default:
                return extractPortableParameter(fieldName, obj);
        }
    }

    /**
     * Get field value from POJO for use as query parameter.
     *
     * @param cacheName Cache name.
     * @param typeName Type name.
     * @param fieldName Field name.
     * @param obj Cache object.
     * @return Field value from object.
     * @throws CacheException in case of error.
     */
    @Nullable private Object extractPojoParameter(@Nullable String cacheName, String typeName, String fieldName,
        Object obj) throws CacheException {
        try {
            Map<String, PojoMethodsCache> cacheMethods = pojosMethods.get(cacheName);

            if (cacheMethods == null)
                throw new CacheException("Failed to find POJO type metadata for cache: " + U.maskName(cacheName));

            PojoMethodsCache mc = cacheMethods.get(typeName);

            if (mc == null)
                throw new CacheException("Failed to find POJO type metadata for type: " + typeName);

            Method getter = mc.getters.get(fieldName);

            if (getter == null)
                throw new CacheLoaderException("Failed to find getter in POJO class [clsName=" + typeName +
                    ", prop=" + fieldName + "]");

            return getter.invoke(obj);
        }
        catch (Exception e) {
            throw new CacheException("Failed to read object of class: " + typeName, e);
        }
    }

    /**
     * Get field value from Portable for use as query parameter.
     *
     * @param fieldName Field name to extract query parameter for.
     * @param obj Object to process.
     * @return Field value from object.
     * @throws CacheException in case of error.
     */
    private Object extractPortableParameter(String fieldName, Object obj) throws CacheException {
        if (obj instanceof PortableObject) {
            PortableObject pobj = (PortableObject)obj;

            return pobj.field(fieldName);
        }

        throw new CacheException("Failed to read property value from non portable object [class name=" +
            obj.getClass() + ", property=" + fieldName + "]");
    }

    /** {@inheritDoc} */
    @Override protected <R> R buildObject(@Nullable String cacheName, String typeName,
        JdbcTypeField[] fields, Collection<String> hashFields, Map<String, Integer> loadColIdxs, ResultSet rs)
        throws CacheLoaderException {
        switch (typeKind(cacheName, typeName)) {
            case SIMPLE:
                return (R)buildSimpleObject(typeName, fields, loadColIdxs, rs);
            case POJO:
                return (R)buildPojoObject(cacheName, typeName, fields, loadColIdxs, rs);
            default:
                return (R)buildPortableObject(cacheName, typeName, fields, hashFields, loadColIdxs, rs);
        }
    }

    /**
     * Construct simple object from query result.
     *
     * @param typeName Type name.
     * @param fields Fields descriptors.
     * @param loadColIdxs Select query columns index.
     * @param rs ResultSet.
     * @return Constructed POJO.
     * @throws CacheLoaderException If failed to construct POJO.
     */
    private Object buildSimpleObject(String typeName, JdbcTypeField[] fields, Map<String, Integer> loadColIdxs,
        ResultSet rs) throws CacheLoaderException {
        try {
            JdbcTypeField field = fields[0];

            return getColumnValue(rs, loadColIdxs.get(field.getDatabaseFieldName()), field.getJavaFieldType());
        }
        catch (SQLException e) {
            throw new CacheLoaderException("Failed to read object of class: " + typeName, e);
        }
    }

    /**
     * Construct POJO from query result.
     *
     * @param cacheName Cache name.
     * @param typeName Type name.
     * @param fields Fields descriptors.
     * @param loadColIdxs Select query columns index.
     * @param rs ResultSet.
     * @return Constructed POJO.
     * @throws CacheLoaderException If failed to construct POJO.
     */
    private Object buildPojoObject(@Nullable String cacheName, String typeName,
        JdbcTypeField[] fields, Map<String, Integer> loadColIdxs, ResultSet rs)
        throws CacheLoaderException {

        Map<String, PojoMethodsCache> cacheMethods = pojosMethods.get(cacheName);

        if (cacheMethods == null)
            throw new CacheLoaderException("Failed to find POJO types metadata for cache: " + U.maskName(cacheName));

        PojoMethodsCache mc = cacheMethods.get(typeName);

        if (mc == null)
            throw new CacheLoaderException("Failed to find POJO type metadata for type: " + typeName);

        try {
            Object obj = mc.ctor.newInstance();

            for (JdbcTypeField field : fields) {
                String fldJavaName = field.getJavaFieldName();

                Method setter = mc.setters.get(fldJavaName);

                if (setter == null)
                    throw new IllegalStateException("Failed to find setter in POJO class [clsName=" + typeName +
                        ", prop=" + fldJavaName + "]");

                String fldDbName = field.getDatabaseFieldName();

                Integer colIdx = loadColIdxs.get(fldDbName);

                try {
                    Object colVal = getColumnValue(rs, colIdx, field.getJavaFieldType());

                    try {
                        setter.invoke(obj, colVal);
                    }
                    catch (Exception e) {
                        throw new CacheLoaderException("Failed to set property in POJO class [clsName=" + typeName +
                            ", prop=" + fldJavaName + ", col=" + colIdx + ", dbName=" + fldDbName + "]", e);
                    }
                }
                catch (SQLException e) {
                    throw new CacheLoaderException("Failed to read object property [clsName=: " + typeName +
                        ", prop=" + fldJavaName + ", col=" + colIdx + ", dbName=" + fldDbName + "]", e);
                }
            }

            return obj;
        }
        catch (Exception e) {
            throw new CacheLoaderException("Failed to construct instance of class: " + typeName, e);
        }
    }

    /**
     * Construct portable object from query result.
     *
     * @param cacheName Cache name.
     * @param typeName Type name.
     * @param fields Fields descriptors.
     * @param hashFields Collection of fields to build hash for.
     * @param loadColIdxs Select query columns index.
     * @param rs ResultSet.
     * @return Constructed portable object.
     * @throws CacheLoaderException If failed to construct portable object.
     */
    protected Object buildPortableObject(String cacheName, String typeName, JdbcTypeField[] fields,
        Collection<String> hashFields, Map<String, Integer> loadColIdxs, ResultSet rs) throws CacheLoaderException {
        Map<String, Integer> cacheTypeIds = portablesTypeIds.get(cacheName);

        if (cacheTypeIds == null)
            throw new CacheLoaderException("Failed to find portable types IDs for cache: " + U.maskName(cacheName));

        Integer typeId = cacheTypeIds.get(typeName);

        if (typeId == null)
            throw new CacheLoaderException("Failed to find portable type ID for type: " + typeName);

        try {
            PortableBuilder builder = ignite.portables().builder(typeId);

            boolean calcHash = hashFields != null;

            Collection<Object> hashValues = calcHash ? new ArrayList<>(hashFields.size()) : null;

            for (JdbcTypeField field : fields) {
                Integer colIdx = loadColIdxs.get(field.getDatabaseFieldName());

                Object colVal = getColumnValue(rs, colIdx, field.getJavaFieldType());

                builder.setField(field.getJavaFieldName(), colVal);

                if (calcHash)
                    hashValues.add(colVal);
            }

            if (calcHash)
                builder.hashCode(hasher.hashCode(hashValues));

            return builder.build();
        }
        catch (SQLException e) {
            throw new CacheException("Failed to read portable object", e);
        }
    }

    /**
     * Calculate type ID for object.
     *
     * @param obj Object to calculate type ID for.
     * @return Type ID.
     * @throws CacheException If failed to calculate type ID for given object.
     */
    @Override protected Object typeIdForObject(Object obj) throws CacheException {
        if (obj instanceof IgniteObject)
            return ((IgniteObject)obj).typeId();

        return obj.getClass();
    }

    /** {@inheritDoc} */
    @Override protected Object typeIdForTypeName(TypeKind kind, String typeName) throws CacheException {
        if (kind == TypeKind.PORTABLE)
            return ignite.portables().typeId(typeName);

        try {
            return Class.forName(typeName);
        }
        catch (ClassNotFoundException e) {
            throw new CacheException("Failed to find class: " + typeName, e);
        }
    }

    /**
     * Prepare internal store specific builders for provided types metadata.
     *
     * @param cacheName Cache name to prepare builders for.
     * @param types Collection of types.
     * @throws CacheException If failed to prepare internal builders for types.
     */
    @Override protected void prepareBuilders(@Nullable String cacheName, Collection<JdbcType> types)
        throws CacheException {
        Map<String, PojoMethodsCache> pojoMethods = U.newHashMap(types.size() * 2);
        Map<String, Integer> typeIds = U.newHashMap(types.size() * 2);

        IgnitePortables portables = ignite.portables();

        for (JdbcType type : types) {
            String keyTypeName = type.getKeyType();

            TypeKind keyKind = typeKind(cacheName, keyTypeName);

            if (keyKind == TypeKind.POJO) {
                if (pojoMethods.containsKey(keyTypeName))
                    throw new CacheException("Found duplicate key type [cache=" + U.maskName(cacheName) +
                        ", keyType=" + keyTypeName + "]");

                pojoMethods.put(keyTypeName, new PojoMethodsCache(keyTypeName, type.getKeyFields()));
            }
            else if (keyKind == TypeKind.PORTABLE)
                typeIds.put(keyTypeName, portables.typeId(keyTypeName));

            String valTypeName = type.getValueType();

            TypeKind valKind = typeKind(cacheName, valTypeName);

            if (valKind == TypeKind.POJO)
                pojoMethods.put(valTypeName, new PojoMethodsCache(valTypeName, type.getValueFields()));
            else if (valKind == TypeKind.PORTABLE)
                typeIds.put(valTypeName, portables.typeId(valTypeName));
        }

        if (!pojoMethods.isEmpty()) {
            Map<String, Map<String, PojoMethodsCache>> newPojosMethods = new HashMap<>(pojosMethods);

            newPojosMethods.put(cacheName, pojoMethods);

            pojosMethods = newPojosMethods;
        }

        if (!typeIds.isEmpty()) {
            Map<String, Map<String, Integer>> newPortablesTypeIds = new HashMap<>(portablesTypeIds);

            newPortablesTypeIds.put(cacheName, typeIds);

            portablesTypeIds = newPortablesTypeIds;
        }
    }

    /**
     * POJO methods cache.
     */
    private static class PojoMethodsCache {
        /** POJO class. */
        private final Class<?> cls;

        /** Constructor for POJO object. */
        private Constructor ctor;

        /** Cached setters for POJO object. */
        private Map<String, Method> getters;

        /** Cached getters for POJO object. */
        private Map<String, Method> setters;

        /**
         * POJO methods cache.
         *
         * @param clsName Class name.
         * @param fields Fields.
         * @throws CacheException If failed to construct type cache.
         */
        private PojoMethodsCache(String clsName, JdbcTypeField[] fields) throws CacheException {
            try {
                cls = Class.forName(clsName);

                ctor = cls.getDeclaredConstructor();

                if (!ctor.isAccessible())
                    ctor.setAccessible(true);
            }
            catch (ClassNotFoundException e) {
                throw new CacheException("Failed to find class: " + clsName, e);
            }
            catch (NoSuchMethodException e) {
                throw new CacheException("Failed to find default constructor for class: " + clsName, e);
            }

            setters = U.newHashMap(fields.length);

            getters = U.newHashMap(fields.length);

            for (JdbcTypeField field : fields) {
                String prop = capitalFirst(field.getJavaFieldName());

                try {
                    getters.put(field.getJavaFieldName(), cls.getMethod("get" + prop));
                }
                catch (NoSuchMethodException ignored) {
                    try {
                        getters.put(field.getJavaFieldName(), cls.getMethod("is" + prop));
                    }
                    catch (NoSuchMethodException e) {
                        throw new CacheException("Failed to find getter in POJO class [clsName=" + clsName +
                            ", prop=" + field.getJavaFieldName() + "]", e);
                    }
                }

                try {
                    setters.put(field.getJavaFieldName(), cls.getMethod("set" + prop, field.getJavaFieldType()));
                }
                catch (NoSuchMethodException e) {
                    throw new CacheException("Failed to find setter in POJO class [clsName=" + clsName +
                        ", prop=" + field.getJavaFieldName() + "]", e);
                }
            }
        }

        /**
         * Capitalizes the first character of the given string.
         *
         * @param str String.
         * @return String with capitalized first character.
         */
        @Nullable private String capitalFirst(@Nullable String str) {
            return str == null ? null :
                str.isEmpty() ? "" : Character.toUpperCase(str.charAt(0)) + str.substring(1);
        }
    }
}

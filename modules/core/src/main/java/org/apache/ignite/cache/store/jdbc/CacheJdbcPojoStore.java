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

import org.apache.ignite.*;
import org.apache.ignite.cache.IgniteObject;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.portable.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import javax.cache.integration.*;
import java.lang.reflect.*;
import java.sql.*;
import java.util.*;

/**
 * Implementation of {@link CacheStore} backed by JDBC and POJO via reflection.
 *
 * This implementation stores objects in underlying database using java beans mapping description via reflection. <p>
 * Use {@link CacheJdbcPojoStoreFactory} factory to pass {@link CacheJdbcPojoStore} to {@link CacheConfiguration}.
 */
public class CacheJdbcPojoStore<K, V> extends CacheAbstractJdbcStore<K, V> {
    /** POJO methods cache. */
    private volatile Map<String, Map<String, PojoMethodsCache>> pojoMethods = Collections.emptyMap();

    /** Portables builders cache. */
    private volatile Map<String, Map<String, IgniteBiTuple<Boolean, Integer>>> portableTypeIds = Collections.emptyMap();

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
        return isKeepSerialized(cacheName, typeName)
            ? extractPortableParameter(fieldName, obj)
            : extractPojoParameter(cacheName, typeName, fieldName, obj);
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
            Map<String, PojoMethodsCache> cacheMethods = pojoMethods.get(cacheName);

            if (cacheMethods == null)
                throw new CacheException("Failed to find POJO type metadata for cache: " +  U.maskName(cacheName));

            PojoMethodsCache mc = cacheMethods.get(typeName);

            if (mc == null)
                throw new CacheException("Failed to find POJO type metadata for type: " + typeName);

            if (mc.simple)
                return obj;

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
        return (R)(isKeepSerialized(cacheName, typeName)
            ? buildPortableObject(cacheName, typeName, fields, hashFields, loadColIdxs, rs)
            : buildPojoObject(cacheName, typeName, fields, loadColIdxs, rs));
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

        Map<String, PojoMethodsCache> cacheMethods = pojoMethods.get(cacheName);

        if (cacheMethods == null)
            throw new CacheLoaderException("Failed to find POJO types metadata for cache: " +  U.maskName(cacheName));

        PojoMethodsCache mc = cacheMethods.get(typeName);

        if (mc == null)
            throw new CacheLoaderException("Failed to find POJO type metadata for type: " + typeName);

        try {
            if (mc.simple) {
                JdbcTypeField field = fields[0];

                return getColumnValue(rs, loadColIdxs.get(field.getDatabaseFieldName()), mc.cls);
            }

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
                    setter.invoke(obj, getColumnValue(rs, colIdx, field.getJavaFieldType()));
                }
                catch (Exception e) {
                    throw new IllegalStateException("Failed to set property in POJO class [clsName=" + typeName +
                        ", prop=" + fldJavaName + ", col=" + colIdx + ", dbName=" + fldDbName + "]", e);
                }
            }

            return obj;
        }
        catch (SQLException e) {
            throw new CacheLoaderException("Failed to read object of class: " + typeName, e);
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
        Map<String, IgniteBiTuple<Boolean, Integer>> cacheTypeIds = portableTypeIds.get(cacheName);

        if (cacheTypeIds == null)
            throw new CacheLoaderException("Failed to find portable types IDs for cache: " +  U.maskName(cacheName));

        IgniteBiTuple<Boolean, Integer> tuple = cacheTypeIds.get(typeName);

        if (tuple == null)
            throw new CacheLoaderException("Failed to find portable type ID for type: " + typeName);

        try {
            if (tuple.get1()) {
                Object simple = null;

                if (fields.length > 0) {
                    JdbcTypeField field = fields[0];

                    Integer colIdx = loadColIdxs.get(field.getDatabaseFieldName());

                    simple = getColumnValue(rs, colIdx, field.getJavaFieldType());
                }

                return simple;
            }
            else {
                PortableBuilder builder = ignite.portables().builder(tuple.get2());

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

    /**
     * Calculate type ID for given type name.
     *
     * @param keepSerialized If {@code true} then calculate type ID for portable object otherwise for POJO.
     * @param typeName String description of type name.
     * @return Type ID.
     * @throws CacheException If failed to get type ID for given type name.
     */
    @Override protected Object typeIdForTypeName(boolean keepSerialized, String typeName) throws CacheException {
        if (keepSerialized)
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
        preparePojoBuilders(cacheName, types);
        preparePortableBuilders(cacheName, types);
    }


    /**
     * Prepare builders for POJOs via reflection (getters and setters).
     *
     * @param cacheName Cache name to prepare builders for.
     * @param types Collection of types.
     * @throws CacheException If failed to prepare internal builders for types.
     */
    private void preparePojoBuilders(@Nullable String cacheName, Collection<JdbcType> types)
        throws CacheException {
        Map<String, PojoMethodsCache> typeMethods = U.newHashMap(types.size() * 2);

        for (JdbcType type : types) {
            if (!type.isKeepSerialized()) {
                String keyType = type.getKeyType();

                if (typeMethods.containsKey(keyType))
                    throw new CacheException("Found duplicate key type [cache=" +  U.maskName(cacheName) +
                        ", keyType=" + keyType + "]");

                typeMethods.put(keyType, new PojoMethodsCache(keyType, type.getKeyFields()));

                String valType = type.getValueType();
                typeMethods.put(valType, new PojoMethodsCache(valType, type.getValueFields()));
            }
        }

        if (!typeMethods.isEmpty()) {
            Map<String, Map<String, PojoMethodsCache>> newMtdsCache = new HashMap<>(pojoMethods);

            newMtdsCache.put(cacheName, typeMethods);

            pojoMethods = newMtdsCache;
        }
    }

    /**
     * Prepare builders for portable objects via portable builder.
     *
     * @param cacheName Cache name to prepare builders for.
     * @param types Collection of types.
     * @throws CacheException If failed to prepare internal builders for types.
     */
    private void preparePortableBuilders(@Nullable String cacheName, Collection<JdbcType> types)
        throws CacheException {
        Map<String, IgniteBiTuple<Boolean, Integer>> typeIds = U.newHashMap(types.size() * 2);

        for (JdbcType type : types) {
            if (type.isKeepSerialized()) {
                Ignite ignite = ignite();

                IgnitePortables portables = ignite.portables();

                String keyType = type.getKeyType();
                typeIds.put(keyType, new IgniteBiTuple<>(SIMPLE_TYPES.contains(keyType), portables.typeId(keyType)));

                String valType = type.getValueType();
                typeIds.put(valType, new IgniteBiTuple<>(SIMPLE_TYPES.contains(valType), portables.typeId(valType)));
            }
        }

        if (!typeIds.isEmpty()) {
            Map<String, Map<String, IgniteBiTuple<Boolean, Integer>>> newBuilders = new HashMap<>(portableTypeIds);

            newBuilders.put(cacheName, typeIds);

            portableTypeIds = newBuilders;
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

        /** {@code true} if object is a simple type. */
        private final boolean simple;

        /** Cached setters for POJO object. */
        private Map<String, Method> getters;

        /** Cached getters for POJO object. */
        private Map<String, Method> setters;


        /**
         * Object is a simple type.
         *
         * @param cls Class.
         * @return {@code True} if object is a simple type.
         */
        private boolean simpleType(Class<?> cls) {
            return (Number.class.isAssignableFrom(cls) || String.class.isAssignableFrom(cls) ||
                java.util.Date.class.isAssignableFrom(cls) || Boolean.class.isAssignableFrom(cls) ||
                UUID.class.isAssignableFrom(cls));
        }
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

                if (simple = simpleType(cls))
                    return;

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

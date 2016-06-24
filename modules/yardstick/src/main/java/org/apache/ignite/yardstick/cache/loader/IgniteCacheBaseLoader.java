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

package org.apache.ignite.yardstick.cache.loader;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import javafx.util.Pair;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheTypeMetadata;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.yardstickframework.BenchmarkUtils;

import java.util.LinkedHashMap;

/**
 * Ignite benchmark for performs filling cache.
 */
public class IgniteCacheBaseLoader extends Thread {
    /** Cache instance. */
    protected IgniteCache<String, BinaryObject> cache;

    /** All fields from currenct cache. */
    protected LinkedHashMap<String, String> cacheFields;

    /** Ignite instance. */
    protected Ignite ignite = Ignition.ignite();

    /** Amount rows for preloading. */
    protected int preloadAmount;

    /** Name of current cache. */
    protected String cacheName;

    /** Value type of current cache. */
    protected String valueType;

    /**
     * Constructor.
     *
     * * @param cacheName cache name. * @param valueType cache value type. * @param preloadAmount amount to add
     * records.
     */
    public IgniteCacheBaseLoader(String cacheName, String valueType, int preloadAmount) {
        this.cacheName = cacheName;
        this.valueType = valueType;
        this.preloadAmount = preloadAmount;

        cache = ignite.cache(cacheName);

        if (cache == null)
            throw new IgniteException("Cache [" + cacheName + "] is not found, check ignite config file");

        cacheFields = getFields(cache);
    }

    /**
     *
     * Create BinaryObject object.
     *
     * @param rowId - object Id
     * @return Pair BinaryObject and Id
     */
    public Pair<String, BinaryObject> createBinaryObject(Integer rowId) {
        BinaryObjectBuilder builder = ignite.binary().builder(valueType);
        String strId = "" + rowId;

        for (String fieldKey : cacheFields.keySet()) {
            String fieldType = cacheFields.get(fieldKey);
            String fieldKeyUpper = fieldKey.toUpperCase();

            if ("ID".equals(fieldKey))
                builder.setField(fieldKey, strId);
            else if (fieldKeyUpper.contains("_ID"))
                builder.setField(fieldKey, strId);
            else {
                Object valueRnd = getRandomValue(fieldKey, fieldType);
                builder.setField(fieldKey, valueRnd);
            }
        }

        Pair<String, BinaryObject> pair = new Pair(strId, builder.build());

        return pair;
    }

    /**
     * Generatte and fill cache values.
     *
     * @param addRows - amount added items
     */
    public void fillCache(int addRows) {
        try (IgniteDataStreamer<String, BinaryObject> stmr = ignite.dataStreamer(cacheName)) {
            for (int ind = 1; ind <= addRows; ind++) {
                if (ind % 50_000 == 0 || ind == addRows)
                    BenchmarkUtils.println("Filling cache[" + cache.getName() + "], " + ind + "/" + addRows + " rows...");

                Pair<String, BinaryObject> addressPair = createBinaryObject(ind);
                stmr.addData(addressPair.getKey(), addressPair.getValue());
            }
        }

        checkCacheSize(addRows);
    }

    /**
     * Start new Thread
     */
    @Override public void run() {
        fillCache(preloadAmount);
    }

    /**
     * Check cache size.
     *
     * @param validCacheSize  valid cache size
     */
    public void checkCacheSize(int validCacheSize) {
        String querySql = "select count(*) from " + valueType;
        long totalRows = 0;

        QueryCursor<List<?>> qcur = cache.query(new SqlFieldsQuery(querySql));
        List<List<?>> listRes = qcur.getAll();

        if (listRes != null && listRes.size() > 0){
            Object value = listRes.get(0).get(0);
            totalRows = (Long)value;
        }

        StringBuilder sb = new StringBuilder(System.lineSeparator());
        sb.append("------------------------------------------------" + System.lineSeparator());
        sb.append("Cache [" + cache.getName() + "], valueType[" + valueType + "] ");
        sb.append("size " + cache.size() + System.lineSeparator());


        if (validCacheSize != totalRows || totalRows != cache.size()){
            sb.append(querySql + " == " + totalRows + " NOT EQUAL cache.size()" + System.lineSeparator());
            sb.append("------------------------------------------------");
            throw new IgniteException(sb.toString());
        }

        sb.append(querySql + System.lineSeparator());
        sb.append("Query returns " + cache.size() + " rows" + System.lineSeparator());
        sb.append("------------------------------------------------");
        BenchmarkUtils.println(sb.toString());
    }

    /**
     * Collect cache configured fields
     *
     * @param cache Ignite cache.
     * @return Cache configured fields.
     */
    public static <K, V> LinkedHashMap<String, String> getFields(IgniteCache<K, V> cache) {
        LinkedHashMap<String, String> fielsdMap = new LinkedHashMap<>();

        CacheConfiguration cfg = cache.getConfiguration(CacheConfiguration.class);
        Collection<QueryEntity> queryEntities = cfg.getQueryEntities();
        if (queryEntities != null) {
            for (QueryEntity queryEntity : queryEntities) {
                LinkedHashMap<String, String> fields = queryEntity.getFields();
                if (fields == null) continue;

                for (String fieldKey: fields.keySet()) {
                    fielsdMap.put(fieldKey, fields.get(fieldKey));
                }
            }
        }

        Collection<CacheTypeMetadata> typeMetadata = cfg.getTypeMetadata();
        if (typeMetadata != null) {
            for (CacheTypeMetadata cacheTypeMetadata : typeMetadata) {
                Map<String, Class<?>> queryFields = cacheTypeMetadata.getQueryFields();

                for (String fieldKey: queryFields.keySet()) {
                    Class clazz = queryFields.get(fieldKey);
                    fielsdMap.put(fieldKey, clazz.getName());
                }
            }
        }
        return fielsdMap;
    }

    /**
     * Round value to 2 digits
     */
    public static double round2(Double val) {
        double res = Math.round(val * 100) / 100;
        return res;
    }

    /**
     * Generate random value
     */
    public static Object getRandomValue(String fieldKey, String fieldType) {
        Object value;
        int minValue = 1;
        int maxValue = 120000;

        String fieldKeyUpper = fieldKey.toUpperCase();
        if (fieldKeyUpper.contains("SEX")){
            minValue = 1;
            maxValue = 3; // max value is not included
        }
        else if (fieldKeyUpper.contains("RATE")){
            minValue = 3;
            maxValue = 9;
        }

        if ("java.lang.String".equals(fieldType))
            value = fieldKey + "-" + UUID.randomUUID().toString();
        else if ("java.lang.Integer".equals(fieldType))
            value = ThreadLocalRandom.current().nextInt(minValue, maxValue);
        else if ("java.lang.Long".equals(fieldType))
            value = ThreadLocalRandom.current().nextInt(minValue, maxValue);
        else if ("java.lang.Double".equals(fieldType))
            value = round2(ThreadLocalRandom.current().nextDouble(minValue, maxValue));
        else if ("java.lang.Float".equals(fieldType))
            value = (float)round2(ThreadLocalRandom.current().nextDouble(minValue, maxValue));
        else if ("java.math.BigDecimal".equals(fieldType)) {
            double valueRnd = round2(ThreadLocalRandom.current().nextDouble(minValue, maxValue));
            value = BigDecimal.valueOf(valueRnd);
        }
        else if ("java.util.Date".equals(fieldType)){
            int randomDays = ThreadLocalRandom.current().nextInt(1, 365*30);
            Calendar cal = Calendar.getInstance();
            cal.add(Calendar.DATE, -randomDays);
            value = cal.getTime();
        }
        else
            throw new IgniteException("Unknown data type [" + fieldType + "] for " + fieldKey);

        return value;
    }
}

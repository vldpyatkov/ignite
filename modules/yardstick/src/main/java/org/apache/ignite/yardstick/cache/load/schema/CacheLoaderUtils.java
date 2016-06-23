package org.apache.ignite.yardstick.cache.load.schema;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheTypeMetadata;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.yardstickframework.BenchmarkUtils;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Ignite cache utility class, used for betchmark tests.
 */
public class CacheLoaderUtils {
    /**
     * Validate and pritnt cache info.
     */
    public static <K, V> void validateCache(IgniteCache<K, V> cache, String valueType, int validRows) {
        String querySql = "select count(*) from " + valueType;
        QueryCursor<List<?>> qcur = cache.query(new SqlFieldsQuery(querySql));
        List<List<?>> listRes = qcur.getAll();

        long totalRows = 0;
        if (listRes != null && listRes.size() > 0){
            Object value = listRes.get(0).get(0);
            totalRows = (Long)value;
        }
        if (validRows != totalRows || totalRows != cache.size()){
            String msg = querySql + " == " + totalRows + " NOT EQUAL cache.size() == "  + cache.size();
            throw new IgniteException(msg );
        }

        StringBuilder sb = new StringBuilder(System.lineSeparator());
        sb.append("------------------------------------------------------------" + System.lineSeparator());
        sb.append("Cache [" + cache.getName() + "], valueType[" + valueType + "] size " + totalRows + " rows");
        sb.append(System.lineSeparator() + querySql + System.lineSeparator());
        sb.append("Query returns " + cache.size() + " rows" + System.lineSeparator());
        sb.append("------------------------------------------------------------");
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
        Object value = null;
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

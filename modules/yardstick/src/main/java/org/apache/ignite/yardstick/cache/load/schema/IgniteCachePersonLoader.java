package org.apache.ignite.yardstick.cache.load.schema;

import javafx.util.Pair;
import org.apache.ignite.*;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.yardstickframework.BenchmarkUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.UUID;

/**
 * Ignite benchmark for performs filling Person cache.
 */
public class IgniteCachePersonLoader extends IgniteCacheBaseLoader{

    /** {@inheritDoc} */
    public IgniteCachePersonLoader(String cacheName, String valueType, int preloadAmount) {
        super(cacheName, valueType, preloadAmount);
    }

    /** {@inheritDoc} */
    @Override public Pair<String, BinaryObject> createBinaryObject(Integer rowId) {
        BinaryObjectBuilder builder = ignite.binary().builder("Person");
        String strId = "" + rowId;

        for (String fieldKey : cacheFields.keySet()) {
            String fieldType = cacheFields.get(fieldKey);
            String fieldKeyUpper = fieldKey.toUpperCase();

            if ("ID".equals(fieldKeyUpper)) {
                builder.setField(fieldKey, strId);
            } else if (fieldKeyUpper.contains("_ID")) {
                builder.setField(fieldKey, strId);
            } else {
                Object valueRnd = CacheLoaderUtils.getRandomValue(fieldKey, fieldType);
                builder.setField(fieldKey, valueRnd);
            }
        }
        Pair<String, BinaryObject> pair = new Pair(strId, builder.build());
        return pair;
    }
}

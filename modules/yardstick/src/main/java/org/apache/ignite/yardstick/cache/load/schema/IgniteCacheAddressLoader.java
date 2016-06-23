package org.apache.ignite.yardstick.cache.load.schema;

import javafx.util.Pair;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;

/**
 * Ignite benchmark for performs filling Address cache.
 */
public class IgniteCacheAddressLoader extends IgniteCacheBaseLoader{

    /** {@inheritDoc} */
    public IgniteCacheAddressLoader(String cacheName, String valueType, int preloadAmount) {
        super(cacheName, valueType, preloadAmount);
    }

    /**
     * Create binary object.
     */
    @Override public Pair<String, BinaryObject> createBinaryObject(Integer rowId) {
        BinaryObjectBuilder builder = ignite.binary().builder("Address");
        String strId = "" + rowId;

        for (String fieldKey : cacheFields.keySet()) {
            String fieldType = cacheFields.get(fieldKey);
            String fieldKeyUpper = fieldKey.toUpperCase();

            if ("ID".equals(fieldKey))
                builder.setField(fieldKey, strId);
            else if (fieldKeyUpper.contains("_ID"))
                builder.setField(fieldKey, strId);
            else {
                Object valueRnd = CacheLoaderUtils.getRandomValue(fieldKey, fieldType);
                builder.setField(fieldKey, valueRnd);
            }
        }
        Pair<String, BinaryObject> pair = new Pair(strId, builder.build());
        return pair;
    }
}

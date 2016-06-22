package org.apache.ignite.yardstick.cache.load.schema;

import javafx.util.Pair;
import org.apache.ignite.*;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.yardstickframework.BenchmarkUtils;
import java.util.LinkedHashMap;

/**
 * Ignite benchmark for performs filling Address cache.
 */
public class IgniteCacheAddressLoader {

    public static final String CACHE_NAME = "CLIENT_ADDRESS";

    private IgniteCache<String, BinaryObject> cacheAddress;
    private LinkedHashMap<String, String> addressFields;
    private Ignite ignite  = Ignition.ignite();

    public IgniteCacheAddressLoader() {
        cacheAddress = ignite.cache(CACHE_NAME);
        if (cacheAddress == null) throw new IgniteException("Cache [" + CACHE_NAME + "] is not found, check ignite config file");
        addressFields = CacheLoaderUtils.getFields(cacheAddress);
    }

    public Pair<String, BinaryObject> createAddress(Integer rowId) {
        BinaryObjectBuilder builder = ignite.binary().builder("Address");
        String strId = "" + rowId;

        for (String fieldKey : addressFields.keySet()) {
            String fieldType = addressFields.get(fieldKey);
            String fieldKeyUpper = fieldKey.toUpperCase();

            if ("ID".equals(fieldKey)) {
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

    public void fillCache(int addRows) {
        try (IgniteDataStreamer<String, BinaryObject> stmr = ignite.dataStreamer(cacheAddress.getName())) {
            for (int ind = 1; ind <= addRows; ind++) {
                if (ind % 50_000 == 0 || ind == addRows) {
                    BenchmarkUtils.println("Filling cache[" + cacheAddress.getName() + "], " + ind + "/" + addRows + " rows...");
                }

                Pair<String, BinaryObject> addressPair = createAddress(ind);
                stmr.addData(addressPair.getKey(), addressPair.getValue());
            }
        }
        CacheLoaderUtils.validateCache(cacheAddress, "Address", addRows);
    }

}

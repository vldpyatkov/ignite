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
public class IgniteCachePersonLoader {

    public static final String CACHE_NAME = "CLIENT_PERSON";

    private IgniteCache<String, BinaryObject> cachePerson;
    private LinkedHashMap<String, String> personFields;
    private Ignite ignite  = Ignition.ignite();

    public IgniteCachePersonLoader() {
        cachePerson = ignite.cache(CACHE_NAME);
        if (cachePerson == null) throw new IgniteException("Cache [" + CACHE_NAME + "] is not found, check ignite config file");
        personFields = CacheLoaderUtils.getFields(cachePerson);
    }

    public Pair<String, BinaryObject> createPerson(Integer rowId) {
        BinaryObjectBuilder builder = ignite.binary().builder("Person");
        String strId = "" + rowId;

        for (String fieldKey : personFields.keySet()) {
            String fieldType = personFields.get(fieldKey);
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

    public void fillCache(int addRows) {
        try (IgniteDataStreamer<String, BinaryObject> stmr = ignite.dataStreamer(cachePerson.getName())) {
            for (int ind = 1; ind <= addRows; ind++) {
                if (ind % 50_000 == 0 || ind == addRows) {
                    BenchmarkUtils.println("Filling cache[" + cachePerson.getName() + "], " + ind + "/" + addRows + " rows...");
                }

                Pair<String, BinaryObject> personPair = createPerson(ind);
                stmr.addData(personPair.getKey(), personPair.getValue());
            }
        }
        CacheLoaderUtils.validateCache(cachePerson, "Person", addRows);
    }
}

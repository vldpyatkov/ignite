package org.apache.ignite.yardstick.cache.load.schema;

import javafx.util.Pair;
import org.apache.ignite.*;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.yardstickframework.BenchmarkUtils;

import java.util.LinkedHashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Ignite benchmark for performs filling Depost cache.
 */
public class IgniteCacheDepositLoader {

    public static final String CACHE_NAME = "DEPOSIT_DEPOSIT";

    private IgniteCache<String, BinaryObject> cacheDeposit;
    private LinkedHashMap<String, String> depositFields;
    private Ignite ignite  = Ignition.ignite();

    public IgniteCacheDepositLoader() {
        cacheDeposit = ignite.cache(CACHE_NAME);
        if (cacheDeposit == null) throw new IgniteException("Cache [" + CACHE_NAME + "] is not found, check ignite config file");
        depositFields = CacheLoaderUtils.getFields(cacheDeposit);
    }

    public Pair<String, BinaryObject> createDeposit(Integer rowId) {
        BinaryObjectBuilder builder = ignite.binary().builder("Deposit");
        String strId = "" + rowId;

        for (String fieldKey : depositFields.keySet()) {
            String fieldType = depositFields.get(fieldKey);
            String fieldKeyUpper = fieldKey.toUpperCase();

            if ("ID".equals(fieldKey)) {
                builder.setField(fieldKey, strId);

            } else if ("BALANCEF".equals(fieldKey)) {
                continue;
            } else if ("BALANCE".equals(fieldKey)) {
                Object valueRnd = CacheLoaderUtils.getRandomValue(fieldKey, fieldType);
                builder.setField("BALANCE", valueRnd);
                builder.setField("BALANCEF", valueRnd);

            } else if (fieldKeyUpper.contains("PERSON_ID")) {
                int randomDep = ThreadLocalRandom.current().nextInt(1, 4);
                int personId = rowId / randomDep; // one person has many deposits
                builder.setField(fieldKey, "" + personId);
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
        try (IgniteDataStreamer<String, BinaryObject> stmr = ignite.dataStreamer(cacheDeposit.getName())) {
            for (int ind = 1; ind <= addRows; ind++) {
                if (ind % 50_000 == 0 || ind == addRows) {
                    BenchmarkUtils.println("Filling cache[" + cacheDeposit.getName() + "], " + ind + "/" + addRows + " rows...");
                }

                Pair<String, BinaryObject> addressPair = createDeposit(ind);
                stmr.addData(addressPair.getKey(), addressPair.getValue());
            }
        }
        CacheLoaderUtils.validateCache(cacheDeposit, "Deposit", addRows);
    }

}

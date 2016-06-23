package org.apache.ignite.yardstick.cache.load.schema;

import javafx.util.Pair;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Ignite benchmark for performs filling Depost cache.
 */
public class IgniteCacheDepositLoader extends IgniteCacheBaseLoader {

    /** {@inheritDoc} */
    public IgniteCacheDepositLoader(String cacheName, String valueType, int preloadAmount) {
        super(cacheName, valueType, preloadAmount);
    }

    /** {@inheritDoc} */
    @Override public Pair<String, BinaryObject> createBinaryObject(Integer rowId) {
        BinaryObjectBuilder builder = ignite.binary().builder("Deposit");
        String strId = "" + rowId;

        for (String fieldKey : cacheFields.keySet()) {
            String fieldType = cacheFields.get(fieldKey);
            String fieldKeyUpper = fieldKey.toUpperCase();

            if ("ID".equals(fieldKey))
                builder.setField(fieldKey, strId);
            else if ("BALANCEF".equals(fieldKey))
                continue;
            else if ("BALANCE".equals(fieldKey)) {
                Object valueRnd = CacheLoaderUtils.getRandomValue(fieldKey, fieldType);
                builder.setField("BALANCE", valueRnd);
                builder.setField("BALANCEF", valueRnd);

            } else if (fieldKeyUpper.contains("PERSON_ID")) {
                // one person has more then one deposit
                int personId = (ThreadLocalRandom.current().nextDouble() < 0.5d) ? rowId : rowId / 2;
                builder.setField(fieldKey, "" + personId);
            } else if (fieldKeyUpper.contains("_ID"))
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

package org.apache.ignite.yardstick.cache.load.schema;

import javafx.util.Pair;
import org.apache.ignite.*;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.yardstickframework.BenchmarkUtils;

import java.util.LinkedHashMap;

/**
 * Ignite benchmark for performs filling cache.
 */
public abstract class IgniteCacheBaseLoader extends Thread{

    protected IgniteCache<String, BinaryObject> cache;
    protected LinkedHashMap<String, String> cacheFields;
    protected Ignite ignite  = Ignition.ignite();

    protected int preloadAmount;
    protected String cacheName;
    protected String valueType;

    /**
     * Constructor.
     *
     * * @param cacheName cache name.
     * * @param valueType cache value type.
     * * @param preloadAmount amount to add records.
     */
    public IgniteCacheBaseLoader(String cacheName, String valueType, int preloadAmount){
        this.cacheName = cacheName;
        this.valueType = valueType;
        this.preloadAmount = preloadAmount;

        cache = ignite.cache(cacheName);
        if (cache == null) throw new IgniteException("Cache [" + cacheName + "] is not found, check ignite config file");
        cacheFields = CacheLoaderUtils.getFields(cache);
    }

    /**
     * Create binary object.
     */
    abstract Pair<String, BinaryObject> createBinaryObject(Integer rowId);

    /**
     * Generatte and fill cache values.
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
        CacheLoaderUtils.validateCache(cache, valueType, addRows);
    }

    /**
     * Start new Thread
     */
    @Override public void run() {
        fillCache(preloadAmount);
    }
}

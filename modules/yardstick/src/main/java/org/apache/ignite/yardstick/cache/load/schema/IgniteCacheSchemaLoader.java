package org.apache.ignite.yardstick.cache.load.schema;

import javafx.util.Pair;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkUtils;


import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;
import java.util.LinkedHashMap;

/**
 * Ignite benchmark that performs put operations.
 * in the original config files remove nodes contains
 *      GridGainCacheConfiguration
 *      pluginConfigurations
 */
public class IgniteCacheSchemaLoader extends IgniteAbstractBenchmark {

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> map) throws Exception {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);
        preLoading();
    }

    /**
     * Loading values to cache.
     */
    private void preLoading() throws Exception {
        int preloadAmount = args.preloadAmount();
        IgniteCacheAddressLoader addr = new IgniteCacheAddressLoader("CLIENT_ADDRESS", "Address", preloadAmount);
        IgniteCachePersonLoader pers = new IgniteCachePersonLoader("CLIENT_PERSON", "Person", preloadAmount);
        IgniteCacheDepositLoader deps = new IgniteCacheDepositLoader("DEPOSIT_DEPOSIT", "Deposit", preloadAmount);

        addr.start();
        pers.start();
        deps.start();

        addr.join();;
        pers.join();;
        deps.join();;
        BenchmarkUtils.println("preLoading completed");
    }
}


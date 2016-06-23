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

    public static void main(String[] args) {
        String configPath = "modules/yardstick/config/ignite-capitalization-config.xml";

        try (Ignite ignite = Ignition.start(configPath)) {
            IgniteCacheSchemaLoader bm = new IgniteCacheSchemaLoader();
            bm.preLoading();
        } catch (Throwable ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public boolean test(Map<Object, Object> map) throws Exception {
        return false;
    }

    @Override
    public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);
        preLoading();
    }

    private void preLoading() throws Exception {
        final int preloadAmount = args.preloadAmount();
        final IgniteCacheAddressLoader addr = new IgniteCacheAddressLoader();
        final IgniteCachePersonLoader pers = new IgniteCachePersonLoader();
        final IgniteCacheDepositLoader deps = new IgniteCacheDepositLoader();

        Thread addrThread = new Thread() {
            @Override public void run() {
                addr.fillCache(preloadAmount);
            }
        };
        addrThread.start();

        Thread persThread = new Thread() {
            @Override public void run() {
                pers.fillCache(preloadAmount);
            }
        };
        persThread.start();

        Thread depsThread = new Thread() {
            @Override public void run() {
                deps.fillCache(preloadAmount);
            }
        };
        depsThread.start();

        addrThread.join();;
        persThread.join();;
        depsThread.join();;
        BenchmarkUtils.println("preLoading completed");
    }
}


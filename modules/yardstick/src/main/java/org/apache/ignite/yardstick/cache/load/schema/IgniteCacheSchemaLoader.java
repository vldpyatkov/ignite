/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.yardstick.cache.load.schema;

import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkUtils;
import java.util.Map;

/**
 * Ignite benchmark that performs put operations.
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

        addr.join();
        pers.join();
        deps.join();

        BenchmarkUtils.println("preLoading completed");
    }
}

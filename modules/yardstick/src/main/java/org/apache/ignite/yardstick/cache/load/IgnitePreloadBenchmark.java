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

package org.apache.ignite.yardstick.cache.load;

import java.math.BigDecimal;
import java.util.Map;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.yardstickframework.BenchmarkConfiguration;

/**
 */
public class IgnitePreloadBenchmark extends IgniteAbstractBenchmark {

    /** Person cache name. */
    static final String PERSON_CACHE = "CLIENT_PERSON";

    /** Deposit cache name. */
    private static final String DEPOSIT_CACHE = "DEPOSIT_DEPOSIT";

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        //TODO: Remove it.
        preLoading();
    }

    /**
     * @throws Exception If fail.
     */
    private void preLoading() throws Exception {

        Thread preloadPerson = new Thread() {
            @Override public void run() {
                setName("preloadPerson");

                try (IgniteDataStreamer dataLdr = ignite().dataStreamer(PERSON_CACHE)) {
                    for (int i = 0; i < args.preloadAmount() && !isInterrupted(); i++) {
                        String clientKey = "clientId=" + i;
                        dataLdr.addData(clientKey, createPerson(clientKey, i));
                    }
                }
            }
        };

        preloadPerson.start();

        Thread preloadDeposit = new Thread() {
            @Override public void run() {
                setName("preloadDeposit");

                try (IgniteDataStreamer dataLdr = ignite().dataStreamer(DEPOSIT_CACHE)) {
                    for (int i = 0; i < args.preloadAmount() && !isInterrupted(); i++) {
                        int personId = nextRandom(args.preloadAmount());

                        String clientKey = "clientId=" + personId;

                        String key = clientKey + "&depositId=" + i;

                        dataLdr.addData(key, createDeposit(key, clientKey, i));
                    }
                }
            }
        };

        preloadDeposit.start();

        preloadDeposit.join();
        preloadPerson.join();

    }

    /**
     * @param id Identifier.
     * @param num Num.
     * @return Person entity as binary object.
     */
    private BinaryObject createPerson(String id, int num) {
        BinaryObject client = ignite().binary()
            .builder("Person")
            .setField("ID", id)
            .setField("FIRSTNAME", "First name " + id)
            .setField("SECONDNAME", "Second name " + id)
            .setField("EMAIL", "client" + id + "@mail.org")
            .build();

        return client;
    }

    /**
     * @param id Identifier.
     * @param clientId Key.
     * @param num Num.
     * @return Deposit entity as binary object.
     */
    private BinaryObject createDeposit(String id, String clientId, int num) {
        double startBalance = 100 + nextRandom(100) / 1.123;

        BinaryObject deposit = ignite().binary()
            .builder("Deposit")
            .setField("ID", id)
            .setField("PERSON_ID", clientId)
            .setField("ACCOUNT", num)
            .setField("BALANCE", new BigDecimal(startBalance))
            .setField("DEPOSITRATE", new BigDecimal(0.1))
            .setField("BALANCEF", new BigDecimal(startBalance))
            .build();

        return deposit;
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> map) throws Exception {
        return false;
    }
}

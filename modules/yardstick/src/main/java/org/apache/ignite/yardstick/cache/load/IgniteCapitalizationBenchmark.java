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
import java.util.Date;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.apache.ignite.yardstick.IgniteBenchmarkUtils;
import org.yardstickframework.BenchmarkConfiguration;

import javax.cache.Cache;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.yardstickframework.BenchmarkUtils;

/**
 * Ignite capitalization benchmark.
 * Topology should NOT be changed during benchmark processing.
 */
public class IgniteCapitalizationBenchmark extends IgniteAbstractBenchmark {

    /** Person cache name. */
    static final String PERSON_CACHE = "CLIENT_PERSON";

    /** Deposit cache name. */
    private static final String DEPOSIT_CACHE = "DEPOSIT_DEPOSIT";

    /** History of operation over deposit. */
    private static final String DEPOSIT_HISTORY_CACHE = "DEPOSIT_DEPOHIST";

    /** Count of capitalization operations by deposit. */
    private static final String DEPOSIT_OPERATION_COUNT_SQL = "SELECT COUNT(*) FROM \"" + DEPOSIT_HISTORY_CACHE
        + "\".Operation WHERE DEPOSIT_ID=?";

    /** Find deposit SQL query. */
    static final String FIND_DEPOSIT_SQL = "SELECT _key FROM \"" + DEPOSIT_CACHE + "\".Deposit WHERE PERSON_ID=?";

    /** Distribution of partitions by nodes. */
    private Map<UUID, List<Integer>> partitionsMap;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        partitionsMap = personCachePartitions();

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
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        ScanQueryBroadcastClosure c = new ScanQueryBroadcastClosure(partitionsMap);

        ClusterGroup clusterGrp = ignite().cluster().forNodeIds(partitionsMap.keySet());

        IgniteCompute compute = ignite().compute(clusterGrp);

        compute.broadcast(c);


        return true;
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        try {
            BenchmarkUtils.println("Checking deposits");

            IgniteCache<String, BinaryObject> cache = ignite().cache(DEPOSIT_CACHE).withKeepBinary();

            for (Cache.Entry<String, BinaryObject> entry: cache) {
                BinaryObject deposit = entry.getValue();

                String depositKey = entry.getKey();

                BigDecimal startBalance = deposit.field("BALANCEF");

                BigDecimal balance = deposit.field("BALANCE");

                BigDecimal rate = deposit.field("DEPOSITRATE");

                BigDecimal expectedBalance;

                SqlFieldsQuery findDepositHistory = new SqlFieldsQuery(DEPOSIT_OPERATION_COUNT_SQL);

                try (QueryCursor cursor1 = cache.query(findDepositHistory.setArgs(depositKey))) {
                    Long count = (Long)((ArrayList)cursor1.iterator().next()).get(0);

                    expectedBalance = startBalance.multiply(rate.add(BigDecimal.ONE).pow(count.intValue()));
                }

                expectedBalance.setScale(2, BigDecimal.ROUND_DOWN);
                balance.setScale(2, BigDecimal.ROUND_DOWN);

                if (!expectedBalance.equals(balance))
                    BenchmarkUtils.println("Deposit " + depositKey + " has incorrect balace "
                        + balance + " when expected " + expectedBalance);

            }

            BenchmarkUtils.println("Deposits checked");
        }
        finally {
            super.tearDown();
        }
    }

    /**
     * Building a map that contains mapping of node ID to a list of partitions stored on the node.
     *
     * @param cacheName Name of Ignite cache.
     * @return Node to partitions map.
     */
    private Map<UUID, List<Integer>> personCachePartitions() {
        // Getting affinity for person cache.
        Affinity affinity = ignite().affinity(PERSON_CACHE);

        // Building a list of all partitions numbers.
        List<Integer> partitionNumbers = new ArrayList<>(affinity.partitions());

        for (int i = 0; i < affinity.partitions(); i++)
            partitionNumbers.add(i);

        // Getting partition to node mapping.
        Map<Integer, ClusterNode> partPerNodes = affinity.mapPartitionsToNodes(partitionNumbers);

        // Building node to partitions mapping.
        Map<UUID, List<Integer>> nodesToPart = new HashMap<>();

        for (Map.Entry<Integer, ClusterNode> entry : partPerNodes.entrySet()) {
            List<Integer> nodeParts = nodesToPart.get(entry.getValue().id());

            if (nodeParts == null) {
                nodeParts = new ArrayList<>();

                nodesToPart.put(entry.getValue().id(), nodeParts);
            }
            nodeParts.add(entry.getKey());
        }

        return nodesToPart;
    }

    /**
     * Closure for scan query executing.
     */
    private static class ScanQueryBroadcastClosure implements IgniteRunnable {
        /**
         * Ignite node.
         */
        @IgniteInstanceResource
        private Ignite node;

        /**
         * Information about partition.
         */
        private Map<UUID, List<Integer>> cachePart;

        /**
         * @param cachePart Partition by node for Ignite cache.
         */
        private ScanQueryBroadcastClosure(Map<UUID, List<Integer>> cachePart) {
            this.cachePart = cachePart;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            IgniteCache cache = node.cache(PERSON_CACHE).withKeepBinary();

            // Getting a list of the partitions owned by this node.
            List<Integer> myPartitions = cachePart.get(node.cluster().localNode().id());

            for (Integer part : myPartitions) {
                ScanQuery scanQry = new ScanQuery();

                scanQry.setPartition(part);

                try (QueryCursor<Cache.Entry<String, BinaryObject>> cursor = cache.query(scanQry)) {
                    for (Cache.Entry<String, BinaryObject> entry : cursor) {
                        String clientId = entry.getKey();

                        IgniteCache<String, BinaryObject> depositCache = node.cache(DEPOSIT_CACHE).withKeepBinary();

                        SqlFieldsQuery findDepositQuery = new SqlFieldsQuery(FIND_DEPOSIT_SQL).setLocal(true);

                        try (QueryCursor cursor1 = depositCache.query(findDepositQuery.setArgs(clientId))) {
                            for (Object obj : cursor1) {
                                List<String> depositIds = (List<String>)obj;
                                for (String depositId: depositIds)
                                    try {
                                        updateDeposit(depositCache, depositId);
                                    }
                                    catch (Exception e) {
                                        BenchmarkUtils.error("Can not update deposit " + depositId, e);
                                    }
                            }
                        }
                    }
                }
            }
        }

        /**
         * @param depositCache Ignite cache of deposit.
         * @param depositKey Key of deposit.
         * @throws Exception If failed.
         */
        private void updateDeposit(final IgniteCache<String, BinaryObject> depositCache, final String depositKey)
            throws Exception {
            final IgniteCache historyCache = node.cache(DEPOSIT_HISTORY_CACHE).withKeepBinary();

            IgniteBenchmarkUtils.doInTransaction(node.transactions(), TransactionConcurrency.PESSIMISTIC,
                TransactionIsolation.REPEATABLE_READ, new IgniteCallable<Object>() {
                    @Override public Object call() throws Exception {
                        BinaryObject deposit = depositCache.get(depositKey);

                        BigDecimal amount = deposit.field("BALANCE");
                        BigDecimal rate = deposit.field("DEPOSITRATE");

                        BigDecimal newBalance = amount.multiply(rate.add(BigDecimal.ONE));

                        deposit = deposit.toBuilder()
                            .setField("BALANCE", newBalance)
                            .build();

                        String depositHistoryKey = depositKey + "&histId=" + System.nanoTime();

                        BinaryObject depositHistoryRow = node.binary().builder("Operation")
                            .setField("ID", depositHistoryKey)
                            .setField("DEPOSIT_ID", depositKey)
                            .setField("OPDAY", new Date())
                            .setField("BALANCE", newBalance)
                            .build();

                        historyCache.put(depositHistoryKey, depositHistoryRow);

                        depositCache.put(depositKey, deposit);

                        return null;
                    }
                });
        }
    }
}

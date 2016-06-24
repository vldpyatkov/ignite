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

package org.apache.ignite.yardstick.cache.loader;

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
                Object valueRnd = getRandomValue(fieldKey, fieldType);
                builder.setField("BALANCE", valueRnd);
                builder.setField("BALANCEF", valueRnd);

            }
            else if (fieldKeyUpper.contains("PERSON_ID")) {
                // one person has more then one deposit
                int personId = (ThreadLocalRandom.current().nextDouble() < 0.5d) ? rowId : rowId / 2;
                builder.setField(fieldKey, "" + personId);
            }
            else if (fieldKeyUpper.contains("_ID"))
                builder.setField(fieldKey, strId);
            else {
                Object valueRnd = getRandomValue(fieldKey, fieldType);
                builder.setField(fieldKey, valueRnd);
            }
        }

        Pair<String, BinaryObject> pair = new Pair(strId, builder.build());
        return pair;
    }
}

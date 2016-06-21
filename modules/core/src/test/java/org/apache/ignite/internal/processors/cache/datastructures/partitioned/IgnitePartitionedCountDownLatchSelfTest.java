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

package org.apache.ignite.internal.processors.cache.datastructures.partitioned;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteCountDownLatchAbstractSelfTest;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 *
 */
public class IgnitePartitionedCountDownLatchSelfTest extends IgniteCountDownLatchAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected CacheMode atomicsCacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override public void testLatch() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-1793");
    }

    public void testLatchMultinode111() throws Exception {
        for (int i=0; i<5000; i++)
            testLatchMultinode11();
    }

    public void testLatchMultinode11() throws Exception {

        if (gridCount() == 1)
            return;

        IgniteCountDownLatch latch = grid(0).countDownLatch("l1", 10,
            true,
            true);

        for (int i = 0; i < gridCount(); i++) {
            final Ignite ignite = grid(i);

            latch = ignite.countDownLatch("l1", 10,
                true,
                false);

            assertNotNull(latch);
        }

        for (int i = 0; i < 10; i++)
            latch.countDown();

        latch.close();

        IgniteCountDownLatch latch2 = grid(0).countDownLatch("l2", 10,
            true,
            true);

        for (int i = gridCount() - 1; i >= 0; i--) {
            final Ignite ignite = grid(i);

            latch2 = ignite.countDownLatch("l2", 10,
                true,
                false);

            assertNotNull(latch);
        }

        for (int i = 0; i < 10; i++)
            latch2.countDown();

        latch2.close();

    }
}

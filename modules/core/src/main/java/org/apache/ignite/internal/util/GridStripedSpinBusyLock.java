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

package org.apache.ignite.internal.util;

import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.thread.IgniteThread;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * Striped spin busy lock. Aimed to provide efficient "read" lock semantics while still maintaining safety when
 * entering "busy" state.
 */
public class GridStripedSpinBusyLock {
    /** Writer mask. */
    private static int WRITER_MASK = 1 << 30;

    /** Default amount of stripes. */
    private static final int DFLT_STRIPE_CNT = Runtime.getRuntime().availableProcessors() * 4;

    /** Thread index generator. */
    private static final AtomicInteger THREAD_IDX_GEN = new AtomicInteger();

    /** Thread index. */
    private static final ThreadLocal<Integer> THREAD_IDX = new ThreadLocal<Integer>() {
        @Override protected Integer initialValue() {
            return THREAD_IDX_GEN.incrementAndGet();
        }
    };

    /** Amount of stripes. */
    private final int stripeCnt;

    /** States. */
    private final AtomicIntegerArray states;

    /**
     * Default constructor.
     */
    public GridStripedSpinBusyLock() {
        this(DFLT_STRIPE_CNT);
    }

    /**
     * Constructor.
     *
     * @param stripeCnt Amount of stripes.
     */
    public GridStripedSpinBusyLock(int stripeCnt) {
        A.ensure(stripeCnt > 0, "stripeCnt > 0");

        this.stripeCnt = stripeCnt;

        // Each state must be located 64 bytes from the other to avoid false sharing.
        states = new AtomicIntegerArray(adjusted(stripeCnt));
    }

    /**
     * Enter busy state.
     *
     * @return {@code True} if entered busy state.
     */
    public boolean enterBusy() {
        int val = states.incrementAndGet(index());

        if ((val & WRITER_MASK) == WRITER_MASK) {
            leaveBusy();

            return false;
        }
        else
            return true;
    }

    /**
     * Leave busy state.
     */
    public void leaveBusy() {
        states.decrementAndGet(index());
    }

    /**
     * Block.
     */
    public void block() {
        // 1. CAS-loop to set a writer bit.
        for (int i = 0; i < stripeCnt; i++) {
            int idx = adjusted(i);

            while (true) {
                int oldVal = states.get(idx);

                if (states.compareAndSet(idx, oldVal, oldVal | WRITER_MASK))
                    break;
            }
        }

        // 2. Wait until all readers are out.
        boolean interrupt = false;

        for (int i = 0; i < stripeCnt; i++) {
            int idx = adjusted(i);

            while (states.get(idx) != WRITER_MASK) {
                try {
                    Thread.sleep(10);
                }
                catch (InterruptedException e) {
                    interrupt = true;
                }
            }
        }

        if (interrupt)
            Thread.currentThread().interrupt();
    }

    /**
     * Get index for the given thread.
     *
     * @return Index for the given thread.
     */
    private int index() {
        Thread t = Thread.currentThread();

        if (t instanceof IgniteThread) {
            int idx = ((IgniteThread) t).groupIndex();

            if (idx != IgniteThread.GRP_IDX_UNASSIGNED)
                return idx;
        }

        return adjusted(THREAD_IDX.get() % stripeCnt);
    }

    /**
     * Gets value adjusted for striping.
     *
     * @param val Value.
     * @return Value.
     */
    private static int adjusted(int val) {
        return val << 4;
    }
}
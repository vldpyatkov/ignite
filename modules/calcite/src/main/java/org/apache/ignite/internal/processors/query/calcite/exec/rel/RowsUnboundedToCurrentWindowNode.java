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

package org.apache.ignite.internal.processors.query.calcite.exec.rel;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.function.Supplier;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AccumulatorWrapper;
import org.apache.ignite.internal.util.typedef.F;

public class RowsUnboundedToCurrentWindowNode<Row> extends AbstractNode<Row>
    implements SingleNode<Row>, Downstream<Row> {
    private final ImmutableBitSet partKeys;
    private final Supplier<List<AccumulatorWrapper<Row>>> accFactory;
    private final RowFactory<Row> rowFactory;

    private List<AccumulatorWrapper<Row>> accs;
    private Object[] curPartKey;

    private final Deque<Row> outBuf = new ArrayDeque<>(IN_BUFFER_SIZE);

    private int requested;
    private int waiting;
    private boolean inLoop;

    public RowsUnboundedToCurrentWindowNode(
        ExecutionContext<Row> ctx,
        RelDataType rowType,
        ImmutableBitSet partKeys,
        Supplier<List<AccumulatorWrapper<Row>>> accFactory,
        RowFactory<Row> rowFactory
    ) {
        super(ctx, rowType);
        this.partKeys = partKeys;
        this.accFactory = accFactory;
        this.rowFactory = rowFactory;
    }

    @Override public void request(int rowsCnt) throws Exception {
        assert !F.isEmpty(sources()) && sources().size() == 1;
        assert rowsCnt > 0 && requested == 0;

        checkState();

        requested = rowsCnt;

        if (waiting == 0)
            source().request(waiting = IN_BUFFER_SIZE);
        else if (!inLoop)
            context().execute(this::flush, this::onError);
    }

    @Override public void push(Row row) throws Exception {
        assert downstream() != null;
        assert waiting > 0;

        checkState();

        waiting--;

        RowHandler<Row> hnd = context().rowHandler();

        Object[] newKey = extractKey(hnd, row, partKeys);

        if (curPartKey == null || !equalsKey(curPartKey, newKey)) {
            curPartKey = newKey;
            accs = accFactory.get();
        }

        for (AccumulatorWrapper<Row> acc : accs)
            acc.add(row);

        Object[] out = new Object[hnd.columnCount(row) + accs.size()];

        for (int i = 0; i < hnd.columnCount(row); i++)
            out[i] = hnd.get(i, row);

        int base = hnd.columnCount(row);
        for (int i = 0; i < accs.size(); i++)
            out[base + i] = accs.get(i).end();

        outBuf.add(rowFactory.create(out));

        flush();
    }

    @Override public void end() throws Exception {
        assert downstream() != null;
        assert waiting > 0;

        checkState();

        waiting = -1;

        flush();
    }

    @Override protected Downstream<Row> requestDownstream(int idx) {
        if (idx != 0)
            throw new IndexOutOfBoundsException();

        return this;
    }

    @Override protected void rewindInternal() {
        requested = 0;
        waiting = 0;
        inLoop = false;
        outBuf.clear();
        accs = null;
        curPartKey = null;
    }

    private void flush() throws Exception {
        if (isClosed())
            return;

        inLoop = true;
        try {
            while (requested > 0 && !outBuf.isEmpty()) {
                checkState();

                requested--;
                downstream().push(outBuf.remove());
            }
        }
        finally {
            inLoop = false;
        }

        if (outBuf.isEmpty() && waiting == 0)
            source().request(waiting = IN_BUFFER_SIZE);

        if (waiting == -1 && requested > 0 && outBuf.isEmpty()) {
            requested = 0;
            downstream().end();
        }
    }

    private Object[] extractKey(RowHandler<Row> hnd, Row row, ImmutableBitSet keys) {
        Object[] res = new Object[keys.cardinality()];
        int i = 0;

        for (int k = keys.nextSetBit(0); k >= 0; k = keys.nextSetBit(k + 1))
            res[i++] = hnd.get(k, row);

        return res;
    }

    private static boolean equalsKey(Object[] a, Object[] b) {
        if (a.length != b.length)
            return false;

        for (int i = 0; i < a.length; i++) {
            Object x = a[i];
            Object y = b[i];

            if (x == null ? y != null : !x.equals(y))
                return false;
        }

        return true;
    }
}

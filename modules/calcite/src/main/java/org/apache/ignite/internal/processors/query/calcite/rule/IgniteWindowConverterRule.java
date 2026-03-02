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

package org.apache.ignite.internal.processors.query.calcite.rule;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteWindow;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;

import static org.apache.calcite.rel.RelFieldCollation.Direction.ASCENDING;
import static org.apache.calcite.rel.RelFieldCollation.NullDirection.LAST;

public class IgniteWindowConverterRule extends AbstractIgniteConverterRule<LogicalWindow> {
    public static final IgniteWindowConverterRule INSTANCE = new IgniteWindowConverterRule();

    private IgniteWindowConverterRule() {
        super(LogicalWindow.class, "IgniteWindowConverterRule");
    }

    @Override public boolean matches(RelOptRuleCall call) {
        LogicalWindow win = call.rel(0);

        if (win.groups.size() != 1)
            return false;

        LogicalWindow.Group grp = win.groups.get(0);

        return isRowsUnboundedToCurrent(grp);
    }

    @Override public PhysicalNode convert(RelOptPlanner planner, RelMetadataQuery mq, LogicalWindow win) {
        RelOptCluster cluster = win.getCluster();
        RelNode input = win.getInput();

        for (int grpIdx = 0; grpIdx < win.groups.size(); grpIdx++) {
            LogicalWindow.Group grp = win.groups.get(grpIdx);

            RelCollation collation = buildWindowCollation(grp);

            RelTraitSet desiredTraits = cluster
                .traitSetOf(IgniteConvention.INSTANCE)
                .replace(IgniteDistributions.single())
                .replace(collation);

            RelDataType dataType = buildWindowRowType(cluster.getTypeFactory(), input, grp);

            input = new IgniteWindow(
                win.getCluster(),
                win.getTraitSet().merge(desiredTraits),
                convert(win.getInput(), desiredTraits),
                win.getConstants(),
                dataType,
                grp
            );
        }

        return (PhysicalNode) input;
    }

    /**
     * Builds a row type for a window with aggregate calls.
     *
     * @param typeFactory Type factory.
     * @param input Input relation.
     * @param grp Window group.
     * @return A row type combining the input fields and windowed aggregate results.
     */
    private static RelDataType buildWindowRowType(
        RelDataTypeFactory typeFactory,
        RelNode input,
        LogicalWindow.Group grp
    ) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();

        builder.addAll(input.getRowType().getFieldList());

        for (int i = 0; i < grp.aggCalls.size(); i++) {
            Window.RexWinAggCall winAggCall = grp.aggCalls.get(i);

            String name = "agg$" + i;

            RelDataType type = winAggCall.getType();

            builder.add(name, type);
        }

        return builder.build();
    }

    private static boolean isRowsUnboundedToCurrent(LogicalWindow.Group grp) {
        return isUnboundedPreceding(grp.lowerBound) && isCurrentRow(grp.upperBound);
    }

    /**
     * Lower frame bound defaults to UNBOUNDED PRECEDING when omitted.
     */
    private static boolean isUnboundedPreceding(org.apache.calcite.rex.RexWindowBound bnd) {
        return bnd == null || (bnd.isUnbounded() && bnd.isPreceding());
    }

    /**
     * Upper frame bound defaults to CURRENT ROW when omitted.
     */
    private static boolean isCurrentRow(org.apache.calcite.rex.RexWindowBound bnd) {
        return bnd == null || bnd.isCurrentRow();
    }

    private static RelCollation buildWindowCollation(LogicalWindow.Group grp) {
        List<RelFieldCollation> fields = new ArrayList<>();

        ImmutableBitSet keys = grp.keys;

        for (int k = keys.nextSetBit(0); k >= 0; k = keys.nextSetBit(k + 1))
            fields.add(new RelFieldCollation(k, ASCENDING, LAST));

        fields.addAll(grp.orderKeys.getFieldCollations());

        return RelCollations.of(fields);
    }
}

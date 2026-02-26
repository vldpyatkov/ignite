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
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteWindow;

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

        LogicalWindow.Group grp = win.groups.get(0);

        RelCollation collation = buildWindowCollation(grp);

        RelTraitSet desiredTraits = cluster
            .traitSetOf(IgniteConvention.INSTANCE)
//            .replace(IgniteDistributions.single())
            .replace(collation);

        return new IgniteWindow(
            win.getCluster(),
            win.getTraitSet().merge(desiredTraits),
            convert(win.getInput(), desiredTraits),
            win.getConstants(),
            win.getRowType(),
            win.groups
        );
    }

    private static boolean isRowsUnboundedToCurrent(LogicalWindow.Group grp) {
        return grp.isRows
            && grp.lowerBound != null && grp.lowerBound.isUnbounded() && grp.lowerBound.isPreceding()
            && grp.upperBound != null && grp.upperBound.isCurrentRow();
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

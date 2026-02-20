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

package org.apache.ignite.internal.processors.query.calcite.rule.logical;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.immutables.value.Value;

/**
 * Rewrites {@link LogicalWindow} with unbounded OVER() into
 * (Aggregate over all rows) + (Cross Join) + (Project).
 */
@Value.Enclosing
public class IgniteLogicalWindowRewriteRule extends RelRule<IgniteLogicalWindowRewriteRule.Config> {
    /** Rule instance. */
    public static final RelOptRule INSTANCE = new IgniteLogicalWindowRewriteRule(Config.DEFAULT);

    /**
     * Constructor.
     *
     * @param config Rule configuration.
     */
    private IgniteLogicalWindowRewriteRule(Config config) {
        super(config);
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        LogicalWindow win = call.rel(0);

        if (win.groups.size() != 1) {
            throw new IgniteSQLException("Only a single window group is supported.",
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
        }

        LogicalWindow.Group group = win.groups.get(0);

        validateSupported(group);

        RelNode input = win.getInput();
        RexBuilder rexBuilder = win.getCluster().getRexBuilder();

        List<AggregateCall> aggCalls = new ArrayList<>(group.aggCalls.size());

        for (Window.RexWinAggCall winAggCall : group.aggCalls) {
            aggCalls.add(toAggregateCall(winAggCall));
        }

        RelNode agg = LogicalAggregate.create(
            input,
            ImmutableBitSet.of(),
            null,
            aggCalls
        );

        RexNode condition = rexBuilder.makeLiteral(true);

        RelNode join = LogicalJoin.create(
            input,
            agg,
            Collections.emptyList(),
            condition,
            Collections.emptySet(),
            JoinRelType.INNER
        );

        // 5) Project: input fields + aggregates.
        RelNode project = LogicalProject.create(
            join,
            List.of(),
            buildProjection(join, win),
            win.getRowType().getFieldNames(),
            ImmutableSet.of()
        );

        call.transformTo(project);
    }

    /** */
    private void validateSupported(LogicalWindow.Group group) {
        if (!group.keys.isEmpty()) {
            throw new IgniteSQLException("PARTITION BY in OVER() is not supported yet.",
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
        }

        if (!group.orderKeys.getKeys().isEmpty()) {
            throw new IgniteSQLException("ORDER BY in OVER() is not supported yet.",
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
        }

        if (!isUnbounded(group.lowerBound, group.upperBound)) {
            throw new IgniteSQLException("Window frame bounds are not supported yet.",
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
        }
    }

    private static boolean isUnbounded(RexWindowBound lower, RexWindowBound upper) {
        if (lower == null && upper == null)
            return true;

        if (lower == null || upper == null)
            return false;

        return lower.isUnbounded() && lower.isPreceding()
            && upper.isUnbounded() && upper.isFollowing();
    }

    private static AggregateCall toAggregateCall(Window.RexWinAggCall winAggCall) {
        List<RexNode> rexList = new ArrayList<>(winAggCall.getOperands().size());
        List<Integer> argList = new ArrayList<>(winAggCall.getOperands().size());

        for (RexNode operand : winAggCall.getOperands()) {
            if (operand instanceof RexInputRef) {
                RexInputRef ref = (RexInputRef)operand;
                rexList.add(ref);
                argList.add(ref.getIndex());
            }
            else {
                throw new IgniteSQLException("Window aggregate arguments must be input references.",
                    IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
            }
        }

        SqlAggFunction agg = (SqlAggFunction)winAggCall.getOperator();

        return AggregateCall.create(
            agg,
            false, // winAggCall.isDistinct(),
            false,
            false,
            rexList,
            argList,
            -1,
            null,
            RelCollations.EMPTY,
            winAggCall.getType(),
            null
        );
    }

    private static List<RexNode> buildProjection(RelNode join, LogicalWindow win) {
        RexBuilder rexBuilder = win.getCluster().getRexBuilder();
        int inputFieldCnt = win.getInput().getRowType().getFieldCount();
        int aggFieldCnt = join.getRowType().getFieldCount() - inputFieldCnt;

        List<RexNode> projects = new ArrayList<>(inputFieldCnt + aggFieldCnt);

        for (int i = 0; i < inputFieldCnt; i++)
            projects.add(rexBuilder.makeInputRef(join, i));

        for (int i = 0; i < aggFieldCnt; i++)
            projects.add(rexBuilder.makeInputRef(join, inputFieldCnt + i));

        return projects;
    }

    /** Rule configuration. */
    @SuppressWarnings("ClassNameSameAsAncestorName")
    @Value.Immutable
    public interface Config extends RuleFactoryConfig<Config> {
        /** Default configuration. */
        Config DEFAULT = ImmutableIgniteLogicalWindowRewriteRule.Config.builder()
            .withRuleFactory(IgniteLogicalWindowRewriteRule::new)
            .withDescription("IgniteLogicalWindowRewriteRule: rewrites LogicalWindow to LogicalAggregate LogicalJoin LogicalProject")
            .withOperandSupplier(b -> {
                return b.operand(LogicalWindow.class).anyInputs();
            })
            .build();

        @Override @Value.Default
        default java.util.function.Function<Config, RelOptRule> ruleFactory() {
            return IgniteLogicalWindowRewriteRule::new;
        }
    }
}

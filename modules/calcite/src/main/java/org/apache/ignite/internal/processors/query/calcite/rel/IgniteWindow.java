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

package org.apache.ignite.internal.processors.query.calcite.rel;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;

/**
 * Physical window operator for Ignite.
 */
public class IgniteWindow extends Window implements IgniteRel {
    /** */
    private final Group grp;

    public IgniteWindow(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        List<RexLiteral> constants,
        RelDataType rowType,
        Group grp
    ) {
        super(cluster, traitSet, input, constants, rowType, List.of(grp));

        this.grp = grp;
    }

    /** {@inheritDoc} */
    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new IgniteWindow(getCluster(), traitSet, sole(inputs), constants, rowType, grp);
    }

    /** {@inheritDoc} */
    @Override public Window copy(List<RexLiteral> constants) {
        return new IgniteWindow(
            getCluster(),
            getTraitSet(),
            getInput(),
            constants,
            rowType,
            grp
        );
    }

    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteWindow(
            cluster,
            getTraitSet(),
            sole(inputs),
            constants,
            rowType,
            grp
        );
    }

    @Override public Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(RelTraitSet required) {
        if (required.getConvention() != IgniteConvention.INSTANCE)
            return null;

        // Требуем корректное распределение по PARTITION BY
        IgniteDistribution requiredDistr = TraitUtils.distribution(required);
        IgniteDistribution expectedDistr = expectedDistribution();

        if (!expectedDistr.equals(requiredDistr))
            return null;

        // Требуем collation: PARTITION BY + ORDER BY
        RelCollation requiredColl = TraitUtils.collation(required);
        RelCollation expectedColl = expectedCollation();

        if (!requiredColl.satisfies(expectedColl))
            return null;

        return Pair.of(required, List.of(required));
    }

    @Override public Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(RelTraitSet childTraits, int childId) {
        assert childId == 0;

        if (childTraits.getConvention() != IgniteConvention.INSTANCE)
            return null;

        IgniteDistribution childDistr = TraitUtils.distribution(childTraits);
        IgniteDistribution expectedDistr = expectedDistribution();

        if (!expectedDistr.equals(childDistr))
            return null;

        RelCollation childColl = TraitUtils.collation(childTraits);
        RelCollation expectedColl = expectedCollation();

        if (!childColl.satisfies(expectedColl))
            return null;

        return Pair.of(childTraits, List.of(childTraits));
    }

    private IgniteDistribution expectedDistribution() {
        Group grp = groups.get(0);

        if (grp.keys.isEmpty())
            return IgniteDistributions.single();

        return IgniteDistributions.hash(grp.keys.asList());
    }

    private RelCollation expectedCollation() {
        Group grp = groups.get(0);

        List<RelFieldCollation> fields = new ArrayList<>();

        for (int k = grp.keys.nextSetBit(0); k >= 0; k = grp.keys.nextSetBit(k + 1))
            fields.add(new RelFieldCollation(k));

        fields.addAll(grp.orderKeys.getFieldCollations());

        return RelCollations.of(fields);
    }

    public Group group() {
        return grp;
    }
}

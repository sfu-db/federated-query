package ai.dataprep.federated;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.apache.calcite.plan.RelOptUtil.conjunctions;

public abstract class FilterJoinRulePatch<C extends FilterJoinRule.Config> extends FilterJoinRule<C> {
    /**
     * Creates a FilterJoinRule.
     *
     * @param config
     */
    protected FilterJoinRulePatch(C config) {
        super(config);
    }

    protected void perform(RelOptRuleCall call, @Nullable Filter filter,
                           Join join) {
        final List<RexNode> joinFilters =
                RelOptUtil.conjunctions(join.getCondition());
        final List<RexNode> origJoinFilters = ImmutableList.copyOf(joinFilters);

        // If there is only the joinRel,
        // make sure it does not match a cartesian product joinRel
        // (with "true" condition), otherwise this rule will be applied
        // again on the new cartesian product joinRel.
        if (filter == null && joinFilters.isEmpty()) {
            return;
        }

        final List<RexNode> aboveFilters =
                filter != null
                        ? getConjunctions(filter)
                        : new ArrayList<>();
        final ImmutableList<RexNode> origAboveFilters =
                ImmutableList.copyOf(aboveFilters);

        // Simplify Outer Joins
        JoinRelType joinType = join.getJoinType();
        if (config.isSmart()
                && !origAboveFilters.isEmpty()
                && join.getJoinType() != JoinRelType.INNER) {
            joinType = RelOptUtil.simplifyJoin(join, origAboveFilters, joinType);
        }

        final List<RexNode> leftFilters = new ArrayList<>();
        final List<RexNode> rightFilters = new ArrayList<>();

        // TODO - add logic to derive additional filters.  E.g., from
        // (t1.a = 1 AND t2.a = 2) OR (t1.b = 3 AND t2.b = 4), you can
        // derive table filters:
        // (t1.a = 1 OR t1.b = 3)
        // (t2.a = 2 OR t2.b = 4)

        // Try to push down above filters. These are typically where clause
        // filters. They can be pushed down if they are not on the NULL
        // generating side.
        boolean filterPushed = false;
        if (RelOptUtil.classifyFilters(
                join,
                aboveFilters,
                joinType.canPushIntoFromAbove(),
                joinType.canPushLeftFromAbove(),
                joinType.canPushRightFromAbove(),
                joinFilters,
                leftFilters,
                rightFilters)) {
            filterPushed = true;
        }

        // Move join filters up if needed
        validateJoinFilters(aboveFilters, joinFilters, join, joinType);

        // If no filter got pushed after validate, reset filterPushed flag
        if (leftFilters.isEmpty()
                && rightFilters.isEmpty()
                && joinFilters.size() == origJoinFilters.size()
                && aboveFilters.size() == origAboveFilters.size()) {
            if (Sets.newHashSet(joinFilters)
                    .equals(Sets.newHashSet(origJoinFilters))) {
                filterPushed = false;
            }
        }

        // Try to push down filters in ON clause. A ON clause filter can only be
        // pushed down if it does not affect the non-matching set, i.e. it is
        // not on the side which is preserved.

        // Anti-join on conditions can not be pushed into left or right, e.g. for plan:
        //
        //     Join(condition=[AND(cond1, $2)], joinType=[anti])
        //     :  - prj(f0=[$0], f1=[$1], f2=[$2])
        //     :  - prj(f0=[$0])
        //
        // The semantic would change if join condition $2 is pushed into left,
        // that is, the result set may be smaller. The right can not be pushed
        // into for the same reason.
        if (RelOptUtil.classifyFilters(
                join,
                joinFilters,
                false,
                joinType.canPushLeftFromWithin(),
                joinType.canPushRightFromWithin(),
                joinFilters,
                leftFilters,
                rightFilters)) {
            filterPushed = true;
        }

        // if nothing actually got pushed and there is nothing leftover,
        // then this rule is a no-op
        if ((!filterPushed
                && joinType == join.getJoinType())
                || (joinFilters.isEmpty()
                && leftFilters.isEmpty()
                && rightFilters.isEmpty())) {
            return;
        }

        // create Filters on top of the children if any filters were
        // pushed to them
        final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
        final RelBuilder relBuilder = call.builder();
        final RelNode leftRel =
                relBuilder.push(join.getLeft()).filter(leftFilters).build();
        final RelNode rightRel =
                relBuilder.push(join.getRight()).filter(rightFilters).build();

        // create the new join node referencing the new children and
        // containing its new join filters (if there are any)
        final ImmutableList<RelDataType> fieldTypes =
                ImmutableList.<RelDataType>builder()
                        .addAll(RelOptUtil.getFieldTypeList(leftRel.getRowType()))
                        .addAll(RelOptUtil.getFieldTypeList(rightRel.getRowType())).build();
        final RexNode joinFilter =
                RexUtil.composeConjunction(rexBuilder,
                        RexUtil.fixUp(rexBuilder, joinFilters, fieldTypes));

        // If nothing actually got pushed and there is nothing leftover,
        // then this rule is a no-op
        if (joinFilter.isAlwaysTrue()
                && leftFilters.isEmpty()
                && rightFilters.isEmpty()
                && joinType == join.getJoinType()) {
            return;
        }

        RelNode newJoinRel =
                join.copy(
                        join.getTraitSet(),
                        joinFilter,
                        leftRel,
                        rightRel,
                        joinType,
                        join.isSemiJoinDone());
        call.getPlanner().onCopy(join, newJoinRel);
        if (!leftFilters.isEmpty() && filter != null) {
            call.getPlanner().onCopy(filter, leftRel);
        }
        if (!rightFilters.isEmpty() && filter != null) {
            call.getPlanner().onCopy(filter, rightRel);
        }

        relBuilder.push(newJoinRel);

        // Create a project on top of the join if some of the columns have become
        // NOT NULL due to the join-type getting stricter.
        relBuilder.convert(join.getRowType(), false);

        // create a FilterRel on top of the join if needed
        relBuilder.filter(
                RexUtil.fixUp(rexBuilder, aboveFilters,
                        RelOptUtil.getFieldTypeList(relBuilder.peek().getRowType())));
        call.transformTo(relBuilder.build());
    }

    /**
     * Infer more equal conditions for the Join Condition.
     *
     * <p> For example, in {@code SELECT * FROM T1, T2, T3 WHERE T1.id = T3.id AND T2.id = T3.id},
     * we can infer {@code T1.id = T2.id} for the first Join node from second Join node's condition:
     * {@code T1.id = T3.id AND T2.id = T3.id}.
     *
     * <p>For the above SQL, the second Join's condition is {@code T1.id = T3.id AND T2.id = T3.id}.
     * After inference, the final condition would be: {@code T1.id = T2.id AND T1.id = T3.id}, the
     * {@code T1.id = T2.id} can be further pushed into LHS.
     *
     * @param rexNodes the Join condition
     * @param join the Join node
     */
    protected void inferJoinEqualConditions(List<RexNode> rexNodes, Join join) {
        final List<Set<Integer>> equalSets = new ArrayList<>();
        final List<RexNode> result = new ArrayList<>(rexNodes.size());
        for (RexNode rexNode : rexNodes) {
            if (rexNode.isA(SqlKind.EQUALS)) {
                final RexNode op1 = ((RexCall) rexNode).getOperands().get(0);
                final RexNode op2 = ((RexCall) rexNode).getOperands().get(1);
                if (op1 instanceof RexInputRef && op2 instanceof RexInputRef) {
                    final RexInputRef in1 = (RexInputRef) op1;
                    final RexInputRef in2 = (RexInputRef) op2;
                    Set<Integer> set = null;
                    for (Set<Integer> s : equalSets) {
                        if (s.contains(in1.getIndex()) || s.contains(in2.getIndex())) {
                            set = s;
                            break;
                        }
                    }
                    if (set == null) {
                        set = new LinkedHashSet<>(); // to make the result deterministic
                        equalSets.add(set);
                    }
                    set.add(in1.getIndex());
                    set.add(in2.getIndex());
                } else {
                    result.add(rexNode);
                }
            } else {
                result.add(rexNode);
            }
        }

        boolean needOptimize = false;
        for (Set<Integer> set : equalSets) {
            if (set.size() > 2) {
                needOptimize = true;
                break;
            }
        }
        if (!needOptimize) {
            // keep the conditions unchanged.
            return;
        }

        final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
        for (Set<Integer> set : equalSets) {
            final List<Integer> leftSet = new ArrayList<>();
            final List<Integer> rightSet = new ArrayList<>();
            for (int i : set) {
                if (i < join.getLeft().getRowType().getFieldCount()) {
                    leftSet.add(i);
                } else {
                    rightSet.add(i);
                }
            }
            // add left side conditions
            if (leftSet.size() > 1) {
                for (int i = 1; i < leftSet.size(); ++i) {
                    result.add(
                            rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                                    rexBuilder.makeInputRef(join, leftSet.get(0)),
                                    rexBuilder.makeInputRef(join, leftSet.get(i))));
                }
            }
            // add right side conditions
            if (rightSet.size() > 1) {
                for (int i = 1; i < rightSet.size(); ++i) {
                    result.add(
                            rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                                    rexBuilder.makeInputRef(join, rightSet.get(0)),
                                    rexBuilder.makeInputRef(join, rightSet.get(i))));
                }
            }
            // only need one equal condition for each equal set
            if (leftSet.size() > 0 && rightSet.size() > 0) {
                result.add(
                        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                                rexBuilder.makeInputRef(join, leftSet.get(0)),
                                rexBuilder.makeInputRef(join, rightSet.get(0))));
            }
        }

        rexNodes.clear();
        rexNodes.addAll(result);
    }

    /**
     * Get conjunctions of filter's condition but with collapsed
     * {@code IS NOT DISTINCT FROM} expressions if needed.
     *
     * @param filter filter containing condition
     * @return condition conjunctions with collapsed {@code IS NOT DISTINCT FROM}
     * expressions if any
     * @see RelOptUtil#conjunctions(RexNode)
     */
    private static List<RexNode> getConjunctions(Filter filter) {
        List<RexNode> conjunctions = conjunctions(filter.getCondition());
        RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
        for (int i = 0; i < conjunctions.size(); i++) {
            RexNode node = conjunctions.get(i);
            if (node instanceof RexCall) {
                conjunctions.set(i,
                        RelOptUtil.collapseExpandedIsNotDistinctFromExpr((RexCall) node, rexBuilder));
            }
        }
        return conjunctions;
    }
}

package ai.dataprep.federated;

import org.apache.calcite.adapter.enumerable.EnumerableNestedLoopJoin;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.tools.RelBuilder;
import org.immutables.value.Value;

@Value.Enclosing
public final class NestedLoopJoinExtractFilterRule extends RelRule<NestedLoopJoinExtractFilterRule.Config> {

    protected NestedLoopJoinExtractFilterRule(Config config) {
        super(config);
    }

    @Override public boolean matches(RelOptRuleCall call) {
        final EnumerableNestedLoopJoin join = call.rel(0);
        return join.getJoinType() == JoinRelType.INNER && !join.getCondition().isAlwaysTrue();
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final EnumerableNestedLoopJoin join = call.rel(0);

        RelBuilder builder = call.builder();
        final RelNode cartesianJoin =
                join.copy(
                        join.getTraitSet(),
                        builder.literal(true),
                        join.getLeft(),
                        join.getRight(),
                        join.getJoinType(),
                        join.isSemiJoinDone());

        builder.push(cartesianJoin)
                .filter(join.getCondition());

        call.transformTo(builder.build());
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        NestedLoopJoinExtractFilterRule.Config DEFAULT = ImmutableNestedLoopJoinExtractFilterRule.Config.builder().build()
                .withOperandFor(EnumerableNestedLoopJoin.class);

        @Override default NestedLoopJoinExtractFilterRule toRule() {
            return new NestedLoopJoinExtractFilterRule(this);
        }

        default NestedLoopJoinExtractFilterRule.Config withOperandFor(Class<? extends EnumerableNestedLoopJoin> joinClass) {
            return withOperandSupplier(b0 ->
                    b0.operand(joinClass).anyInputs())
                    .as(NestedLoopJoinExtractFilterRule.Config.class);
        }
    }
}

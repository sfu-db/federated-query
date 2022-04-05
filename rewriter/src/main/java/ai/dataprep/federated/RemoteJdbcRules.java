package ai.dataprep.federated;

import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcRules;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelNodes;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.trace.CalciteTrace;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;

import java.rmi.Remote;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class RemoteJdbcRules {
    protected static final Logger LOGGER = CalciteTrace.getPlannerTracer();

    public static class RemoteJdbcJoinRule extends JdbcRules.JdbcJoinRule {
        /** Creates a JdbcJoinRule. */
        public static RemoteJdbcJoinRule create(JdbcConvention out) {
            return Config.INSTANCE
                    .withConversion(Join.class, Convention.NONE, out, "RemoteJdbcJoinRule")
                    .withRuleFactory(RemoteJdbcJoinRule::new)
                    .toRule(RemoteJdbcJoinRule.class);
        }

        /**
         * Called from the Config.
         *
         * @param config
         */
        protected RemoteJdbcJoinRule(Config config) {
            super(config);
        }

        /**
         * Converts a {@code Join} into a {@code JdbcJoin}.
         *
         * @param join Join operator to convert
         * @param convertInputTraits Whether to convert input to {@code join}'s
         *                            JDBC convention
         * @return A new JdbcJoin
         */
        public @Nullable RelNode convert(Join join, boolean convertInputTraits) {
            final List<RelNode> newInputs = new ArrayList<>();
            for (RelNode input : join.getInputs()) {
                if (convertInputTraits && input.getConvention() != getOutTrait()) {
                    input =
                            convert(input,
                                    input.getTraitSet().replace(out));
                }
                newInputs.add(input);
            }
            if (convertInputTraits && !canJoinOnCondition(join.getCondition())) {
                return null;
            }
            try {
                return new RemoteJdbcJoin(
                        join.getCluster(),
                        join.getTraitSet().replace(out),
                        newInputs.get(0),
                        newInputs.get(1),
                        join.getCondition(),
                        join.getVariablesSet(),
                        join.getJoinType());
            } catch (InvalidRelException e) {
                LOGGER.debug(e.toString());
                return null;
            }
        }


        /**
         * Returns whether a condition is supported by {@link JdbcRules.JdbcJoin}.
         *
         * <p>Corresponds to the capabilities of
         * {@link SqlImplementor#convertConditionToSqlNode}.
         *
         * @param node Condition
         * @return Whether condition is supported
         */
        private static boolean canJoinOnCondition(RexNode node) {
            final List<RexNode> operands;
            switch (node.getKind()) {
                case AND:
                case OR:
                    operands = ((RexCall) node).getOperands();
                    for (RexNode operand : operands) {
                        if (!canJoinOnCondition(operand)) {
                            return false;
                        }
                    }
                    return true;

                case EQUALS:
                case IS_NOT_DISTINCT_FROM:
                case NOT_EQUALS:
                case GREATER_THAN:
                case GREATER_THAN_OR_EQUAL:
                case LESS_THAN:
                case LESS_THAN_OR_EQUAL:
                    operands = ((RexCall) node).getOperands();
                    if ((operands.get(0) instanceof RexInputRef)
                            && (operands.get(1) instanceof RexInputRef)) {
                        return true;
                    }
                    // fall through

                default:
                    return false;
            }
        }
    }

    public static class RemoteJdbcJoin extends JdbcRules.JdbcJoin {

        public RemoteJdbcJoin(RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right, RexNode condition, Set<CorrelationId> variablesSet, JoinRelType joinType) throws InvalidRelException {
            super(cluster, traitSet, left, right, condition, variablesSet, joinType);
        }

        @Override public RemoteJdbcJoin copy(RelTraitSet traitSet, RexNode condition,
                                                 RelNode left, RelNode right, JoinRelType joinType,
                                                 boolean semiJoinDone) {
            try {
                return new RemoteJdbcJoin(getCluster(), traitSet, left, right,
                        condition, variablesSet, joinType);
            } catch (InvalidRelException e) {
                // Semantic error not possible. Must be a bug. Convert to
                // internal error.
                throw new AssertionError(e);
            }
        }

        @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
                                                              RelMetadataQuery mq) {
            double rowCount = mq.getRowCount(this);

            RelOptCost cost = planner.getCostFactory().makeCost(rowCount, 0, 0);
            // assume remote join is fast
            return cost.multiplyBy(.01d);
        }
    }
}

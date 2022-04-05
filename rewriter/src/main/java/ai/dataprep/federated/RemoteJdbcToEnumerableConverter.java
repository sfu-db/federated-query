package ai.dataprep.federated;

import org.apache.calcite.adapter.jdbc.JdbcToEnumerableConverter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class RemoteJdbcToEnumerableConverter extends JdbcToEnumerableConverter {
    protected RemoteJdbcToEnumerableConverter(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
        super(cluster, traits, input);
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new RemoteJdbcToEnumerableConverter(
                getCluster(), traitSet, sole(inputs));
    }

    @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
                                                          RelMetadataQuery mq) {

        RelOptCost cost = super.computeSelfCost(planner, mq);
        if (cost == null) {
            return null;
        }
        // make cost on network transfer large and sensitive to # columns
        return cost.multiplyBy(10 * getInput().getRowType().getFieldCount());
    }
}

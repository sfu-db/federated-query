package ai.dataprep.federated;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.util.trace.CalciteTrace;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;

public class DataFusionAggregateRule extends ConverterRule {
    protected static final Logger LOGGER = CalciteTrace.getPlannerTracer();

    static final Config DEFAULT_CONFIG = Config.INSTANCE
            .withConversion(LogicalAggregate.class, Convention.NONE,
                    EnumerableConvention.INSTANCE, "DataFusionAggregateRule")
            .withRuleFactory(DataFusionAggregateRule::new);

    protected DataFusionAggregateRule(Config config) {
        super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
        final Aggregate agg = (Aggregate) rel;
        final RelTraitSet traitSet = rel.getCluster()
                .traitSet().replace(EnumerableConvention.INSTANCE);
        try {
            return new DataFusionAggregate(
                    rel.getCluster(),
                    traitSet,
                    convert(agg.getInput(), traitSet),
                    agg.getGroupSet(),
                    agg.getGroupSets(),
                    agg.getAggCallList());
        } catch (InvalidRelException e) {
            LOGGER.debug(e.toString());
            return null;
        }
    }
}

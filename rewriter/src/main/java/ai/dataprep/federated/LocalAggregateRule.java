package ai.dataprep.federated;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.checkerframework.checker.nullness.qual.Nullable;

public class LocalAggregateRule extends ConverterRule {
    static final Config DEFAULT_CONFIG = Config.INSTANCE
            .withConversion(LogicalAggregate.class, Convention.NONE,
                    EnumerableConvention.INSTANCE, "LocalAggregateRule")
            .withRuleFactory(LocalAggregateRule::new);

    protected LocalAggregateRule(Config config) {
        super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
        final Aggregate agg = (Aggregate) rel;
        final RelTraitSet traitSet = rel.getCluster()
                .traitSet().replace(EnumerableConvention.INSTANCE);
        try {
            return new LocalAggregate(
                    rel.getCluster(),
                    traitSet,
                    convert(agg.getInput(), traitSet),
                    agg.getGroupSet(),
                    agg.getGroupSets(),
                    agg.getAggCallList());
        } catch (InvalidRelException e) {
//            EnumerableRules.LOGGER.debug(e.toString());
            return null;
        }
    }
}

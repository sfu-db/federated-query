package ai.dataprep.federated;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcToEnumerableConverter;
import org.apache.calcite.adapter.jdbc.JdbcToEnumerableConverterRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.checkerframework.checker.nullness.qual.Nullable;

public class RemoteJdbcToEnumerableConverterRule extends JdbcToEnumerableConverterRule {
    /**
     * Called from the Config.
     *
     * @param config
     */
    protected RemoteJdbcToEnumerableConverterRule(Config config) {
        super(config);
    }

    public static RemoteJdbcToEnumerableConverterRule create(JdbcConvention out) {
        return Config.INSTANCE
                .withConversion(RelNode.class, out, EnumerableConvention.INSTANCE,
                        "RemoteJdbcToEnumerableConverterRule")
                .withRuleFactory(RemoteJdbcToEnumerableConverterRule::new)
                .toRule(RemoteJdbcToEnumerableConverterRule.class);
    }

    @Override public @Nullable RelNode convert(RelNode rel) {
        RelTraitSet newTraitSet = rel.getTraitSet().replace(getOutTrait());
        return new RemoteJdbcToEnumerableConverter(rel.getCluster(), newTraitSet, rel);
    }
}

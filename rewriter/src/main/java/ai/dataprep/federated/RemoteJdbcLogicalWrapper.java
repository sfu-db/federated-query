package ai.dataprep.federated;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcTableScan;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class RemoteJdbcLogicalWrapper extends SingleRel {

    public static final EnumerableWrapperRule ENUMERABLE_WRAPPER_RULE =
            EnumerableWrapperRule.DEFAULT_CONFIG.toRule(EnumerableWrapperRule.class);

    protected RemoteJdbcLogicalWrapper(RelOptCluster cluster, RelNode input) {
        super(cluster, cluster.traitSetOf(Convention.NONE), input);
    }

    protected RemoteJdbcLogicalWrapper(RelOptCluster cluster, RelTraitSet traitSet, RelNode input) {
        super(cluster, traitSet, input);
    }

    @Override public final RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new RemoteJdbcLogicalWrapper(getCluster(), traitSet, sole(inputs));
    }

    public static class Visitor extends RelVisitor {
        @Override public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
            if (node instanceof JdbcTableScan) {
                if (parent instanceof LogicalProject || parent instanceof LogicalFilter) {
                    return;
                }
                // Otherwise, add a logical project "*" to enable join associate
                JdbcTableScan scan = (JdbcTableScan) node;
                RexBuilder rexBuilder = node.getCluster().getRexBuilder();
                List<RexNode> fields = rexBuilder.identityProjects(scan.getRowType());
                LogicalProject project = LogicalProject.create(node, ImmutableList.of(), fields, scan.getRowType());
                parent.replaceInput(ordinal, project);
//                RemoteJdbcLogicalWrapper wrapper = new RemoteJdbcLogicalWrapper(node.getCluster(), node);
//                parent.replaceInput(ordinal, wrapper);
            }
            else {
                // Traverse child node
                super.visit(node, ordinal, parent);
            }
        }
    }

    public static class EnumerableWrapperRule extends ConverterRule {

        static final Config DEFAULT_CONFIG = Config.INSTANCE
                .withConversion(RemoteJdbcLogicalWrapper.class, Convention.NONE,
                        EnumerableConvention.INSTANCE, "EnumerableWrapperRule")
                .withRuleFactory(EnumerableWrapperRule::new);

        protected EnumerableWrapperRule(Config config) {
            super(config);
        }

        @Override
        public @Nullable RelNode convert(RelNode rel) {
            final RemoteJdbcLogicalWrapper wrapper = (RemoteJdbcLogicalWrapper) rel;
            return wrapper.input;
        }
    }

    public static class RemoteJdbcWrapperRule extends RemoteJdbcRules.JdbcConverterRule {

        public static RemoteJdbcWrapperRule create(JdbcConvention out) {
            return Config.INSTANCE
                    .withConversion(RemoteJdbcLogicalWrapper.class, Convention.NONE, out, "RemoteJdbcWrapperRule")
                    .withRuleFactory(RemoteJdbcWrapperRule::new)
                    .toRule(RemoteJdbcWrapperRule.class);
        }

        protected RemoteJdbcWrapperRule(Config config) {
            super(config);
        }

        @Override
        public @Nullable RelNode convert(RelNode rel) {
            final RemoteJdbcLogicalWrapper wrapper = (RemoteJdbcLogicalWrapper) rel;
            return wrapper.input;
        }
    }

}

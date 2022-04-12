package ai.dataprep.federated;

import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcRules;
import org.apache.calcite.adapter.jdbc.JdbcToEnumerableConverterRule;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.sql.SqlDialect;

public class RemoteJdbcConvention extends JdbcConvention {
    public static final double COST_MULTIPLIER = 0.1d;

    public RemoteJdbcConvention(SqlDialect dialect, Expression expression, String name) {
        super(dialect, expression, name);
    }

    @Override public void register(RelOptPlanner planner) {
        for (RelOptRule rule : RemoteJdbcRules.rules(this)) {
            planner.addRule(rule);
        }
        planner.addRule(CoreRules.FILTER_SET_OP_TRANSPOSE);
        planner.addRule(CoreRules.PROJECT_REMOVE);
    }
}

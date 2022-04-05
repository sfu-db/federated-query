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
    public RemoteJdbcConvention(SqlDialect dialect, Expression expression, String name) {
        super(dialect, expression, name);
    }

    @Override public void register(RelOptPlanner planner) {
        for (RelOptRule rule : JdbcRules.rules(this)) {
            if (rule instanceof JdbcToEnumerableConverterRule) {
                // replace with self-defined converter rule
                rule = RemoteJdbcToEnumerableConverterRule.create(this);
            } else if (rule instanceof JdbcRules.JdbcJoinRule) {
                rule = RemoteJdbcRules.RemoteJdbcJoinRule.create(this);
            }
            planner.addRule(rule);
        }
        planner.addRule(CoreRules.FILTER_SET_OP_TRANSPOSE);
        planner.addRule(CoreRules.PROJECT_REMOVE);
    }
}

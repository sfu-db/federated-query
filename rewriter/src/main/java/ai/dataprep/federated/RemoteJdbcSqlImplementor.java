package ai.dataprep.federated;

import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rel.rules.AggregateProjectConstantToDummyJoinRule;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.util.Util;

public class RemoteJdbcSqlImplementor extends RelToSqlConverter {
    /**
     * Creates a RelToSqlConverter.
     *
     * @param dialect
     */
    public RemoteJdbcSqlImplementor(SqlDialect dialect) {
        super(dialect);
    }

    @Override
    protected boolean isAnon() {
        return false;
    }
}

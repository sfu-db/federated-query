package ai.dataprep.federated;

import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcRel;
import org.apache.calcite.adapter.jdbc.JdbcToEnumerableConverter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlNode;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;

import static java.util.Objects.requireNonNull;

public class DBSourceVisitor extends RelVisitor {

    private FederatedPlan plan = new FederatedPlan();
    private RelOptCluster cluster;

    public FederatedPlan getPlan() {
        return plan;
    }

//    public DBSourceVisitor(RelOptCluster cluster) {
//        this.cluster = cluster;
//    }

    @Override public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
        if (node instanceof JdbcToEnumerableConverter) {
            // Convert subtree to sql, do not go deeper
            final JdbcRel child = (JdbcRel) node.getInput(0);
            final JdbcConvention jdbcConvention =
                    (JdbcConvention) requireNonNull(child.getConvention(),
                            () -> "child.getConvention() is null for " + child);
            RelToSqlConverter sqlConverter = new RelToSqlConverter(jdbcConvention.dialect);
            RelToSqlConverter.Result res = sqlConverter.visitRoot(child);
            SqlNode resSqlNode = res.asQueryOrValues();
            String resSql = resSqlNode.toSqlString(jdbcConvention.dialect).getSql();
            String dbName = jdbcConvention.getName().substring(5); // remove prefix "JDBC:"
            dbName = plan.add(dbName, resSql);

            LocalTable table = new LocalTable(Collections.singletonList(dbName), child.getRowType());
            EnumerableTableScan scan = EnumerableTableScan.create(node.getCluster(), table);
            parent.replaceInput(ordinal, scan);
        }
        else {
            // Traverse child node
            super.visit(node, ordinal, parent);
        }
    }
}

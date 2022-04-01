package ai.dataprep.federated;

import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.jdbc.*;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.checkerframework.checker.nullness.qual.Nullable;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.*;

import static java.util.Objects.requireNonNull;

public class FederatedQueryRewriter {

    private static final RelOptTable.ViewExpander NOOP_EXPANDER = (type, query,
                                                                   schema, path) -> null;

    public static void main(String[] args) throws Exception {
        String sql = args[0];
        System.out.println("[Input]");
        System.out.println(sql);

        FederatedQueryRewriter rewriter = new FederatedQueryRewriter();
        List<DBExecInfo> dbExecInfos = rewriter.rewrite(sql);
        System.out.println("[Rewrite]");
        dbExecInfos.forEach(t -> System.out.println(t.getDbName() + ": " + t.getSql() + ";\n"));
    }

    public List<DBExecInfo> rewrite(String sql) throws Exception {
        // Parse the query into an AST
        SqlParser parser = SqlParser.create(sql);
        SqlNode sqlNode = parser.parseQuery();
        System.out.println("[Parsed query]\n" + sqlNode.toString());

        // Configure connection and schema via JDBC
        Connection connection = DriverManager.getConnection("jdbc:calcite:");
        final CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        final SchemaPlus rootSchema = calciteConnection.getRootSchema();
        final DataSource ds1 = JdbcSchema.dataSource(
                "jdbc:postgresql://127.0.0.1:5432/tpchsf1",
                "org.postgresql.Driver",
                "postgres",
                "postgres");
        rootSchema.add("DB1", JdbcSchema.create(rootSchema, "DB1", ds1, null, null));
        final DataSource ds2 = JdbcSchema.dataSource(
                "jdbc:mysql://127.0.0.1:3306/tpchsf1",
                "com.mysql.cj.jdbc.Driver",
                "root",
                "mysql");
        rootSchema.add("DB2", JdbcSchema.create(rootSchema, "DB2", ds2, null, null));

        // Configure validator
        Properties props = new Properties();
        props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
                "false");
        CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);
        SqlTypeFactoryImpl typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        CalciteCatalogReader catalogReader = new CalciteCatalogReader(CalciteSchema.from(rootSchema),
                Arrays.asList("DB1", "DB2"),
                typeFactory, config);
        SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(),
                catalogReader, typeFactory,
                SqlValidator.Config.DEFAULT);

        // Validate the initial AST
        SqlNode validNode = validator.validate(sqlNode);

        // Configure planner cluster
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));

        // Configure SqlToRelConverter
        SqlToRelConverter relConverter = new SqlToRelConverter(
                NOOP_EXPANDER, // do not use views
                validator,
                catalogReader,
                cluster,
                StandardConvertletTable.INSTANCE,
                SqlToRelConverter.config());

        // Convert the valid AST into a logical plan
        RelNode logPlan = relConverter.convertQuery(validNode, false, true).rel;
        System.out.println(
                RelOptUtil.dumpPlan("[Logical plan]", logPlan, SqlExplainFormat.TEXT,
                        SqlExplainLevel.EXPPLAN_ATTRIBUTES));

        // Initialize optimizer/planner with the necessary rules
        planner.addRule(CoreRules.FILTER_INTO_JOIN); // necessary to enable JDBCJoinRule
//        planner.addRule(CoreRules.JOIN_CONDITION_PUSH);
//        planner.addRule(CoreRules.JOIN_ASSOCIATE);
//        planner.addRule(CoreRules.JOIN_COMMUTE);
//        planner.addRule(CoreRules.PROJECT_REMOVE);
//        planner.addRule(JoinPushThroughJoinRule.RIGHT);
//        planner.addRule(JoinPushThroughJoinRule.LEFT);
//        planner.addRule(CoreRules.FILTER_MULTI_JOIN_MERGE);
//        planner.addRule(CoreRules.JOIN_TO_MULTI_JOIN);
//        planner.addRule(CoreRules.JOIN_PROJECT_BOTH_TRANSPOSE);
//        planner.addRule(CoreRules.FILTER_TO_CALC);
//        planner.addRule(CoreRules.PROJECT_TO_CALC);
//        planner.addRule(CoreRules.FILTER_CALC_MERGE);
//        planner.addRule(CoreRules.PROJECT_CALC_MERGE);
        planner.addRule(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_PROJECT_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_FILTER_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_CALC_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_JOIN_RULE);

        // Define the type of the output plan (in this case we want a physical plan in EnumerableConvention)
        logPlan = planner.changeTraits(logPlan, cluster.traitSet().replace(EnumerableConvention.INSTANCE));
        planner.setRoot(logPlan);

        // Start the optimization process to obtain the most efficient physical plan
        System.out.println("[Rules]\n" + planner.getRules());
        EnumerableRel phyPlan = (EnumerableRel) planner.findBestExp();

        System.out.println(
                RelOptUtil.dumpPlan("[Physical plan]", phyPlan, SqlExplainFormat.TEXT,
                        SqlExplainLevel.EXPPLAN_ATTRIBUTES));

        DBSourceVisitor visitor = new DBSourceVisitor(cluster);
        phyPlan.childrenAccept(visitor);

        System.out.println(
                RelOptUtil.dumpPlan("[Remaining physical plan]", phyPlan, SqlExplainFormat.TEXT,
                        SqlExplainLevel.EXPPLAN_ATTRIBUTES));

        // Configure RelToSqlConverter
        SqlDialect dialect = SqlDialect.DatabaseProduct.POSTGRESQL.getDialect();
        RelToSqlConverter sqlConverter = new RelToSqlConverter(dialect);

        // Convert physical plan to sql
        RelToSqlConverter.Result res = sqlConverter.visitInput(phyPlan, 0);
        SqlNode resSqlNode = res.asQueryOrValues();
        String resSql = resSqlNode.toSqlString(dialect).getSql();

        List<DBExecInfo> execInfoList = visitor.dbList;
        execInfoList.add(new DBExecInfo("LOCAL", resSql));

        return execInfoList;
    }

    private class DBSourceVisitor extends RelVisitor {

        private List<DBExecInfo> dbList = new ArrayList<DBExecInfo>();
        private RelOptCluster cluster;

        public DBSourceVisitor(RelOptCluster cluster) {
            this.cluster = cluster;
        }

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
                DBExecInfo info = new DBExecInfo(dbName, resSql);
                dbList.add(info);

                LocalTable table = new LocalTable(Collections.singletonList(dbName), child.getRowType());
                EnumerableTableScan scan = EnumerableTableScan.create(cluster, table);
                parent.replaceInput(ordinal, scan);
            }
            else {
                // Traverse child node
                super.visit(node, ordinal, parent);
            }
        }
    }

    public class DBExecInfo {
        public String dbName;
        public String sql;

        public DBExecInfo(String dbName, String sql) {
            this.dbName = dbName;
            this.sql = sql;
        }

        public String getDbName() {
            return dbName;
        }

        public String getSql() {
            return sql;
        }
    }

}

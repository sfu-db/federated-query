package ai.dataprep.federated;

import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.adapter.jdbc.*;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.tree.Expression;
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
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class FederatedQueryRewriter {

    private static final RelOptTable.ViewExpander NOOP_EXPANDER = (type, query,
                                                                   schema, path) -> null;
    private static final Logger logger = LoggerFactory.getLogger(FederatedQueryRewriter.class);

    public static void main(String[] args) throws Exception {
        Logger logger = LoggerFactory.getLogger(FederatedQueryRewriter.class);

        String sql = args[0];
        logger.info("[Input]\n{}", sql);

        FederatedQueryRewriter rewriter = new FederatedQueryRewriter();
        FederatedPlan plan = rewriter.rewrite(sql);
        logger.info("[Rewrite]\n{}", plan.toString());
    }

    public static JdbcSchema createJdbcSchema(
            SchemaPlus parentSchema,
            String name,
            DataSource dataSource,
            @Nullable String catalog,
            @Nullable String schema) {
        SqlDialectFactory dialectFactory = SqlDialectFactoryImpl.INSTANCE;
        final Expression expression =
                Schemas.subSchemaExpression(parentSchema, name, JdbcSchema.class);
        final SqlDialect dialect = JdbcSchema.createDialect(dialectFactory, dataSource);
        final JdbcConvention convention =
                new RemoteJdbcConvention(dialect, expression, name);
        return new JdbcSchema(dataSource, dialect, convention, catalog, schema);
    }

    public FederatedPlan rewrite(String sql) throws Exception {

        // Parse the query into an AST
        SqlParser parser = SqlParser.create(sql);
        SqlNode sqlNode = parser.parseQuery();
        logger.debug("[Parsed query]\n{}", sqlNode.toString());

        // Configure connection and schema via JDBC
        Connection connection = DriverManager.getConnection("jdbc:calcite:");
        final CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        final SchemaPlus rootSchema = calciteConnection.getRootSchema();


        final DataSource ds1 = JdbcSchema.dataSource(
                "jdbc:postgresql://127.0.0.1:5432/tpchsf1",
                "org.postgresql.Driver",
                "postgres",
                "postgres");
        rootSchema.add("db1", createJdbcSchema(rootSchema, "db1", ds1, null, null));
        final DataSource ds2 = JdbcSchema.dataSource(
                "jdbc:mysql://127.0.0.1:3306/tpchsf1",
                "com.mysql.cj.jdbc.Driver",
                "root",
                "mysql");
        rootSchema.add("db2", createJdbcSchema(rootSchema, "db2", ds2, null, null));

        // Configure validator
        Properties props = new Properties();
        props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
                "false");
        CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);
        SqlTypeFactoryImpl typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        CalciteCatalogReader catalogReader = new CalciteCatalogReader(CalciteSchema.from(rootSchema),
                Arrays.asList("db1", "db2"),
                typeFactory, config);
        SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(),
                catalogReader, typeFactory,
                SqlValidator.Config.DEFAULT);

        // Validate the initial AST
        SqlNode validNode = validator.validate(sqlNode);

        // Configure planner cluster
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.setTopDownOpt(true);
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
        logger.debug(
                RelOptUtil.dumpPlan("[Logical plan]", logPlan, SqlExplainFormat.TEXT,
                        SqlExplainLevel.EXPPLAN_ATTRIBUTES));

        // Initialize optimizer/planner with the necessary rules
        planner.addRule(CoreRules.FILTER_INTO_JOIN); // necessary to enable JDBCJoinRule
        planner.addRule(CoreRules.JOIN_CONDITION_PUSH);
        planner.addRule(CoreRules.PROJECT_JOIN_TRANSPOSE);
        planner.addRule(CoreRules.PROJECT_REMOVE);
//        planner.addRule(CoreRules.PROJECT_TABLE_SCAN);
//        planner.addRule(CoreRules.PROJECT_INTERPRETER_TABLE_SCAN);
//        planner.addRule(CoreRules.PROJECT_JOIN_REMOVE);
//        planner.addRule(CoreRules.PROJECT_SET_OP_TRANSPOSE);
//        planner.addRule(CoreRules.JOIN_ASSOCIATE);
//        planner.addRule(CoreRules.JOIN_COMMUTE);
//        planner.addRule(CoreRules.PROJECT_REMOVE);
//        planner.addRule(JoinPushThroughJoinRule.RIGHT);
//        planner.addRule(JoinPushThroughJoinRule.LEFT);
//        planner.addRule(CoreRules.FILTER_MULTI_JOIN_MERGE);
//        planner.addRule(CoreRules.JOIN_TO_MULTI_JOIN);
//        planner.addRule(CoreRules.JOIN_PROJECT_BOTH_TRANSPOSE);
        planner.addRule(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_PROJECT_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_FILTER_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_AGGREGATE_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_SORT_RULE);

        // Define the type of the output plan (in this case we want a physical plan in EnumerableConvention)
        logPlan = planner.changeTraits(logPlan, cluster.traitSet().replace(EnumerableConvention.INSTANCE));
        planner.setRoot(logPlan);

        // Start the optimization process to obtain the most efficient physical plan
        logger.debug("[Rules]\n{}", planner.getRules());
        EnumerableRel phyPlan = (EnumerableRel) planner.findBestExp();

        logger.debug(
                RelOptUtil.dumpPlan("[Physical plan]", phyPlan, SqlExplainFormat.TEXT,
                        SqlExplainLevel.EXPPLAN_ATTRIBUTES));

//        logger.debug("Graph:\n{}", planner.toDot());

        DBSourceVisitor visitor = new DBSourceVisitor(cluster);
        phyPlan.childrenAccept(visitor);

        logger.debug(
                RelOptUtil.dumpPlan("[Remaining physical plan]", phyPlan, SqlExplainFormat.TEXT,
                        SqlExplainLevel.EXPPLAN_ATTRIBUTES));

        // Configure RelToSqlConverter
        SqlDialect dialect = SqlDialect.DatabaseProduct.POSTGRESQL.getDialect();
        RelToSqlConverter sqlConverter = new RelToSqlConverter(dialect);

        // Convert physical plan to sql
        RelToSqlConverter.Result res = sqlConverter.visitRoot(phyPlan);
        SqlNode resSqlNode = res.asQueryOrValues();
        String resSql = resSqlNode.toSqlString(dialect).getSql();

        FederatedPlan plan = visitor.plan;
        plan.add(new DBExecInfo("LOCAL", resSql));

        return plan;
    }

    private class DBSourceVisitor extends RelVisitor {

        private FederatedPlan plan = new FederatedPlan();
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
                plan.add(info);

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

    public class FederatedPlan {
        public List<DBExecInfo> plan = new ArrayList<>();

        public void add(DBExecInfo info) {
            plan.add(info);
        }

        public int getCount() {
            return plan.size();
        }

        public String getDBName(int idx) {
            return plan.get(idx).dbName;
        }

        public String getSql(int idx) {
            return plan.get(idx).sql;
        }

        @Override
        public String toString() {
            return String.join("\n\n", plan.stream().map(DBExecInfo::toString).collect(Collectors.toList()));
        }
    }

    public class DBExecInfo {
        public String dbName;
        public String sql;

        public DBExecInfo(String dbName, String sql) {
            this.dbName = dbName;
            this.sql = sql;
        }

        @Override
        public String toString() {
            return dbName + ": " + sql + ";";
        }
    }

}

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
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
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

public class FederatedQueryRewriter {

    private static final RelOptTable.ViewExpander NOOP_EXPANDER = (type, query,
                                                                   schema, path) -> null;
    private static final Logger logger = LoggerFactory.getLogger(FederatedQueryRewriter.class);

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

    public FederatedPlan rewrite(HashMap<String, DataSource> dbConns, String sql) throws Exception {

        // Parse the query into an AST
        SqlParser parser = SqlParser.create(sql);
        SqlNode sqlNode = parser.parseQuery();
        logger.debug("[Parsed query]\n{}", sqlNode.toString());

        // Configure connection and schema via JDBC
        Connection connection = DriverManager.getConnection("jdbc:calcite:");
        final CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        final SchemaPlus rootSchema = calciteConnection.getRootSchema();

        for (Map.Entry<String, DataSource> entry: dbConns.entrySet()) {
            String name = entry.getKey();
            DataSource dataSource = entry.getValue();
            rootSchema.add(name, createJdbcSchema(rootSchema, name, dataSource, null, null));
        }

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
//        planner.addRule(CoreRules.FILTER_INTO_JOIN); // necessary to enable JDBCJoinRule
//        planner.addRule(CoreRules.JOIN_CONDITION_PUSH);
        planner.addRule(FilterJoinRulePatch.FilterIntoJoinRule.FilterIntoJoinRuleConfig.DEFAULT.toRule());
        planner.addRule(FilterJoinRulePatch.JoinConditionPushRule.JoinConditionPushRuleConfig.DEFAULT.toRule());
        planner.addRule(CoreRules.PROJECT_JOIN_TRANSPOSE);
        planner.addRule(CoreRules.PROJECT_REMOVE);
//        planner.addRule(CoreRules.PROJECT_TABLE_SCAN);
//        planner.addRule(CoreRules.PROJECT_INTERPRETER_TABLE_SCAN);
//        planner.addRule(CoreRules.PROJECT_JOIN_REMOVE);
//        planner.addRule(CoreRules.PROJECT_SET_OP_TRANSPOSE);
        planner.addRule(CoreRules.JOIN_ASSOCIATE);
        planner.addRule(CoreRules.JOIN_COMMUTE);
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

        FederatedPlan plan = visitor.getPlan();
        plan.add("LOCAL", resSql);

        return plan;
    }

}

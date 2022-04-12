package ai.dataprep.federated;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.jdbc.JdbcRules;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
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
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.Properties;

public class HepQueryRewriter {

    private static final RelOptTable.ViewExpander NOOP_EXPANDER = (type, query,
                                                                   schema, path) -> null;

    public static String rewrite(String sql) throws Exception {
        // Parse the query into an AST
        SqlParser parser = SqlParser.create(sql);
        SqlNode sqlNode = parser.parseQuery();
        System.out.println("[Parsed query]\n" + sqlNode.toString());

        // Configure connection and schema via JDBC
        Connection connection = DriverManager.getConnection("jdbc:calcite:");
        final CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        final SchemaPlus rootSchema = calciteConnection.getRootSchema();
        final DataSource ds = JdbcSchema.dataSource(
                "jdbc:postgresql://127.0.0.1:5432/tpchsf1",
                "org.postgresql.Driver",
                "postgres",
                "postgres");
        rootSchema.add("DB1", JdbcSchema.create(rootSchema, "DB1", ds, null, null));

        // Configure validator
        Properties props = new Properties();
        props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
                "false");
        CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);
        SqlTypeFactoryImpl typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        CalciteCatalogReader catalogReader = new CalciteCatalogReader(CalciteSchema.from(rootSchema),
                Collections.singletonList("DB1"),
                typeFactory, config);
        SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(),
                catalogReader, typeFactory,
                SqlValidator.Config.DEFAULT);

        // Validate the initial AST
        SqlNode validNode = validator.validate(sqlNode);

        // Configure planner cluster
        HepProgramBuilder builder = new HepProgramBuilder();
        builder.addRuleInstance(CoreRules.FILTER_INTO_JOIN);
        builder.addRuleInstance(CoreRules.JOIN_CONDITION_PUSH);
//        builder.addRuleInstance(CoreRules.JOIN_ASSOCIATE);
//        builder.addRuleInstance(CoreRules.JOIN_COMMUTE);
        builder.addRuleInstance(CoreRules.PROJECT_JOIN_TRANSPOSE);
        HepPlanner planner = new HepPlanner(builder.build(), null, false, null, RelOptCostImpl.FACTORY);

//        VolcanoPlanner planner = new VolcanoPlanner();
//        planner.setTopDownOpt(true);
//        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
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

        // Define the type of the output plan (in this case we want a physical plan in
        // EnumerableConvention)
//        logPlan = planner.changeTraits(logPlan, cluster.traitSet().replace(EnumerableConvention.INSTANCE));

        planner.setRoot(logPlan);

        // Start the optimization process to obtain the most efficient physical plan
//        System.out.println(
//                RelOptUtil.dumpPlan("[Logical plan before optimize]", logPlan, SqlExplainFormat.TEXT,
//                        SqlExplainLevel.ALL_ATTRIBUTES));
        System.out.println("[Rules]\n" + planner.getRules());
//        System.out.println("[Rule Queue]\n" + planner.getRuleQueue());
        RelNode phyPlan = planner.findBestExp();
//        EnumerableRel phyPlan = (EnumerableRel) planner.findBestExp();

        // Print planner info in graph
//        System.out.println("Graph:\n" + planner.toDot());

        System.out.println(
                RelOptUtil.dumpPlan("[Physical plan]", phyPlan, SqlExplainFormat.TEXT,
                        SqlExplainLevel.EXPPLAN_ATTRIBUTES));

        // Configure RelToSqlConverter
        SqlDialect dialect = SqlDialect.DatabaseProduct.POSTGRESQL.getDialect();
        RelToSqlConverter sqlConverter = new RelToSqlConverter(dialect);

        // Convert physical plan to sql
        RelToSqlConverter.Result res = sqlConverter.visitInput(phyPlan, 0);
        SqlNode resSqlNode = res.asQueryOrValues();
        String resSql = resSqlNode.toSqlString(dialect).getSql();

        return resSql;
    }

}

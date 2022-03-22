package ai.dataprep.federated;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
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
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
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

public class QueryRewriter {

    private static final RelOptTable.ViewExpander NOOP_EXPANDER = (type, query,
                                                                   schema, path) -> null;

    public static void main(String[] args) throws Exception {
        String sql = args[0];
        System.out.println("[Input]");
        System.out.println(sql);

        String rewriteSql = rewrite(sql);
        System.out.println("[Rewrite]");
        System.out.println(rewriteSql);
    }

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

        // Define the type of the output plan (in this case we want a physical plan in
        // EnumerableConvention)
        logPlan = planner.changeTraits(logPlan, cluster.traitSet().replace(EnumerableConvention.INSTANCE));

        planner.setRoot(logPlan);

        // Start the optimization process to obtain the most efficient physical plan
        System.out.println(
                RelOptUtil.dumpPlan("[Logical plan before optimize]", logPlan, SqlExplainFormat.TEXT,
                        SqlExplainLevel.ALL_ATTRIBUTES));
        EnumerableRel phyPlan = (EnumerableRel) planner.findBestExp();

        // Print planner info in graph
//        PrintWriter pw = new PrintWriter(System.out, true);
//        planner.dump(pw);

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

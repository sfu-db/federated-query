package ai.dataprep.federated;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.HashMap;

public class FederatedQueryRewriterTest {
    private static final Logger logger = LoggerFactory.getLogger(FederatedQueryRewriter.class);

    public static void main(String[] args) throws Exception {
        Logger logger = LoggerFactory.getLogger(FederatedQueryRewriter.class);

        String sql = args[0];
        logger.info("[Input]\n{}", sql);

        final DataSource ds1 = JdbcSchema.dataSource(
                "jdbc:postgresql://127.0.0.1:5432/tpchsf1",
                "org.postgresql.Driver",
                "postgres",
                "postgres");

        final DataSource ds2 = JdbcSchema.dataSource(
                "jdbc:mysql://127.0.0.1:3306/tpchsf1",
                "com.mysql.cj.jdbc.Driver",
                "root",
                "mysql");
        HashMap<String, DataSource> dbConns = new HashMap<>();
        dbConns.put("db1", ds1);
        dbConns.put("db2", ds2);

        FederatedQueryRewriter rewriter = new FederatedQueryRewriter();
        FederatedPlan plan = rewriter.rewrite(dbConns, sql);
        logger.info("[Rewrite]\n{}", plan.toString());
    }
}

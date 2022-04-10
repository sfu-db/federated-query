package ai.dataprep.federated;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryRewriterTest {
    public static void main(String[] args) throws Exception {
        Logger logger = LoggerFactory.getLogger(QueryRewriter.class);
        String sql = args[0];
        System.out.println("[Input]");
        System.out.println(sql);

        String rewriteSql = QueryRewriter.rewrite(sql);
        System.out.println("[Rewrite]");
        System.out.println(rewriteSql);
    }
}

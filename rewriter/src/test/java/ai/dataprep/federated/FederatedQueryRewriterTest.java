package ai.dataprep.federated;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FederatedQueryRewriterTest {
    private static final Logger logger = LoggerFactory.getLogger(FederatedQueryRewriter.class);

    public static void main(String[] args) throws Exception {
        Logger logger = LoggerFactory.getLogger(FederatedQueryRewriter.class);

        String sql = args[0];
        logger.info("[Input]\n{}", sql);

        FederatedQueryRewriter rewriter = new FederatedQueryRewriter();
        FederatedQueryRewriter.FederatedPlan plan = rewriter.rewrite(sql);
        logger.info("[Rewrite]\n{}", plan.toString());
    }
}

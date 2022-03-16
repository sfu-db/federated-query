package ai.dataprep.federated;

public class QueryRewriter {
    public static void main(String[] args) throws Exception {
        String sql = args[0];
        System.out.println("[Input] " + sql);

        String rewriteSql = rewrite(sql);
        System.out.println("[Rewrite] " + rewriteSql);
    }

    public static String rewrite(String sql) throws Exception {
        return sql;
    }

}

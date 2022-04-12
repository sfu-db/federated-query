package ai.dataprep.federated;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class HepQueryRewriterTest {
    public static void main(String[] args) throws Exception {

        String sql = new String(Files.readAllBytes(Paths.get(args[0])), StandardCharsets.UTF_8);

        System.out.println("[Input]");
        System.out.println(sql);

        String rewriteSql = HepQueryRewriter.rewrite(sql);
        System.out.println("[Rewrite]");
        System.out.println(rewriteSql);
    }
}

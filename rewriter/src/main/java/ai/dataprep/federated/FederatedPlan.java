package ai.dataprep.federated;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class FederatedPlan {
    public List<DBExecutionInfo> plan = new ArrayList<>();
    private HashMap<String, Integer> counter = new HashMap<>();

    public String add(String dbName, String sql) {
        Integer count = counter.getOrDefault(dbName, 0);
        String aliasDbName = dbName;
        if (count > 0) {
            aliasDbName = dbName + "_" + count;
        }
        DBExecutionInfo info = new DBExecutionInfo(dbName, aliasDbName, sql);
        plan.add(info);
        counter.put(info.dbName, count+1);
        return aliasDbName;
    }

    public int getCount() {
        return plan.size();
    }

    public String getDBName(int idx) {
        return plan.get(idx).dbName;
    }

    public String getAliasDBName(int idx) {
        return plan.get(idx).aliasDbName;
    }

    public String getSql(int idx) {
        return plan.get(idx).sql;
    }

    @Override
    public String toString() {
        return String.join("\n\n", plan.stream().map(DBExecutionInfo::toString).collect(Collectors.toList()));
    }

    public static class DBExecutionInfo {
        public String dbName;
        public String aliasDbName;
        public String sql;

        public DBExecutionInfo(String dbName, String aliasDbName, String sql) {
            this.dbName = dbName;
            this.aliasDbName = aliasDbName;
            this.sql = sql;
        }

        @Override
        public String toString() {
            return dbName + "[" + aliasDbName + "]" + ": " + sql + ";";
        }
    }
}

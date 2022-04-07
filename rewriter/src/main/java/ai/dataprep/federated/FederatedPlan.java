package ai.dataprep.federated;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class FederatedPlan {
    public List<DBExecutionInfo> plan = new ArrayList<>();

    public void add(DBExecutionInfo info) {
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
        return String.join("\n\n", plan.stream().map(DBExecutionInfo::toString).collect(Collectors.toList()));
    }

    public static class DBExecutionInfo {
        public String dbName;
        public String sql;

        public DBExecutionInfo(String dbName, String sql) {
            this.dbName = dbName;
            this.sql = sql;
        }

        @Override
        public String toString() {
            return dbName + ": " + sql + ";";
        }
    }
}

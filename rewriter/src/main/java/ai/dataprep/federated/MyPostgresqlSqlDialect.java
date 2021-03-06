package ai.dataprep.federated;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;

public class MyPostgresqlSqlDialect extends PostgresqlSqlDialect {
    /**
     * Creates a PostgresqlSqlDialect.
     *
     * @param context
     */
    public MyPostgresqlSqlDialect(Context context) {
        super(context);
    }

    @Override public void unparseCall(SqlWriter writer, SqlCall call,
                                      int leftPrec, int rightPrec) {
        switch (call.getKind()) {
            case MIN:
                // exists/in queries will be converted to select key, min(true) ... group by key with join
                // postgres does not support min(true), need to remove the "min()"
                SqlNode operand = call.getOperandList().get(0);
                if (operand instanceof SqlLiteral && ((SqlLiteral) operand).getValue() instanceof Boolean) {
                    operand.unparse(writer, leftPrec, rightPrec);
                }
                break;

            default:
                super.unparseCall(writer, call, leftPrec, rightPrec);
        }
    }

    @Override public boolean supportsAggregateFunction(SqlKind kind) {
        switch (kind) {
            case COUNT:
            case SUM:
            case SUM0:
            case MIN:
            case MAX:
            case AVG:
                return true;
            default:
                break;
        }
        return false;
    }
}

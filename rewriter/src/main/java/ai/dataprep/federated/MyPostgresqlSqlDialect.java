package ai.dataprep.federated;

import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.fun.SqlFloorFunction;

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
}

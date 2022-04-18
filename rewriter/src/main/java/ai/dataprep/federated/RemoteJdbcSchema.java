package ai.dataprep.federated;

import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDialectFactory;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.checkerframework.checker.nullness.qual.Nullable;

import javax.sql.DataSource;

public class RemoteJdbcSchema extends JdbcSchema {
    /**
     * Creates a JDBC schema.
     *
     * @param dataSource Data source
     * @param dialect    SQL dialect
     * @param convention Calling convention
     * @param catalog    Catalog name, or null
     * @param schema     Schema name pattern
     */
    public RemoteJdbcSchema(DataSource dataSource, SqlDialect dialect, JdbcConvention convention, @Nullable String catalog, @Nullable String schema) {
        super(dataSource, dialect, convention, catalog, schema);
    }

    public static SqlDialect createDialect(SqlDialectFactory dialectFactory,
                                           DataSource dataSource) {
        SqlDialect dialect = JdbcSchema.createDialect(dialectFactory, dataSource);
        if (dialect instanceof PostgresqlSqlDialect) {
            dialect = new MyPostgresqlSqlDialect(PostgresqlSqlDialect.DEFAULT_CONTEXT);
        }
        return dialect;
    }
}

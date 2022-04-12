package ai.dataprep.federated;

import org.apache.calcite.rel.metadata.*;
import org.checkerframework.checker.nullness.qual.Nullable;

public class FederatedRelMdRowCount extends RelMdRowCount {
    public static final RelMetadataProvider SOURCE =
            ReflectiveRelMetadataProvider.reflectiveSource(
                    new FederatedRelMdRowCount(), BuiltInMetadata.RowCount.Handler.class);

    //~ Methods ----------------------------------------------------------------

    public @Nullable Double getRowCount(RemoteJdbcRules.JdbcJoin rel, RelMetadataQuery mq) {
        return rel.estimateRowCount(mq);
    }

    public @Nullable Double getRowCount(RemoteJdbcToEnumerableConverter rel, RelMetadataQuery mq) {
        return rel.estimateRowCount(mq);
    }
}

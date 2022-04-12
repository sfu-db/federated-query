package ai.dataprep.federated;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.metadata.*;

public class FederatedRelMetadataProvider extends ChainedRelMetadataProvider {
    public static final FederatedRelMetadataProvider INSTANCE =
            new FederatedRelMetadataProvider();

    protected FederatedRelMetadataProvider() {
        super(
                ImmutableList.of(
                        RelMdPercentageOriginalRows.SOURCE,
                        RelMdColumnOrigins.SOURCE,
                        RelMdExpressionLineage.SOURCE,
                        RelMdTableReferences.SOURCE,
                        RelMdNodeTypes.SOURCE,
                        FederatedRelMdRowCount.SOURCE, // locally defined
                        RelMdMaxRowCount.SOURCE,
                        RelMdMinRowCount.SOURCE,
                        RelMdUniqueKeys.SOURCE,
                        RelMdColumnUniqueness.SOURCE,
                        RelMdPopulationSize.SOURCE,
                        RelMdSize.SOURCE,
                        RelMdParallelism.SOURCE,
                        RelMdDistribution.SOURCE,
                        RelMdLowerBoundCost.SOURCE,
                        RelMdMemory.SOURCE,
                        RelMdDistinctRowCount.SOURCE,
                        RelMdSelectivity.SOURCE,
                        RelMdExplainVisibility.SOURCE,
                        RelMdPredicates.SOURCE,
                        RelMdAllPredicates.SOURCE,
                        RelMdCollation.SOURCE));
    }
}

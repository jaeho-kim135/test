package org.knime.bigdata.spark3_5.dx.jobs.preproc.unpivot;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_5.api.Spark_3_5_CompatibilityChecker;

/**
 * Provides the unpivot job run factory for Spark 3.5.
 */
public class UnpivotJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public UnpivotJobRunFactoryProvider() {
        super(Spark_3_5_CompatibilityChecker.INSTANCE,
            new UnpivotJobRunFactory());
    }
}

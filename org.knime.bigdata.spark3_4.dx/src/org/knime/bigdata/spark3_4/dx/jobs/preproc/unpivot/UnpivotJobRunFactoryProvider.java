package org.knime.bigdata.spark3_4.dx.jobs.preproc.unpivot;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_4.api.Spark_3_4_CompatibilityChecker;

/**
 * Provides the unpivot job run factory for Spark 3.4.
 */
public class UnpivotJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public UnpivotJobRunFactoryProvider() {
        super(Spark_3_4_CompatibilityChecker.INSTANCE,
            new UnpivotJobRunFactory());
    }
}

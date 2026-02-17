package org.knime.bigdata.spark3_5.dx.jobs.sql.multiquery;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_5.api.Spark_3_5_CompatibilityChecker;

/**
 * Provides the multi query job run factory for Spark 3.5.
 */
public class MultiQueryJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public MultiQueryJobRunFactoryProvider() {
        super(Spark_3_5_CompatibilityChecker.INSTANCE,
            new MultiQueryJobRunFactory());
    }
}

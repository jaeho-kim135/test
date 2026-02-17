package org.knime.bigdata.spark3_4.dx.jobs.sql.multiquery;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_4.api.Spark_3_4_CompatibilityChecker;

/**
 * Provides the multi query job run factory for Spark 3.4.
 */
public class MultiQueryJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public MultiQueryJobRunFactoryProvider() {
        super(Spark_3_4_CompatibilityChecker.INSTANCE,
            new MultiQueryJobRunFactory());
    }
}

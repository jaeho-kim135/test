package org.knime.bigdata.spark3_5.dx.jobs.sql.expression;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_5.api.Spark_3_5_CompatibilityChecker;

/**
 * Provides the expression job run factory for Spark 3.5.
 */
public class ExpressionJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public ExpressionJobRunFactoryProvider() {
        super(Spark_3_5_CompatibilityChecker.INSTANCE,
            new ExpressionJobRunFactory());
    }
}

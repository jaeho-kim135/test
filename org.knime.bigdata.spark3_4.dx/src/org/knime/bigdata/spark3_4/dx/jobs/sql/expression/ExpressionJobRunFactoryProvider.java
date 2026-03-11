package org.knime.bigdata.spark3_4.dx.jobs.sql.expression;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactoryProvider;
import org.knime.bigdata.spark3_4.api.Spark_3_4_CompatibilityChecker;

/**
 * Provides the expression job run factory for Spark 3.4.
 */
public class ExpressionJobRunFactoryProvider extends DefaultJobRunFactoryProvider {

    /** Constructor. */
    public ExpressionJobRunFactoryProvider() {
        super(Spark_3_4_CompatibilityChecker.INSTANCE,
            new ExpressionJobRunFactory());
    }
}

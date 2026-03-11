package org.knime.bigdata.spark3_4.dx.jobs.sql.expression;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactory;
import org.knime.bigdata.spark.dx.node.sql.expression.SparkExpressionJobInput;
import org.knime.bigdata.spark.dx.node.sql.expression.SparkExpressionJobOutput;
import org.knime.bigdata.spark.dx.node.sql.expression.SparkExpressionNodeModel;

/**
 * Expression job run factory for Spark 3.4.
 */
public class ExpressionJobRunFactory extends DefaultJobRunFactory<SparkExpressionJobInput, SparkExpressionJobOutput> {

    /** Constructor. */
    public ExpressionJobRunFactory() {
        super(SparkExpressionNodeModel.JOB_ID, ExpressionJob.class, SparkExpressionJobOutput.class);
    }
}

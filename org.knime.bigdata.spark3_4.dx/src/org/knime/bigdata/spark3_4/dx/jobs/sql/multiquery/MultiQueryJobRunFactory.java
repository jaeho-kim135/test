package org.knime.bigdata.spark3_4.dx.jobs.sql.multiquery;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactory;
import org.knime.bigdata.spark.dx.node.sql.multiquery.SparkMultiQueryJobInput;
import org.knime.bigdata.spark.dx.node.sql.multiquery.SparkMultiQueryJobOutput;
import org.knime.bigdata.spark.dx.node.sql.multiquery.SparkMultiQueryNodeModel;

/**
 * Multi Query job run factory for Spark 3.4.
 */
public class MultiQueryJobRunFactory extends DefaultJobRunFactory<SparkMultiQueryJobInput, SparkMultiQueryJobOutput> {

    /** Constructor. */
    public MultiQueryJobRunFactory() {
        super(SparkMultiQueryNodeModel.JOB_ID, MultiQueryJob.class, SparkMultiQueryJobOutput.class);
    }
}

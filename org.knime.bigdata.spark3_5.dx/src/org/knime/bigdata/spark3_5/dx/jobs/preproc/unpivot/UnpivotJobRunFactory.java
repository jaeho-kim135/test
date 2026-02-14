package org.knime.bigdata.spark3_5.dx.jobs.preproc.unpivot;

import org.knime.bigdata.spark.core.job.DefaultJobRunFactory;
import org.knime.bigdata.spark.dx.node.preproc.unpivot.SparkUnpivotJobInput;
import org.knime.bigdata.spark.dx.node.preproc.unpivot.SparkUnpivotJobOutput;
import org.knime.bigdata.spark.dx.node.preproc.unpivot.SparkUnpivotNodeModel;

/**
 * Unpivot job run factory for Spark 3.5.
 */
public class UnpivotJobRunFactory extends DefaultJobRunFactory<SparkUnpivotJobInput, SparkUnpivotJobOutput> {

    /** Constructor. */
    public UnpivotJobRunFactory() {
        super(SparkUnpivotNodeModel.JOB_ID, UnpivotJob.class, SparkUnpivotJobOutput.class);
    }
}

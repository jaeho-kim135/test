package org.knime.bigdata.spark.dx.node.preproc.unpivot;

import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;

/**
 * Job output for the Spark Unpivot job.
 */
@SparkClass
public class SparkUnpivotJobOutput extends JobOutput {

    /** Deserialization constructor. */
    public SparkUnpivotJobOutput() {
    }

    /**
     * Constructor.
     *
     * @param outputObject the named output object ID
     * @param outputSpec the output schema
     */
    public SparkUnpivotJobOutput(final String outputObject, final IntermediateSpec outputSpec) {
        withSpec(outputObject, outputSpec);
    }
}

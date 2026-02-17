package org.knime.bigdata.spark.dx.node.sql.multiquery;

import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;

/**
 * Job output for the Spark Multi Query job.
 */
@SparkClass
public class SparkMultiQueryJobOutput extends JobOutput {

    private static final String PREVIEW_DATA = "previewData";

    /** Deserialization constructor. */
    public SparkMultiQueryJobOutput() {
    }

    /**
     * Constructor.
     *
     * @param outputObject the named output object ID
     * @param outputSpec the output schema
     */
    public SparkMultiQueryJobOutput(final String outputObject, final IntermediateSpec outputSpec) {
        if (outputObject != null && outputSpec != null) {
            withSpec(outputObject, outputSpec);
        }
    }

    /**
     * Sets the preview data string (formatted sample rows).
     *
     * @param previewData formatted result of showString()
     */
    public void setPreviewData(final String previewData) {
        set(PREVIEW_DATA, previewData);
    }

    /**
     * @return the preview data string, or null if not set
     */
    public String getPreviewData() {
        return getOrDefault(PREVIEW_DATA, null);
    }
}

package org.knime.bigdata.spark.dx.node.preproc.unpivot;

import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;

/**
 * Job output for the Spark Unpivot job.
 */
@SparkClass
public class SparkUnpivotJobOutput extends JobOutput {

    private static final String PREVIEW_DATA = "previewData";
    private static final String INPUT_ROW_COUNT = "inputRowCount";

    /** Deserialization constructor. */
    public SparkUnpivotJobOutput() {
    }

    /**
     * Constructor.
     *
     * @param outputObject the named output object ID (null for validation-only)
     * @param outputSpec the output schema (null for validation-only)
     */
    public SparkUnpivotJobOutput(final String outputObject, final IntermediateSpec outputSpec) {
        if (outputObject != null && outputSpec != null) {
            withSpec(outputObject, outputSpec);
        }
    }

    /** Sets the preview data string. */
    public void setPreviewData(final String previewData) {
        set(PREVIEW_DATA, previewData);
    }

    /** @return the preview data string, or null if not set */
    public String getPreviewData() {
        return getOrDefault(PREVIEW_DATA, null);
    }

    /** Sets the input row count. */
    public void setInputRowCount(final long count) {
        set(INPUT_ROW_COUNT, String.valueOf(count));
    }

    /** @return the input row count, or -1 if not set */
    public long getInputRowCount() {
        final String val = getOrDefault(INPUT_ROW_COUNT, null);
        if (val != null) {
            try {
                return Long.parseLong(val);
            } catch (final NumberFormatException e) {
                return -1L;
            }
        }
        return -1L;
    }
}

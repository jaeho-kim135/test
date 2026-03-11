package org.knime.bigdata.spark.dx.node.sql.expression;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Job input for the Spark Expression job.
 * Contains arrays of expressions, output modes, and column names.
 */
@SparkClass
public class SparkExpressionJobInput extends JobInput {

    private static final String EXPRESSIONS = "expressions";
    private static final String OUTPUT_MODES = "outputModes";
    private static final String COLUMN_NAMES = "columnNames";
    private static final String VALIDATE_ONLY = "validateOnly";

    /** Deserialization constructor. */
    public SparkExpressionJobInput() {
    }

    /**
     * Constructor for normal execution.
     *
     * @param inputObject named input object ID
     * @param outputObject named output object ID
     * @param expressions Spark SQL expression strings
     * @param outputModes output modes ("APPEND" or "REPLACE")
     * @param columnNames output column names
     */
    public SparkExpressionJobInput(final String inputObject, final String outputObject,
            final String[] expressions, final String[] outputModes, final String[] columnNames) {
        addNamedInputObject(inputObject);
        addNamedOutputObject(outputObject);
        set(EXPRESSIONS, expressions);
        set(OUTPUT_MODES, outputModes);
        set(COLUMN_NAMES, columnNames);
        set(VALIDATE_ONLY, false);
    }

    /**
     * Constructor for validation-only execution.
     *
     * @param inputObject named input object ID
     * @param expressions Spark SQL expression strings
     * @param outputModes output modes
     * @param columnNames output column names
     */
    public SparkExpressionJobInput(final String inputObject,
            final String[] expressions, final String[] outputModes, final String[] columnNames) {
        addNamedInputObject(inputObject);
        set(EXPRESSIONS, expressions);
        set(OUTPUT_MODES, outputModes);
        set(COLUMN_NAMES, columnNames);
        set(VALIDATE_ONLY, true);
    }

    /** @return the Spark SQL expressions */
    public String[] getExpressions() {
        return get(EXPRESSIONS);
    }

    /** @return the output modes ("APPEND" or "REPLACE") */
    public String[] getOutputModes() {
        return get(OUTPUT_MODES);
    }

    /** @return the output column names */
    public String[] getColumnNames() {
        return get(COLUMN_NAMES);
    }

    /** @return whether this is a validation-only run */
    public boolean isValidateOnly() {
        return get(VALIDATE_ONLY);
    }
}

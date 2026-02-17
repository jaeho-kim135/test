package org.knime.bigdata.spark.dx.node.sql.multiquery;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Job input for the Spark Multi Query job.
 */
@SparkClass
public class SparkMultiQueryJobInput extends JobInput {

    private static final String TARGET_COLUMNS = "targetColumns";
    private static final String SQL_EXPRESSION = "sqlExpression";
    private static final String VALIDATE_ONLY = "validateOnly";
    private static final String KEEP_ORIGINAL = "keepOriginal";
    private static final String OUTPUT_PATTERN = "outputPattern";

    /** Deserialization constructor. */
    public SparkMultiQueryJobInput() {
    }

    /**
     * Constructor for normal execution.
     *
     * @param inputObject named input object ID
     * @param outputObject named output object ID
     * @param targetColumns columns to apply the SQL expression to
     * @param sqlExpression the SQL expression template with $columnS placeholder
     * @param keepOriginal whether to keep original columns alongside transformed ones
     * @param outputPattern output column name pattern (uses $columnS)
     */
    public SparkMultiQueryJobInput(final String inputObject, final String outputObject,
            final String[] targetColumns, final String sqlExpression,
            final boolean keepOriginal, final String outputPattern) {

        addNamedInputObject(inputObject);
        addNamedOutputObject(outputObject);
        set(TARGET_COLUMNS, targetColumns);
        set(SQL_EXPRESSION, sqlExpression);
        set(VALIDATE_ONLY, false);
        set(KEEP_ORIGINAL, keepOriginal);
        set(OUTPUT_PATTERN, outputPattern);
    }

    /**
     * Constructor for validation-only execution.
     * Tests the expression against ALL target columns.
     *
     * @param inputObject named input object ID
     * @param targetColumns all target columns to validate
     * @param sqlExpression the SQL expression template
     */
    public SparkMultiQueryJobInput(final String inputObject,
            final String[] targetColumns, final String sqlExpression) {

        addNamedInputObject(inputObject);
        set(TARGET_COLUMNS, targetColumns);
        set(SQL_EXPRESSION, sqlExpression);
        set(VALIDATE_ONLY, true);
        set(KEEP_ORIGINAL, false);
        set(OUTPUT_PATTERN, "$columnS");
    }

    /** @return the target column names */
    public String[] getTargetColumns() {
        return get(TARGET_COLUMNS);
    }

    /** @return the SQL expression template */
    public String getSqlExpression() {
        return get(SQL_EXPRESSION);
    }

    /** @return whether this is a validation-only run */
    public boolean isValidateOnly() {
        return get(VALIDATE_ONLY);
    }

    /** @return whether to keep original columns */
    public boolean keepOriginal() {
        return get(KEEP_ORIGINAL);
    }

    /** @return the output column name pattern */
    public String getOutputPattern() {
        return get(OUTPUT_PATTERN);
    }
}

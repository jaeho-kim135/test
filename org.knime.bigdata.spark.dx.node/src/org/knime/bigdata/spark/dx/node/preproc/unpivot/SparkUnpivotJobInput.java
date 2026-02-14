package org.knime.bigdata.spark.dx.node.preproc.unpivot;

import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Job input for the Spark Unpivot job.
 */
@SparkClass
public class SparkUnpivotJobInput extends JobInput {

    private static final String RETAINED_COLUMNS = "retainedColumns";
    private static final String VALUE_COLUMNS = "valueColumns";
    private static final String VARIABLE_COL_NAME = "variableColName";
    private static final String VALUE_COL_NAME = "valueColName";
    private static final String SKIP_MISSING_VALUES = "skipMissingValues";

    /** Deserialization constructor. */
    public SparkUnpivotJobInput() {
    }

    /**
     * Constructor.
     *
     * @param inputObject named input object ID
     * @param outputObject named output object ID
     * @param retainedColumns columns to retain
     * @param valueColumns columns to unpivot
     * @param variableColName name for the variable output column
     * @param valueColName name for the value output column
     * @param skipMissingValues whether to skip rows with null values
     */
    public SparkUnpivotJobInput(final String inputObject, final String outputObject,
            final String[] retainedColumns, final String[] valueColumns,
            final String variableColName, final String valueColName,
            final boolean skipMissingValues) {

        addNamedInputObject(inputObject);
        addNamedOutputObject(outputObject);
        set(RETAINED_COLUMNS, retainedColumns);
        set(VALUE_COLUMNS, valueColumns);
        set(VARIABLE_COL_NAME, variableColName);
        set(VALUE_COL_NAME, valueColName);
        set(SKIP_MISSING_VALUES, skipMissingValues);
    }

    /** @return the retained column names */
    public String[] getRetainedColumns() {
        return get(RETAINED_COLUMNS);
    }

    /** @return the value column names to unpivot */
    public String[] getValueColumns() {
        return get(VALUE_COLUMNS);
    }

    /** @return the variable output column name */
    public String getVariableColName() {
        return get(VARIABLE_COL_NAME);
    }

    /** @return the value output column name */
    public String getValueColName() {
        return get(VALUE_COL_NAME);
    }

    /** @return whether to skip rows with null values */
    public boolean skipMissingValues() {
        return get(SKIP_MISSING_VALUES);
    }
}

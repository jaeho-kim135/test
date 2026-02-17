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
    private static final String CAST_TO_STRING = "castToString";
    private static final String VALIDATE_ONLY = "validateOnly";
    private static final String SORT_OPTION = "sortOption";
    private static final String VAR_MAP_KEYS = "varMapKeys";
    private static final String VAR_MAP_VALUES = "varMapValues";

    /** Deserialization constructor. */
    public SparkUnpivotJobInput() {
    }

    /**
     * Constructor for normal execution.
     */
    public SparkUnpivotJobInput(final String inputObject, final String outputObject,
            final String[] retainedColumns, final String[] valueColumns,
            final String variableColName, final String valueColName,
            final boolean skipMissingValues, final boolean castToString,
            final String sortOption, final String[] varMapKeys, final String[] varMapValues) {

        addNamedInputObject(inputObject);
        addNamedOutputObject(outputObject);
        set(RETAINED_COLUMNS, retainedColumns);
        set(VALUE_COLUMNS, valueColumns);
        set(VARIABLE_COL_NAME, variableColName);
        set(VALUE_COL_NAME, valueColName);
        set(SKIP_MISSING_VALUES, skipMissingValues);
        set(CAST_TO_STRING, castToString);
        set(VALIDATE_ONLY, false);
        set(SORT_OPTION, sortOption);
        set(VAR_MAP_KEYS, varMapKeys);
        set(VAR_MAP_VALUES, varMapValues);
    }

    /**
     * Constructor for validation-only execution.
     */
    public SparkUnpivotJobInput(final String inputObject,
            final String[] retainedColumns, final String[] valueColumns,
            final String variableColName, final String valueColName,
            final boolean skipMissingValues, final boolean castToString,
            final String sortOption, final String[] varMapKeys, final String[] varMapValues) {

        addNamedInputObject(inputObject);
        set(RETAINED_COLUMNS, retainedColumns);
        set(VALUE_COLUMNS, valueColumns);
        set(VARIABLE_COL_NAME, variableColName);
        set(VALUE_COL_NAME, valueColName);
        set(SKIP_MISSING_VALUES, skipMissingValues);
        set(CAST_TO_STRING, castToString);
        set(VALIDATE_ONLY, true);
        set(SORT_OPTION, sortOption);
        set(VAR_MAP_KEYS, varMapKeys);
        set(VAR_MAP_VALUES, varMapValues);
    }

    public String[] getRetainedColumns() { return get(RETAINED_COLUMNS); }
    public String[] getValueColumns() { return get(VALUE_COLUMNS); }
    public String getVariableColName() { return get(VARIABLE_COL_NAME); }
    public String getValueColName() { return get(VALUE_COL_NAME); }
    public boolean skipMissingValues() { return get(SKIP_MISSING_VALUES); }
    public boolean castToString() { return get(CAST_TO_STRING); }
    public boolean isValidateOnly() { return get(VALIDATE_ONLY); }
    public String getSortOption() { return getOrDefault(SORT_OPTION, "none"); }
    public String[] getVarMapKeys() { return getOrDefault(VAR_MAP_KEYS, new String[0]); }
    public String[] getVarMapValues() { return getOrDefault(VAR_MAP_VALUES, new String[0]); }
}

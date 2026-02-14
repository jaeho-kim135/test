package org.knime.bigdata.spark.dx.node.preproc.unpivot;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelFilterString;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 * Settings for the Spark Unpivot node.
 */
public final class SparkUnpivotSettings {

    static final String CFG_RETAINED_COLUMNS = "retainedColumns";
    static final String CFG_VALUE_COLUMNS = "valueColumns";
    static final String CFG_VARIABLE_COL_NAME = "variableColName";
    static final String CFG_VALUE_COL_NAME = "valueColName";
    static final String CFG_SKIP_MISSING_VALUES = "skipMissingValues";

    private final SettingsModelFilterString m_retainedColumns =
        new SettingsModelFilterString(CFG_RETAINED_COLUMNS);

    private final SettingsModelFilterString m_valueColumns =
        new SettingsModelFilterString(CFG_VALUE_COLUMNS);

    private final SettingsModelString m_variableColName =
        new SettingsModelString(CFG_VARIABLE_COL_NAME, "variable");

    private final SettingsModelString m_valueColName =
        new SettingsModelString(CFG_VALUE_COL_NAME, "value");

    private final SettingsModelBoolean m_skipMissingValues =
        new SettingsModelBoolean(CFG_SKIP_MISSING_VALUES, true);

    /** @return the retained columns filter model */
    public SettingsModelFilterString getRetainedColumnsModel() {
        return m_retainedColumns;
    }

    /** @return the value columns filter model */
    public SettingsModelFilterString getValueColumnsModel() {
        return m_valueColumns;
    }

    /** @return the variable column name model */
    public SettingsModelString getVariableColNameModel() {
        return m_variableColName;
    }

    /** @return the value column name model */
    public SettingsModelString getValueColNameModel() {
        return m_valueColName;
    }

    /** @return the skip missing values model */
    public SettingsModelBoolean getSkipMissingValuesModel() {
        return m_skipMissingValues;
    }

    /** @return list of retained column names */
    public java.util.List<String> getRetainedColumns() {
        return m_retainedColumns.getIncludeList();
    }

    /** @return list of value column names */
    public java.util.List<String> getValueColumns() {
        return m_valueColumns.getIncludeList();
    }

    /** @return the variable column name */
    public String getVariableColName() {
        return m_variableColName.getStringValue();
    }

    /** @return the value column name */
    public String getValueColName() {
        return m_valueColName.getStringValue();
    }

    /** @return whether to skip missing values */
    public boolean skipMissingValues() {
        return m_skipMissingValues.getBooleanValue();
    }

    /**
     * Save settings.
     * @param settings the settings to save to
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        m_retainedColumns.saveSettingsTo(settings);
        m_valueColumns.saveSettingsTo(settings);
        m_variableColName.saveSettingsTo(settings);
        m_valueColName.saveSettingsTo(settings);
        m_skipMissingValues.saveSettingsTo(settings);
    }

    /**
     * Validate settings.
     * @param settings the settings to validate
     * @throws InvalidSettingsException if settings are invalid
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_retainedColumns.validateSettings(settings);
        m_valueColumns.validateSettings(settings);
        m_variableColName.validateSettings(settings);
        m_valueColName.validateSettings(settings);
        m_skipMissingValues.validateSettings(settings);
    }

    /**
     * Load validated settings.
     * @param settings the settings to load from
     * @throws InvalidSettingsException if settings cannot be loaded
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_retainedColumns.loadSettingsFrom(settings);
        m_valueColumns.loadSettingsFrom(settings);
        m_variableColName.loadSettingsFrom(settings);
        m_valueColName.loadSettingsFrom(settings);
        m_skipMissingValues.loadSettingsFrom(settings);
    }
}

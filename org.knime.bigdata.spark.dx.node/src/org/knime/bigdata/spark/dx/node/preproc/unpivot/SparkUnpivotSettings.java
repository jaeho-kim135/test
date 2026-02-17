package org.knime.bigdata.spark.dx.node.preproc.unpivot;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
    static final String CFG_CAST_TO_STRING = "castToString";
    static final String CFG_SORT_OPTION = "sortOption";
    static final String CFG_VARIABLE_VALUE_MAP = "variableValueMap";

    /** Sort option constants. */
    public static final String SORT_NONE = "none";
    public static final String SORT_BY_RETAINED = "retained";
    public static final String SORT_BY_VARIABLE = "variable";

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

    private final SettingsModelBoolean m_castToString =
        new SettingsModelBoolean(CFG_CAST_TO_STRING, false);

    private String m_sortOption = SORT_NONE;

    /** Map from original column name → custom variable label. Empty map = use column names as-is. */
    private final Map<String, String> m_variableValueMap = new LinkedHashMap<>();

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

    /** @return the cast to string model */
    public SettingsModelBoolean getCastToStringModel() {
        return m_castToString;
    }

    /** @return list of retained column names */
    public List<String> getRetainedColumns() {
        return m_retainedColumns.getIncludeList();
    }

    /** @return list of value column names */
    public List<String> getValueColumns() {
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

    /** @return whether to cast all value columns to string before unpivoting */
    public boolean castToString() {
        return m_castToString.getBooleanValue();
    }

    /** @return the sort option */
    public String getSortOption() {
        return m_sortOption;
    }

    /** @param sortOption the sort option to set */
    public void setSortOption(final String sortOption) {
        m_sortOption = sortOption;
    }

    /** @return the variable value map (column name → custom label) */
    public Map<String, String> getVariableValueMap() {
        return m_variableValueMap;
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
        m_castToString.saveSettingsTo(settings);
        settings.addString(CFG_SORT_OPTION, m_sortOption);
        // Save variable value map as parallel arrays
        final String[] keys = m_variableValueMap.keySet().toArray(new String[0]);
        final String[] vals = m_variableValueMap.values().toArray(new String[0]);
        settings.addStringArray(CFG_VARIABLE_VALUE_MAP + "_keys", keys);
        settings.addStringArray(CFG_VARIABLE_VALUE_MAP + "_values", vals);
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
        if (settings.containsKey(CFG_CAST_TO_STRING)) {
            m_castToString.validateSettings(settings);
        }
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
        if (settings.containsKey(CFG_CAST_TO_STRING)) {
            m_castToString.loadSettingsFrom(settings);
        }
        m_sortOption = settings.getString(CFG_SORT_OPTION, SORT_NONE);
        // Load variable value map
        m_variableValueMap.clear();
        final String keysKey = CFG_VARIABLE_VALUE_MAP + "_keys";
        final String valsKey = CFG_VARIABLE_VALUE_MAP + "_values";
        if (settings.containsKey(keysKey) && settings.containsKey(valsKey)) {
            final String[] keys = settings.getStringArray(keysKey);
            final String[] vals = settings.getStringArray(valsKey);
            for (int i = 0; i < Math.min(keys.length, vals.length); i++) {
                if (vals[i] != null && !vals[i].isEmpty() && !vals[i].equals(keys[i])) {
                    m_variableValueMap.put(keys[i], vals[i]);
                }
            }
        }
    }
}

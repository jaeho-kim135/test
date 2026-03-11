package org.knime.bigdata.spark.dx.node.sql.multiquery;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelFilterString;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 * Settings for the Spark Multi Query node.
 */
public final class SparkMultiQuerySettings {

    /** Placeholder token replaced with each target column name at execution time. */
    public static final String COLUMN_PLACEHOLDER = "$columnS";

    static final String CFG_TARGET_COLUMNS = "targetColumns";
    static final String CFG_SQL_EXPRESSION = "sqlExpression";
    static final String CFG_KEEP_ORIGINAL = "keepOriginalColumns";
    static final String CFG_OUTPUT_PATTERN = "outputColumnPattern";

    private final SettingsModelFilterString m_targetColumns =
        new SettingsModelFilterString(CFG_TARGET_COLUMNS);

    private final SettingsModelString m_sqlExpression =
        new SettingsModelString(CFG_SQL_EXPRESSION, "string(" + COLUMN_PLACEHOLDER + ")");

    private final SettingsModelBoolean m_keepOriginal =
        new SettingsModelBoolean(CFG_KEEP_ORIGINAL, false);

    private final SettingsModelString m_outputPattern =
        new SettingsModelString(CFG_OUTPUT_PATTERN, COLUMN_PLACEHOLDER);

    /** @return the target columns filter model */
    public SettingsModelFilterString getTargetColumnsModel() {
        return m_targetColumns;
    }

    /** @return the SQL expression model */
    public SettingsModelString getSqlExpressionModel() {
        return m_sqlExpression;
    }

    /** @return the keep original columns model */
    public SettingsModelBoolean getKeepOriginalModel() {
        return m_keepOriginal;
    }

    /** @return the output column pattern model */
    public SettingsModelString getOutputPatternModel() {
        return m_outputPattern;
    }

    /** @return list of target column names */
    public java.util.List<String> getTargetColumns() {
        return m_targetColumns.getIncludeList();
    }

    /** @return the SQL expression template */
    public String getSqlExpression() {
        return m_sqlExpression.getStringValue();
    }

    /** @return whether to keep original columns alongside transformed ones */
    public boolean keepOriginalColumns() {
        return m_keepOriginal.getBooleanValue();
    }

    /** @return the output column name pattern (uses $columnS placeholder) */
    public String getOutputColumnPattern() {
        return m_outputPattern.getStringValue();
    }

    /**
     * Save settings.
     * @param settings the settings to save to
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        m_targetColumns.saveSettingsTo(settings);
        m_sqlExpression.saveSettingsTo(settings);
        m_keepOriginal.saveSettingsTo(settings);
        m_outputPattern.saveSettingsTo(settings);
    }

    /**
     * Validate settings.
     * @param settings the settings to validate
     * @throws InvalidSettingsException if settings are invalid
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_targetColumns.validateSettings(settings);
        m_sqlExpression.validateSettings(settings);
        if (settings.containsKey(CFG_KEEP_ORIGINAL)) {
            m_keepOriginal.validateSettings(settings);
        }
        if (settings.containsKey(CFG_OUTPUT_PATTERN)) {
            m_outputPattern.validateSettings(settings);
        }
    }

    /**
     * Load validated settings.
     * @param settings the settings to load from
     * @throws InvalidSettingsException if settings cannot be loaded
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_targetColumns.loadSettingsFrom(settings);
        m_sqlExpression.loadSettingsFrom(settings);
        if (settings.containsKey(CFG_KEEP_ORIGINAL)) {
            m_keepOriginal.loadSettingsFrom(settings);
        }
        if (settings.containsKey(CFG_OUTPUT_PATTERN)) {
            m_outputPattern.loadSettingsFrom(settings);
        }
    }
}

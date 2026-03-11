package org.knime.bigdata.spark.dx.node.preproc.unpivot;

import java.util.Arrays;
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
 *
 * <p>Column filter settings are stored in NameFilterConfiguration format
 * (filter-type / included_names / excluded_names / enforce_option), which is the format
 * used by the WebUI dialog's LegacyColumnFilterPersistor. For backward compatibility,
 * loadSettingsFrom() also accepts the old SettingsModelFilterString format
 * (InclList / ExclList / keep_all_columns_selected).</p>
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
    static final String CFG_CONFIGURED = "nodeConfigured";

    /** Key used by NameFilterConfiguration / LegacyColumnFilterPersistor for the filter type. */
    private static final String KEY_FILTER_TYPE = "filter-type";
    /** Filter type value for manual (name-based) selection. */
    private static final String FILTER_TYPE_STANDARD = "STANDARD";
    /** Key used by NameFilterConfiguration for the included names list. */
    private static final String KEY_INCLUDED_NAMES = "included_names";
    /** Key used by NameFilterConfiguration for the excluded names list. */
    private static final String KEY_EXCLUDED_NAMES = "excluded_names";
    /** Key used by NameFilterConfiguration for the enforce option. */
    private static final String KEY_ENFORCE_OPTION = "enforce_option";
    /** EnforceOption value meaning "exclude unknown columns" (i.e. include list is authoritative). */
    private static final String ENFORCE_INCLUSION = "EnforceInclusion";

    /** Sort option constants. */
    public static final String SORT_NONE = "none";
    public static final String SORT_BY_RETAINED = "retained";
    public static final String SORT_BY_VARIABLE = "variable";

    private final SettingsModelFilterString m_retainedColumns =
        new SettingsModelFilterString(CFG_RETAINED_COLUMNS, new String[0], new String[0], false);

    private final SettingsModelFilterString m_valueColumns =
        new SettingsModelFilterString(CFG_VALUE_COLUMNS, new String[0], new String[0], false);

    private final SettingsModelString m_variableColName =
        new SettingsModelString(CFG_VARIABLE_COL_NAME, "column");

    private final SettingsModelString m_valueColName =
        new SettingsModelString(CFG_VALUE_COL_NAME, "value");

    private final SettingsModelBoolean m_skipMissingValues =
        new SettingsModelBoolean(CFG_SKIP_MISSING_VALUES, true);

    private final SettingsModelBoolean m_castToString =
        new SettingsModelBoolean(CFG_CAST_TO_STRING, false);

    private String m_sortOption = SORT_NONE;

    /** Map from original column name → custom variable label. Empty map = use column names as-is. */
    private final Map<String, String> m_variableValueMap = new LinkedHashMap<>();

    /** True once the user has accepted the dialog settings with OK at least once. */
    private boolean m_nodeConfigured = false;

    /** @return true if the node has been configured (user clicked OK at least once) */
    public boolean isNodeConfigured() {
        return m_nodeConfigured;
    }

    /** @param configured set to true when user accepts the dialog */
    public void setNodeConfigured(final boolean configured) {
        m_nodeConfigured = configured;
    }

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
     * Column filters are written in NameFilterConfiguration format (compatible with WebUI dialog).
     * @param settings the settings to save to
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        writeColumnFilter(settings, CFG_RETAINED_COLUMNS, m_retainedColumns.getIncludeList());
        writeColumnFilter(settings, CFG_VALUE_COLUMNS, m_valueColumns.getIncludeList());
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
        // Only written once the user has accepted the dialog with OK; omitted for fresh nodes
        if (m_nodeConfigured) {
            settings.addBoolean(CFG_CONFIGURED, true);
        }
    }

    /**
     * Validate settings.
     * Accepts both NameFilterConfiguration format (new) and SettingsModelFilterString format (old).
     * @param settings the settings to validate
     * @throws InvalidSettingsException if settings are invalid
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        validateColumnFilter(settings, CFG_RETAINED_COLUMNS);
        validateColumnFilter(settings, CFG_VALUE_COLUMNS);
        m_variableColName.validateSettings(settings);
        m_valueColName.validateSettings(settings);
        m_skipMissingValues.validateSettings(settings);
        if (settings.containsKey(CFG_CAST_TO_STRING)) {
            m_castToString.validateSettings(settings);
        }
    }

    /**
     * Load validated settings.
     * Handles both NameFilterConfiguration format (new, written by WebUI dialog) and
     * SettingsModelFilterString format (old, for backward compatibility with saved workflows).
     * @param settings the settings to load from
     * @throws InvalidSettingsException if settings cannot be loaded
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        loadColumnFilter(settings, CFG_RETAINED_COLUMNS, m_retainedColumns);
        loadColumnFilter(settings, CFG_VALUE_COLUMNS, m_valueColumns);
        m_variableColName.loadSettingsFrom(settings);
        m_valueColName.loadSettingsFrom(settings);
        m_skipMissingValues.loadSettingsFrom(settings);
        if (settings.containsKey(CFG_CAST_TO_STRING)) {
            m_castToString.loadSettingsFrom(settings);
        }
        m_sortOption = settings.getString(CFG_SORT_OPTION, SORT_NONE);
        m_nodeConfigured = settings.containsKey(CFG_CONFIGURED);
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

    // ── Column filter format helpers ──────────────────────────────────────────

    /**
     * Writes a column filter in NameFilterConfiguration format (filter-type / included_names /
     * excluded_names / enforce_option). This format is compatible with LegacyColumnFilterPersistor
     * used by the WebUI dialog.
     */
    private static void writeColumnFilter(final NodeSettingsWO settings, final String key,
            final List<String> included) {
        final NodeSettingsWO sub = settings.addNodeSettings(key);
        sub.addString(KEY_FILTER_TYPE, FILTER_TYPE_STANDARD);
        sub.addStringArray(KEY_INCLUDED_NAMES, included.toArray(new String[0]));
        sub.addStringArray(KEY_EXCLUDED_NAMES, new String[0]);
        sub.addString(KEY_ENFORCE_OPTION, ENFORCE_INCLUSION);
    }

    /**
     * Loads a column filter, handling both formats:
     * <ul>
     *   <li>New (NameFilterConfiguration): sub-config contains {@code included_names}</li>
     *   <li>Old (SettingsModelFilterString): sub-config contains {@code InclList}</li>
     * </ul>
     * If the key is absent, the model is left at its default value.
     */
    private static void loadColumnFilter(final NodeSettingsRO settings, final String key,
            final SettingsModelFilterString model) throws InvalidSettingsException {
        if (!settings.containsKey(key)) {
            return;
        }
        try {
            final NodeSettingsRO sub = settings.getNodeSettings(key);
            if (sub.containsKey(KEY_INCLUDED_NAMES)) {
                // New format: NameFilterConfiguration / LegacyColumnFilterPersistor
                final String[] incl = sub.getStringArray(KEY_INCLUDED_NAMES, new String[0]);
                model.setIncludeList(Arrays.asList(incl));
                return;
            }
        } catch (final InvalidSettingsException e) {
            // Sub-config is Config type (old format) — fall through to old-format handler
        }
        // Old format: SettingsModelFilterString (InclList / ExclList / keep_all_columns_selected)
        model.loadSettingsFrom(settings);
    }

    /**
     * Validates a column filter entry. Accepts both new (NameFilterConfiguration) and
     * old (SettingsModelFilterString) formats. Simply checks the key is present.
     */
    private static void validateColumnFilter(final NodeSettingsRO settings, final String key)
            throws InvalidSettingsException {
        if (!settings.containsKey(key)) {
            throw new InvalidSettingsException(
                "Missing column filter configuration for key '" + key + "'.");
        }
        // Both formats are acceptable — detailed content validation is left to loadSettingsFrom()
    }
}

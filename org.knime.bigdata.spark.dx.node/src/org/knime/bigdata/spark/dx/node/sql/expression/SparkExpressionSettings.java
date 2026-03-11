package org.knime.bigdata.spark.dx.node.sql.expression;

import java.util.ArrayList;
import java.util.List;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

/**
 * Settings for the Spark Expression node.
 * Stores a list of expression entries, each consisting of:
 * <ul>
 *   <li>A Spark SQL expression string</li>
 *   <li>An output mode (APPEND or REPLACE)</li>
 *   <li>An output column name</li>
 * </ul>
 */
public final class SparkExpressionSettings {

    /** Output mode: append a new column or replace an existing one. */
    public enum OutputMode {
        /** Append a new column to the output. */
        APPEND,
        /** Replace an existing column in the output. */
        REPLACE
    }

    static final String CFG_EXPRESSIONS = "expressions";
    static final String CFG_OUTPUT_MODES = "outputModes";
    static final String CFG_COLUMN_NAMES = "columnNames";
    static final String CFG_CONFIGURED = "nodeConfigured";

    private final List<String> m_expressions = new ArrayList<>();
    private final List<OutputMode> m_outputModes = new ArrayList<>();
    private final List<String> m_columnNames = new ArrayList<>();
    private boolean m_nodeConfigured = false;

    /** Creates settings with a single empty expression row. */
    public SparkExpressionSettings() {
        m_expressions.add("");
        m_outputModes.add(OutputMode.APPEND);
        m_columnNames.add("new_column");
    }

    // ── Getters ──────────────────────────────────────────────────────────────

    /** @return number of expression entries */
    public int getExpressionCount() {
        return m_expressions.size();
    }

    /** @return list of expression strings */
    public List<String> getExpressions() {
        return m_expressions;
    }

    /** @return list of output modes */
    public List<OutputMode> getOutputModes() {
        return m_outputModes;
    }

    /** @return list of output column names */
    public List<String> getColumnNames() {
        return m_columnNames;
    }

    /** @return true if the node has been configured (dialog was opened and OK'd) */
    public boolean isNodeConfigured() {
        return m_nodeConfigured;
    }

    // ── Setters ──────────────────────────────────────────────────────────────

    /** Replaces all expression entries with the given lists. */
    public void setExpressions(final List<String> expressions, final List<OutputMode> modes,
            final List<String> columnNames) {
        m_expressions.clear();
        m_outputModes.clear();
        m_columnNames.clear();
        m_expressions.addAll(expressions);
        m_outputModes.addAll(modes);
        m_columnNames.addAll(columnNames);
    }

    /** @param configured set to true when user accepts the dialog */
    public void setNodeConfigured(final boolean configured) {
        m_nodeConfigured = configured;
    }

    // ── Persistence ──────────────────────────────────────────────────────────

    /**
     * Save settings.
     * @param settings the settings to save to
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        settings.addStringArray(CFG_EXPRESSIONS, m_expressions.toArray(new String[0]));
        final String[] modes = m_outputModes.stream()
            .map(OutputMode::name)
            .toArray(String[]::new);
        settings.addStringArray(CFG_OUTPUT_MODES, modes);
        settings.addStringArray(CFG_COLUMN_NAMES, m_columnNames.toArray(new String[0]));
        if (m_nodeConfigured) {
            settings.addBoolean(CFG_CONFIGURED, true);
        }
    }

    /**
     * Validate settings.
     * @param settings the settings to validate
     * @throws InvalidSettingsException if settings are invalid
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        final String[] expressions = settings.getStringArray(CFG_EXPRESSIONS);
        final String[] modes = settings.getStringArray(CFG_OUTPUT_MODES);
        final String[] names = settings.getStringArray(CFG_COLUMN_NAMES);

        if (expressions.length == 0) {
            throw new InvalidSettingsException("At least one expression is required.");
        }
        if (expressions.length != modes.length || expressions.length != names.length) {
            throw new InvalidSettingsException(
                "Expressions, output modes, and column names must have the same number of entries.");
        }

        for (int i = 0; i < expressions.length; i++) {
            if (expressions[i] == null || expressions[i].trim().isEmpty()) {
                throw new InvalidSettingsException(
                    "Expression " + (i + 1) + " is empty. Enter a Spark SQL expression.");
            }
            if (names[i] == null || names[i].trim().isEmpty()) {
                throw new InvalidSettingsException(
                    "Output column name for Expression " + (i + 1) + " is empty.");
            }
            try {
                OutputMode.valueOf(modes[i]);
            } catch (final IllegalArgumentException e) {
                throw new InvalidSettingsException(
                    "Invalid output mode '" + modes[i] + "' for Expression " + (i + 1) + ".");
            }
        }
    }

    /**
     * Load validated settings.
     * @param settings the settings to load from
     * @throws InvalidSettingsException if settings cannot be loaded
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        final String[] expressions = settings.getStringArray(CFG_EXPRESSIONS);
        final String[] modes = settings.getStringArray(CFG_OUTPUT_MODES);
        final String[] names = settings.getStringArray(CFG_COLUMN_NAMES);

        m_expressions.clear();
        m_outputModes.clear();
        m_columnNames.clear();

        for (int i = 0; i < expressions.length; i++) {
            m_expressions.add(expressions[i]);
            m_outputModes.add(OutputMode.valueOf(modes[i]));
            m_columnNames.add(names[i]);
        }

        m_nodeConfigured = settings.containsKey(CFG_CONFIGURED);
    }
}

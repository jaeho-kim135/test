package org.knime.bigdata.spark.dx.node.sql.expression;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.webui.node.dialog.NodeAndVariableSettingsRO;
import org.knime.core.webui.node.dialog.NodeAndVariableSettingsWO;
import org.knime.core.webui.node.dialog.SettingsType;
import org.knime.scripting.editor.GenericSettingsIOManager;
import org.knime.scripting.editor.ScriptingNodeSettings;

/**
 * WebUI settings adapter for the Spark Expression node.
 * Bridges between the existing NodeSettings format ({@link SparkExpressionSettings})
 * and the JSON format consumed by the WebUI frontend.
 *
 * <p>Extends {@link ScriptingNodeSettings} and implements {@link GenericSettingsIOManager}
 * to work with {@link org.knime.scripting.editor.ScriptingNodeSettingsService}.
 */
@SuppressWarnings("restriction")
final class SparkExpressionWebSettings extends ScriptingNodeSettings implements GenericSettingsIOManager {

    // JSON keys for frontend exchange
    private static final String JSON_KEY_EXPRESSIONS = "expressions";
    private static final String JSON_KEY_OUTPUT_MODES = "outputModes";
    private static final String JSON_KEY_COLUMN_NAMES = "columnNames";

    private List<String> m_expressions;
    private List<String> m_outputModes;
    private List<String> m_columnNames;
    private boolean m_nodeConfigured;

    /** Creates default settings with one empty expression. */
    SparkExpressionWebSettings() {
        super(SettingsType.MODEL);
        m_expressions = new ArrayList<>(Arrays.asList(""));
        m_outputModes = new ArrayList<>(Arrays.asList("APPEND"));
        m_columnNames = new ArrayList<>(Arrays.asList("new_column"));
        m_nodeConfigured = false;
    }

    @Override
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        // Guard for new/unconfigured nodes that have no saved settings yet
        if (!settings.containsKey(SparkExpressionSettings.CFG_EXPRESSIONS)) {
            return; // keep defaults from constructor
        }
        m_expressions = new ArrayList<>(
            Arrays.asList(settings.getStringArray(SparkExpressionSettings.CFG_EXPRESSIONS)));
        m_outputModes = new ArrayList<>(
            Arrays.asList(settings.getStringArray(SparkExpressionSettings.CFG_OUTPUT_MODES)));
        m_columnNames = new ArrayList<>(
            Arrays.asList(settings.getStringArray(SparkExpressionSettings.CFG_COLUMN_NAMES)));
        m_nodeConfigured = settings.containsKey(SparkExpressionSettings.CFG_CONFIGURED);
    }

    @Override
    public void saveSettingsTo(final NodeSettingsWO settings) {
        settings.addStringArray(SparkExpressionSettings.CFG_EXPRESSIONS,
            m_expressions.toArray(new String[0]));
        settings.addStringArray(SparkExpressionSettings.CFG_OUTPUT_MODES,
            m_outputModes.toArray(new String[0]));
        settings.addStringArray(SparkExpressionSettings.CFG_COLUMN_NAMES,
            m_columnNames.toArray(new String[0]));
        settings.addBoolean(SparkExpressionSettings.CFG_CONFIGURED, true);
    }

    @Override
    public Map<String, Object> convertNodeSettingsToMap(
            final Map<SettingsType, NodeAndVariableSettingsRO> settings) throws InvalidSettingsException {
        loadSettingsFrom(settings);

        final Map<String, Object> map = new HashMap<>();
        map.put(JSON_KEY_EXPRESSIONS, m_expressions);
        map.put(JSON_KEY_OUTPUT_MODES, m_outputModes);
        map.put(JSON_KEY_COLUMN_NAMES, m_columnNames);
        return map;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void writeMapToNodeSettings(final Map<String, Object> data,
            final Map<SettingsType, NodeAndVariableSettingsRO> previousSettings,
            final Map<SettingsType, NodeAndVariableSettingsWO> settings) throws InvalidSettingsException {

        final var rawExpressions = data.get(JSON_KEY_EXPRESSIONS);
        final var rawOutputModes = data.get(JSON_KEY_OUTPUT_MODES);
        final var rawColumnNames = data.get(JSON_KEY_COLUMN_NAMES);

        if (rawExpressions == null || rawOutputModes == null || rawColumnNames == null) {
            throw new InvalidSettingsException("Missing required settings data from the dialog.");
        }

        m_expressions = new ArrayList<>((List<String>) rawExpressions);
        m_outputModes = ((List<Object>) rawOutputModes).stream()
            .map(Object::toString)
            .collect(Collectors.toList());
        m_columnNames = new ArrayList<>((List<String>) rawColumnNames);

        // Validate array lengths match
        if (m_expressions.size() != m_outputModes.size()
                || m_expressions.size() != m_columnNames.size()) {
            throw new InvalidSettingsException(
                "Expressions, output modes, and column names must have the same number of entries.");
        }

        // Validate output mode values against the enum
        for (int i = 0; i < m_outputModes.size(); i++) {
            try {
                SparkExpressionSettings.OutputMode.valueOf(m_outputModes.get(i));
            } catch (final IllegalArgumentException e) {
                throw new InvalidSettingsException(
                    "Invalid output mode '" + m_outputModes.get(i) + "' for Expression " + (i + 1) + ".");
            }
        }

        m_nodeConfigured = true;

        // Save directly to output settings — no flow variable correction needed
        // because this node does not support per-setting flow variable overrides.
        saveSettingsTo(settings.get(SettingsType.MODEL));
        copyVariableSettings(previousSettings, settings);
    }
}

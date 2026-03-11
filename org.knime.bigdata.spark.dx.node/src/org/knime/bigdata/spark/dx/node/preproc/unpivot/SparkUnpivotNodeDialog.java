package org.knime.bigdata.spark.dx.node.preproc.unpivot;

import java.awt.BorderLayout;
import java.awt.CardLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Container;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.swing.BorderFactory;
import javax.swing.DefaultListCellRenderer;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.SwingUtilities;
import javax.swing.SwingWorker;
import javax.swing.table.DefaultTableModel;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.node.DataAwareNodeDialogPane;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnFilter;
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelFilterString;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;

/**
 * Dialog for the Spark Unpivot node.
 * Enhanced with: type display, row estimation, validation check, variable mapping, sort, auto-detect.
 */
public final class SparkUnpivotNodeDialog extends DataAwareNodeDialogPane {

    private static final Color COLOR_SUCCESS = new Color(0, 128, 0);
    private static final Color COLOR_ERROR = new Color(200, 0, 0);
    private static final Color COLOR_CHECK_BG = new Color(255, 204, 0);

    // --- Settings models ---
    private final SettingsModelFilterString m_retainedColumns =
        new SettingsModelFilterString(SparkUnpivotSettings.CFG_RETAINED_COLUMNS, new String[0], new String[0], false);
    private final SettingsModelFilterString m_valueColumns =
        new SettingsModelFilterString(SparkUnpivotSettings.CFG_VALUE_COLUMNS, new String[0], new String[0], false);
    private final SettingsModelString m_variableColName =
        new SettingsModelString(SparkUnpivotSettings.CFG_VARIABLE_COL_NAME, "variable");
    private final SettingsModelString m_valueColName =
        new SettingsModelString(SparkUnpivotSettings.CFG_VALUE_COL_NAME, "value");
    private final SettingsModelBoolean m_skipMissingValues =
        new SettingsModelBoolean(SparkUnpivotSettings.CFG_SKIP_MISSING_VALUES, true);
    private final SettingsModelBoolean m_castToString =
        new SettingsModelBoolean(SparkUnpivotSettings.CFG_CAST_TO_STRING, false);

    // --- Column filter components ---
    private final DialogComponentColumnFilter m_retainedColFilter =
        new DialogComponentColumnFilter(m_retainedColumns, 0, false);
    private final DialogComponentColumnFilter m_valueColFilter =
        new DialogComponentColumnFilter(m_valueColumns, 0, false);

    // --- Options components ---
    private final DialogComponentString m_variableColNameComp =
        new DialogComponentString(m_variableColName, "Variable column name: ");
    private final DialogComponentString m_valueColNameComp =
        new DialogComponentString(m_valueColName, "Value column name: ");
    private final DialogComponentBoolean m_skipMissingComp =
        new DialogComponentBoolean(m_skipMissingValues, "Skip missing values");
    private final DialogComponentBoolean m_castToStringComp =
        new DialogComponentBoolean(m_castToString, "Cast all value columns to String");

    // --- New UI components ---
    private final JComboBox<String> m_sortCombo = new JComboBox<>(
        new String[]{"No sorting", "Sort by retained columns", "Sort by variable column"});
    private final JLabel m_rowEstimateLabel = new JLabel(" ");
    private final DefaultTableModel m_varMapTableModel = new DefaultTableModel(
        new String[]{"Column Name", "Variable Label"}, 0);
    private final JTable m_varMapTable = new JTable(m_varMapTableModel);

    // --- Validation components ---
    private final JButton m_checkButton = new JButton("Check");
    private final JTextArea m_validationArea = new JTextArea(6, 40);

    // --- State ---
    private SparkContextID m_contextID;
    private String m_dataFrameID;
    private DataTableSpec m_tableSpec;
    private long m_inputRowCount = -1;
    /**
     * True once the user has successfully clicked OK in this dialog session.
     * Used in addition to CFG_CONFIGURED (which is persisted in NodeModel settings)
     * to prevent incorrectly using the fresh branch after OK was clicked this session.
     */
    private boolean m_everSavedWithOk = false;

    /** Constructor. */
    public SparkUnpivotNodeDialog() {
        m_retainedColFilter.setIncludeTitle(" Retained column(s) ");
        m_retainedColFilter.setExcludeTitle(" Available column(s) ");

        m_valueColFilter.setIncludeTitle(" Value column(s) ");
        m_valueColFilter.setExcludeTitle(" Available column(s) ");

        // Listen for column/option changes → update row estimate + variable map table
        m_retainedColumns.addChangeListener(e -> updateRowEstimate());
        m_valueColumns.addChangeListener(e -> {
            updateRowEstimate();
            updateVarMapTable();
        });
        m_skipMissingValues.addChangeListener(e -> updateRowEstimate());

        // Row estimation label
        m_rowEstimateLabel.setFont(m_rowEstimateLabel.getFont().deriveFont(Font.ITALIC, 11f));

        // Validation area
        m_validationArea.setEditable(false);
        m_validationArea.setLineWrap(true);
        m_validationArea.setWrapStyleWord(true);
        m_validationArea.setFont(new Font("Monospaced", Font.PLAIN, 11));
        m_validationArea.setOpaque(false);
        m_validationArea.setBorder(null);

        // Check button
        m_checkButton.setBackground(COLOR_CHECK_BG);
        m_checkButton.setOpaque(true);
        m_checkButton.setFocusPainted(false);
        m_checkButton.addActionListener(e -> runValidation());

        // Variable map table
        m_varMapTable.setRowHeight(22);
        m_varMapTable.getColumnModel().getColumn(0).setPreferredWidth(150);
        m_varMapTable.getColumnModel().getColumn(1).setPreferredWidth(150);

        // --- Build tabs ---
        addTab("Retained Columns", m_retainedColFilter.getComponentPanel());

        // Value Columns tab
        addTab("Value Columns", m_valueColFilter.getComponentPanel());

        // Options tab
        addTab("Options", buildOptionsPanel());

        // Validation tab
        addTab("Validation", buildValidationPanel());
    }

    private JPanel buildOptionsPanel() {
        final JPanel panel = new JPanel(new GridBagLayout());
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(4, 6, 4, 6);
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.weightx = 1.0;

        // --- Output Settings ---
        final JPanel outPanel = new JPanel(new GridBagLayout());
        outPanel.setBorder(BorderFactory.createTitledBorder("Output Settings"));
        final GridBagConstraints ogbc = new GridBagConstraints();
        ogbc.insets = new Insets(3, 5, 3, 5);
        ogbc.anchor = GridBagConstraints.WEST;
        ogbc.fill = GridBagConstraints.HORIZONTAL;
        ogbc.weightx = 1.0;
        ogbc.gridx = 0;
        ogbc.gridy = 0;
        outPanel.add(m_variableColNameComp.getComponentPanel(), ogbc);
        ogbc.gridy++;
        outPanel.add(m_valueColNameComp.getComponentPanel(), ogbc);
        ogbc.gridy++;
        outPanel.add(m_skipMissingComp.getComponentPanel(), ogbc);
        ogbc.gridy++;
        outPanel.add(m_castToStringComp.getComponentPanel(), ogbc);
        ogbc.gridy++;
        // Sort option
        final JPanel sortPanel = new JPanel(new BorderLayout(5, 0));
        sortPanel.add(new JLabel("Sort output:"), BorderLayout.WEST);
        sortPanel.add(m_sortCombo, BorderLayout.CENTER);
        outPanel.add(sortPanel, ogbc);
        ogbc.gridy++;
        // Row estimate
        outPanel.add(m_rowEstimateLabel, ogbc);
        panel.add(outPanel, gbc);

        // --- Variable Value Mapping ---
        gbc.gridy++;
        gbc.fill = GridBagConstraints.BOTH;
        gbc.weighty = 1.0;
        final JPanel mapPanel = new JPanel(new BorderLayout());
        mapPanel.setBorder(BorderFactory.createTitledBorder("Variable Value Mapping (optional)"));
        final JScrollPane mapScroll = new JScrollPane(m_varMapTable);
        mapScroll.setPreferredSize(new Dimension(400, 120));
        mapPanel.add(mapScroll, BorderLayout.CENTER);
        final JLabel mapHint = new JLabel(" Edit 'Variable Label' to customize variable column values");
        mapHint.setFont(mapHint.getFont().deriveFont(Font.ITALIC, 11f));
        mapPanel.add(mapHint, BorderLayout.SOUTH);
        panel.add(mapPanel, gbc);

        return panel;
    }

    private JPanel buildValidationPanel() {
        final JPanel panel = new JPanel(new BorderLayout(5, 5));
        panel.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));

        final JPanel topPanel = new JPanel(new BorderLayout(10, 0));
        topPanel.add(m_checkButton, BorderLayout.WEST);
        topPanel.add(new JLabel("Run test unpivot with sample data (LIMIT 5)"), BorderLayout.CENTER);
        panel.add(topPanel, BorderLayout.NORTH);

        final JScrollPane validScroll = new JScrollPane(m_validationArea);
        validScroll.setPreferredSize(new Dimension(400, 200));
        panel.add(validScroll, BorderLayout.CENTER);

        return panel;
    }

    // =================================================================
    // Row estimation (#2)
    // =================================================================

    private void updateRowEstimate() {
        final List<String> valueCols = m_valueColumns.getIncludeList();
        final int numValueCols = (valueCols != null) ? valueCols.size() : 0;
        final boolean skipMissing = m_skipMissingValues.getBooleanValue();
        final String skipNote = skipMissing
            ? "  (max - actual may be less due to Skip missing values)" : "";
        if (numValueCols == 0) {
            m_rowEstimateLabel.setText("Estimated output: (select value columns)");
        } else if (m_inputRowCount >= 0) {
            final long totalRows = m_inputRowCount * numValueCols;
            m_rowEstimateLabel.setText("Estimated output: "
                + String.format("%,d", m_inputRowCount) + " rows x " + numValueCols
                + " value columns = " + String.format("%,d", totalRows) + " rows" + skipNote);
        } else {
            m_rowEstimateLabel.setText("Estimated output: ? rows x " + numValueCols
                + " value columns = ? rows  (counting...)");
        }
    }

    // =================================================================
    // Variable value map table (#4)
    // =================================================================

    private void updateVarMapTable() {
        // Stop editing to avoid data loss
        if (m_varMapTable.isEditing()) {
            m_varMapTable.getCellEditor().stopCellEditing();
        }
        // Save current edits
        final Map<String, String> currentEdits = getVarMapFromTable();
        // Rebuild table
        m_varMapTableModel.setRowCount(0);
        final List<String> valueCols = m_valueColumns.getIncludeList();
        if (valueCols != null) {
            for (String col : valueCols) {
                final String label = currentEdits.getOrDefault(col, col);
                m_varMapTableModel.addRow(new Object[]{col, label});
            }
        }
    }

    private Map<String, String> getVarMapFromTable() {
        if (m_varMapTable.isEditing()) {
            m_varMapTable.getCellEditor().stopCellEditing();
        }
        final Map<String, String> map = new HashMap<>();
        for (int i = 0; i < m_varMapTableModel.getRowCount(); i++) {
            final String key = (String) m_varMapTableModel.getValueAt(i, 0);
            final String val = (String) m_varMapTableModel.getValueAt(i, 1);
            if (val != null && !val.isEmpty() && !val.equals(key)) {
                map.put(key, val);
            }
        }
        return map;
    }

    // =================================================================
    // Load / Save
    // =================================================================

    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObject[] input)
            throws NotConfigurableException {
        if (input == null || input.length < 1 || input[0] == null) {
            throw new NotConfigurableException("No input connection found.");
        }
        final SparkDataPortObject sparkPort = (SparkDataPortObject) input[0];
        m_contextID = sparkPort.getContextID();
        m_dataFrameID = sparkPort.getData().getID();
        m_tableSpec = sparkPort.getSpec().getTableSpec();
        loadCommonSettings(settings, m_tableSpec);
        fetchInputRowCount();
    }

    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
            throws NotConfigurableException {
        if (specs == null || specs.length < 1 || specs[0] == null) {
            throw new NotConfigurableException("No input connection found.");
        }
        final SparkDataPortObjectSpec sparkSpec = (SparkDataPortObjectSpec) specs[0];
        m_contextID = sparkSpec.getContextID();
        m_dataFrameID = null;
        m_tableSpec = sparkSpec.getTableSpec();
        loadCommonSettings(settings, m_tableSpec);
    }

    private void loadCommonSettings(final NodeSettingsRO settings, final DataTableSpec tableSpec)
            throws NotConfigurableException {
        final DataTableSpec[] tableSpecs = new DataTableSpec[]{tableSpec};
        // Use non-fresh branch if:
        //   (a) user has clicked OK this session (m_everSavedWithOk), OR
        //   (b) the settings have CFG_CONFIGURED persisted from a previous session.
        // Otherwise treat as a fresh node and put all columns in Available.
        if (m_everSavedWithOk || settings.containsKey(SparkUnpivotSettings.CFG_CONFIGURED)) {
            m_retainedColFilter.loadSettingsFrom(settings, tableSpecs);
            m_valueColFilter.loadSettingsFrom(settings, tableSpecs);
        } else {
            // Fresh/unconfigured node: put all columns in the Available (exclude) side
            if (tableSpec != null && tableSpec.getNumColumns() > 0) {
                final String[] allCols = tableSpec.getColumnNames();
                final NodeSettings freshSettings = new NodeSettings("defaults");
                new SettingsModelFilterString(SparkUnpivotSettings.CFG_RETAINED_COLUMNS,
                    new String[0], allCols, false).saveSettingsTo(freshSettings);
                new SettingsModelFilterString(SparkUnpivotSettings.CFG_VALUE_COLUMNS,
                    new String[0], allCols, false).saveSettingsTo(freshSettings);
                m_retainedColFilter.loadSettingsFrom(freshSettings, tableSpecs);
                m_valueColFilter.loadSettingsFrom(freshSettings, tableSpecs);
            } else {
                m_retainedColFilter.loadSettingsFrom(settings, tableSpecs);
                m_valueColFilter.loadSettingsFrom(settings, tableSpecs);
            }
        }

        try { m_variableColName.loadSettingsFrom(settings); }
        catch (final InvalidSettingsException e) { /* default */ }
        try { m_valueColName.loadSettingsFrom(settings); }
        catch (final InvalidSettingsException e) { /* default */ }
        try { m_skipMissingValues.loadSettingsFrom(settings); }
        catch (final InvalidSettingsException e) { /* default */ }
        try { m_castToString.loadSettingsFrom(settings); }
        catch (final InvalidSettingsException e) { /* default */ }

        // Load sort option
        final String sortOpt = settings.getString(SparkUnpivotSettings.CFG_SORT_OPTION,
            SparkUnpivotSettings.SORT_NONE);
        if (SparkUnpivotSettings.SORT_BY_RETAINED.equals(sortOpt)) {
            m_sortCombo.setSelectedIndex(1);
        } else if (SparkUnpivotSettings.SORT_BY_VARIABLE.equals(sortOpt)) {
            m_sortCombo.setSelectedIndex(2);
        } else {
            m_sortCombo.setSelectedIndex(0);
        }

        // Load variable value map into table
        m_varMapTableModel.setRowCount(0);
        final Map<String, String> savedMap = new HashMap<>();
        final String keysKey = SparkUnpivotSettings.CFG_VARIABLE_VALUE_MAP + "_keys";
        final String valsKey = SparkUnpivotSettings.CFG_VARIABLE_VALUE_MAP + "_values";
        try {
            if (settings.containsKey(keysKey) && settings.containsKey(valsKey)) {
                final String[] keys = settings.getStringArray(keysKey);
                final String[] vals = settings.getStringArray(valsKey);
                for (int i = 0; i < Math.min(keys.length, vals.length); i++) {
                    savedMap.put(keys[i], vals[i]);
                }
            }
        } catch (final InvalidSettingsException e) {
            // ignore
        }
        final List<String> valueCols = m_valueColumns.getIncludeList();
        if (valueCols != null) {
            for (String col : valueCols) {
                m_varMapTableModel.addRow(new Object[]{col, savedMap.getOrDefault(col, col)});
            }
        }

        m_inputRowCount = -1;
        m_validationArea.setText(" ");
        installTypeRenderers(m_retainedColFilter.getComponentPanel());
        installTypeRenderers(m_valueColFilter.getComponentPanel());
        updateRowEstimate();
        // Defer layout recalculation until after the dialog is fully shown.
        // Synchronous revalidate() fires before the dialog window is visible,
        // so the JScrollPanes inside ColumnFilterPanel still have zero size.
        // invokeLater ensures this runs after the dialog becomes visible.
        // ColumnFilterPanel.update() repopulates the table models but does NOT call
        // updateFilterView(), so the CardLayout can remain stuck on the PLACEHOLDER card
        // (shown when Available was empty after "Add All"). Fix it immediately after load,
        // and also defer once in case the dialog is not yet visible.
        fixCardLayouts(m_retainedColFilter.getComponentPanel());
        fixCardLayouts(m_valueColFilter.getComponentPanel());
        SwingUtilities.invokeLater(() -> {
            fixCardLayouts(m_retainedColFilter.getComponentPanel());
            fixCardLayouts(m_valueColFilter.getComponentPanel());
        });
    }

    /**
     * Fetches the input row count from Spark in the background.
     * Uses a validate-only job with minimal columns to get the count.
     */
    private void fetchInputRowCount() {
        if (m_contextID == null || m_dataFrameID == null) return;

        m_rowEstimateLabel.setText("Estimated output: counting input rows...");

        new SwingWorker<Long, Void>() {
            @Override
            protected Long doInBackground() throws Exception {
                // Run a minimal validate-only job just to get the count
                // Need at least 2 columns: one for retained, one for value
                if (m_tableSpec.getNumColumns() < 2) {
                    return -1L;
                }
                final String[] minRetained = new String[]{m_tableSpec.getColumnSpec(0).getName()};
                final String[] minValue = new String[]{m_tableSpec.getColumnSpec(1).getName()};
                final SparkUnpivotJobInput jobInput = new SparkUnpivotJobInput(
                    m_dataFrameID,
                    minRetained,
                    minValue,
                    "_var_tmp_", "_val_tmp_",
                    false, false,
                    SparkUnpivotSettings.SORT_NONE,
                    new String[0], new String[0]);
                final SparkUnpivotJobOutput output = SparkContextUtil
                    .<SparkUnpivotJobInput, SparkUnpivotJobOutput>getJobRunFactory(
                        m_contextID, SparkUnpivotNodeModel.JOB_ID)
                    .createRun(jobInput)
                    .run(m_contextID, new ExecutionMonitor());
                return output.getInputRowCount();
            }

            @Override
            protected void done() {
                try {
                    final long count = get();
                    if (count >= 0) {
                        m_inputRowCount = count;
                        updateRowEstimate();
                    }
                } catch (final Exception e) {
                    // Silently fail - row estimate just won't show actual count
                    updateRowEstimate();
                }
            }
        }.execute();
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        // Sync filter models from UI before validation
        syncFilterModels();

        validateSettings();

        m_retainedColFilter.saveSettingsTo(settings);
        m_valueColFilter.saveSettingsTo(settings);
        m_variableColName.saveSettingsTo(settings);
        m_valueColName.saveSettingsTo(settings);
        m_skipMissingValues.saveSettingsTo(settings);
        m_castToString.saveSettingsTo(settings);

        // Save sort option
        final int sortIdx = m_sortCombo.getSelectedIndex();
        final String sortOption;
        if (sortIdx == 1) sortOption = SparkUnpivotSettings.SORT_BY_RETAINED;
        else if (sortIdx == 2) sortOption = SparkUnpivotSettings.SORT_BY_VARIABLE;
        else sortOption = SparkUnpivotSettings.SORT_NONE;
        settings.addString(SparkUnpivotSettings.CFG_SORT_OPTION, sortOption);

        // Save variable value map
        final Map<String, String> varMap = getVarMapFromTable();
        settings.addStringArray(SparkUnpivotSettings.CFG_VARIABLE_VALUE_MAP + "_keys",
            varMap.keySet().toArray(new String[0]));
        settings.addStringArray(SparkUnpivotSettings.CFG_VARIABLE_VALUE_MAP + "_values",
            varMap.values().toArray(new String[0]));
        // Mark node as configured so NodeModel.saveSettingsTo() also persists this flag
        settings.addBoolean(SparkUnpivotSettings.CFG_CONFIGURED, true);
        // Track in-session: OK was clicked successfully this dialog lifecycle
        m_everSavedWithOk = true;
    }

    private void validateSettings() throws InvalidSettingsException {
        final List<String> retainedCols = m_retainedColumns.getIncludeList();
        final List<String> valueCols = m_valueColumns.getIncludeList();

        if (retainedCols == null || retainedCols.isEmpty()) {
            throw new InvalidSettingsException("No retained columns selected.");
        }
        if (valueCols == null || valueCols.isEmpty()) {
            throw new InvalidSettingsException("No value columns selected.");
        }

        final String varName = m_variableColName.getStringValue();
        if (varName == null || varName.trim().isEmpty()) {
            throw new InvalidSettingsException("Variable column name must not be empty.");
        }
        final String valName = m_valueColName.getStringValue();
        if (valName == null || valName.trim().isEmpty()) {
            throw new InvalidSettingsException("Value column name must not be empty.");
        }
        if (varName.trim().equals(valName.trim())) {
            throw new InvalidSettingsException("Variable and Value column names must be different.");
        }

        final Set<String> retainedSet = new HashSet<>(retainedCols);
        if (retainedSet.contains(varName.trim())) {
            throw new InvalidSettingsException("Variable column name conflicts with retained column.");
        }
        if (retainedSet.contains(valName.trim())) {
            throw new InvalidSettingsException("Value column name conflicts with retained column.");
        }

        if (!m_castToString.getBooleanValue() && m_tableSpec != null) {
            boolean hasNumeric = false;
            boolean hasString = false;
            final List<String> typeDetails = new ArrayList<>();
            for (String col : valueCols) {
                final DataColumnSpec colSpec = m_tableSpec.getColumnSpec(col);
                if (colSpec != null) {
                    final DataType type = colSpec.getType();
                    typeDetails.add(type.getName() + " (" + col + ")");
                    if (type.isCompatible(org.knime.core.data.DoubleValue.class)) {
                        hasNumeric = true;
                    } else {
                        hasString = true;
                    }
                }
            }
            if (hasNumeric && hasString) {
                throw new InvalidSettingsException(
                    "Incompatible value column types: " + String.join(", ", typeDetails)
                    + ". Enable 'Cast all value columns to String'.");
            }
        }
    }

    // =================================================================
    // Sync filter models from UI
    // =================================================================

    /**
     * Forces the SettingsModelFilterString to sync with the current UI state.
     * DialogComponentColumnFilter only syncs models on saveSettingsTo(), so
     * reading from the model before that may return stale/empty lists.
     */
    private void syncFilterModels() {
        try {
            final NodeSettings tmp = new NodeSettings("tmp");
            m_retainedColFilter.saveSettingsTo(tmp);
            m_valueColFilter.saveSettingsTo(tmp);
        } catch (final InvalidSettingsException e) {
            // ignore - best effort sync
        }
    }

    // =================================================================
    // Validation Check (#3)
    // =================================================================

    private void runValidation() {
        // Sync filter models from UI (models may not reflect current panel state)
        syncFilterModels();

        final List<String> retainedCols = m_retainedColumns.getIncludeList();
        final List<String> valueCols = m_valueColumns.getIncludeList();

        if (retainedCols == null || retainedCols.isEmpty()) {
            showError("No retained columns selected."); return;
        }
        if (valueCols == null || valueCols.isEmpty()) {
            showError("No value columns selected."); return;
        }
        if (m_contextID == null) {
            showError("No Spark context available."); return;
        }
        if (m_dataFrameID == null) {
            showError("Execute the upstream node first."); return;
        }

        m_checkButton.setEnabled(false);
        m_validationArea.setText("Validating...");
        m_validationArea.setForeground(Color.GRAY);

        final int sortIdx = m_sortCombo.getSelectedIndex();
        final String sortOption;
        if (sortIdx == 1) sortOption = SparkUnpivotSettings.SORT_BY_RETAINED;
        else if (sortIdx == 2) sortOption = SparkUnpivotSettings.SORT_BY_VARIABLE;
        else sortOption = SparkUnpivotSettings.SORT_NONE;

        final Map<String, String> varMap = getVarMapFromTable();
        final String[] mapKeys = varMap.keySet().toArray(new String[0]);
        final String[] mapValues = varMap.values().toArray(new String[0]);

        final SparkUnpivotJobInput jobInput = new SparkUnpivotJobInput(
            m_dataFrameID,
            retainedCols.toArray(new String[0]),
            valueCols.toArray(new String[0]),
            m_variableColName.getStringValue(),
            m_valueColName.getStringValue(),
            m_skipMissingValues.getBooleanValue(),
            m_castToString.getBooleanValue(),
            sortOption,
            mapKeys,
            mapValues);

        new SwingWorker<SparkUnpivotJobOutput, Void>() {
            @Override
            protected SparkUnpivotJobOutput doInBackground() throws Exception {
                return SparkContextUtil
                    .<SparkUnpivotJobInput, SparkUnpivotJobOutput>getJobRunFactory(
                        m_contextID, SparkUnpivotNodeModel.JOB_ID)
                    .createRun(jobInput)
                    .run(m_contextID, new ExecutionMonitor());
            }

            @Override
            protected void done() {
                m_checkButton.setEnabled(true);
                try {
                    final SparkUnpivotJobOutput output = get();
                    // Update input row count for estimation
                    final long count = output.getInputRowCount();
                    if (count >= 0) {
                        m_inputRowCount = count;
                        updateRowEstimate();
                    }
                    final StringBuilder msg = new StringBuilder("OK - Unpivot validation passed.\n");
                    final String preview = output.getPreviewData();
                    if (preview != null && !preview.isEmpty()) {
                        msg.append("\nSample data (up to 5 rows):\n").append(preview);
                    }
                    showSuccess(msg.toString());
                } catch (final Exception ex) {
                    String msg = ex.getMessage();
                    if (msg == null && ex.getCause() != null) {
                        msg = ex.getCause().getMessage();
                    }
                    showError(msg != null ? msg : "Unknown error");
                }
            }
        }.execute();
    }

    private void showSuccess(final String message) {
        m_validationArea.setForeground(COLOR_SUCCESS);
        m_validationArea.setText(message);
        m_validationArea.setCaretPosition(0);
    }

    private void showError(final String message) {
        m_validationArea.setForeground(COLOR_ERROR);
        m_validationArea.setText(message);
        m_validationArea.setCaretPosition(0);
    }

    // =================================================================
    // Layout helpers
    // =================================================================

    /**
     * Fixes the CardLayout panels inside DialogComponentColumnFilter.
     * Root cause: ColumnFilterPanel.update() (called from loadSettingsFrom) repopulates
     * the exclude/include table models but does NOT call updateFilterView(). When the user
     * moves all columns to Include ("Add All"), updateFilterView() switches the Available
     * (exclude) CardLayout to PLACEHOLDER. After cancel + reopen, update() restores the
     * columns to the exclude model but the CardLayout stays on PLACEHOLDER → columns exist
     * in the model but are invisible.
     * Fix: switch any CardLayout panel to "LIST" when its JScrollPane child's JTable
     * has rows, without touching panels whose tables are empty (PLACEHOLDER is correct there).
     */
    private void fixCardLayouts(final Container container) {
        for (final Component comp : container.getComponents()) {
            if (comp instanceof JPanel) {
                final JPanel panel = (JPanel) comp;
                if (panel.getLayout() instanceof CardLayout) {
                    for (final Component child : panel.getComponents()) {
                        if (child instanceof JScrollPane) {
                            final Component view = ((JScrollPane) child).getViewport().getView();
                            if (view instanceof JTable && ((JTable) view).getModel().getRowCount() > 0) {
                                ((CardLayout) panel.getLayout()).show(panel, "LIST");
                                break;
                            }
                        }
                    }
                }
            }
            if (comp instanceof Container) {
                fixCardLayouts((Container) comp);
            }
        }
    }

    // =================================================================
    // Column type display (#1)
    // =================================================================

    @SuppressWarnings("unchecked")
    private void installTypeRenderers(final Container container) {
        for (final Component comp : container.getComponents()) {
            if (comp instanceof JList) {
                ((JList<Object>) comp).setCellRenderer(createTypedRenderer());
            } else if (comp instanceof JScrollPane) {
                final Component view = ((JScrollPane) comp).getViewport().getView();
                if (view instanceof JList) {
                    ((JList<Object>) view).setCellRenderer(createTypedRenderer());
                }
            }
            if (comp instanceof Container) {
                installTypeRenderers((Container) comp);
            }
        }
    }

    private DefaultListCellRenderer createTypedRenderer() {
        return new DefaultListCellRenderer() {
            private static final long serialVersionUID = 1L;

            @Override
            public Component getListCellRendererComponent(final JList<?> list, final Object value,
                    final int index, final boolean isSelected, final boolean cellHasFocus) {
                Object display = value;
                if (m_tableSpec != null && value != null) {
                    final String colName = value.toString();
                    final DataColumnSpec colSpec = m_tableSpec.getColumnSpec(colName);
                    if (colSpec != null) {
                        display = colName + "  (" + colSpec.getType().getName() + ")";
                    }
                }
                return super.getListCellRendererComponent(list, display, index, isSelected, cellHasFocus);
            }
        };
    }
}

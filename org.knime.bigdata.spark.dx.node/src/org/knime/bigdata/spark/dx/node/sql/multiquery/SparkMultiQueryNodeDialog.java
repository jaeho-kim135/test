package org.knime.bigdata.spark.dx.node.sql.multiquery;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Container;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.ArrayList;
import java.util.List;

import javax.swing.BorderFactory;
import javax.swing.DefaultListCellRenderer;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.SwingWorker;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.DataAwareNodeDialogPane;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnFilter;
import org.knime.core.node.defaultnodesettings.SettingsModelFilterString;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;

/**
 * Dialog for the Spark Multi Query node.
 * Features: column selection, expression templates, SQL preview, options, and Spark validation.
 */
public final class SparkMultiQueryNodeDialog extends DataAwareNodeDialogPane {

    private static final Color COLOR_SUCCESS = new Color(0, 128, 0);
    private static final Color COLOR_ERROR = new Color(200, 0, 0);
    private static final Color COLOR_CHECK_BG = new Color(255, 204, 0);
    private static final String PH = SparkMultiQuerySettings.COLUMN_PLACEHOLDER;

    // --- Expression templates ---
    private static final String[][] TEMPLATES = {
        {"(Custom)", ""},
        {"Cast to String", "string(" + PH + ")"},
        {"Cast to Integer", "CAST(" + PH + " AS INT)"},
        {"Cast to Double", "CAST(" + PH + " AS DOUBLE)"},
        {"Uppercase", "UPPER(" + PH + ")"},
        {"Lowercase", "LOWER(" + PH + ")"},
        {"Trim", "TRIM(" + PH + ")"},
        {"Replace NULL (empty string)", "COALESCE(" + PH + ", '')"},
        {"Replace NULL (zero)", "COALESCE(" + PH + ", 0)"},
        {"Parse Date (yyyyMMdd)", "TO_DATE(" + PH + ", 'yyyyMMdd')"},
        {"Regex Replace", "REGEXP_REPLACE(" + PH + ", 'pattern', 'replacement')"},
    };

    // --- Settings models ---
    private final SettingsModelFilterString m_targetColumns =
        new SettingsModelFilterString(SparkMultiQuerySettings.CFG_TARGET_COLUMNS);
    private final SettingsModelString m_sqlExpression =
        new SettingsModelString(SparkMultiQuerySettings.CFG_SQL_EXPRESSION, "string(" + PH + ")");

    // --- UI components ---
    private final DialogComponentColumnFilter m_targetColFilter =
        new DialogComponentColumnFilter(m_targetColumns, 0, false);
    private final JComboBox<String> m_templateCombo = new JComboBox<>();
    private final JTextArea m_sqlTextArea = new JTextArea(3, 40);
    private final JCheckBox m_keepOriginalCheck = new JCheckBox("Keep original columns (add transformed as new columns)");
    private final JTextField m_outputPatternField = new JTextField(PH, 20);
    private final JTextArea m_previewArea = new JTextArea(3, 40);
    private final JTextArea m_selectedColsArea = new JTextArea(2, 40);
    private final JButton m_checkButton = new JButton("Check");
    private final JTextArea m_validationArea = new JTextArea(6, 40);

    // --- State ---
    private SparkContextID m_contextID;
    private String m_dataFrameID;
    private DataTableSpec m_tableSpec;
    private boolean m_suppressTemplateUpdate = false;

    /** Constructor. */
    public SparkMultiQueryNodeDialog() {
        m_targetColFilter.setIncludeTitle(" Target column(s) ");
        m_targetColFilter.setExcludeTitle(" Available column(s) ");
        m_targetColFilter.setShowInvalidIncludeColumns(true);

        // Listen for column selection changes → update preview + column summary
        m_targetColumns.addChangeListener(e -> {
            updateSelectedColumnsInfo();
            updatePreview();
        });

        // Selected columns info (read-only)
        m_selectedColsArea.setEditable(false);
        m_selectedColsArea.setLineWrap(true);
        m_selectedColsArea.setWrapStyleWord(true);
        m_selectedColsArea.setFont(new Font("SansSerif", Font.PLAIN, 11));
        m_selectedColsArea.setBackground(new Color(240, 245, 255));
        m_selectedColsArea.setBorder(BorderFactory.createEmptyBorder(4, 6, 4, 6));

        // Template combo setup
        for (String[] t : TEMPLATES) {
            m_templateCombo.addItem(t[0]);
        }
        m_templateCombo.addActionListener(e -> {
            if (!m_suppressTemplateUpdate) {
                final int idx = m_templateCombo.getSelectedIndex();
                if (idx > 0 && idx < TEMPLATES.length) {
                    m_sqlTextArea.setText(TEMPLATES[idx][1]);
                }
            }
        });

        // SQL text area
        m_sqlTextArea.setFont(new Font("Monospaced", Font.PLAIN, 13));
        m_sqlTextArea.setLineWrap(true);
        m_sqlTextArea.setWrapStyleWord(true);
        m_sqlTextArea.getDocument().addDocumentListener(new DocumentListener() {
            @Override public void insertUpdate(DocumentEvent e) { onExpressionChanged(); }
            @Override public void removeUpdate(DocumentEvent e) { onExpressionChanged(); }
            @Override public void changedUpdate(DocumentEvent e) { onExpressionChanged(); }
        });

        // Output pattern field listener
        m_outputPatternField.getDocument().addDocumentListener(new DocumentListener() {
            @Override public void insertUpdate(DocumentEvent e) { updatePreview(); }
            @Override public void removeUpdate(DocumentEvent e) { updatePreview(); }
            @Override public void changedUpdate(DocumentEvent e) { updatePreview(); }
        });

        // Keep original checkbox listener
        m_keepOriginalCheck.addActionListener(e -> updatePreview());

        // Preview area (read-only)
        m_previewArea.setEditable(false);
        m_previewArea.setFont(new Font("Monospaced", Font.PLAIN, 11));
        m_previewArea.setLineWrap(true);
        m_previewArea.setWrapStyleWord(true);
        m_previewArea.setBackground(new Color(245, 245, 245));

        // Check button
        m_checkButton.setBackground(COLOR_CHECK_BG);
        m_checkButton.setOpaque(true);
        m_checkButton.setFocusPainted(false);
        m_checkButton.addActionListener(e -> runValidation());

        // Validation area
        m_validationArea.setEditable(false);
        m_validationArea.setLineWrap(true);
        m_validationArea.setWrapStyleWord(true);
        m_validationArea.setFont(new Font("Monospaced", Font.PLAIN, 11));
        m_validationArea.setOpaque(false);
        m_validationArea.setBorder(null);

        // --- Build column selection tab ---
        addTab("Column Selection", m_targetColFilter.getComponentPanel());

        // --- Build expression & options tab ---
        addTab("Expression & Options", buildExpressionPanel());
    }

    private JPanel buildExpressionPanel() {
        final JPanel panel = new JPanel(new GridBagLayout());
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(4, 6, 4, 6);
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.weightx = 1.0;

        // --- Selected columns info ---
        final JPanel colInfoPanel = new JPanel(new BorderLayout());
        colInfoPanel.setBorder(BorderFactory.createTitledBorder("Target Columns"));
        final JScrollPane colInfoScroll = new JScrollPane(m_selectedColsArea);
        colInfoScroll.setPreferredSize(new Dimension(400, 50));
        colInfoPanel.add(colInfoScroll, BorderLayout.CENTER);
        panel.add(colInfoPanel, gbc);

        // --- SQL Expression section ---
        gbc.gridy++;
        final JPanel exprPanel = new JPanel(new GridBagLayout());
        exprPanel.setBorder(BorderFactory.createTitledBorder("SQL Expression"));
        final GridBagConstraints egbc = new GridBagConstraints();
        egbc.insets = new Insets(3, 5, 3, 5);
        egbc.fill = GridBagConstraints.HORIZONTAL;
        egbc.gridx = 0;
        egbc.gridy = 0;
        egbc.weightx = 0.0;
        exprPanel.add(new JLabel("Template:"), egbc);
        egbc.gridx = 1;
        egbc.weightx = 1.0;
        exprPanel.add(m_templateCombo, egbc);

        egbc.gridx = 0;
        egbc.gridy++;
        egbc.gridwidth = 2;
        egbc.fill = GridBagConstraints.BOTH;
        egbc.weighty = 1.0;
        final JScrollPane sqlScroll = new JScrollPane(m_sqlTextArea);
        sqlScroll.setPreferredSize(new Dimension(400, 70));
        exprPanel.add(sqlScroll, egbc);

        egbc.gridy++;
        egbc.weighty = 0.0;
        egbc.fill = GridBagConstraints.HORIZONTAL;
        final JLabel hintLabel = new JLabel("Use " + PH + " as placeholder for each target column name");
        hintLabel.setFont(hintLabel.getFont().deriveFont(Font.ITALIC, 11f));
        exprPanel.add(hintLabel, egbc);

        panel.add(exprPanel, gbc);

        // --- Options section ---
        gbc.gridy++;
        final JPanel optPanel = new JPanel(new GridBagLayout());
        optPanel.setBorder(BorderFactory.createTitledBorder("Options"));
        final GridBagConstraints ogbc = new GridBagConstraints();
        ogbc.insets = new Insets(3, 5, 3, 5);
        ogbc.anchor = GridBagConstraints.WEST;
        ogbc.gridx = 0;
        ogbc.gridy = 0;
        ogbc.gridwidth = 2;
        optPanel.add(m_keepOriginalCheck, ogbc);

        ogbc.gridy++;
        ogbc.gridwidth = 1;
        ogbc.weightx = 0.0;
        optPanel.add(new JLabel("Output column pattern:"), ogbc);
        ogbc.gridx = 1;
        ogbc.weightx = 1.0;
        ogbc.fill = GridBagConstraints.HORIZONTAL;
        optPanel.add(m_outputPatternField, ogbc);

        ogbc.gridx = 0;
        ogbc.gridy++;
        ogbc.gridwidth = 2;
        ogbc.fill = GridBagConstraints.NONE;
        final JLabel patternHint = new JLabel(
            "e.g. " + PH + " (replace), " + PH + "_str (new column)");
        patternHint.setFont(patternHint.getFont().deriveFont(Font.ITALIC, 11f));
        optPanel.add(patternHint, ogbc);

        panel.add(optPanel, gbc);

        // --- SQL Preview section ---
        gbc.gridy++;
        final JPanel previewPanel = new JPanel(new BorderLayout());
        previewPanel.setBorder(BorderFactory.createTitledBorder("SQL Preview"));
        final JScrollPane previewScroll = new JScrollPane(m_previewArea);
        previewScroll.setPreferredSize(new Dimension(400, 60));
        previewPanel.add(previewScroll, BorderLayout.CENTER);
        panel.add(previewPanel, gbc);

        // --- Validation section ---
        gbc.gridy++;
        final JPanel validPanel = new JPanel(new BorderLayout(5, 5));
        validPanel.setBorder(BorderFactory.createTitledBorder("Validation"));
        validPanel.add(m_checkButton, BorderLayout.WEST);
        final JScrollPane validScroll = new JScrollPane(m_validationArea);
        validScroll.setPreferredSize(new Dimension(400, 120));
        validScroll.setBorder(null);
        validPanel.add(validScroll, BorderLayout.CENTER);
        panel.add(validPanel, gbc);

        // Push content to top
        gbc.gridy++;
        gbc.weighty = 1.0;
        panel.add(new JPanel(), gbc);

        return panel;
    }

    private void updateSelectedColumnsInfo() {
        syncFilterModel();
        final List<String> targetCols = m_targetColumns.getIncludeList();
        if (targetCols == null || targetCols.isEmpty()) {
            m_selectedColsArea.setText("(no columns selected)");
            return;
        }
        final StringBuilder sb = new StringBuilder();
        sb.append(targetCols.size()).append(" column(s): ");
        for (int i = 0; i < targetCols.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            final String col = targetCols.get(i);
            sb.append(col);
            if (m_tableSpec != null) {
                final DataColumnSpec colSpec = m_tableSpec.getColumnSpec(col);
                if (colSpec != null) {
                    sb.append(" (").append(colSpec.getType().getName()).append(")");
                }
            }
        }
        m_selectedColsArea.setText(sb.toString());
        m_selectedColsArea.setCaretPosition(0);
    }

    private void onExpressionChanged() {
        // Reset template combo to (Custom) when user edits manually
        final String text = m_sqlTextArea.getText();
        m_suppressTemplateUpdate = true;
        boolean matched = false;
        for (int i = 1; i < TEMPLATES.length; i++) {
            if (TEMPLATES[i][1].equals(text)) {
                m_templateCombo.setSelectedIndex(i);
                matched = true;
                break;
            }
        }
        if (!matched) {
            m_templateCombo.setSelectedIndex(0);
        }
        m_suppressTemplateUpdate = false;
        updatePreview();
    }

    private void updatePreview() {
        syncFilterModel();
        final List<String> targetCols = m_targetColumns.getIncludeList();
        final String expr = m_sqlTextArea.getText();
        final boolean keepOrig = m_keepOriginalCheck.isSelected();
        final String pattern = m_outputPatternField.getText();

        if (targetCols == null || targetCols.isEmpty() || expr == null || expr.trim().isEmpty()) {
            m_previewArea.setText("(select target columns and enter expression)");
            return;
        }

        final StringBuilder sb = new StringBuilder("SELECT ");
        boolean first = true;

        // Show a sample with target columns + "..." for others
        for (String col : targetCols) {
            if (!first) {
                sb.append(", ");
            }
            first = false;
            if (keepOrig) {
                sb.append("`").append(col).append("`, ");
            }
            final String resolved = expr.replace(PH, "`" + col + "`");
            final String alias = (pattern != null && !pattern.isEmpty())
                ? pattern.replace(PH, col) : col;
            sb.append(resolved).append(" AS `").append(alias).append("`");
        }
        sb.append(", ... FROM input");
        m_previewArea.setText(sb.toString());
        m_previewArea.setCaretPosition(0);
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

        m_targetColFilter.loadSettingsFrom(settings, new DataTableSpec[]{tableSpec});

        try {
            m_sqlExpression.loadSettingsFrom(settings);
            m_sqlTextArea.setText(m_sqlExpression.getStringValue());
        } catch (final InvalidSettingsException e) {
            m_sqlTextArea.setText("string(" + PH + ")");
        }

        // Load keep original
        try {
            final boolean keepOrig = settings.getBoolean(SparkMultiQuerySettings.CFG_KEEP_ORIGINAL, false);
            m_keepOriginalCheck.setSelected(keepOrig);
        } catch (final Exception e) {
            m_keepOriginalCheck.setSelected(false);
        }

        // Load output pattern
        try {
            final String pattern = settings.getString(SparkMultiQuerySettings.CFG_OUTPUT_PATTERN, PH);
            m_outputPatternField.setText(pattern);
        } catch (final Exception e) {
            m_outputPatternField.setText(PH);
        }

        m_validationArea.setText(" ");
        installTypeRenderers(m_targetColFilter.getComponentPanel());
        updateSelectedColumnsInfo();
        updatePreview();
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        syncFilterModel();
        validateSettings();

        m_targetColFilter.saveSettingsTo(settings);
        m_sqlExpression.setStringValue(m_sqlTextArea.getText().trim());
        m_sqlExpression.saveSettingsTo(settings);

        settings.addBoolean(SparkMultiQuerySettings.CFG_KEEP_ORIGINAL, m_keepOriginalCheck.isSelected());
        settings.addString(SparkMultiQuerySettings.CFG_OUTPUT_PATTERN, m_outputPatternField.getText().trim());
    }

    private void validateSettings() throws InvalidSettingsException {
        final List<String> targetCols = m_targetColumns.getIncludeList();
        if (targetCols == null || targetCols.isEmpty()) {
            throw new InvalidSettingsException("No target columns selected.");
        }

        final String expr = m_sqlTextArea.getText();
        if (expr == null || expr.trim().isEmpty()) {
            throw new InvalidSettingsException("SQL expression must not be empty.");
        }
        if (!expr.contains(PH)) {
            throw new InvalidSettingsException("SQL expression must contain '" + PH + "'.");
        }

        final String pattern = m_outputPatternField.getText();
        if (pattern == null || pattern.trim().isEmpty()) {
            throw new InvalidSettingsException("Output column pattern must not be empty.");
        }
        if (!pattern.contains(PH)) {
            throw new InvalidSettingsException("Output column pattern must contain '" + PH + "'.");
        }
        if (m_keepOriginalCheck.isSelected() && PH.equals(pattern.trim())) {
            throw new InvalidSettingsException(
                "When 'Keep original columns' is enabled, output pattern must differ from '"
                + PH + "'. Example: " + PH + "_new");
        }
    }

    // =================================================================
    // Sync filter model from UI
    // =================================================================

    /**
     * Forces the SettingsModelFilterString to sync with the current UI state.
     * DialogComponentColumnFilter only syncs models on saveSettingsTo(), so
     * reading from the model before that may return stale/empty lists.
     */
    private void syncFilterModel() {
        try {
            final NodeSettings tmp = new NodeSettings("tmp");
            m_targetColFilter.saveSettingsTo(tmp);
        } catch (final InvalidSettingsException e) {
            // ignore
        }
    }

    // =================================================================
    // Validation (Spark Check)
    // =================================================================

    private void runValidation() {
        syncFilterModel();
        // Local validation first
        final List<String> targetCols = m_targetColumns.getIncludeList();
        if (targetCols == null || targetCols.isEmpty()) {
            showError("No target columns selected."); return;
        }
        final String expr = m_sqlTextArea.getText();
        if (expr == null || expr.trim().isEmpty()) {
            showError("SQL expression is empty."); return;
        }
        if (!expr.contains(PH)) {
            showError("Expression must contain '" + PH + "'."); return;
        }
        if (m_contextID == null) {
            showError("No Spark context available."); return;
        }
        if (m_dataFrameID == null) {
            showError("Execute the upstream node first."); return;
        }

        // Run Spark validation (all columns, then per-column on failure)
        m_checkButton.setEnabled(false);
        final String[] allTargetCols = targetCols.toArray(new String[0]);
        m_validationArea.setText("Validating " + allTargetCols.length + " column(s)...");
        m_validationArea.setForeground(Color.GRAY);
        updatePreview();

        new SwingWorker<SparkMultiQueryJobOutput, Void>() {
            @Override
            protected SparkMultiQueryJobOutput doInBackground() throws Exception {
                try {
                    // Test all columns at once
                    return runSparkValidation(allTargetCols, expr.trim());
                } catch (final Exception allEx) {
                    // On failure: test each column individually to identify which failed
                    final List<String> failedCols = new ArrayList<>();
                    final List<String> passedCols = new ArrayList<>();
                    String lastError = allEx.getMessage();
                    if (allEx.getCause() != null) {
                        lastError = allEx.getCause().getMessage();
                    }

                    for (String col : allTargetCols) {
                        try {
                            runSparkValidation(new String[]{col}, expr.trim());
                            passedCols.add(col);
                        } catch (final Exception colEx) {
                            failedCols.add(col);
                        }
                    }

                    final StringBuilder msg = new StringBuilder();
                    if (!failedCols.isEmpty()) {
                        msg.append("FAILED column(s): ").append(String.join(", ", failedCols));
                    }
                    if (!passedCols.isEmpty()) {
                        msg.append("\nPassed column(s): ").append(String.join(", ", passedCols));
                    }
                    msg.append("\n\nError: ").append(lastError != null ? lastError : "Unknown error");
                    throw new Exception(msg.toString());
                }
            }

            @Override
            protected void done() {
                m_checkButton.setEnabled(true);
                try {
                    final SparkMultiQueryJobOutput output = get();
                    final StringBuilder msg = new StringBuilder();
                    msg.append("OK - ").append(allTargetCols.length).append(" column(s) validated.\n");
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

    private SparkMultiQueryJobOutput runSparkValidation(final String[] cols, final String expr) throws Exception {
        final SparkMultiQueryJobInput jobInput = new SparkMultiQueryJobInput(
            m_dataFrameID, cols, expr);

        return SparkContextUtil
            .<SparkMultiQueryJobInput, SparkMultiQueryJobOutput>getJobRunFactory(
                m_contextID, SparkMultiQueryNodeModel.JOB_ID)
            .createRun(jobInput)
            .run(m_contextID, new ExecutionMonitor());
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
    // Column type display in filter lists
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

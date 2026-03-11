package org.knime.bigdata.spark.dx.node.sql.expression;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Cursor;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.swing.BorderFactory;
import javax.swing.DefaultListCellRenderer;
import javax.swing.DefaultListModel;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTabbedPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.ListSelectionModel;
import javax.swing.SwingWorker;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.dx.node.sql.expression.SparkExpressionSettings.OutputMode;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.DataAwareNodeDialogPane;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;

/**
 * Dialog for the Spark Expression node.
 * Layout mirrors KNIME's Expression node:
 * <pre>
 * ┌────────────┬──────────────────────────────────────────┐
 * │            │  [Expr 1] [Expr 2] [+] [-]   (tabs)     │
 * │  Input     │  ┌──────────────────────────────────────┐ │
 * │  Columns   │  │                                      │ │
 * │            │  │   Expression Editor (large area)      │ │
 * │  ────────  │  │   (monospace font)                    │ │
 * │            │  │                                      │ │
 * │  Function  │  ├──────────────────────────────────────┤ │
 * │  Catalog   │  │ Output: [APPEND ▼]  Column: [____]  │ │
 * │            │  └──────────────────────────────────────┘ │
 * │            │                                           │
 * │            │  ─── Output Preview ─────────────────── │
 * │            │  [Evaluate] rows                          │
 * │            │  ┌──────────────────────────────────────┐ │
 * │            │  │ preview output                        │ │
 * │            │  └──────────────────────────────────────┘ │
 * └────────────┴──────────────────────────────────────────┘
 * </pre>
 */
public final class SparkExpressionNodeDialog extends DataAwareNodeDialogPane {

    private static final Color COLOR_SUCCESS = new Color(0, 128, 0);
    private static final Color COLOR_ERROR = new Color(200, 0, 0);
    private static final Color BG_EDITOR = new Color(253, 253, 253);
    private static final Font MONO = new Font("Monospaced", Font.PLAIN, 13);
    private static final Font MONO_SMALL = new Font("Monospaced", Font.PLAIN, 11);

    // ── Spark SQL function catalog ───────────────────────────────────────────

    private static final String[][] FUNCTION_CATALOG = {
        {"── String ──", ""},
        {"UPPER(col)", "UPPER()"},
        {"LOWER(col)", "LOWER()"},
        {"TRIM(col)", "TRIM()"},
        {"LTRIM(col)", "LTRIM()"},
        {"RTRIM(col)", "RTRIM()"},
        {"LENGTH(col)", "LENGTH()"},
        {"CONCAT(a, b)", "CONCAT(, )"},
        {"SUBSTRING(col, pos, len)", "SUBSTRING(, 1, )"},
        {"REPLACE(col, old, new)", "REPLACE(, '', '')"},
        {"REGEXP_REPLACE(col, pat, rep)", "REGEXP_REPLACE(, '', '')"},
        {"REGEXP_EXTRACT(col, pat, grp)", "REGEXP_EXTRACT(, '', 0)"},
        {"SPLIT(col, delim)", "SPLIT(, ',')"},
        {"LPAD(col, len, pad)", "LPAD(, 10, '0')"},
        {"INITCAP(col)", "INITCAP()"},
        {"REVERSE(col)", "REVERSE()"},
        {"── Math ──", ""},
        {"ABS(col)", "ABS()"},
        {"ROUND(col, scale)", "ROUND(, 2)"},
        {"CEIL(col)", "CEIL()"},
        {"FLOOR(col)", "FLOOR()"},
        {"SQRT(col)", "SQRT()"},
        {"POW(base, exp)", "POW(, )"},
        {"MOD(a, b)", "MOD(, )"},
        {"GREATEST(a, b, ...)", "GREATEST(, )"},
        {"LEAST(a, b, ...)", "LEAST(, )"},
        {"── Date/Time ──", ""},
        {"CURRENT_DATE()", "CURRENT_DATE()"},
        {"CURRENT_TIMESTAMP()", "CURRENT_TIMESTAMP()"},
        {"TO_DATE(col, fmt)", "TO_DATE(, 'yyyy-MM-dd')"},
        {"TO_TIMESTAMP(col, fmt)", "TO_TIMESTAMP(, 'yyyy-MM-dd HH:mm:ss')"},
        {"DATE_FORMAT(col, fmt)", "DATE_FORMAT(, 'yyyy-MM-dd')"},
        {"DATEDIFF(end, start)", "DATEDIFF(, )"},
        {"DATE_ADD(col, days)", "DATE_ADD(, 1)"},
        {"DATE_SUB(col, days)", "DATE_SUB(, 1)"},
        {"YEAR(col)", "YEAR()"},
        {"MONTH(col)", "MONTH()"},
        {"DAY(col)", "DAY()"},
        {"── Null Handling ──", ""},
        {"COALESCE(a, b, ...)", "COALESCE(, )"},
        {"IFNULL(col, default)", "IFNULL(, '')"},
        {"NVL(col, default)", "NVL(, '')"},
        {"NVL2(col, notNull, isNull)", "NVL2(, , )"},
        {"ISNULL(col)", "ISNULL()"},
        {"ISNOTNULL(col)", "ISNOTNULL()"},
        {"── Type Cast ──", ""},
        {"CAST(col AS type)", "CAST( AS STRING)"},
        {"STRING(col)", "STRING()"},
        {"INT(col)", "INT()"},
        {"DOUBLE(col)", "DOUBLE()"},
        {"BOOLEAN(col)", "BOOLEAN()"},
        {"── Conditional ──", ""},
        {"CASE WHEN ... THEN ... END", "CASE WHEN  THEN  ELSE  END"},
        {"IF(cond, true, false)", "IF(, , )"},
        {"WHEN(cond, val)", "WHEN(, )"},
    };

    // ── Per-expression data ──────────────────────────────────────────────────

    /** Holds data for one expression tab. */
    private static final class ExpressionEntry {
        final JTextArea editor = new JTextArea();
        final JComboBox<OutputMode> modeCombo = new JComboBox<>(OutputMode.values());
        final JTextField columnNameField = new JTextField("new_column", 15);

        ExpressionEntry(final String expr, final OutputMode mode, final String colName) {
            editor.setText(expr);
            editor.setFont(MONO);
            editor.setBackground(BG_EDITOR);
            editor.setTabSize(4);
            modeCombo.setSelectedItem(mode);
            columnNameField.setText(colName);
            columnNameField.setFont(MONO);
        }
    }

    private final List<ExpressionEntry> m_entries = new ArrayList<>();

    // ── UI components ────────────────────────────────────────────────────────

    private final JTabbedPane m_exprTabs = new JTabbedPane(JTabbedPane.TOP, JTabbedPane.SCROLL_TAB_LAYOUT);
    private final JButton m_addTabButton = new JButton("+");
    private final JButton m_removeTabButton = new JButton("-");
    private final DefaultListModel<String> m_columnListModel = new DefaultListModel<>();
    private final JList<String> m_columnList = new JList<>(m_columnListModel);
    private final DefaultListModel<String> m_functionListModel = new DefaultListModel<>();
    private final JList<String> m_functionList = new JList<>(m_functionListModel);
    private final JButton m_evaluateButton = new JButton("Evaluate");
    private final JTextArea m_previewArea = new JTextArea(8, 40);

    // ── State ────────────────────────────────────────────────────────────────

    private SparkContextID m_contextID;
    private String m_dataFrameID;
    private DataTableSpec m_tableSpec;
    private boolean m_everSavedWithOk = false;

    /** Constructor. */
    public SparkExpressionNodeDialog() {
        // Tab add/remove buttons
        m_addTabButton.setToolTipText("Add new expression");
        m_addTabButton.setMargin(new Insets(2, 6, 2, 6));
        m_addTabButton.addActionListener(e -> addExpressionTab("", OutputMode.APPEND, "new_column"));

        m_removeTabButton.setToolTipText("Remove current expression");
        m_removeTabButton.setMargin(new Insets(2, 6, 2, 6));
        m_removeTabButton.addActionListener(e -> removeCurrentTab());

        // Column list (left panel, top)
        m_columnList.setFont(MONO_SMALL);
        m_columnList.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        m_columnList.setCellRenderer(new ColumnCellRenderer());
        m_columnList.setToolTipText("Double-click to insert column name into expression");
        m_columnList.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(final MouseEvent e) {
                if (e.getClickCount() == 2) {
                    insertSelectedColumn();
                }
            }
        });

        // Function list (left panel, bottom)
        for (final String[] fn : FUNCTION_CATALOG) {
            m_functionListModel.addElement(fn[0]);
        }
        m_functionList.setFont(MONO_SMALL);
        m_functionList.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        m_functionList.setCellRenderer(new FunctionCellRenderer());
        m_functionList.setToolTipText("Double-click to insert function into expression");
        m_functionList.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(final MouseEvent e) {
                if (e.getClickCount() == 2) {
                    insertSelectedFunction();
                }
            }
        });

        // Evaluate button
        m_evaluateButton.setFont(m_evaluateButton.getFont().deriveFont(Font.BOLD));
        m_evaluateButton.addActionListener(e -> runValidation());

        // Preview area
        m_previewArea.setEditable(false);
        m_previewArea.setFont(MONO_SMALL);
        m_previewArea.setBackground(new Color(245, 245, 245));

        // Build the single-tab layout
        addTab("Expression", buildMainPanel());
    }

    // ── Main panel layout ────────────────────────────────────────────────────

    private JPanel buildMainPanel() {
        final JPanel mainPanel = new JPanel(new BorderLayout());

        // Left: Column inspector + Function catalog
        final JPanel leftPanel = buildLeftPanel();

        // Right: Expression tabs + Preview
        final JPanel rightPanel = buildRightPanel();

        // Horizontal split: left (columns/functions) | right (editors/preview)
        final JSplitPane hSplit = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, leftPanel, rightPanel);
        hSplit.setDividerLocation(220);
        hSplit.setResizeWeight(0.0);
        hSplit.setContinuousLayout(true);

        mainPanel.add(hSplit, BorderLayout.CENTER);
        return mainPanel;
    }

    private JPanel buildLeftPanel() {
        final JPanel panel = new JPanel(new BorderLayout());
        panel.setMinimumSize(new Dimension(180, 200));
        panel.setPreferredSize(new Dimension(220, 500));

        // Column list at top
        final JPanel colPanel = new JPanel(new BorderLayout());
        final JLabel colLabel = new JLabel(" Input Columns");
        colLabel.setFont(colLabel.getFont().deriveFont(Font.BOLD, 12f));
        colLabel.setBorder(BorderFactory.createEmptyBorder(4, 2, 4, 2));
        colPanel.add(colLabel, BorderLayout.NORTH);
        colPanel.add(new JScrollPane(m_columnList), BorderLayout.CENTER);

        // Function catalog at bottom
        final JPanel fnPanel = new JPanel(new BorderLayout());
        final JLabel fnLabel = new JLabel(" Function Catalog");
        fnLabel.setFont(fnLabel.getFont().deriveFont(Font.BOLD, 12f));
        fnLabel.setBorder(BorderFactory.createEmptyBorder(4, 2, 4, 2));
        fnPanel.add(fnLabel, BorderLayout.NORTH);
        fnPanel.add(new JScrollPane(m_functionList), BorderLayout.CENTER);

        // Vertical split: columns on top, functions on bottom
        final JSplitPane vSplit = new JSplitPane(JSplitPane.VERTICAL_SPLIT, colPanel, fnPanel);
        vSplit.setDividerLocation(250);
        vSplit.setResizeWeight(0.5);
        vSplit.setContinuousLayout(true);

        panel.add(vSplit, BorderLayout.CENTER);

        // Hint
        final JLabel hint = new JLabel("<html><i>Double-click to insert</i></html>");
        hint.setFont(hint.getFont().deriveFont(10f));
        hint.setBorder(BorderFactory.createEmptyBorder(2, 4, 4, 4));
        panel.add(hint, BorderLayout.SOUTH);

        return panel;
    }

    private JPanel buildRightPanel() {
        final JPanel panel = new JPanel(new BorderLayout());

        // Top: expression tab controls (+ / - buttons)
        final JPanel tabCtrlPanel = new JPanel(new BorderLayout());
        tabCtrlPanel.add(m_exprTabs, BorderLayout.CENTER);

        final JPanel tabButtons = new JPanel(new FlowLayout(FlowLayout.LEFT, 2, 2));
        tabButtons.add(m_addTabButton);
        tabButtons.add(m_removeTabButton);
        tabCtrlPanel.add(tabButtons, BorderLayout.EAST);

        // Bottom: Preview pane
        final JPanel previewPanel = new JPanel(new BorderLayout(4, 4));
        previewPanel.setBorder(BorderFactory.createTitledBorder("Output Preview"));

        final JPanel evalBar = new JPanel(new FlowLayout(FlowLayout.LEFT, 6, 2));
        evalBar.add(m_evaluateButton);
        evalBar.add(new JLabel("Preview first 5 rows on Spark"));
        previewPanel.add(evalBar, BorderLayout.NORTH);

        final JScrollPane previewScroll = new JScrollPane(m_previewArea);
        previewScroll.setPreferredSize(new Dimension(400, 160));
        previewPanel.add(previewScroll, BorderLayout.CENTER);

        // Vertical split: expression editors on top, preview on bottom
        final JSplitPane vSplit = new JSplitPane(JSplitPane.VERTICAL_SPLIT, tabCtrlPanel, previewPanel);
        vSplit.setDividerLocation(350);
        vSplit.setResizeWeight(0.7);
        vSplit.setContinuousLayout(true);

        panel.add(vSplit, BorderLayout.CENTER);
        return panel;
    }

    // ── Expression tab management ────────────────────────────────────────────

    private void addExpressionTab(final String expr, final OutputMode mode, final String colName) {
        final ExpressionEntry entry = new ExpressionEntry(expr, mode, colName);
        m_entries.add(entry);

        final JPanel tabContent = buildExpressionTabContent(entry);
        final int index = m_entries.size();
        m_exprTabs.addTab("Expression " + index, tabContent);
        m_exprTabs.setSelectedIndex(m_exprTabs.getTabCount() - 1);
        updateRemoveButton();
    }

    private void removeCurrentTab() {
        final int idx = m_exprTabs.getSelectedIndex();
        if (idx >= 0 && m_entries.size() > 1) {
            m_entries.remove(idx);
            m_exprTabs.removeTabAt(idx);
            // Re-number tabs
            for (int i = 0; i < m_exprTabs.getTabCount(); i++) {
                m_exprTabs.setTitleAt(i, "Expression " + (i + 1));
            }
            updateRemoveButton();
        }
    }

    private void updateRemoveButton() {
        m_removeTabButton.setEnabled(m_entries.size() > 1);
    }

    private JPanel buildExpressionTabContent(final ExpressionEntry entry) {
        final JPanel panel = new JPanel(new BorderLayout(4, 4));
        panel.setBorder(BorderFactory.createEmptyBorder(4, 4, 4, 4));

        // Expression editor (center, takes most space)
        final JScrollPane editorScroll = new JScrollPane(entry.editor);
        editorScroll.setPreferredSize(new Dimension(400, 200));
        panel.add(editorScroll, BorderLayout.CENTER);

        // Output controls at bottom of editor
        final JPanel outputPanel = new JPanel(new GridBagLayout());
        outputPanel.setBorder(BorderFactory.createCompoundBorder(
            BorderFactory.createMatteBorder(1, 0, 0, 0, Color.LIGHT_GRAY),
            BorderFactory.createEmptyBorder(4, 4, 4, 4)));

        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(2, 4, 2, 4);
        gbc.anchor = GridBagConstraints.WEST;

        gbc.gridx = 0; gbc.gridy = 0;
        outputPanel.add(new JLabel("Output:"), gbc);

        gbc.gridx = 1;
        outputPanel.add(entry.modeCombo, gbc);

        gbc.gridx = 2;
        outputPanel.add(new JLabel("Column:"), gbc);

        gbc.gridx = 3;
        gbc.weightx = 1.0;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        outputPanel.add(entry.columnNameField, gbc);

        panel.add(outputPanel, BorderLayout.SOUTH);

        return panel;
    }

    // ── Insert helpers ───────────────────────────────────────────────────────

    private void insertSelectedColumn() {
        final int idx = m_columnList.getSelectedIndex();
        if (idx < 0 || m_tableSpec == null) {
            return;
        }
        final DataColumnSpec colSpec = m_tableSpec.getColumnSpec(idx);
        final String colName = colSpec.getName();
        // Use backticks for names with spaces/special chars
        final String insert = colName.contains(" ") || colName.contains("-")
            ? "`" + colName + "`" : colName;
        insertIntoCurrentEditor(insert);
    }

    private void insertSelectedFunction() {
        final int idx = m_functionList.getSelectedIndex();
        if (idx < 0 || idx >= FUNCTION_CATALOG.length) {
            return;
        }
        final String template = FUNCTION_CATALOG[idx][1];
        if (!template.isEmpty()) {
            insertIntoCurrentEditor(template);
        }
    }

    private void insertIntoCurrentEditor(final String text) {
        final int tabIdx = m_exprTabs.getSelectedIndex();
        if (tabIdx < 0 || tabIdx >= m_entries.size()) {
            return;
        }
        final JTextArea editor = m_entries.get(tabIdx).editor;
        final int caretPos = editor.getCaretPosition();
        editor.insert(text, caretPos);
        editor.requestFocusInWindow();
    }

    // ── Column list update ───────────────────────────────────────────────────

    private void updateColumnList() {
        m_columnListModel.clear();
        if (m_tableSpec == null) {
            return;
        }
        for (int i = 0; i < m_tableSpec.getNumColumns(); i++) {
            final DataColumnSpec colSpec = m_tableSpec.getColumnSpec(i);
            m_columnListModel.addElement(colSpec.getName() + "  (" + colSpec.getType().getName() + ")");
        }
    }

    // ── Load / Save ──────────────────────────────────────────────────────────

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
        loadCommonSettings(settings);
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
        loadCommonSettings(settings);
    }

    private void loadCommonSettings(final NodeSettingsRO settings) {
        // Clear existing tabs
        m_entries.clear();
        m_exprTabs.removeAll();

        final boolean isConfigured = m_everSavedWithOk
            || settings.containsKey(SparkExpressionSettings.CFG_CONFIGURED);

        if (isConfigured) {
            try {
                final String[] expressions = settings.getStringArray(SparkExpressionSettings.CFG_EXPRESSIONS);
                final String[] modes = settings.getStringArray(SparkExpressionSettings.CFG_OUTPUT_MODES);
                final String[] names = settings.getStringArray(SparkExpressionSettings.CFG_COLUMN_NAMES);

                for (int i = 0; i < expressions.length; i++) {
                    OutputMode mode;
                    try {
                        mode = OutputMode.valueOf(modes[i]);
                    } catch (final Exception e) {
                        mode = OutputMode.APPEND;
                    }
                    addExpressionTab(expressions[i], mode, names[i]);
                }
            } catch (final InvalidSettingsException e) {
                addExpressionTab("", OutputMode.APPEND, "new_column");
            }
        } else {
            addExpressionTab("", OutputMode.APPEND, "new_column");
        }

        m_previewArea.setText("");
        updateColumnList();
        updateRemoveButton();
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        validateDialog();

        final int count = m_entries.size();
        final String[] expressions = new String[count];
        final String[] modes = new String[count];
        final String[] names = new String[count];

        for (int i = 0; i < count; i++) {
            final ExpressionEntry entry = m_entries.get(i);
            expressions[i] = entry.editor.getText();
            modes[i] = ((OutputMode) entry.modeCombo.getSelectedItem()).name();
            names[i] = entry.columnNameField.getText();
        }

        settings.addStringArray(SparkExpressionSettings.CFG_EXPRESSIONS, expressions);
        settings.addStringArray(SparkExpressionSettings.CFG_OUTPUT_MODES, modes);
        settings.addStringArray(SparkExpressionSettings.CFG_COLUMN_NAMES, names);
        settings.addBoolean(SparkExpressionSettings.CFG_CONFIGURED, true);
        m_everSavedWithOk = true;
    }

    private void validateDialog() throws InvalidSettingsException {
        final int count = m_entries.size();
        if (count == 0) {
            throw new InvalidSettingsException("At least one expression is required.");
        }

        final Set<String> outputNames = new HashSet<>();
        for (int i = 0; i < count; i++) {
            final ExpressionEntry entry = m_entries.get(i);
            final String expr = entry.editor.getText();
            final String colName = entry.columnNameField.getText();
            final OutputMode mode = (OutputMode) entry.modeCombo.getSelectedItem();

            if (expr == null || expr.trim().isEmpty()) {
                m_exprTabs.setSelectedIndex(i);
                throw new InvalidSettingsException(
                    "Expression " + (i + 1) + " is empty. Enter a Spark SQL expression.");
            }
            if (colName == null || colName.trim().isEmpty()) {
                m_exprTabs.setSelectedIndex(i);
                throw new InvalidSettingsException(
                    "Output column name for Expression " + (i + 1) + " is empty.");
            }

            if (!outputNames.add(colName)) {
                m_exprTabs.setSelectedIndex(i);
                throw new InvalidSettingsException(
                    "Duplicate output column name '" + colName + "' in Expression " + (i + 1)
                    + ". Each expression must target a unique output column name.");
            }

            if (mode == OutputMode.REPLACE && m_tableSpec != null) {
                if (m_tableSpec.findColumnIndex(colName) == -1) {
                    m_exprTabs.setSelectedIndex(i);
                    throw new InvalidSettingsException(
                        "Expression " + (i + 1) + ": cannot replace column '" + colName
                        + "' — it does not exist in the input table.");
                }
            }

            if (mode == OutputMode.APPEND && m_tableSpec != null) {
                if (m_tableSpec.findColumnIndex(colName) != -1) {
                    m_exprTabs.setSelectedIndex(i);
                    throw new InvalidSettingsException(
                        "Expression " + (i + 1) + ": output column '" + colName
                        + "' already exists. Use REPLACE mode or a different name.");
                }
            }
        }
    }

    // ── Spark Validation ─────────────────────────────────────────────────────

    private void runValidation() {
        try {
            validateDialog();
        } catch (final InvalidSettingsException e) {
            showError(e.getMessage());
            return;
        }

        if (m_contextID == null) {
            showError("No Spark context available.");
            return;
        }
        if (m_dataFrameID == null) {
            showError("Execute the upstream node first to enable evaluation.");
            return;
        }

        final int count = m_entries.size();
        final String[] expressions = new String[count];
        final String[] modes = new String[count];
        final String[] names = new String[count];
        for (int i = 0; i < count; i++) {
            final ExpressionEntry entry = m_entries.get(i);
            expressions[i] = entry.editor.getText();
            modes[i] = ((OutputMode) entry.modeCombo.getSelectedItem()).name();
            names[i] = entry.columnNameField.getText();
        }

        m_evaluateButton.setEnabled(false);
        m_previewArea.setText("Evaluating " + count + " expression(s) on Spark...");
        m_previewArea.setForeground(Color.GRAY);

        new SwingWorker<SparkExpressionJobOutput, Void>() {
            @Override
            protected SparkExpressionJobOutput doInBackground() throws Exception {
                final SparkExpressionJobInput jobInput = new SparkExpressionJobInput(
                    m_dataFrameID, expressions, modes, names);
                return SparkContextUtil
                    .<SparkExpressionJobInput, SparkExpressionJobOutput>getJobRunFactory(
                        m_contextID, SparkExpressionNodeModel.JOB_ID)
                    .createRun(jobInput)
                    .run(m_contextID, new ExecutionMonitor());
            }

            @Override
            protected void done() {
                m_evaluateButton.setEnabled(true);
                try {
                    final SparkExpressionJobOutput output = get();
                    final StringBuilder msg = new StringBuilder();
                    msg.append("All ").append(count).append(" expression(s) evaluated successfully.\n\n");
                    final String preview = output.getPreviewData();
                    if (preview != null && !preview.isEmpty()) {
                        msg.append(preview);
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
        m_previewArea.setForeground(COLOR_SUCCESS);
        m_previewArea.setText(message);
        m_previewArea.setCaretPosition(0);
    }

    private void showError(final String message) {
        m_previewArea.setForeground(COLOR_ERROR);
        m_previewArea.setText(message);
        m_previewArea.setCaretPosition(0);
    }

    // ── Cell Renderers ───────────────────────────────────────────────────────

    /** Renders column list items with type info styled. */
    private static final class ColumnCellRenderer extends DefaultListCellRenderer {
        private static final long serialVersionUID = 1L;

        @Override
        public Component getListCellRendererComponent(final JList<?> list, final Object value,
                final int index, final boolean isSelected, final boolean cellHasFocus) {
            super.getListCellRendererComponent(list, value, index, isSelected, cellHasFocus);
            setFont(MONO_SMALL);
            setCursor(Cursor.getPredefinedCursor(Cursor.HAND_CURSOR));
            return this;
        }
    }

    /** Renders function catalog with category headers styled differently. */
    private static final class FunctionCellRenderer extends DefaultListCellRenderer {
        private static final long serialVersionUID = 1L;

        @Override
        public Component getListCellRendererComponent(final JList<?> list, final Object value,
                final int index, final boolean isSelected, final boolean cellHasFocus) {
            super.getListCellRendererComponent(list, value, index, isSelected, cellHasFocus);
            final String text = value != null ? value.toString() : "";
            if (text.startsWith("──")) {
                // Category header
                setFont(getFont().deriveFont(Font.BOLD, 11f));
                setForeground(new Color(80, 80, 80));
                setBackground(new Color(235, 235, 240));
                setEnabled(false);
            } else {
                setFont(MONO_SMALL);
                setCursor(Cursor.getPredefinedCursor(Cursor.HAND_CURSOR));
            }
            return this;
        }
    }
}

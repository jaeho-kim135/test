package org.knime.bigdata.spark.dx.node.preproc.unpivot;

import java.awt.BorderLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.swing.BorderFactory;
import javax.swing.JPanel;

import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnFilter;
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelFilterString;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObjectSpec;

/**
 * Dialog for the Spark Unpivot node.
 */
public final class SparkUnpivotNodeDialog extends NodeDialogPane {

    private final SettingsModelFilterString m_retainedColumns =
        new SettingsModelFilterString(SparkUnpivotSettings.CFG_RETAINED_COLUMNS);

    private final SettingsModelFilterString m_valueColumns =
        new SettingsModelFilterString(SparkUnpivotSettings.CFG_VALUE_COLUMNS);

    private final SettingsModelString m_variableColName =
        new SettingsModelString(SparkUnpivotSettings.CFG_VARIABLE_COL_NAME, "variable");

    private final SettingsModelString m_valueColName =
        new SettingsModelString(SparkUnpivotSettings.CFG_VALUE_COL_NAME, "value");

    private final SettingsModelBoolean m_skipMissingValues =
        new SettingsModelBoolean(SparkUnpivotSettings.CFG_SKIP_MISSING_VALUES, true);

    private final DialogComponentColumnFilter m_retainedColFilter =
        new DialogComponentColumnFilter(m_retainedColumns, 0, false);

    private final DialogComponentColumnFilter m_valueColFilter =
        new DialogComponentColumnFilter(m_valueColumns, 0, false);

    private final DialogComponentString m_variableColNameComp =
        new DialogComponentString(m_variableColName, "Variable column name: ");

    private final DialogComponentString m_valueColNameComp =
        new DialogComponentString(m_valueColName, "Value column name: ");

    private final DialogComponentBoolean m_skipMissingComp =
        new DialogComponentBoolean(m_skipMissingValues, "Skip missing values");

    /** Constructor. */
    public SparkUnpivotNodeDialog() {
        m_retainedColFilter.setIncludeTitle(" Retained column(s) ");
        m_retainedColFilter.setExcludeTitle(" Available column(s) ");
        m_retainedColFilter.setShowInvalidIncludeColumns(true);

        m_valueColFilter.setIncludeTitle(" Value column(s) ");
        m_valueColFilter.setExcludeTitle(" Available column(s) ");
        m_valueColFilter.setShowInvalidIncludeColumns(true);

        // Retained Columns tab
        addTab("Retained Columns", m_retainedColFilter.getComponentPanel());

        // Value Columns tab
        addTab("Value Columns", m_valueColFilter.getComponentPanel());

        // Options tab
        final JPanel optionsPanel = new JPanel(new GridBagLayout());
        optionsPanel.setBorder(BorderFactory.createTitledBorder("Output Settings"));
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(5, 5, 5, 5);
        gbc.anchor = GridBagConstraints.WEST;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.weightx = 1.0;
        gbc.gridx = 0;
        gbc.gridy = 0;
        optionsPanel.add(m_variableColNameComp.getComponentPanel(), gbc);
        gbc.gridy++;
        optionsPanel.add(m_valueColNameComp.getComponentPanel(), gbc);
        gbc.gridy++;
        optionsPanel.add(m_skipMissingComp.getComponentPanel(), gbc);

        // Wrap in a panel that pushes content to top
        final JPanel optionsWrapper = new JPanel(new BorderLayout());
        optionsWrapper.add(optionsPanel, BorderLayout.NORTH);
        addTab("Options", optionsWrapper);
    }

    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
            throws NotConfigurableException {
        if (specs == null || specs.length < 1 || specs[0] == null) {
            throw new NotConfigurableException("No input connection found.");
        }

        // Extract DataTableSpec from SparkDataPortObjectSpec
        final SparkDataPortObjectSpec sparkSpec = (SparkDataPortObjectSpec) specs[0];
        final DataTableSpec tableSpec = sparkSpec.getTableSpec();
        final DataTableSpec[] tableSpecs = new DataTableSpec[]{tableSpec};

        m_retainedColFilter.loadSettingsFrom(settings, tableSpecs);
        m_valueColFilter.loadSettingsFrom(settings, tableSpecs);
        try {
            m_variableColName.loadSettingsFrom(settings);
        } catch (final InvalidSettingsException e) {
            // use default
        }
        try {
            m_valueColName.loadSettingsFrom(settings);
        } catch (final InvalidSettingsException e) {
            // use default
        }
        try {
            m_skipMissingValues.loadSettingsFrom(settings);
        } catch (final InvalidSettingsException e) {
            // use default
        }
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        // Validate before saving
        validateSettings();

        m_retainedColFilter.saveSettingsTo(settings);
        m_valueColFilter.saveSettingsTo(settings);
        m_variableColName.saveSettingsTo(settings);
        m_valueColName.saveSettingsTo(settings);
        m_skipMissingValues.saveSettingsTo(settings);
    }

    private void validateSettings() throws InvalidSettingsException {
        final List<String> retainedCols = m_retainedColumns.getIncludeList();
        final List<String> valueCols = m_valueColumns.getIncludeList();

        // Check retained columns
        if (retainedCols == null || retainedCols.isEmpty()) {
            throw new InvalidSettingsException("No retained columns selected. "
                + "Please select at least one retained (ID) column.");
        }

        // Check value columns
        if (valueCols == null || valueCols.isEmpty()) {
            throw new InvalidSettingsException("No value columns selected. "
                + "Please select at least one value column to unpivot.");
        }

        // Check overlap between retained and value columns
        final Set<String> overlap = new HashSet<>(retainedCols);
        overlap.retainAll(valueCols);
        if (!overlap.isEmpty()) {
            throw new InvalidSettingsException("The following columns are selected as both "
                + "retained and value columns: " + overlap
                + ". A column cannot be in both lists.");
        }

        // Check variable column name
        final String varName = m_variableColName.getStringValue();
        if (varName == null || varName.trim().isEmpty()) {
            throw new InvalidSettingsException("Variable column name must not be empty.");
        }

        // Check value column name
        final String valName = m_valueColName.getStringValue();
        if (valName == null || valName.trim().isEmpty()) {
            throw new InvalidSettingsException("Value column name must not be empty.");
        }
    }
}

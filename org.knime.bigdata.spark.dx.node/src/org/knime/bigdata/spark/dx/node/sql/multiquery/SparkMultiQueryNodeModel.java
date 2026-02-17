package org.knime.bigdata.spark.dx.node.sql.multiquery;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.core.port.data.SparkDataTable;
import org.knime.bigdata.spark.core.types.converter.knime.KNIMEToIntermediateConverterRegistry;
import org.knime.bigdata.spark.core.util.SparkIDs;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

/**
 * Node model for the Spark Multi Query node. Applies a SQL expression template
 * to each selected column, replacing the $columnS placeholder with the column name.
 */
public class SparkMultiQueryNodeModel extends SparkNodeModel {

    /** The unique Spark job id. */
    public static final String JOB_ID = SparkMultiQueryNodeModel.class.getCanonicalName();

    private final SparkMultiQuerySettings m_settings = new SparkMultiQuerySettings();

    /** Constructor. */
    public SparkMultiQueryNodeModel() {
        super(new PortType[]{SparkDataPortObject.TYPE},
              new PortType[]{SparkDataPortObject.TYPE});
    }

    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length < 1 || inSpecs[0] == null) {
            throw new InvalidSettingsException("No input Spark DataFrame/RDD available");
        }

        final SparkDataPortObjectSpec sparkSpec = (SparkDataPortObjectSpec) inSpecs[0];
        final DataTableSpec tableSpec = sparkSpec.getTableSpec();

        // Validate target columns
        final List<String> targetColumns = m_settings.getTargetColumns();
        if (targetColumns == null || targetColumns.isEmpty()) {
            throw new InvalidSettingsException("No target columns selected. "
                + "Please select at least one column to apply the SQL expression to.");
        }

        for (String col : targetColumns) {
            if (tableSpec.findColumnIndex(col) == -1) {
                throw new InvalidSettingsException("Target column '" + col + "' not found in input table.");
            }
        }

        // Validate SQL expression
        final String expr = m_settings.getSqlExpression();
        if (expr == null || expr.trim().isEmpty()) {
            throw new InvalidSettingsException("SQL expression must not be empty.");
        }

        if (!expr.contains(SparkMultiQuerySettings.COLUMN_PLACEHOLDER)) {
            throw new InvalidSettingsException(
                "SQL expression must contain the placeholder '" + SparkMultiQuerySettings.COLUMN_PLACEHOLDER
                + "' which will be replaced with each target column name.");
        }

        // Validate output column pattern
        final String outputPattern = m_settings.getOutputColumnPattern();
        if (outputPattern == null || outputPattern.trim().isEmpty()) {
            throw new InvalidSettingsException("Output column pattern must not be empty.");
        }

        if (!outputPattern.contains(SparkMultiQuerySettings.COLUMN_PLACEHOLDER)) {
            throw new InvalidSettingsException(
                "Output column pattern must contain '" + SparkMultiQuerySettings.COLUMN_PLACEHOLDER + "'.");
        }

        // Validate: keepOriginal=true requires pattern different from $columnS
        if (m_settings.keepOriginalColumns()
                && SparkMultiQuerySettings.COLUMN_PLACEHOLDER.equals(outputPattern.trim())) {
            throw new InvalidSettingsException(
                "When 'Keep original columns' is enabled, the output column pattern must differ from '"
                + SparkMultiQuerySettings.COLUMN_PLACEHOLDER
                + "' to avoid duplicate column names. Example: " + SparkMultiQuerySettings.COLUMN_PLACEHOLDER + "_new");
        }

        // Validate no output alias conflicts with existing non-target columns
        if (m_settings.keepOriginalColumns()) {
            final Set<String> existingCols = new HashSet<>();
            for (int i = 0; i < tableSpec.getNumColumns(); i++) {
                existingCols.add(tableSpec.getColumnSpec(i).getName());
            }
            for (String col : targetColumns) {
                final String alias = outputPattern.replace(SparkMultiQuerySettings.COLUMN_PLACEHOLDER, col);
                if (existingCols.contains(alias) && !targetColumns.contains(alias)) {
                    throw new InvalidSettingsException(
                        "Output column name '" + alias + "' conflicts with an existing column. "
                        + "Change the output column pattern.");
                }
            }
        }

        // Output spec is null because the SQL expression may change column types
        return new PortObjectSpec[]{null};
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject sparkPort = (SparkDataPortObject) inData[0];
        final SparkContextID contextID = sparkPort.getContextID();
        final String inputObject = sparkPort.getData().getID();
        final String outputObject = SparkIDs.createSparkDataObjectID();

        final List<String> targetColumns = m_settings.getTargetColumns();

        final SparkMultiQueryJobInput jobInput = new SparkMultiQueryJobInput(
            inputObject,
            outputObject,
            targetColumns.toArray(new String[0]),
            m_settings.getSqlExpression(),
            m_settings.keepOriginalColumns(),
            m_settings.getOutputColumnPattern());

        exec.setMessage("Executing Spark multi query job...");
        final SparkMultiQueryJobOutput jobOutput = SparkContextUtil
            .<SparkMultiQueryJobInput, SparkMultiQueryJobOutput>getJobRunFactory(contextID, JOB_ID)
            .createRun(jobInput)
            .run(contextID, exec);

        final DataTableSpec outputSpec =
            KNIMEToIntermediateConverterRegistry.convertSpec(jobOutput.getSpec(outputObject));
        final SparkDataTable resultTable = new SparkDataTable(contextID, outputObject, outputSpec);
        return new PortObject[]{new SparkDataPortObject(resultTable)};
    }

    @Override
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveSettingsTo(settings);
    }

    @Override
    protected void validateAdditionalSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.validateSettings(settings);
    }

    @Override
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadSettingsFrom(settings);
    }
}

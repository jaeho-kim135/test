package org.knime.bigdata.spark.dx.node.preproc.unpivot;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.core.port.data.SparkDataTable;
import org.knime.bigdata.spark.core.types.converter.knime.KNIMEToIntermediateConverterRegistry;
import org.knime.bigdata.spark.core.util.SparkIDs;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

/**
 * Node model for the Spark Unpivot node. Transforms wide format data to long format
 * using Spark's stack() function.
 */
public class SparkUnpivotNodeModel extends SparkNodeModel {

    /** The unique Spark job id. */
    public static final String JOB_ID = SparkUnpivotNodeModel.class.getCanonicalName();

    private final SparkUnpivotSettings m_settings = new SparkUnpivotSettings();

    /** Constructor. */
    public SparkUnpivotNodeModel() {
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

        // Validate value columns
        final List<String> valueColumns = m_settings.getValueColumns();
        if (valueColumns == null || valueColumns.isEmpty()) {
            throw new InvalidSettingsException("No value columns selected for unpivoting.");
        }

        for (String col : valueColumns) {
            if (tableSpec.findColumnIndex(col) == -1) {
                throw new InvalidSettingsException("Value column '" + col + "' not found in input table.");
            }
        }

        // Validate retained columns
        for (String col : m_settings.getRetainedColumns()) {
            if (tableSpec.findColumnIndex(col) == -1) {
                throw new InvalidSettingsException("Retained column '" + col + "' not found in input table.");
            }
        }

        // Validate output column names
        if (StringUtils.isBlank(m_settings.getVariableColName())) {
            throw new InvalidSettingsException("Variable column name must not be empty.");
        }

        if (StringUtils.isBlank(m_settings.getValueColName())) {
            throw new InvalidSettingsException("Value column name must not be empty.");
        }

        // Build output spec: [retained columns] + [variable: String] + [value: String]
        final DataTableSpec outputSpec = createOutputSpec(tableSpec);
        return new PortObjectSpec[]{new SparkDataPortObjectSpec(sparkSpec.getContextID(), outputSpec)};
    }

    private DataTableSpec createOutputSpec(final DataTableSpec inputSpec) {
        final List<DataColumnSpec> outputCols = new ArrayList<>();

        // Add retained columns with original types
        for (String col : m_settings.getRetainedColumns()) {
            final DataColumnSpec colSpec = inputSpec.getColumnSpec(col);
            if (colSpec != null) {
                outputCols.add(colSpec);
            }
        }

        // Add variable column (String)
        outputCols.add(new DataColumnSpecCreator(m_settings.getVariableColName(), StringCell.TYPE).createSpec());

        // Add value column (String)
        outputCols.add(new DataColumnSpecCreator(m_settings.getValueColName(), StringCell.TYPE).createSpec());

        return new DataTableSpec(outputCols.toArray(new DataColumnSpec[0]));
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject sparkPort = (SparkDataPortObject) inData[0];
        final SparkContextID contextID = sparkPort.getContextID();
        final String inputObject = sparkPort.getData().getID();
        final String outputObject = SparkIDs.createSparkDataObjectID();

        final List<String> retainedColumns = m_settings.getRetainedColumns();
        final List<String> valueColumns = m_settings.getValueColumns();

        final SparkUnpivotJobInput jobInput = new SparkUnpivotJobInput(
            inputObject,
            outputObject,
            retainedColumns.toArray(new String[0]),
            valueColumns.toArray(new String[0]),
            m_settings.getVariableColName(),
            m_settings.getValueColName(),
            m_settings.skipMissingValues());

        exec.setMessage("Executing Spark unpivot job...");
        final SparkUnpivotJobOutput jobOutput = SparkContextUtil
            .<SparkUnpivotJobInput, SparkUnpivotJobOutput>getJobRunFactory(contextID, JOB_ID)
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

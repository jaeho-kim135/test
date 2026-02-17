package org.knime.bigdata.spark.dx.node.preproc.unpivot;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import org.knime.core.data.DataType;
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

        // Validate retained columns
        final List<String> retainedColumns = m_settings.getRetainedColumns();
        if (retainedColumns == null || retainedColumns.isEmpty()) {
            throw new InvalidSettingsException("No retained columns selected. "
                + "Please select at least one retained (ID) column.");
        }

        for (String col : retainedColumns) {
            if (tableSpec.findColumnIndex(col) == -1) {
                throw new InvalidSettingsException("Retained column '" + col + "' not found in input table.");
            }
        }

        // Validate value columns
        final List<String> valueColumns = m_settings.getValueColumns();
        if (valueColumns == null || valueColumns.isEmpty()) {
            throw new InvalidSettingsException("No value columns selected. "
                + "Please select at least one value column to unpivot.");
        }

        for (String col : valueColumns) {
            if (tableSpec.findColumnIndex(col) == -1) {
                throw new InvalidSettingsException("Value column '" + col + "' not found in input table.");
            }
        }

        // Validate no overlap between retained and value columns
        final Set<String> overlap = new HashSet<>(retainedColumns);
        overlap.retainAll(valueColumns);
        if (!overlap.isEmpty()) {
            throw new InvalidSettingsException("The following columns are selected as both "
                + "retained and value columns: " + overlap
                + ". A column cannot be in both lists.");
        }

        // Validate output column names
        if (m_settings.getVariableColName() == null || m_settings.getVariableColName().trim().isEmpty()) {
            throw new InvalidSettingsException("Variable column name must not be empty.");
        }

        if (m_settings.getValueColName() == null || m_settings.getValueColName().trim().isEmpty()) {
            throw new InvalidSettingsException("Value column name must not be empty.");
        }

        // Validate variable and value column names are different
        if (m_settings.getVariableColName().trim().equals(m_settings.getValueColName().trim())) {
            throw new InvalidSettingsException(
                "Variable column name and Value column name must be different. "
                + "Both are set to '" + m_settings.getVariableColName().trim() + "'.");
        }

        // Validate output column names don't conflict with retained column names
        final Set<String> retainedSet = new HashSet<>(retainedColumns);
        final String varColName = m_settings.getVariableColName().trim();
        final String valColName = m_settings.getValueColName().trim();
        if (retainedSet.contains(varColName)) {
            throw new InvalidSettingsException(
                "Variable column name '" + varColName + "' conflicts with a retained column name. "
                + "Please choose a different name.");
        }
        if (retainedSet.contains(valColName)) {
            throw new InvalidSettingsException(
                "Value column name '" + valColName + "' conflicts with a retained column name. "
                + "Please choose a different name.");
        }

        // Check type compatibility of value columns
        if (!m_settings.castToString()) {
            checkValueColumnTypes(tableSpec, valueColumns);
        }

        // Build output spec: [retained columns] + [variable: String] + [value: type]
        final DataTableSpec outputSpec = createOutputSpec(tableSpec);
        return new PortObjectSpec[]{new SparkDataPortObjectSpec(sparkSpec.getContextID(), outputSpec)};
    }

    /**
     * Check if value columns have compatible types. Numeric types (int, long, double) are compatible.
     * String mixed with numeric types are not compatible.
     */
    private void checkValueColumnTypes(final DataTableSpec tableSpec, final List<String> valueColumns)
            throws InvalidSettingsException {

        boolean hasNumeric = false;
        boolean hasString = false;
        final List<String> typeDetails = new ArrayList<>();

        for (String col : valueColumns) {
            final DataColumnSpec colSpec = tableSpec.getColumnSpec(col);
            if (colSpec != null) {
                final DataType type = colSpec.getType();
                final String typeName = type.getName();
                typeDetails.add(typeName + " (`" + col + "`)");

                if (type.isCompatible(org.knime.core.data.DoubleValue.class)) {
                    hasNumeric = true;
                } else {
                    hasString = true;
                }
            }
        }

        if (hasNumeric && hasString) {
            throw new InvalidSettingsException(
                "Value columns have incompatible types: " + String.join(", ", typeDetails)
                + ". Spark unpivot requires all value columns to share a common type. "
                + "Enable 'Cast all value columns to String' option to convert all values to String.");
        }
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

        final Map<String, String> varMap = m_settings.getVariableValueMap();
        final String[] varMapKeys = varMap.keySet().toArray(new String[0]);
        final String[] varMapValues = varMap.values().toArray(new String[0]);

        final SparkUnpivotJobInput jobInput = new SparkUnpivotJobInput(
            inputObject,
            outputObject,
            retainedColumns.toArray(new String[0]),
            valueColumns.toArray(new String[0]),
            m_settings.getVariableColName(),
            m_settings.getValueColName(),
            m_settings.skipMissingValues(),
            m_settings.castToString(),
            m_settings.getSortOption(),
            varMapKeys,
            varMapValues);

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

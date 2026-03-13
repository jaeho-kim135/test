package org.knime.bigdata.spark.dx.node.sql.expression;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.core.port.data.SparkDataTable;
import org.knime.bigdata.spark.core.types.converter.knime.KNIMEToIntermediateConverterRegistry;
import org.knime.bigdata.spark.core.util.SparkIDs;
import org.knime.bigdata.spark.dx.node.sql.expression.SparkExpressionSettings.OutputMode;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.node.workflow.NativeNodeContainer;
import org.knime.core.node.workflow.NodeContainer;
import org.knime.core.node.workflow.NodeContext;

/**
 * Node model for the Spark Expression node.
 * Applies multiple Spark SQL expressions to transform/add columns.
 */
public class SparkExpressionNodeModel extends SparkNodeModel {

    /** The unique Spark job id. */
    public static final String JOB_ID = SparkExpressionNodeModel.class.getCanonicalName();

    /** Pattern for flow variable placeholders: $${varName} */
    private static final Pattern FLOW_VAR_PATTERN = Pattern.compile("\\$\\$\\{([^}]+)\\}");

    private final SparkExpressionSettings m_settings = new SparkExpressionSettings();

    /** Constructor. */
    public SparkExpressionNodeModel() {
        super(new PortType[]{SparkDataPortObject.TYPE},
              new PortType[]{SparkDataPortObject.TYPE});
    }

    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length < 1 || inSpecs[0] == null) {
            throw new InvalidSettingsException("No input Spark DataFrame available.");
        }

        if (!m_settings.isNodeConfigured()) {
            throw new InvalidSettingsException(
                "Node has not been configured. Open the dialog and enter at least one expression.");
        }

        final SparkDataPortObjectSpec sparkSpec = (SparkDataPortObjectSpec) inSpecs[0];
        final DataTableSpec tableSpec = sparkSpec.getTableSpec();

        validateExpressions(tableSpec);

        // Output spec is null because expressions may produce unknown column types
        return new PortObjectSpec[]{null};
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject sparkPort = (SparkDataPortObject) inData[0];
        final SparkContextID contextID = sparkPort.getContextID();
        final String inputObject = sparkPort.getData().getID();
        final String outputObject = SparkIDs.createSparkDataObjectID();

        final List<String> expressions = m_settings.getExpressions();
        final List<OutputMode> modes = m_settings.getOutputModes();
        final List<String> columnNames = m_settings.getColumnNames();

        // Resolve $${varName} flow variable placeholders
        final String[] resolvedExprs = resolveFlowVariables(expressions);

        final String[] modesStr = modes.stream()
            .map(OutputMode::name)
            .toArray(String[]::new);

        final SparkExpressionJobInput jobInput = new SparkExpressionJobInput(
            inputObject, outputObject,
            resolvedExprs,
            modesStr,
            columnNames.toArray(new String[0]));

        exec.setMessage("Executing Spark expression job...");
        final SparkExpressionJobOutput jobOutput = SparkContextUtil
            .<SparkExpressionJobInput, SparkExpressionJobOutput>getJobRunFactory(contextID, JOB_ID)
            .createRun(jobInput)
            .run(contextID, exec);

        final DataTableSpec outputSpec =
            KNIMEToIntermediateConverterRegistry.convertSpec(jobOutput.getSpec(outputObject));
        final SparkDataTable resultTable = new SparkDataTable(contextID, outputObject, outputSpec);
        return new PortObject[]{new SparkDataPortObject(resultTable)};
    }

    /**
     * Validates the expression settings against the input table spec.
     */
    private void validateExpressions(final DataTableSpec tableSpec) throws InvalidSettingsException {
        final List<String> expressions = m_settings.getExpressions();
        final List<OutputMode> modes = m_settings.getOutputModes();
        final List<String> columnNames = m_settings.getColumnNames();

        if (expressions.isEmpty()) {
            throw new InvalidSettingsException("At least one expression is required.");
        }

        final Set<String> existingColumns = new HashSet<>();
        for (int i = 0; i < tableSpec.getNumColumns(); i++) {
            existingColumns.add(tableSpec.getColumnSpec(i).getName());
        }

        // Track columns as they're added/replaced through the expression chain
        final Set<String> currentColumns = new HashSet<>(existingColumns);
        final Set<String> outputNames = new HashSet<>();

        for (int i = 0; i < expressions.size(); i++) {
            final String expr = expressions.get(i);
            final OutputMode mode = modes.get(i);
            final String colName = columnNames.get(i);

            // Check empty expression
            if (expr == null || expr.trim().isEmpty()) {
                throw new InvalidSettingsException(
                    "Expression " + (i + 1) + " is empty. Enter a Spark SQL expression.");
            }

            // Check empty column name
            if (colName == null || colName.trim().isEmpty()) {
                throw new InvalidSettingsException(
                    "Output column name for Expression " + (i + 1) + " is empty.");
            }

            // Check for duplicate output column names within the expression list
            if (!outputNames.add(colName)) {
                throw new InvalidSettingsException(
                    "Duplicate output column name '" + colName + "' in Expression " + (i + 1)
                    + ". Each expression must target a unique output column name.");
            }

            if (mode == OutputMode.REPLACE) {
                // REPLACE mode: the column must exist (either originally or added by a previous expression)
                if (!currentColumns.contains(colName)) {
                    throw new InvalidSettingsException(
                        "Expression " + (i + 1) + ": cannot replace column '" + colName
                        + "' because it does not exist in the input table. "
                        + "Use APPEND mode to create a new column.");
                }
            } else {
                // APPEND mode: the column must NOT already exist in the original input
                if (existingColumns.contains(colName)) {
                    throw new InvalidSettingsException(
                        "Expression " + (i + 1) + ": output column '" + colName
                        + "' already exists in the input table. "
                        + "Use REPLACE mode or choose a different column name.");
                }
                currentColumns.add(colName);
            }
        }
    }

    /**
     * Resolves {@code $${varName}} placeholders in expressions with actual flow variable values.
     * String variables are SQL-quoted, numeric types are inserted as literals.
     */
    @SuppressWarnings("deprecation")
    private String[] resolveFlowVariables(final List<String> expressions) {
        Map<String, FlowVariable> flowVars = Map.of();
        try {
            final NodeContainer nc = NodeContext.getContext().getNodeContainer();
            if (nc instanceof NativeNodeContainer) {
                final var stack = ((NativeNodeContainer) nc).getFlowObjectStack();
                if (stack != null) {
                    flowVars = stack.getAvailableFlowVariables(FlowVariable.Type.values());
                }
            }
        } catch (final Exception e) {
            // Fall through with empty map — flow variable placeholders will remain unresolved
        }

        final String[] result = new String[expressions.size()];
        for (int i = 0; i < expressions.size(); i++) {
            String expr = expressions.get(i);
            final Matcher matcher = FLOW_VAR_PATTERN.matcher(expr);
            final StringBuilder sb = new StringBuilder();
            while (matcher.find()) {
                final String varName = matcher.group(1);
                final FlowVariable fv = flowVars.get(varName);
                if (fv != null) {
                    final String replacement;
                    switch (fv.getType()) {
                        case INTEGER:
                            replacement = String.valueOf(fv.getIntValue());
                            break;
                        case DOUBLE:
                            replacement = String.valueOf(fv.getDoubleValue());
                            break;
                        case STRING:
                            replacement = "'" + fv.getStringValue().replace("'", "''") + "'";
                            break;
                        default:
                            replacement = "'" + fv.getValueAsString().replace("'", "''") + "'";
                            break;
                    }
                    matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
                }
            }
            matcher.appendTail(sb);
            result[i] = sb.toString();
        }
        return result;
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

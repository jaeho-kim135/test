package org.knime.bigdata.spark.dx.node.sql.expression;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.node.workflow.NodeContext;
import org.knime.core.node.workflow.ConnectionContainer;
import org.knime.core.node.workflow.NativeNodeContainer;
import org.knime.core.node.workflow.NodeContainer;
import org.knime.core.node.workflow.WorkflowManager;

/**
 * RPC service for the Spark Expression WebUI dialog.
 * Provides expression validation and preview by running Spark jobs
 * via {@link SparkContextUtil#getJobRunFactory}.
 *
 * <p>This service is registered in {@link SparkExpressionWebNodeDialog#createRpcDataService()}
 * and called from the frontend JavaScript via JSON-RPC.
 */
@SuppressWarnings("restriction")
public final class SparkExpressionRpcService {

    /** Pattern for flow variable placeholders: $${varName} */
    private static final Pattern FLOW_VAR_PATTERN = Pattern.compile("\\$\\$\\{([^}]+)\\}");

    private final NodeContext m_nodeContext;

    /**
     * @param nodeContext captured at dialog creation time
     */
    SparkExpressionRpcService(final NodeContext nodeContext) {
        m_nodeContext = nodeContext;
    }

    /**
     * Validate and preview expressions on the Spark cluster.
     *
     * @param expressions list of Spark SQL expression strings
     * @param outputModes list of output modes ("APPEND" or "REPLACE")
     * @param columnNames list of output column names
     * @param previewRows number of preview rows (currently unused; the job uses 10)
     * @return map with keys: "success" (Boolean), "preview" or "error" (String)
     */
    public Map<String, Object> evaluateExpressions(final List<String> expressions,
            final List<String> outputModes, final List<String> columnNames,
            @SuppressWarnings("unused") final int previewRows) {

        final Map<String, Object> result = new LinkedHashMap<>();

        // Input validation
        if (expressions == null || expressions.isEmpty()) {
            result.put("success", false);
            result.put("error", "No expressions to evaluate.");
            return result;
        }
        if (outputModes == null || outputModes.size() != expressions.size()
                || columnNames == null || columnNames.size() != expressions.size()) {
            result.put("success", false);
            result.put("error", "Expressions, output modes, and column names must have equal length.");
            return result;
        }

        try {
            NodeContext.pushContext(m_nodeContext);

            final SparkDataPortObject sparkPort = findSparkInputPort();
            if (sparkPort == null) {
                result.put("success", false);
                result.put("error", "Execute the upstream node first to enable evaluation.");
                return result;
            }

            final SparkContextID contextID = sparkPort.getContextID();
            final String dataFrameID = sparkPort.getData().getID();

            // Resolve $${varName} flow variable placeholders
            final String[] resolvedExprs = resolveFlowVariables(expressions);

            final SparkExpressionJobInput jobInput = new SparkExpressionJobInput(
                dataFrameID,
                resolvedExprs,
                outputModes.toArray(new String[0]),
                columnNames.toArray(new String[0]));

            final SparkExpressionJobOutput output = SparkContextUtil
                .<SparkExpressionJobInput, SparkExpressionJobOutput>getJobRunFactory(
                    contextID, SparkExpressionNodeModel.JOB_ID)
                .createRun(jobInput)
                .run(contextID, new ExecutionMonitor());

            final String preview = output.getPreviewData();
            result.put("success", true);
            result.put("preview", preview != null ? preview : "");
            result.put("expressionCount", expressions.size());

        } catch (final Exception e) {
            result.put("success", false);
            result.put("error", extractErrorMessage(e));
        } finally {
            NodeContext.removeLastContext();
        }
        return result;
    }

    /**
     * Preview the input table data without applying any expressions.
     * Uses the existing ExpressionJob with empty expression arrays — the job
     * simply calls showString() on the input DataFrame.
     *
     * @return map with keys: "success" (Boolean), "preview" or "error" (String)
     */
    public Map<String, Object> previewInputTable() {
        final Map<String, Object> result = new LinkedHashMap<>();
        try {
            NodeContext.pushContext(m_nodeContext);

            final SparkDataPortObject sparkPort = findSparkInputPort();
            if (sparkPort == null) {
                result.put("success", false);
                result.put("error", "Execute the upstream node first to view input data.");
                return result;
            }

            final SparkContextID contextID = sparkPort.getContextID();
            final String dataFrameID = sparkPort.getData().getID();

            // Empty expressions → ExpressionJob shows the input data as-is
            final SparkExpressionJobInput jobInput = new SparkExpressionJobInput(
                dataFrameID, new String[0], new String[0], new String[0]);

            final SparkExpressionJobOutput output = SparkContextUtil
                .<SparkExpressionJobInput, SparkExpressionJobOutput>getJobRunFactory(
                    contextID, SparkExpressionNodeModel.JOB_ID)
                .createRun(jobInput)
                .run(contextID, new ExecutionMonitor());

            final String preview = output.getPreviewData();
            result.put("success", true);
            result.put("preview", preview != null ? preview : "");

        } catch (final Exception e) {
            result.put("success", false);
            result.put("error", extractErrorMessage(e));
        } finally {
            NodeContext.removeLastContext();
        }
        return result;
    }

    /**
     * Resolves {@code $${varName}} placeholders in expressions with actual flow variable values.
     * String variables are SQL-quoted ({@code 'value'}), numeric types are inserted as literals.
     */
    private String[] resolveFlowVariables(final List<String> expressions) {
        final NodeContainer nc = m_nodeContext.getNodeContainer();
        Map<String, FlowVariable> flowVars = Map.of();
        if (nc instanceof NativeNodeContainer) {
            final var stack = ((NativeNodeContainer) nc).getFlowObjectStack();
            if (stack != null) {
                flowVars = stack.getAvailableFlowVariables(FlowVariable.Type.values());
            }
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
                            // SQL-quote string values, escaping single quotes
                            replacement = "'" + fv.getStringValue().replace("'", "''") + "'";
                            break;
                        default:
                            replacement = "'" + fv.getValueAsString().replace("'", "''") + "'";
                            break;
                    }
                    matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
                }
                // If variable not found, leave the placeholder as-is (will cause a Spark error
                // that tells the user which variable is missing)
            }
            matcher.appendTail(sb);
            result[i] = sb.toString();
        }
        return result;
    }

    /**
     * Traverses the workflow to find the {@link SparkDataPortObject} connected
     * to this node's input. For {@code DefaultSparkNodeFactory} nodes, internal
     * port 0 is flow variables and port 1+ are Spark context / user data ports.
     */
    private SparkDataPortObject findSparkInputPort() {
        try {
            final NodeContainer thisNC = m_nodeContext.getNodeContainer();
            if (!(thisNC instanceof NativeNodeContainer)) {
                return null;
            }
            final NativeNodeContainer nc = (NativeNodeContainer) thisNC;
            final WorkflowManager wfm = nc.getParent();
            final int numPorts = nc.getNrInPorts();

            for (int i = 1; i < numPorts; i++) {
                final ConnectionContainer cc = wfm.getIncomingConnectionFor(nc.getID(), i);
                if (cc == null) {
                    continue;
                }
                final NodeContainer sourceNC = wfm.getNodeContainer(cc.getSource());
                // getOutPort().getPortObject() works for both NativeNodeContainer
                // and WorkflowManager (metanodes/components)
                final var portObject = sourceNC.getOutPort(cc.getSourcePort()).getPortObject();
                if (portObject instanceof SparkDataPortObject) {
                    return (SparkDataPortObject) portObject;
                }
            }
        } catch (final Exception e) {
            // If we can't navigate the workflow, return null
        }
        return null;
    }

    private static String extractErrorMessage(final Exception e) {
        // Traverse the cause chain — Spark errors are often deeply nested
        Throwable current = e;
        String firstMsg = null;
        while (current != null) {
            final String msg = current.getMessage();
            if (msg != null && !msg.isBlank()) {
                if (firstMsg == null) {
                    firstMsg = msg;
                }
                // Prefer messages that mention the expression or column (more actionable)
                if (msg.contains("cannot resolve") || msg.contains("Column")
                        || msg.contains("AnalysisException") || msg.contains("expression")) {
                    return msg;
                }
            }
            current = current.getCause();
        }
        return firstMsg != null ? firstMsg : "Unknown error during expression evaluation.";
    }
}

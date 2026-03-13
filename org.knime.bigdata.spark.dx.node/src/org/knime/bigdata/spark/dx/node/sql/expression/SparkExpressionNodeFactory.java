package org.knime.bigdata.spark.dx.node.sql.expression;

import org.knime.bigdata.spark.core.node.DefaultSparkNodeFactory;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.webui.node.dialog.NodeDialog;
import org.knime.core.webui.node.dialog.NodeDialogFactory;
import org.knime.core.webui.node.dialog.NodeDialogManager;

/**
 * Node factory for the Spark Expression node. Applies multiple Spark SQL expressions
 * to transform or add columns in a Spark DataFrame.
 *
 * <p>Implements {@link NodeDialogFactory} to provide a modern WebUI dialog
 * that supports both embedded side-panel and enlarged full-screen modes.
 */
@SuppressWarnings("restriction")
public final class SparkExpressionNodeFactory extends DefaultSparkNodeFactory<SparkExpressionNodeModel>
    implements NodeDialogFactory {

    /** Default constructor. */
    public SparkExpressionNodeFactory() {
        super("sql");
    }

    @Override
    public SparkExpressionNodeModel createNodeModel() {
        return new SparkExpressionNodeModel();
    }

    @Override
    protected boolean hasDialog() {
        return true;
    }

    @Override
    protected NodeDialogPane createNodeDialogPane() {
        return NodeDialogManager.createLegacyFlowVariableNodeDialog(createNodeDialog());
    }

    @Override
    public NodeDialog createNodeDialog() {
        return new SparkExpressionWebNodeDialog();
    }
}

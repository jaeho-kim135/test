package org.knime.bigdata.spark.dx.node.sql.expression;

import org.knime.bigdata.spark.core.node.DefaultSparkNodeFactory;
import org.knime.core.node.NodeDialogPane;

/**
 * Node factory for the Spark Expression node. Applies multiple Spark SQL expressions
 * to transform or add columns in a Spark DataFrame.
 */
public final class SparkExpressionNodeFactory extends DefaultSparkNodeFactory<SparkExpressionNodeModel> {

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
        return new SparkExpressionNodeDialog();
    }
}

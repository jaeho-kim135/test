package org.knime.bigdata.spark.dx.node.sql.multiquery;

import org.knime.bigdata.spark.core.node.DefaultSparkNodeFactory;
import org.knime.core.node.NodeDialogPane;

/**
 * Node factory for the Spark Multi Query node. Applies a SQL expression to
 * multiple selected columns by replacing the $columnS placeholder.
 */
public final class SparkMultiQueryNodeFactory extends DefaultSparkNodeFactory<SparkMultiQueryNodeModel> {

    /** Default constructor. */
    public SparkMultiQueryNodeFactory() {
        super("sql");
    }

    @Override
    public SparkMultiQueryNodeModel createNodeModel() {
        return new SparkMultiQueryNodeModel();
    }

    @Override
    protected boolean hasDialog() {
        return true;
    }

    @Override
    protected NodeDialogPane createNodeDialogPane() {
        return new SparkMultiQueryNodeDialog();
    }
}

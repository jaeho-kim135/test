package org.knime.bigdata.spark.dx.node.preproc.unpivot;

import org.knime.bigdata.spark.core.node.DefaultSparkNodeFactory;
import org.knime.core.node.NodeDialogPane;

/**
 * Node factory for the Spark Unpivot node. Transforms wide format to long format
 * by converting selected value columns into variable/value rows.
 */
public final class SparkUnpivotNodeFactory extends DefaultSparkNodeFactory<SparkUnpivotNodeModel> {

    /** Default constructor. */
    public SparkUnpivotNodeFactory() {
        super("row");
    }

    @Override
    public SparkUnpivotNodeModel createNodeModel() {
        return new SparkUnpivotNodeModel();
    }

    @Override
    protected boolean hasDialog() {
        return true;
    }

    @Override
    protected NodeDialogPane createNodeDialogPane() {
        return new SparkUnpivotNodeDialog();
    }
}

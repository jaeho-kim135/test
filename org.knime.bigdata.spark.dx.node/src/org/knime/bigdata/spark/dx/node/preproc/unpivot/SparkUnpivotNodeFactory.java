package org.knime.bigdata.spark.dx.node.preproc.unpivot;

import org.knime.bigdata.spark.core.node.SparkNodeFactory;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.config.ConfigRO;
import org.knime.core.webui.node.impl.WebUINodeConfiguration;
import org.knime.core.webui.node.impl.WebUINodeFactory;

/**
 * Node factory for the Spark Unpivot node. Transforms wide format to long format
 * by converting selected value columns into variable/value rows.
 */
@SuppressWarnings("restriction")
public final class SparkUnpivotNodeFactory
    extends WebUINodeFactory<SparkUnpivotNodeModel>
    implements SparkNodeFactory<SparkUnpivotNodeModel> {

    private static final WebUINodeConfiguration CONFIGURATION = WebUINodeConfiguration.builder()
        .name("Spark Unpivot (Hyim)")
        .icon("icon.png")
        .shortDescription("Unpivots (melts) a Spark DataFrame from wide format to long format.")
        .fullDescription("""
            <p>Transforms a wide-format Spark DataFrame into a long-format DataFrame using the
            Spark Dataset.unpivot() API (requires Spark 3.4+).</p>
            <p>Select the columns to retain as identifier columns, and the columns to unpivot
            into rows. Each value column becomes a row with the column name in the variable column
            and the cell value in the value column.</p>
            """)
        .modelSettingsClass(SparkUnpivotNodeParameters.class)
        .addInputPort("Input Data", SparkDataPortObject.TYPE,
            "Spark DataFrame to unpivot.")
        .addOutputPort("Unpivoted Data", SparkDataPortObject.TYPE,
            "Unpivoted long-format Spark DataFrame.")
        .build();

    /** Default constructor. */
    public SparkUnpivotNodeFactory() {
        super(CONFIGURATION);
    }

    @Override
    public SparkUnpivotNodeModel createNodeModel() {
        return new SparkUnpivotNodeModel();
    }

    @Override
    public Class<? extends NodeFactory<SparkUnpivotNodeModel>> getNodeFactory() {
        return SparkUnpivotNodeFactory.class;
    }

    @Override
    public String getId() {
        return SparkUnpivotNodeFactory.class.getName();
    }

    @Override
    public String getCategoryPath() {
        return "row";
    }

    @Override
    public String getAfterID() {
        return "";
    }

    @Override
    public ConfigRO getAdditionalSettings() {
        return null;
    }
}

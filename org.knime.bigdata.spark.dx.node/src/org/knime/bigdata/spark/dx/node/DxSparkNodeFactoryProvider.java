package org.knime.bigdata.spark.dx.node;

import org.knime.bigdata.spark.core.node.DefaultSparkNodeFactoryProvider;
import org.knime.bigdata.spark.core.version.AllVersionCompatibilityChecker;
import org.knime.bigdata.spark.dx.node.preproc.unpivot.SparkUnpivotNodeFactory;
import org.knime.bigdata.spark.dx.node.sql.expression.SparkExpressionNodeFactory;
import org.knime.bigdata.spark.dx.node.sql.multiquery.SparkMultiQueryNodeFactory;

/**
 * Provides DX Spark node factories.
 */
public class DxSparkNodeFactoryProvider extends DefaultSparkNodeFactoryProvider {

    /**
     * Constructor.
     */
    public DxSparkNodeFactoryProvider() {
        super(AllVersionCompatibilityChecker.INSTANCE,
            new SparkUnpivotNodeFactory(),
            new SparkMultiQueryNodeFactory(),
            new SparkExpressionNodeFactory());
    }
}

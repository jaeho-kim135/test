package org.knime.bigdata.spark.dx.node;

import org.knime.bigdata.spark.core.node.DefaultSparkNodeFactoryProvider;
import org.knime.bigdata.spark.core.version.AllVersionCompatibilityChecker;
import org.knime.bigdata.spark.dx.node.preproc.unpivot.SparkUnpivotNodeFactory;

/**
 * Provides DX Spark node factories.
 */
public class DxSparkNodeFactoryProvider extends DefaultSparkNodeFactoryProvider {

    /**
     * Constructor.
     */
    public DxSparkNodeFactoryProvider() {
        super(AllVersionCompatibilityChecker.INSTANCE,
            new SparkUnpivotNodeFactory());
    }
}

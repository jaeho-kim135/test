package org.knime.bigdata.spark.dx.node;

import java.util.regex.Pattern;

import org.knime.bigdata.spark.core.jar.DefaultSparkJarProvider;
import org.knime.bigdata.spark.core.jar.KNIMEPluginScanPredicates;
import org.knime.bigdata.spark.core.version.AllVersionCompatibilityChecker;

/**
 * Jar provider for the DX Spark node plugin. Scans for @SparkClass annotated classes
 * (e.g. SparkUnpivotJobInput, SparkUnpivotJobOutput) so that they are included in the
 * job jar sent to the Spark executor.
 */
public class DxSparkJarProvider extends DefaultSparkJarProvider {

    /** Predicate matching the dx.node plugin's classpath entries. */
    private static final java.util.function.Predicate<String> DX_NODE_PLUGIN_PREDICATE =
        Pattern.compile("org\\.knime\\.bigdata\\.spark\\.dx\\.node").asPredicate();

    /** Constructor. */
    public DxSparkJarProvider() {
        super(AllVersionCompatibilityChecker.INSTANCE,
            KNIMEPluginScanPredicates.KNIME_JAR_PREDICATE,
            DX_NODE_PLUGIN_PREDICATE);
    }
}

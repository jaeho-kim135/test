package org.knime.bigdata.spark3_5.dx.jobs.preproc.unpivot;

import java.util.regex.Pattern;

import org.knime.bigdata.spark.core.jar.DefaultSparkJarProvider;
import org.knime.bigdata.spark.core.jar.KNIMEPluginScanPredicates;
import org.knime.bigdata.spark3_5.api.Spark_3_5_CompatibilityChecker;

/**
 * Jar provider for the DX Spark 3.5 job plugin. Scans for @SparkClass annotated classes
 * so that they are included in the job jar sent to the Spark executor.
 */
public class DxSpark35JarProvider extends DefaultSparkJarProvider {

    private static final java.util.function.Predicate<String> DX_SPARK35_PLUGIN_PREDICATE =
        Pattern.compile("org\\.knime\\.bigdata\\.spark3_5\\.dx").asPredicate();

    /** Constructor. */
    public DxSpark35JarProvider() {
        super(Spark_3_5_CompatibilityChecker.INSTANCE,
            KNIMEPluginScanPredicates.KNIME_JAR_PREDICATE,
            DX_SPARK35_PLUGIN_PREDICATE);
    }
}

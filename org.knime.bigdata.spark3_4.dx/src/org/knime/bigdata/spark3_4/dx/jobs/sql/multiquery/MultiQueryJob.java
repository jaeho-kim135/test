package org.knime.bigdata.spark3_4.dx.jobs.sql.multiquery;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.dx.node.sql.multiquery.SparkMultiQueryJobInput;
import org.knime.bigdata.spark.dx.node.sql.multiquery.SparkMultiQueryJobOutput;
import org.knime.bigdata.spark3_4.api.NamedObjects;
import org.knime.bigdata.spark3_4.api.SparkJob;
import org.knime.bigdata.spark3_4.api.TypeConverters;

/**
 * Spark job that applies a SQL expression template to multiple columns.
 * Supports keep-original mode and output column name pattern.
 */
@SparkClass
public class MultiQueryJob implements SparkJob<SparkMultiQueryJobInput, SparkMultiQueryJobOutput> {

    private static final long serialVersionUID = 1L;

    private static final String PLACEHOLDER = "$columnS";

    @Override
    public SparkMultiQueryJobOutput runJob(final SparkContext sparkContext, final SparkMultiQueryJobInput input,
            final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        final SparkSession spark = SparkSession.builder().sparkContext(sparkContext).getOrCreate();
        final String namedInputObject = input.getFirstNamedInputObject();
        final Dataset<Row> inputFrame = namedObjects.getDataFrame(namedInputObject);

        final String[] targetColumns = input.getTargetColumns();
        final String sqlExpression = input.getSqlExpression();
        final boolean validateOnly = input.isValidateOnly();

        final String tempTable = "multiQuery_" + UUID.randomUUID().toString().replace("-", "");

        try {
            inputFrame.createOrReplaceTempView(tempTable);

            if (validateOnly) {
                // Validation mode: test expression with ALL target columns using LIMIT 5
                final StringBuilder testSelect = new StringBuilder();
                for (int i = 0; i < targetColumns.length; i++) {
                    if (i > 0) {
                        testSelect.append(", ");
                    }
                    final String col = targetColumns[i];
                    final String expr = sqlExpression.replace(PLACEHOLDER, "`" + col + "`");
                    testSelect.append(expr).append(" AS `").append(col).append("`");
                }
                final String testQuery = "SELECT " + testSelect.toString()
                    + " FROM " + tempTable + " LIMIT 5";
                final Dataset<Row> testResult = spark.sql(testQuery);
                final String preview = testResult.showString(5, 20, false);
                final SparkMultiQueryJobOutput output = new SparkMultiQueryJobOutput(null, null);
                output.setPreviewData(preview);
                return output;
            }

            // Normal execution
            final String namedOutputObject = input.getFirstNamedOutputObject();
            final boolean keepOriginal = input.keepOriginal();
            final String outputPattern = input.getOutputPattern();
            final Set<String> targetSet = new HashSet<>(Arrays.asList(targetColumns));
            final String[] allColumns = inputFrame.columns();

            final StringBuilder selectClause = new StringBuilder();
            for (int i = 0; i < allColumns.length; i++) {
                if (selectClause.length() > 0) {
                    selectClause.append(", ");
                }

                final String col = allColumns[i];
                if (targetSet.contains(col)) {
                    if (keepOriginal) {
                        // Keep original column as-is, then add transformed column with alias
                        selectClause.append("`").append(col).append("`, ");
                    }
                    final String expr = sqlExpression.replace(PLACEHOLDER, "`" + col + "`");
                    final String alias = outputPattern.replace(PLACEHOLDER, col);
                    selectClause.append(expr).append(" AS `").append(alias).append("`");
                } else {
                    selectClause.append("`").append(col).append("`");
                }
            }

            final String query = "SELECT " + selectClause.toString() + " FROM " + tempTable;
            final Dataset<Row> result = spark.sql(query);

            namedObjects.addDataFrame(namedOutputObject, result);
            final IntermediateSpec outputSchema = TypeConverters.convertSpec(result.schema());
            return new SparkMultiQueryJobOutput(namedOutputObject, outputSchema);

        } finally {
            spark.catalog().dropTempView(tempTable);
        }
    }
}

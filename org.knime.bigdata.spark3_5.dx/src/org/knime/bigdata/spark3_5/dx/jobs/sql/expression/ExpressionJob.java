package org.knime.bigdata.spark3_5.dx.jobs.sql.expression;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.dx.node.sql.expression.SparkExpressionJobInput;
import org.knime.bigdata.spark.dx.node.sql.expression.SparkExpressionJobOutput;
import org.knime.bigdata.spark3_5.api.NamedObjects;
import org.knime.bigdata.spark3_5.api.SparkJob;
import org.knime.bigdata.spark3_5.api.TypeConverters;

import static org.apache.spark.sql.functions.expr;

/**
 * Spark job that applies multiple SQL expressions to transform/add columns.
 * Uses Spark's {@code withColumn(name, expr(sql))} for each expression.
 */
@SparkClass
public class ExpressionJob implements SparkJob<SparkExpressionJobInput, SparkExpressionJobOutput> {

    private static final long serialVersionUID = 1L;

    @Override
    public SparkExpressionJobOutput runJob(final SparkContext sparkContext, final SparkExpressionJobInput input,
            final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        final SparkSession spark = SparkSession.builder().sparkContext(sparkContext).getOrCreate();
        final String namedInputObject = input.getFirstNamedInputObject();
        final Dataset<Row> inputFrame = namedObjects.getDataFrame(namedInputObject);

        final String[] expressions = input.getExpressions();
        final String[] outputModes = input.getOutputModes();
        final String[] columnNames = input.getColumnNames();
        final boolean validateOnly = input.isValidateOnly();

        if (validateOnly) {
            return runValidation(inputFrame, expressions, outputModes, columnNames);
        }

        return runExecution(input, namedObjects, inputFrame, expressions, outputModes, columnNames);
    }

    private SparkExpressionJobOutput runValidation(final Dataset<Row> inputFrame,
            final String[] expressions, final String[] outputModes, final String[] columnNames)
            throws KNIMESparkException {

        // Apply expressions sequentially and validate each one
        Dataset<Row> result = inputFrame;
        for (int i = 0; i < expressions.length; i++) {
            try {
                result = result.withColumn(columnNames[i], expr(expressions[i]));
            } catch (final Exception e) {
                throw new KNIMESparkException(
                    "Expression " + (i + 1) + " error: " + e.getMessage(), e);
            }
        }

        // Run LIMIT 5 to validate and generate preview
        final String preview = result.showString(5, 20, false);
        final SparkExpressionJobOutput output = new SparkExpressionJobOutput(null, null);
        output.setPreviewData(preview);
        return output;
    }

    private SparkExpressionJobOutput runExecution(final SparkExpressionJobInput input,
            final NamedObjects namedObjects, final Dataset<Row> inputFrame,
            final String[] expressions, final String[] outputModes, final String[] columnNames)
            throws KNIMESparkException {

        final String namedOutputObject = input.getFirstNamedOutputObject();

        // Apply each expression sequentially
        Dataset<Row> result = inputFrame;
        for (int i = 0; i < expressions.length; i++) {
            try {
                result = result.withColumn(columnNames[i], expr(expressions[i]));
            } catch (final Exception e) {
                throw new KNIMESparkException(
                    "Expression " + (i + 1) + " (" + expressions[i] + ") failed: " + e.getMessage(), e);
            }
        }

        namedObjects.addDataFrame(namedOutputObject, result);
        final IntermediateSpec outputSchema = TypeConverters.convertSpec(result.schema());
        return new SparkExpressionJobOutput(namedOutputObject, outputSchema);
    }
}

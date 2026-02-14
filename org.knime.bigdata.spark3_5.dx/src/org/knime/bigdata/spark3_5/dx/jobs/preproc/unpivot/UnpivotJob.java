package org.knime.bigdata.spark3_5.dx.jobs.preproc.unpivot;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.dx.node.preproc.unpivot.SparkUnpivotJobInput;
import org.knime.bigdata.spark.dx.node.preproc.unpivot.SparkUnpivotJobOutput;
import org.knime.bigdata.spark3_5.api.NamedObjects;
import org.knime.bigdata.spark3_5.api.SparkJob;
import org.knime.bigdata.spark3_5.api.TypeConverters;

import static org.apache.spark.sql.functions.col;

/**
 * Spark job that performs the unpivot (melt) operation using the DataFrame unpivot() API.
 *
 * <p>Transforms wide format to long format:
 * <pre>
 * | ID | Q1 | Q2 | Q3 |  →  | ID | variable | value |
 * </pre>
 */
@SparkClass
public class UnpivotJob implements SparkJob<SparkUnpivotJobInput, SparkUnpivotJobOutput> {

    private static final long serialVersionUID = 1L;

    @Override
    public SparkUnpivotJobOutput runJob(final SparkContext sparkContext, final SparkUnpivotJobInput input,
            final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        final String namedInputObject = input.getFirstNamedInputObject();
        final String namedOutputObject = input.getFirstNamedOutputObject();
        final Dataset<Row> inputFrame = namedObjects.getDataFrame(namedInputObject);

        final String[] retainedColumns = input.getRetainedColumns();
        final String[] valueColumns = input.getValueColumns();
        final String variableColName = input.getVariableColName();
        final String valueColName = input.getValueColName();
        final boolean skipMissing = input.skipMissingValues();

        if (valueColumns.length == 0) {
            throw new KNIMESparkException("No value columns specified for unpivoting.");
        }

        // Build Column arrays for the unpivot() API
        final Column[] idCols = new Column[retainedColumns.length];
        for (int i = 0; i < retainedColumns.length; i++) {
            idCols[i] = col(retainedColumns[i]);
        }

        final Column[] valCols = new Column[valueColumns.length];
        for (int i = 0; i < valueColumns.length; i++) {
            valCols[i] = col(valueColumns[i]);
        }

        // Perform unpivot using DataFrame API (available since Spark 3.4)
        Dataset<Row> result = inputFrame.unpivot(idCols, valCols, variableColName, valueColName);

        // Filter out null values if requested
        if (skipMissing) {
            result = result.filter(col(valueColName).isNotNull());
        }

        namedObjects.addDataFrame(namedOutputObject, result);
        final IntermediateSpec outputSchema = TypeConverters.convertSpec(result.schema());
        return new SparkUnpivotJobOutput(namedOutputObject, outputSchema);
    }
}

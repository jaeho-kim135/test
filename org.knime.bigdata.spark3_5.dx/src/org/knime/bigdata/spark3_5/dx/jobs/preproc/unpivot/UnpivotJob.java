package org.knime.bigdata.spark3_5.dx.jobs.preproc.unpivot;

import java.util.HashMap;
import java.util.Map;

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

import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.when;
import static org.apache.spark.sql.functions.lit;

/**
 * Spark job that performs the unpivot (melt) operation.
 * Supports validation mode, sort options, and variable value mapping.
 */
@SparkClass
public class UnpivotJob implements SparkJob<SparkUnpivotJobInput, SparkUnpivotJobOutput> {

    private static final long serialVersionUID = 1L;

    @Override
    public SparkUnpivotJobOutput runJob(final SparkContext sparkContext, final SparkUnpivotJobInput input,
            final NamedObjects namedObjects) throws KNIMESparkException, Exception {

        final String namedInputObject = input.getFirstNamedInputObject();
        final Dataset<Row> inputFrame = namedObjects.getDataFrame(namedInputObject);

        final String[] retainedColumns = input.getRetainedColumns();
        final String[] valueColumns = input.getValueColumns();
        final String variableColName = input.getVariableColName();
        final String valueColName = input.getValueColName();
        final boolean skipMissing = input.skipMissingValues();
        final boolean castToString = input.castToString();
        final boolean validateOnly = input.isValidateOnly();
        final String sortOption = input.getSortOption();

        // Build variable value map
        final Map<String, String> varMap = new HashMap<>();
        final String[] mapKeys = input.getVarMapKeys();
        final String[] mapVals = input.getVarMapValues();
        for (int i = 0; i < Math.min(mapKeys.length, mapVals.length); i++) {
            if (mapVals[i] != null && !mapVals[i].isEmpty()) {
                varMap.put(mapKeys[i], mapVals[i]);
            }
        }

        if (valueColumns.length == 0) {
            throw new KNIMESparkException("No value columns specified for unpivoting.");
        }

        // Cast value columns to String if requested
        Dataset<Row> sourceFrame = inputFrame;
        if (castToString) {
            for (String valCol : valueColumns) {
                sourceFrame = sourceFrame.withColumn(valCol, col(valCol).cast(DataTypes.StringType));
            }
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

        // Perform unpivot
        Dataset<Row> result = sourceFrame.unpivot(idCols, valCols, variableColName, valueColName);

        // Apply variable value mapping (rename variable values)
        if (!varMap.isEmpty()) {
            Column mappingExpr = col(variableColName);
            for (Map.Entry<String, String> entry : varMap.entrySet()) {
                mappingExpr = when(col(variableColName).equalTo(entry.getKey()), lit(entry.getValue()))
                    .otherwise(mappingExpr);
            }
            result = result.withColumn(variableColName, mappingExpr);
        }

        // Filter out null values if requested
        if (skipMissing) {
            result = result.filter(col(valueColName).isNotNull());
        }

        // Apply sort
        if ("retained".equals(sortOption) && retainedColumns.length > 0) {
            final Column[] sortCols = new Column[retainedColumns.length];
            for (int i = 0; i < retainedColumns.length; i++) {
                sortCols[i] = col(retainedColumns[i]);
            }
            result = result.orderBy(sortCols);
        } else if ("variable".equals(sortOption)) {
            result = result.orderBy(col(variableColName));
        }

        if (validateOnly) {
            final long inputCount = inputFrame.count();
            final SparkUnpivotJobOutput output = new SparkUnpivotJobOutput(null, null);
            output.setInputRowCount(inputCount);
            try {
                final String preview = result.showString(5, 20, false);
                output.setPreviewData(preview);
            } catch (final Exception e) {
                output.setPreviewData("Preview failed: " + e.getMessage());
            }
            return output;
        }

        final String namedOutputObject = input.getFirstNamedOutputObject();
        namedObjects.addDataFrame(namedOutputObject, result);
        final IntermediateSpec outputSchema = TypeConverters.convertSpec(result.schema());
        return new SparkUnpivotJobOutput(namedOutputObject, outputSchema);
    }
}

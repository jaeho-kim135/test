package org.knime.bigdata.spark3_4.dx.jobs.preproc.unpivot;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.dx.node.preproc.unpivot.SparkUnpivotJobInput;
import org.knime.bigdata.spark.dx.node.preproc.unpivot.SparkUnpivotJobOutput;
import org.knime.bigdata.spark3_4.api.NamedObjects;
import org.knime.bigdata.spark3_4.api.SparkJob;
import org.knime.bigdata.spark3_4.api.TypeConverters;

import static org.apache.spark.sql.functions.col;

/**
 * Spark job that performs the unpivot (melt) operation using stack().
 *
 * <pre>
 * stack(N, 'col1', `col1`, 'col2', `col2`, ...) as (`variable`, `value`)
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

        // Build the stack() expression:
        // stack(N, 'col1', `col1`, 'col2', `col2`, ...) as (`variable`, `value`)
        final StringBuilder stackExpr = new StringBuilder();
        stackExpr.append("stack(").append(valueColumns.length);
        for (String colName : valueColumns) {
            stackExpr.append(", '").append(escapeQuote(colName)).append("', `")
                     .append(escapeBacktick(colName)).append('`');
        }
        stackExpr.append(") as (`").append(escapeBacktick(variableColName))
                 .append("`, `").append(escapeBacktick(valueColName)).append("`)");

        // Build select expressions: retained columns + stack expression
        final List<String> selectExprs = new ArrayList<>(retainedColumns.length + 1);
        for (String col : retainedColumns) {
            selectExprs.add("`" + escapeBacktick(col) + "`");
        }
        selectExprs.add(stackExpr.toString());

        Dataset<Row> result = inputFrame.selectExpr(selectExprs.toArray(new String[0]));

        // Filter out null values if requested
        if (skipMissing) {
            result = result.filter(col(valueColName).isNotNull());
        }

        namedObjects.addDataFrame(namedOutputObject, result);
        final IntermediateSpec outputSchema = TypeConverters.convertSpec(result.schema());
        return new SparkUnpivotJobOutput(namedOutputObject, outputSchema);
    }

    private static String escapeQuote(final String s) {
        return s.replace("'", "\\'");
    }

    private static String escapeBacktick(final String s) {
        return s.replace("`", "``");
    }
}

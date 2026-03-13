package org.knime.bigdata.spark.dx.node.sql.expression;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.node.workflow.NodeContext;
import org.knime.core.webui.data.RpcDataService;
import org.knime.core.webui.node.dialog.NodeDialog;
import org.knime.core.webui.node.dialog.NodeSettingsService;
import org.knime.core.webui.node.dialog.SettingsType;
import org.knime.core.webui.page.Page;
import org.knime.scripting.editor.GenericInitialDataBuilder;
import org.knime.scripting.editor.ScriptingNodeSettingsService;
import org.knime.scripting.editor.WorkflowControl;

/**
 * WebUI dialog for the Spark Expression node.
 * Provides a modern HTML-based dialog with Monaco editor, input columns panel,
 * function catalog, and output preview — matching KNIME's Expression node UX.
 *
 * <p>Supports both compact side-panel mode and enlarged full-screen mode.
 *
 * <p>Backend communication uses {@link SparkExpressionRpcService} to validate
 * expressions on the Spark cluster via the standard job framework.
 */
@SuppressWarnings("restriction")
final class SparkExpressionWebNodeDialog implements NodeDialog {

    @Override
    public Page getPage() {
        return Page.create()
            .fromFile()
            .bundleClass(SparkExpressionWebNodeDialog.class)
            .basePath("js-src/dist")
            .relativeFilePath("spark-expression.html")
            .addResourceDirectory("assets");
    }

    @Override
    public Set<SettingsType> getSettingsTypes() {
        return Set.of(SettingsType.MODEL);
    }

    @Override
    public NodeSettingsService getNodeSettingsService() {
        final var workflowControl = new WorkflowControl(NodeContext.getContext().getNodeContainer());

        final var initialDataBuilder = GenericInitialDataBuilder
            .createDefaultInitialDataBuilder(NodeContext.getContext())
            .addDataSupplier("columnNamesAndTypes", () -> getColumnInfo(workflowControl))
            .addDataSupplier("flowVariables", () -> getFlowVariableInfo(workflowControl))
            .addDataSupplier("functionCatalog", SparkExpressionWebNodeDialog::getFunctionCatalog);

        return new ScriptingNodeSettingsService(
            SparkExpressionWebSettings::new,
            initialDataBuilder
        );
    }

    @Override
    public Optional<RpcDataService> createRpcDataService() {
        final var rpcService = new SparkExpressionRpcService(NodeContext.getContext());

        return Optional.of(RpcDataService.builder()
            .addService("SparkExpressionService", rpcService)
            .build());
    }

    @Override
    public boolean canBeEnlarged() {
        return true;
    }

    // ── Initial data suppliers ──────────────────────────────────────────────

    /**
     * Returns the column names and types of the first input table.
     * Each entry is a map with "name" and "type" keys.
     */
    private static Object getColumnInfo(final WorkflowControl workflowControl) {
        try {
            final var inputInfo = workflowControl.getInputInfo();
            if (inputInfo == null || inputInfo.length == 0) {
                return List.of();
            }

            // DefaultSparkNodeFactory prepends a hidden Spark context port before user ports,
            // so the SparkDataPortObjectSpec may not be at index 0. Search all ports.
            for (final var info : inputInfo) {
                if (info == null) {
                    continue;
                }
                final var portSpec = info.portSpec();
                if (portSpec instanceof SparkDataPortObjectSpec) {
                    return buildColumnList(((SparkDataPortObjectSpec) portSpec).getTableSpec());
                }
            }
            // Fallback: try any port that provides a DataTableSpec
            for (final var info : inputInfo) {
                if (info == null) {
                    continue;
                }
                final var portSpec = info.portSpec();
                if (portSpec instanceof DataTableSpec) {
                    return buildColumnList((DataTableSpec) portSpec);
                }
            }
        } catch (final Exception e) {
            // Return empty list on error
        }
        return List.of();
    }

    private static List<Map<String, String>> buildColumnList(final DataTableSpec spec) {
        final List<Map<String, String>> columns = new ArrayList<>();
        for (int i = 0; i < spec.getNumColumns(); i++) {
            final DataColumnSpec colSpec = spec.getColumnSpec(i);
            final Map<String, String> col = new LinkedHashMap<>();
            col.put("name", colSpec.getName());
            col.put("type", colSpec.getType().getName());
            columns.add(col);
        }
        return columns;
    }

    /**
     * Returns the available flow variables (excluding global constants).
     * Each entry is a map with "name" and "type" keys.
     */
    @SuppressWarnings("deprecation")
    private static Object getFlowVariableInfo(final WorkflowControl workflowControl) {
        try {
            final var stack = workflowControl.getFlowObjectStack();
            if (stack == null) {
                return List.of();
            }
            final var variables = stack.getAllAvailableFlowVariables();
            final List<Map<String, String>> result = new ArrayList<>();
            for (final var entry : variables.entrySet()) {
                final FlowVariable fv = entry.getValue();
                if (fv.isGlobalConstant()) {
                    continue;
                }
                final Map<String, String> varInfo = new LinkedHashMap<>();
                varInfo.put("name", fv.getName());
                varInfo.put("type", fv.getType().name());
                result.add(varInfo);
            }
            return result;
        } catch (final Exception e) {
            return List.of();
        }
    }

    /**
     * Returns the Spark SQL function catalog organized by category.
     * Comprehensive list of functions compatible with Spark 3.4+ {@code expr()}.
     */
    private static List<Map<String, Object>> getFunctionCatalog() {
        final List<Map<String, Object>> catalog = new ArrayList<>();

        // ── String ──────────────────────────────────────────────────────
        catalog.add(buildCategory("String",
            fn("UPPER(col)", "UPPER()", "Converts a string to uppercase."),
            fn("LOWER(col)", "LOWER()", "Converts a string to lowercase."),
            fn("TRIM(col)", "TRIM()", "Removes leading and trailing whitespace."),
            fn("LTRIM(col)", "LTRIM()", "Removes leading whitespace."),
            fn("RTRIM(col)", "RTRIM()", "Removes trailing whitespace."),
            fn("LENGTH(col)", "LENGTH()", "Returns the character length of a string."),
            fn("CONCAT(a, b, ...)", "CONCAT(, )", "Concatenates multiple strings together."),
            fn("CONCAT_WS(sep, a, b, ...)", "CONCAT_WS(',', , )", "Concatenates strings with a separator."),
            fn("SUBSTRING(col, pos, len)", "SUBSTRING(, 1, )", "Extracts a substring starting at pos for len characters."),
            fn("SUBSTRING_INDEX(col, delim, cnt)", "SUBSTRING_INDEX(, ',', 1)", "Returns the substring before the nth occurrence of a delimiter."),
            fn("[3.5+] LEFT(col, n)", "LEFT(, 5)", "Returns the leftmost n characters of a string."),
            fn("[3.5+] RIGHT(col, n)", "RIGHT(, 5)", "Returns the rightmost n characters of a string."),
            fn("REPLACE(col, old, new)", "REPLACE(, '', '')", "Replaces all occurrences of a substring with another."),
            fn("TRANSLATE(col, from, to)", "TRANSLATE(, 'abc', 'xyz')", "Translates characters one-to-one based on the mapping."),
            fn("OVERLAY(col PLACING rep FROM pos)", "OVERLAY( PLACING '' FROM 1)", "Replaces a portion of a string with another string at a given position."),
            fn("LPAD(col, len, pad)", "LPAD(, 10, '0')", "Left-pads a string to the specified length with a pad string."),
            fn("RPAD(col, len, pad)", "RPAD(, 10, ' ')", "Right-pads a string to the specified length with a pad string."),
            fn("SPLIT(col, delim)", "SPLIT(, ',')", "Splits a string by a delimiter and returns an array."),
            fn("[3.5+] SPLIT_PART(col, delim, idx)", "SPLIT_PART(, ',', 1)", "Splits a string by a delimiter and returns the specified part."),
            fn("INITCAP(col)", "INITCAP()", "Capitalizes the first letter of each word."),
            fn("REVERSE(col)", "REVERSE()", "Reverses a string."),
            fn("REPEAT(col, n)", "REPEAT(, 2)", "Repeats a string n times."),
            fn("SPACE(n)", "SPACE(10)", "Returns a string of n spaces."),
            fn("INSTR(col, substr)", "INSTR(, '')", "Returns the 1-based position of the first occurrence of a substring."),
            fn("LOCATE(substr, col, pos)", "LOCATE('', , 1)", "Returns the position of a substring starting from the given position."),
            fn("[3.5+] CONTAINS(col, substr)", "CONTAINS(, '')", "Returns true if the string contains the substring."),
            fn("[3.5+] STARTSWITH(col, prefix)", "STARTSWITH(, '')", "Returns true if the string starts with the prefix."),
            fn("[3.5+] ENDSWITH(col, suffix)", "ENDSWITH(, '')", "Returns true if the string ends with the suffix."),
            fn("ASCII(col)", "ASCII()", "Returns the ASCII code of the first character."),
            fn("CHR(code)", "CHR(65)", "Returns the character for the given ASCII code."),
            fn("SOUNDEX(col)", "SOUNDEX()", "Returns the Soundex code for a string, useful for phonetic matching."),
            fn("LEVENSHTEIN(a, b)", "LEVENSHTEIN(, '')", "Returns the edit distance between two strings."),
            fn("FORMAT_STRING(fmt, ...)", "FORMAT_STRING('%s', )", "Formats arguments using a printf-style format string."),
            fn("FORMAT_NUMBER(col, d)", "FORMAT_NUMBER(, 2)", "Formats a number with the specified number of decimal places.")
        ));

        // ── Regular Expression ──────────────────────────────────────────
        catalog.add(buildCategory("Regular Expression",
            fn("REGEXP_REPLACE(col, pat, rep)", "REGEXP_REPLACE(, '', '')", "Replaces all substrings matching a regex pattern."),
            fn("REGEXP_EXTRACT(col, pat, grp)", "REGEXP_EXTRACT(, '(.*)', 1)", "Extracts the first match of a regex capture group."),
            fn("[3.5+] REGEXP_EXTRACT_ALL(col, pat [, grp])", "REGEXP_EXTRACT_ALL(, '(\\\\w+)')", "Extracts all substrings matching a regex pattern as an array."),
            fn("[3.5+] REGEXP_COUNT(col, pat)", "REGEXP_COUNT(, '')", "Returns the number of times a regex pattern matches."),
            fn("[3.5+] REGEXP_LIKE(col, pat)", "REGEXP_LIKE(, '')", "Returns true if the string matches the regex pattern."),
            fn("[3.5+] REGEXP_SUBSTR(col, pat)", "REGEXP_SUBSTR(, '')", "Returns the first substring matching the regex pattern."),
            fn("col RLIKE pattern", " RLIKE ''", "Tests if a string matches a regex pattern (returns boolean)."),
            fn("col LIKE pattern", " LIKE '%pattern%'", "Tests if a string matches a SQL LIKE pattern with % and _ wildcards.")
        ));

        // ── Math ────────────────────────────────────────────────────────
        catalog.add(buildCategory("Math",
            fn("ABS(col)", "ABS()", "Returns the absolute value."),
            fn("ROUND(col, scale)", "ROUND(, 2)", "Rounds a number to the specified decimal places."),
            fn("BROUND(col, scale)", "BROUND(, 2)", "Rounds a number using banker's rounding (half-even)."),
            fn("CEIL(col)", "CEIL()", "Rounds a number up to the nearest integer."),
            fn("FLOOR(col)", "FLOOR()", "Rounds a number down to the nearest integer."),
            fn("SQRT(col)", "SQRT()", "Returns the square root."),
            fn("CBRT(col)", "CBRT()", "Returns the cube root."),
            fn("POW(base, exp)", "POW(, 2)", "Raises the base to the power of the exponent."),
            fn("EXP(col)", "EXP()", "Returns e raised to the given power."),
            fn("LN(col)", "LN()", "Returns the natural logarithm (base e)."),
            fn("LOG(base, col)", "LOG(10, )", "Returns the logarithm with the specified base."),
            fn("LOG2(col)", "LOG2()", "Returns the base-2 logarithm."),
            fn("LOG10(col)", "LOG10()", "Returns the base-10 logarithm."),
            fn("MOD(a, b)", "MOD(, )", "Returns the remainder after division."),
            fn("SIGN(col)", "SIGN()", "Returns -1, 0, or 1 based on the sign of the value."),
            fn("FACTORIAL(n)", "FACTORIAL()", "Returns the factorial of a non-negative integer."),
            fn("GREATEST(a, b, ...)", "GREATEST(, )", "Returns the greatest value among the arguments."),
            fn("LEAST(a, b, ...)", "LEAST(, )", "Returns the smallest value among the arguments."),
            fn("RAND()", "RAND()", "Returns a random value between 0.0 and 1.0 (uniform distribution)."),
            fn("RANDN()", "RANDN()", "Returns a random value from a standard normal distribution."),
            fn("PI()", "PI()", "Returns the constant pi."),
            fn("E()", "E()", "Returns Euler's number (the base of natural logarithm)."),
            fn("DEGREES(col)", "DEGREES()", "Converts radians to degrees."),
            fn("RADIANS(col)", "RADIANS()", "Converts degrees to radians."),
            fn("CONV(num, fromBase, toBase)", "CONV(, 10, 16)", "Converts a number string between numeral bases."),
            fn("HEX(col)", "HEX()", "Converts a number or string to hexadecimal."),
            fn("UNHEX(col)", "UNHEX()", "Converts a hexadecimal string to binary."),
            fn("BIN(col)", "BIN()", "Returns the binary representation of a number as a string.")
        ));

        // ── Trigonometric ───────────────────────────────────────────────
        catalog.add(buildCategory("Trigonometric",
            fn("SIN(col)", "SIN()", "Returns the sine of an angle in radians."),
            fn("COS(col)", "COS()", "Returns the cosine of an angle in radians."),
            fn("TAN(col)", "TAN()", "Returns the tangent of an angle in radians."),
            fn("ASIN(col)", "ASIN()", "Returns the arc sine (inverse sine) in radians."),
            fn("ACOS(col)", "ACOS()", "Returns the arc cosine (inverse cosine) in radians."),
            fn("ATAN(col)", "ATAN()", "Returns the arc tangent (inverse tangent) in radians."),
            fn("ATAN2(y, x)", "ATAN2(, )", "Returns the angle in radians between the positive x-axis and the point (x, y)."),
            fn("SINH(col)", "SINH()", "Returns the hyperbolic sine."),
            fn("COSH(col)", "COSH()", "Returns the hyperbolic cosine."),
            fn("TANH(col)", "TANH()", "Returns the hyperbolic tangent."),
            fn("COT(col)", "COT()", "Returns the cotangent of an angle in radians."),
            fn("HYPOT(a, b)", "HYPOT(, )", "Returns the hypotenuse: sqrt(a^2 + b^2).")
        ));

        // ── Date/Time ───────────────────────────────────────────────────
        catalog.add(buildCategory("Date/Time",
            fn("CURRENT_DATE()", "CURRENT_DATE()", "Returns the current date."),
            fn("CURRENT_TIMESTAMP()", "CURRENT_TIMESTAMP()", "Returns the current timestamp."),
            fn("[3.5+] NOW()", "NOW()", "Returns the current timestamp (alias for CURRENT_TIMESTAMP)."),
            fn("TO_DATE(col, fmt)", "TO_DATE(, 'yyyy-MM-dd')", "Parses a string to a date using the given format."),
            fn("TO_TIMESTAMP(col, fmt)", "TO_TIMESTAMP(, 'yyyy-MM-dd HH:mm:ss')", "Parses a string to a timestamp using the given format."),
            fn("DATE_FORMAT(col, fmt)", "DATE_FORMAT(, 'yyyy-MM-dd')", "Formats a date/timestamp as a string using the given pattern."),
            fn("DATEDIFF(end, start)", "DATEDIFF(, )", "Returns the number of days between two dates."),
            fn("DATE_ADD(col, days)", "DATE_ADD(, 1)", "Adds the specified number of days to a date."),
            fn("DATE_SUB(col, days)", "DATE_SUB(, 1)", "Subtracts the specified number of days from a date."),
            fn("ADD_MONTHS(col, months)", "ADD_MONTHS(, 1)", "Adds the specified number of months to a date."),
            fn("MONTHS_BETWEEN(end, start)", "MONTHS_BETWEEN(, )", "Returns the number of months between two dates."),
            fn("LAST_DAY(col)", "LAST_DAY()", "Returns the last day of the month for a given date."),
            fn("NEXT_DAY(col, dayOfWeek)", "NEXT_DAY(, 'Monday')", "Returns the next occurrence of the specified day of week after the date."),
            fn("TRUNC(col, fmt)", "TRUNC(, 'MM')", "Truncates a date to the specified unit (e.g., 'MM' for month)."),
            fn("DATE_TRUNC(fmt, col)", "DATE_TRUNC('month', )", "Truncates a timestamp to the specified unit."),
            fn("YEAR(col)", "YEAR()", "Extracts the year from a date or timestamp."),
            fn("QUARTER(col)", "QUARTER()", "Extracts the quarter (1-4) from a date or timestamp."),
            fn("MONTH(col)", "MONTH()", "Extracts the month (1-12) from a date or timestamp."),
            fn("WEEKOFYEAR(col)", "WEEKOFYEAR()", "Extracts the week number of the year."),
            fn("DAYOFWEEK(col)", "DAYOFWEEK()", "Returns the day of the week (1=Sunday, 7=Saturday)."),
            fn("DAYOFMONTH(col)", "DAYOFMONTH()", "Extracts the day of the month (1-31)."),
            fn("DAYOFYEAR(col)", "DAYOFYEAR()", "Extracts the day of the year (1-366)."),
            fn("HOUR(col)", "HOUR()", "Extracts the hour (0-23) from a timestamp."),
            fn("MINUTE(col)", "MINUTE()", "Extracts the minute (0-59) from a timestamp."),
            fn("SECOND(col)", "SECOND()", "Extracts the second (0-59) from a timestamp."),
            fn("UNIX_TIMESTAMP(col)", "UNIX_TIMESTAMP()", "Converts a date/timestamp to Unix epoch seconds."),
            fn("FROM_UNIXTIME(col, fmt)", "FROM_UNIXTIME(, 'yyyy-MM-dd HH:mm:ss')", "Converts Unix epoch seconds to a formatted date string."),
            fn("FROM_UTC_TIMESTAMP(col, tz)", "FROM_UTC_TIMESTAMP(, 'Asia/Seoul')", "Converts a UTC timestamp to the specified time zone."),
            fn("TO_UTC_TIMESTAMP(col, tz)", "TO_UTC_TIMESTAMP(, 'Asia/Seoul')", "Converts a timestamp from the specified time zone to UTC."),
            fn("MAKE_DATE(y, m, d)", "MAKE_DATE(2024, 1, 1)", "Creates a date from year, month, and day components."),
            fn("MAKE_TIMESTAMP(y,m,d,h,mi,s)", "MAKE_TIMESTAMP(2024, 1, 1, 0, 0, 0)", "Creates a timestamp from individual date and time components.")
        ));

        // ── Null Handling ───────────────────────────────────────────────
        catalog.add(buildCategory("Null Handling",
            fn("COALESCE(a, b, ...)", "COALESCE(, )", "Returns the first non-null argument."),
            fn("IFNULL(col, default)", "IFNULL(, '')", "Returns the default value if the column is null."),
            fn("NVL(col, default)", "NVL(, '')", "Returns the default value if the column is null (same as IFNULL)."),
            fn("NVL2(col, notNull, isNull)", "NVL2(, , )", "Returns notNull if col is not null, otherwise returns isNull."),
            fn("NULLIF(a, b)", "NULLIF(, )", "Returns null if a equals b, otherwise returns a."),
            fn("ISNULL(col)", "ISNULL()", "Returns true if the value is null."),
            fn("ISNOTNULL(col)", "ISNOTNULL()", "Returns true if the value is not null."),
            fn("ISNAN(col)", "ISNAN()", "Returns true if the value is NaN (not a number)."),
            fn("NANVL(col, default)", "NANVL(, 0)", "Returns the default value if the column is NaN.")
        ));

        // ── Type Cast ───────────────────────────────────────────────────
        catalog.add(buildCategory("Type Cast",
            fn("CAST(col AS type)", "CAST( AS STRING)", "Converts a value to the specified data type."),
            fn("STRING(col)", "STRING()", "Casts a value to string type."),
            fn("INT(col)", "INT()", "Casts a value to integer type."),
            fn("BIGINT(col)", "BIGINT()", "Casts a value to long (64-bit integer) type."),
            fn("SMALLINT(col)", "SMALLINT()", "Casts a value to short (16-bit integer) type."),
            fn("FLOAT(col)", "FLOAT()", "Casts a value to float (32-bit) type."),
            fn("DOUBLE(col)", "DOUBLE()", "Casts a value to double (64-bit) type."),
            fn("DECIMAL(col)", "DECIMAL()", "Casts a value to decimal type."),
            fn("BOOLEAN(col)", "BOOLEAN()", "Casts a value to boolean type."),
            fn("DATE(col)", "DATE()", "Casts a value to date type."),
            fn("TIMESTAMP(col)", "TIMESTAMP()", "Casts a value to timestamp type.")
        ));

        // ── Conditional ─────────────────────────────────────────────────
        catalog.add(buildCategory("Conditional",
            fn("CASE WHEN ... THEN ... END", "CASE WHEN  THEN  ELSE  END", "Evaluates conditions in order and returns the matching result."),
            fn("IF(cond, trueVal, falseVal)", "IF(, , )", "Returns trueVal if the condition is true, otherwise falseVal."),
            fn("col BETWEEN low AND high", " BETWEEN  AND ", "Tests if a value is within an inclusive range."),
            fn("col IN (a, b, ...)", " IN (, )", "Tests if a value matches any value in the list."),
            fn("TYPEOF(col)", "TYPEOF()", "Returns the data type of the column as a string."),
            fn("ASSERT_TRUE(cond, msg)", "ASSERT_TRUE(, 'error')", "Throws an error with the given message if the condition is false.")
        ));

        // ── Aggregate (require OVER clause with withColumn) ─────────────
        catalog.add(buildCategory("Aggregate (Window)",
            fn("COUNT(col) OVER(...)", "COUNT() OVER (PARTITION BY )", "Counts the number of rows per partition."),
            fn("COUNT_IF(cond) OVER(...)", "COUNT_IF( > 0) OVER (PARTITION BY )", "Counts the rows matching a condition per partition."),
            fn("SUM(col) OVER(...)", "SUM() OVER (PARTITION BY )", "Computes the sum per partition."),
            fn("AVG(col) OVER(...)", "AVG() OVER (PARTITION BY )", "Computes the average per partition."),
            fn("MIN(col) OVER(...)", "MIN() OVER (PARTITION BY )", "Returns the minimum value per partition."),
            fn("MAX(col) OVER(...)", "MAX() OVER (PARTITION BY )", "Returns the maximum value per partition."),
            fn("FIRST(col) OVER(...)", "FIRST() OVER (PARTITION BY  ORDER BY )", "Returns the first value in the partition."),
            fn("LAST(col) OVER(...)", "LAST() OVER (PARTITION BY  ORDER BY )", "Returns the last value in the partition."),
            fn("ANY_VALUE(col) OVER(...)", "ANY_VALUE() OVER (PARTITION BY )", "Returns any arbitrary value from the partition."),
            fn("COLLECT_LIST(col) OVER(...)", "COLLECT_LIST() OVER (PARTITION BY )", "Collects values into an array (allows duplicates) per partition."),
            fn("COLLECT_SET(col) OVER(...)", "COLLECT_SET() OVER (PARTITION BY )", "Collects distinct values into an array per partition."),
            fn("STDDEV(col) OVER(...)", "STDDEV() OVER (PARTITION BY )", "Computes the sample standard deviation per partition."),
            fn("STDDEV_POP(col) OVER(...)", "STDDEV_POP() OVER (PARTITION BY )", "Computes the population standard deviation per partition."),
            fn("VARIANCE(col) OVER(...)", "VARIANCE() OVER (PARTITION BY )", "Computes the sample variance per partition."),
            fn("VAR_POP(col) OVER(...)", "VAR_POP() OVER (PARTITION BY )", "Computes the population variance per partition.")
        ));

        // ── Window ──────────────────────────────────────────────────────
        catalog.add(buildCategory("Window",
            fn("ROW_NUMBER() OVER(...)", "ROW_NUMBER() OVER (ORDER BY )", "Assigns a sequential row number starting from 1 within the partition."),
            fn("RANK() OVER(...)", "RANK() OVER (ORDER BY )", "Assigns a rank with gaps for ties."),
            fn("DENSE_RANK() OVER(...)", "DENSE_RANK() OVER (ORDER BY )", "Assigns a rank without gaps for ties."),
            fn("NTILE(n) OVER(...)", "NTILE(4) OVER (ORDER BY )", "Divides rows into n roughly equal groups and returns the group number."),
            fn("PERCENT_RANK() OVER(...)", "PERCENT_RANK() OVER (ORDER BY )", "Returns the relative rank as a percentage (0 to 1)."),
            fn("CUME_DIST() OVER(...)", "CUME_DIST() OVER (ORDER BY )", "Returns the cumulative distribution (fraction of rows at or below current)."),
            fn("LAG(col, offset) OVER(...)", "LAG(, 1) OVER (ORDER BY )", "Returns the value from a preceding row by the given offset."),
            fn("LEAD(col, offset) OVER(...)", "LEAD(, 1) OVER (ORDER BY )", "Returns the value from a following row by the given offset."),
            fn("FIRST_VALUE(col) OVER(...)", "FIRST_VALUE() OVER (ORDER BY )", "Returns the first value in the window frame."),
            fn("LAST_VALUE(col) OVER(...)", "LAST_VALUE() OVER (ORDER BY )", "Returns the last value in the window frame."),
            fn("NTH_VALUE(col, n) OVER(...)", "NTH_VALUE(, 1) OVER (ORDER BY )", "Returns the value at the nth row in the window frame."),
            fn("SUM(col) OVER(PARTITION BY ...)", "SUM() OVER (PARTITION BY  ORDER BY )", "Computes a running sum over the partition with ordering."),
            fn("AVG(col) OVER(PARTITION BY ...)", "AVG() OVER (PARTITION BY  ORDER BY )", "Computes a running average over the partition with ordering.")
        ));

        // ── Array ───────────────────────────────────────────────────────
        catalog.add(buildCategory("Array",
            fn("ARRAY(a, b, ...)", "ARRAY(, )", "Creates an array from the given elements."),
            fn("ARRAY_CONTAINS(arr, val)", "ARRAY_CONTAINS(, '')", "Returns true if the array contains the value."),
            fn("ARRAY_DISTINCT(arr)", "ARRAY_DISTINCT()", "Removes duplicate elements from the array."),
            fn("ARRAY_EXCEPT(a, b)", "ARRAY_EXCEPT(, )", "Returns elements in array a that are not in array b."),
            fn("ARRAY_INTERSECT(a, b)", "ARRAY_INTERSECT(, )", "Returns the common elements of two arrays."),
            fn("ARRAY_UNION(a, b)", "ARRAY_UNION(, )", "Returns the union of two arrays without duplicates."),
            fn("ARRAY_JOIN(arr, delim)", "ARRAY_JOIN(, ',')", "Joins array elements into a single string with a delimiter."),
            fn("ARRAY_MAX(arr)", "ARRAY_MAX()", "Returns the maximum value in the array."),
            fn("ARRAY_MIN(arr)", "ARRAY_MIN()", "Returns the minimum value in the array."),
            fn("ARRAY_SIZE(arr)", "ARRAY_SIZE()", "Returns the number of elements in the array."),
            fn("ARRAY_SORT(arr)", "ARRAY_SORT()", "Sorts the array in ascending order."),
            fn("ARRAY_POSITION(arr, val)", "ARRAY_POSITION(, '')", "Returns the 1-based index of the first occurrence of a value in the array."),
            fn("ARRAY_REMOVE(arr, val)", "ARRAY_REMOVE(, '')", "Removes all occurrences of a value from the array."),
            fn("ARRAY_APPEND(arr, val)", "ARRAY_APPEND(, '')", "Appends a value to the end of the array."),
            fn("ARRAY_COMPACT(arr)", "ARRAY_COMPACT()", "Removes null values from the array."),
            fn("ELEMENT_AT(arr, idx)", "ELEMENT_AT(, 1)", "Returns the element at the given 1-based index."),
            fn("FLATTEN(arr)", "FLATTEN()", "Flattens a nested array (array of arrays) into a single array."),
            fn("SLICE(arr, start, len)", "SLICE(, 1, 5)", "Returns a sub-array starting at the given index with the specified length."),
            fn("SEQUENCE(start, stop, step)", "SEQUENCE(1, 10, 1)", "Generates an array of integers from start to stop with a step."),
            fn("SORT_ARRAY(arr, asc)", "SORT_ARRAY(, true)", "Sorts the array in ascending (true) or descending (false) order.")
        ));

        // ── Map ─────────────────────────────────────────────────────────
        catalog.add(buildCategory("Map",
            fn("MAP(k1, v1, k2, v2, ...)", "MAP('key', 'value')", "Creates a map from key-value pairs."),
            fn("MAP_KEYS(map)", "MAP_KEYS()", "Returns an array of all keys in the map."),
            fn("MAP_VALUES(map)", "MAP_VALUES()", "Returns an array of all values in the map."),
            fn("MAP_FROM_ARRAYS(keys, vals)", "MAP_FROM_ARRAYS(, )", "Creates a map from a keys array and a values array."),
            fn("MAP_FROM_ENTRIES(arr)", "MAP_FROM_ENTRIES()", "Creates a map from an array of key-value struct entries."),
            fn("MAP_CONCAT(m1, m2)", "MAP_CONCAT(, )", "Merges two maps into one."),
            fn("MAP_CONTAINS_KEY(map, key)", "MAP_CONTAINS_KEY(, '')", "Returns true if the map contains the specified key."),
            fn("MAP_ENTRIES(map)", "MAP_ENTRIES()", "Returns an array of key-value struct entries from the map."),
            fn("ELEMENT_AT(map, key)", "ELEMENT_AT(, '')", "Returns the value for the given key in the map."),
            fn("STR_TO_MAP(col, pairDel, kvDel)", "STR_TO_MAP(, ',', ':')", "Parses a string into a map using pair and key-value delimiters.")
        ));

        // ── JSON ────────────────────────────────────────────────────────
        catalog.add(buildCategory("JSON",
            fn("GET_JSON_OBJECT(json, path)", "GET_JSON_OBJECT(, '$.key')", "Extracts a value from a JSON string using a JSONPath expression."),
            fn("FROM_JSON(json, schema)", "FROM_JSON(, 'a INT, b STRING')", "Parses a JSON string into a struct using the given schema."),
            fn("TO_JSON(col)", "TO_JSON()", "Converts a struct or map column to a JSON string."),
            fn("SCHEMA_OF_JSON(json)", "SCHEMA_OF_JSON()", "Infers and returns the schema of a JSON string."),
            fn("JSON_ARRAY_LENGTH(json)", "JSON_ARRAY_LENGTH()", "Returns the number of elements in a JSON array string."),
            fn("[3.5+] JSON_OBJECT_KEYS(json)", "JSON_OBJECT_KEYS()", "Returns an array of all keys in a JSON object string.")
        ));

        // ── Hash / Encoding ─────────────────────────────────────────────
        catalog.add(buildCategory("Hash / Encoding",
            fn("MD5(col)", "MD5()", "Computes the MD5 hash of a string and returns a 32-character hex string."),
            fn("SHA1(col)", "SHA1()", "Computes the SHA-1 hash of a string."),
            fn("SHA2(col, bits)", "SHA2(, 256)", "Computes the SHA-2 hash with the specified bit length (224, 256, 384, or 512)."),
            fn("CRC32(col)", "CRC32()", "Computes the CRC32 checksum of a string."),
            fn("HASH(col, ...)", "HASH()", "Computes a hash code of the given columns using Murmur3."),
            fn("XXHASH64(col, ...)", "XXHASH64()", "Computes a 64-bit hash of the given columns using xxHash."),
            fn("BASE64(col)", "BASE64()", "Encodes a binary column to a Base64 string."),
            fn("UNBASE64(col)", "UNBASE64()", "Decodes a Base64 string to binary."),
            fn("ENCODE(col, charset)", "ENCODE(, 'UTF-8')", "Encodes a string to binary using the specified character set."),
            fn("DECODE(col, charset)", "DECODE(, 'UTF-8')", "Decodes binary to a string using the specified character set."),
            fn("[3.5+] URL_ENCODE(col)", "URL_ENCODE()", "Encodes a string into URL-safe format (percent-encoding)."),
            fn("[3.5+] URL_DECODE(col)", "URL_DECODE()", "Decodes a URL-encoded string back to its original form.")
        ));

        // ── Higher-Order Functions ──────────────────────────────────────
        catalog.add(buildCategory("Higher-Order",
            fn("TRANSFORM(arr, x -> expr)", "TRANSFORM(, x -> x + 1)", "Applies a lambda function to each element of an array."),
            fn("FILTER(arr, x -> cond)", "FILTER(, x -> x > 0)", "Filters array elements that satisfy the condition."),
            fn("EXISTS(arr, x -> cond)", "EXISTS(, x -> x > 0)", "Returns true if any array element satisfies the condition."),
            fn("FORALL(arr, x -> cond)", "FORALL(, x -> x > 0)", "Returns true if all array elements satisfy the condition."),
            fn("AGGREGATE(arr, init, merge)", "AGGREGATE(, 0, (acc, x) -> acc + x)", "Reduces an array to a single value using an accumulator function."),
            fn("ZIP_WITH(a, b, (x,y) -> expr)", "ZIP_WITH(, , (x, y) -> x + y)", "Merges two arrays element-wise using a lambda function."),
            fn("TRANSFORM_KEYS(map, (k,v)->expr)", "TRANSFORM_KEYS(, (k, v) -> UPPER(k))", "Applies a lambda to transform all keys of a map."),
            fn("TRANSFORM_VALUES(map, (k,v)->expr)", "TRANSFORM_VALUES(, (k, v) -> v + 1)", "Applies a lambda to transform all values of a map."),
            fn("MAP_FILTER(map, (k,v) -> cond)", "MAP_FILTER(, (k, v) -> v > 0)", "Filters map entries that satisfy the condition.")
        ));

        // ── Miscellaneous ───────────────────────────────────────────────
        catalog.add(buildCategory("Miscellaneous",
            fn("UUID()", "UUID()", "Generates a universally unique identifier (UUID) string."),
            fn("MONOTONICALLY_INCREASING_ID()", "MONOTONICALLY_INCREASING_ID()", "Generates a unique, monotonically increasing 64-bit integer ID per row."),
            fn("SPARK_PARTITION_ID()", "SPARK_PARTITION_ID()", "Returns the partition ID of the current row."),
            fn("INPUT_FILE_NAME()", "INPUT_FILE_NAME()", "Returns the name of the file being read for the current row."),
            fn("CURRENT_USER()", "CURRENT_USER()", "Returns the current user name."),
            fn("CURRENT_DATABASE()", "CURRENT_DATABASE()", "Returns the current database name."),
            fn("STRUCT(a, b, ...)", "STRUCT(, )", "Creates a struct from the given values."),
            fn("NAMED_STRUCT(n1,v1,n2,v2)", "NAMED_STRUCT('col1', , 'col2', )", "Creates a struct with named fields from name-value pairs.")
        ));

        return catalog;
    }

    @SafeVarargs
    private static Map<String, Object> buildCategory(final String name,
            final Map<String, Object>... functions) {
        return Map.of("name", name, "functions", List.of(functions));
    }

    private static Map<String, Object> fn(final String label, final String template, final String description) {
        return Map.of("label", label, "template", template, "description", description);
    }
}

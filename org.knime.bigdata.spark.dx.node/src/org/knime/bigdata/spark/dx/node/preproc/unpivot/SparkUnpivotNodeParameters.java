package org.knime.bigdata.spark.dx.node.preproc.unpivot;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.webui.node.dialog.defaultdialog.internal.button.Icon;
import org.knime.core.webui.node.dialog.defaultdialog.internal.button.SimpleButtonWidget;
import org.knime.core.webui.node.dialog.defaultdialog.setting.filter.util.ManualFilter;
import org.knime.node.parameters.NodeParameters;
import org.knime.node.parameters.NodeParametersInput;
import org.knime.node.parameters.Widget;
import org.knime.node.parameters.layout.After;
import org.knime.node.parameters.layout.Layout;
import org.knime.node.parameters.layout.Section;
import org.knime.node.parameters.persistence.NodeParametersPersistor;
import org.knime.node.parameters.persistence.Persist;
import org.knime.node.parameters.persistence.Persistor;
import org.knime.node.parameters.persistence.legacy.LegacyColumnFilterPersistor;
import org.knime.node.parameters.updates.ButtonReference;
import org.knime.node.parameters.updates.ParameterReference;
import org.knime.node.parameters.updates.StateProvider;
import org.knime.node.parameters.updates.ValueReference;
import org.knime.node.parameters.widget.choices.ColumnChoicesProvider;
import org.knime.node.parameters.widget.choices.Label;
import org.knime.node.parameters.widget.choices.filter.ColumnFilter;
import org.knime.node.parameters.widget.choices.filter.ColumnFilterWidget;
import org.knime.node.parameters.widget.message.TextMessage;

// Note: VariableValueMapping feature removed for simplicity.

/**
 * Node parameters (WebUI dialog settings) for the Spark Unpivot node.
 * Persistors bridge between the WebUI ColumnFilter representation and the
 * SettingsModelFilterString format used by SparkUnpivotSettings, ensuring
 * backward compatibility with existing saved workflows.
 */
@SuppressWarnings("restriction")
class SparkUnpivotNodeParameters implements NodeParameters {

    // ── LAYOUT ────────────────────────────────────────────────────────────────
    // Named DialogSections (not Layout) to avoid shadowing the @Layout annotation import.

    interface DialogSections {
        @Section(title = "Retained Columns",
            description = "Columns to keep as identifier columns in the output.")
        interface RetainedColumnsSection {}

        @Section(title = "Value Columns",
            description = "Columns to unpivot into rows.")
        @After(RetainedColumnsSection.class)
        interface ValueColumnsSection {}

        @Section(title = "Options")
        @After(ValueColumnsSection.class)
        interface OptionsSection {}

        @Section(title = "Validation & Row Estimation")
        @After(OptionsSection.class)
        interface ValidationSection {}
    }

    // ── ENUMS ─────────────────────────────────────────────────────────────────

    enum SortOption {
        @Label(value = "No sorting", description = "Output rows are not sorted.")
        NONE,
        @Label(value = "Sort by retained columns", description = "Sort by the retained (ID) columns.")
        RETAINED,
        @Label(value = "Sort by variable column", description = "Sort by the variable column name.")
        VARIABLE;
    }

    // ── PARAMETER REFERENCES ──────────────────────────────────────────────────

    interface RetainedColumnsRef extends ParameterReference<ColumnFilter> {}
    interface ValueColumnsRef extends ParameterReference<ColumnFilter> {}
    interface SkipMissingRef extends ParameterReference<Boolean> {}
    interface CastToStringRef extends ParameterReference<Boolean> {}
    interface SortOptionRef extends ParameterReference<SortOption> {}
    interface VariableColNameRef extends ParameterReference<String> {}
    interface ValueColNameRef extends ParameterReference<String> {}

    /** Button reference for the Check / Run Validation button. */
    interface CheckButtonRef extends ButtonReference {}


    // ── COLUMN CHOICES PROVIDER ───────────────────────────────────────────────

    static final class SparkColumnChoicesProvider implements ColumnChoicesProvider {
        @Override
        public List<DataColumnSpec> columnChoices(final NodeParametersInput context) {
            return context.getInPortSpec(0)
                .filter(spec -> spec instanceof SparkDataPortObjectSpec)
                .map(spec -> ((SparkDataPortObjectSpec) spec).getTableSpec())
                .map(tableSpec -> tableSpec.stream().toList())
                .orElse(List.of());
        }
    }

    // ── CUSTOM PERSISTORS ─────────────────────────────────────────────────────

    /**
     * Bridges ColumnFilter ↔ the settings under key "retainedColumns".
     * Extending LegacyColumnFilterPersistor ensures the framework uses our instance with the
     * correct config key instead of deriving a snake_case key from the field name.
     * Overrides load() to also handle old SettingsModelFilterString format (InclList/ExclList)
     * written by saved workflows from the legacy Swing dialog.
     */
    static final class RetainedColumnsPersistor extends LegacyColumnFilterPersistor {
        private static final String KEY = SparkUnpivotSettings.CFG_RETAINED_COLUMNS;

        RetainedColumnsPersistor() {
            super(KEY);
        }

        @Override
        public ColumnFilter load(final NodeSettingsRO settings) throws InvalidSettingsException {
            return loadColumnFilterWithFallback(settings, KEY);
        }
    }

    /**
     * Bridges ColumnFilter ↔ the settings under key "valueColumns".
     */
    static final class ValueColumnsPersistor extends LegacyColumnFilterPersistor {
        private static final String KEY = SparkUnpivotSettings.CFG_VALUE_COLUMNS;

        ValueColumnsPersistor() {
            super(KEY);
        }

        @Override
        public ColumnFilter load(final NodeSettingsRO settings) throws InvalidSettingsException {
            return loadColumnFilterWithFallback(settings, KEY);
        }
    }

    /**
     * Loads a ColumnFilter from settings, trying new NameFilterConfiguration format first
     * (filter-type / included_names) and falling back to old SettingsModelFilterString format
     * (InclList / ExclList) for backward compatibility with saved workflows.
     */
    private static ColumnFilter loadColumnFilterWithFallback(final NodeSettingsRO settings,
            final String key) throws InvalidSettingsException {
        try {
            final NodeSettingsRO sub = settings.getNodeSettings(key);
            if (sub.containsKey("included_names")) {
                // New format: NameFilterConfiguration / LegacyColumnFilterPersistor
                return LegacyColumnFilterPersistor.load(settings, key);
            }
            // Old format: SettingsModelFilterString (InclList / ExclList)
            final String[] incl = sub.getStringArray("InclList", new String[0]);
            return buildColumnFilterFromNames(incl, key);
        } catch (final InvalidSettingsException e) {
            // Sub-config missing or type mismatch — return empty filter as safe default
            return new ColumnFilter();
        }
    }

    /**
     * Constructs a ColumnFilter with the given selected column names by building
     * a temporary NodeSettings in NameFilterConfiguration format.
     */
    private static ColumnFilter buildColumnFilterFromNames(final String[] included, final String key)
            throws InvalidSettingsException {
        final NodeSettings temp = new NodeSettings("_temp");
        final NodeSettingsWO sub = temp.addNodeSettings(key);
        sub.addString("filter-type", "STANDARD");
        sub.addStringArray("included_names", included);
        sub.addStringArray("excluded_names", new String[0]);
        sub.addString("enforce_option", "EnforceInclusion");
        return LegacyColumnFilterPersistor.load(temp, key);
    }

    /**
     * Bridges SortOption enum ↔ the lowercase string format used by SparkUnpivotSettings.
     * SparkUnpivotSettings uses "none"/"retained"/"variable"; enum names would be NONE/RETAINED/VARIABLE.
     */
    static final class SortOptionPersistor implements NodeParametersPersistor<SortOption> {
        private static final String CFG_KEY = SparkUnpivotSettings.CFG_SORT_OPTION;

        @Override
        public SortOption load(final NodeSettingsRO settings) throws InvalidSettingsException {
            String val = settings.getString(CFG_KEY, SparkUnpivotSettings.SORT_NONE);
            return switch (val) {
                case SparkUnpivotSettings.SORT_BY_RETAINED -> SortOption.RETAINED;
                case SparkUnpivotSettings.SORT_BY_VARIABLE -> SortOption.VARIABLE;
                default -> SortOption.NONE;
            };
        }

        @Override
        public void save(final SortOption obj, final NodeSettingsWO settings) {
            String val = switch (obj != null ? obj : SortOption.NONE) {
                case RETAINED -> SparkUnpivotSettings.SORT_BY_RETAINED;
                case VARIABLE -> SparkUnpivotSettings.SORT_BY_VARIABLE;
                case NONE -> SparkUnpivotSettings.SORT_NONE;
            };
            settings.addString(CFG_KEY, val);
        }

        @Override
        public String[][] getConfigPaths() {
            return new String[][]{{CFG_KEY}};
        }
    }

    // ── VALIDATION & ROW ESTIMATION STATE PROVIDER ────────────────────────────

    /**
     * Runs the validate-only Spark job after the dialog opens (asynchronously).
     * Shows estimated output row count and a validation result (preview or error).
     */
    static final class ValidationAndEstimationProvider
        implements StateProvider<Optional<TextMessage.Message>> {

        private Supplier<ColumnFilter> m_retainedSupplier;
        private Supplier<ColumnFilter> m_valueSupplier;
        private Supplier<String> m_varColNameSupplier;
        private Supplier<String> m_valColNameSupplier;
        private Supplier<Boolean> m_skipMissingSupplier;
        private Supplier<Boolean> m_castToStringSupplier;
        private Supplier<SortOption> m_sortOptionSupplier;

        @Override
        public void init(final StateProviderInitializer initializer) {
            // Only compute when the Check button is clicked (not on column change or dialog open).
            initializer.computeOnButtonClick(CheckButtonRef.class);
            // All field values are dependencies only (not triggers).
            m_retainedSupplier    = initializer.getValueSupplier(RetainedColumnsRef.class);
            m_valueSupplier       = initializer.getValueSupplier(ValueColumnsRef.class);
            m_varColNameSupplier  = initializer.getValueSupplier(VariableColNameRef.class);
            m_valColNameSupplier  = initializer.getValueSupplier(ValueColNameRef.class);
            m_skipMissingSupplier = initializer.getValueSupplier(SkipMissingRef.class);
            m_castToStringSupplier= initializer.getValueSupplier(CastToStringRef.class);
            m_sortOptionSupplier  = initializer.getValueSupplier(SortOptionRef.class);
        }

        @Override
        public Optional<TextMessage.Message> computeState(final NodeParametersInput context) {
            Optional<PortObject> portObjOpt = context.getInPortObject(0);
            if (portObjOpt.isEmpty()) {
                return Optional.of(new TextMessage.Message(
                    "Execute the upstream node first to enable validation.",
                    "", TextMessage.MessageType.INFO));
            }

            String[] valueCols = getManuallySelected(m_valueSupplier.get());
            if (valueCols.length == 0) {
                return Optional.of(new TextMessage.Message(
                    "Select at least one value column to run validation.",
                    "", TextMessage.MessageType.WARNING));
            }

            SparkDataPortObject sparkPort = (SparkDataPortObject) portObjOpt.get();
            SparkContextID contextID = sparkPort.getContextID();
            String inputObjectId = sparkPort.getData().getID();

            try {
                String[] retainedCols = getManuallySelected(m_retainedSupplier.get());
                String varColName = orDefault(m_varColNameSupplier.get(), "variable");
                String valColName = orDefault(m_valColNameSupplier.get(), "value");
                boolean skipMissing = Boolean.TRUE.equals(m_skipMissingSupplier.get());
                boolean castToString = Boolean.TRUE.equals(m_castToStringSupplier.get());
                String sortStr = toSortString(m_sortOptionSupplier.get());

                SparkUnpivotJobInput jobInput = new SparkUnpivotJobInput(
                    inputObjectId,
                    retainedCols, valueCols,
                    varColName, valColName,
                    skipMissing, castToString,
                    sortStr,
                    new String[0], new String[0]);

                SparkUnpivotJobOutput output = SparkContextUtil
                    .<SparkUnpivotJobInput, SparkUnpivotJobOutput>getJobRunFactory(
                        contextID, SparkUnpivotNodeModel.JOB_ID)
                    .createRun(jobInput)
                    .run(contextID);

                long inputRows = output.getInputRowCount();
                long estimatedOutputRows = inputRows >= 0 ? inputRows * valueCols.length : -1;

                StringBuilder msg = new StringBuilder("Validation succeeded.");
                if (inputRows >= 0) {
                    msg.append(String.format(
                        " Estimated output rows: %,d × %d = %,d (max)",
                        inputRows, valueCols.length, estimatedOutputRows));
                    if (skipMissing) {
                        msg.append(" — may be less due to 'Skip missing values'.");
                    }
                }

                return Optional.of(new TextMessage.Message(
                    msg.toString(), "", TextMessage.MessageType.SUCCESS));

            } catch (Exception e) {
                String errMsg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
                return Optional.of(new TextMessage.Message(
                    "Validation failed: " + errMsg,
                    "", TextMessage.MessageType.ERROR));
            }
        }

        private static String orDefault(final String val, final String def) {
            return (val != null && !val.isBlank()) ? val : def;
        }

        private static String toSortString(final SortOption opt) {
            if (opt == null) {
                return SparkUnpivotSettings.SORT_NONE;
            }
            return switch (opt) {
                case RETAINED -> SparkUnpivotSettings.SORT_BY_RETAINED;
                case VARIABLE -> SparkUnpivotSettings.SORT_BY_VARIABLE;
                case NONE -> SparkUnpivotSettings.SORT_NONE;
            };
        }
    }

    // ── FIELDS ────────────────────────────────────────────────────────────────

    // ── Retained Columns ──────────────────────────────────────────────────────
    @Layout(DialogSections.RetainedColumnsSection.class)
    @Widget(title = "Retained Columns",
        description = "Columns to keep as identifier columns in each output row.")
    @ColumnFilterWidget(choicesProvider = SparkColumnChoicesProvider.class)
    @ValueReference(RetainedColumnsRef.class)
    @Persistor(RetainedColumnsPersistor.class)
    ColumnFilter m_retainedColumns = new ColumnFilter();

    // ── Value Columns ─────────────────────────────────────────────────────────
    @Layout(DialogSections.ValueColumnsSection.class)
    @Widget(title = "Value Columns",
        description = "Columns to unpivot. Each selected column becomes a row in the output.")
    @ColumnFilterWidget(choicesProvider = SparkColumnChoicesProvider.class)
    @ValueReference(ValueColumnsRef.class)
    @Persistor(ValueColumnsPersistor.class)
    ColumnFilter m_valueColumns = new ColumnFilter();

    // ── Options ───────────────────────────────────────────────────────────────
    @Layout(DialogSections.OptionsSection.class)
    @Widget(title = "Variable column name",
        description = "Name for the output column that stores the original column name.")
    @Persist(configKey = "variableColName")
    @ValueReference(VariableColNameRef.class)
    String m_variableColName = "column";

    @Layout(DialogSections.OptionsSection.class)
    @Widget(title = "Value column name",
        description = "Name for the output column that stores the cell value.")
    @Persist(configKey = "valueColName")
    @ValueReference(ValueColNameRef.class)
    String m_valueColName = "value";

    @Layout(DialogSections.OptionsSection.class)
    @Widget(title = "Skip missing values",
        description = "If checked, rows where the value column is null are excluded from the output.")
    @Persist(configKey = "skipMissingValues")
    @ValueReference(SkipMissingRef.class)
    boolean m_skipMissingValues = true;

    @Layout(DialogSections.OptionsSection.class)
    @Widget(title = "Cast all value columns to String",
        description = "Cast all value columns to String before unpivoting. Required when value columns have mixed types.")
    @Persist(configKey = "castToString")
    @ValueReference(CastToStringRef.class)
    boolean m_castToString = false;

    @Layout(DialogSections.OptionsSection.class)
    @Widget(title = "Sort output",
        description = "Choose how the output rows should be sorted.")
    @Persistor(SortOptionPersistor.class)
    @ValueReference(SortOptionRef.class)
    SortOption m_sortOption = SortOption.NONE;

    // ── Validation & Row Estimation ───────────────────────────────────────────
    @Layout(DialogSections.ValidationSection.class)
    @Widget(title = "Run Validation",
        description = "Validate the current configuration and estimate the number of output rows. "
            + "Requires the upstream node to be executed.")
    @SimpleButtonWidget(ref = CheckButtonRef.class, icon = Icon.RELOAD)
    Void m_checkButton;

    @Layout(DialogSections.ValidationSection.class)
    @TextMessage(ValidationAndEstimationProvider.class)
    Void m_validationDisplay;

    // ── CONSTRUCTORS ──────────────────────────────────────────────────────────

    SparkUnpivotNodeParameters() {}

    // ── HELPERS ───────────────────────────────────────────────────────────────

    /**
     * Extracts the manually selected column names from a ColumnFilter.
     * The ColumnFilter is always created in MANUAL mode by the persistors.
     */
    @SuppressWarnings("restriction")
    static String[] getManuallySelected(final ColumnFilter filter) {
        if (filter == null) {
            return new String[0];
        }
        ManualFilter mf = filter.m_manualFilter;
        if (mf == null || mf.m_manuallySelected == null) {
            return new String[0];
        }
        return mf.m_manuallySelected;
    }

}

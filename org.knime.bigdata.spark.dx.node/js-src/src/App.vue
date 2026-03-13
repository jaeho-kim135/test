<template>
  <div class="spark-expression" :class="{ compact: isCompact }">
    <!-- Enlarged (full) mode: 3-panel layout -->
    <template v-if="!isCompact">
      <div class="main-layout">
        <div class="left-panel">
          <InputColumns
            :columns="columns"
            @insert="insertTextIntoEditor"
          />
          <FlowVariables
            :variables="flowVariables"
            @insert="insertTextIntoEditor"
          />
        </div>
        <div class="center-panel">
          <ExpressionEditors
            ref="editors"
            :expressions="settings.expressions"
            :outputModes="settings.outputModes"
            :columnNames="settings.columnNames"
            :columns="columns"
            @update="onSettingsUpdate"
            @evaluate="evaluateExpressions"
          />
          <OutputPreview
            ref="outputPreview"
            :preview="previewData"
            :error="previewError"
            :loading="isEvaluating"
            :inputPreview="inputPreviewData"
            :inputError="inputPreviewError"
            :inputLoading="isLoadingInput"
            @evaluate="evaluateExpressions"
            @loadInput="loadInputTable"
          />
        </div>
        <div class="right-panel">
          <FunctionCatalog
            :catalog="functionCatalog"
            @insert="insertTextIntoEditor"
          />
        </div>
      </div>
    </template>

    <!-- Compact (side-panel) mode -->
    <template v-else>
      <div class="compact-layout">
        <ExpressionEditors
          ref="editors"
          :expressions="settings.expressions"
          :outputModes="settings.outputModes"
          :columnNames="settings.columnNames"
          :columns="columns"
          :compact="true"
          @update="onSettingsUpdate"
          @evaluate="evaluateExpressions"
        />
        <OutputPreview
          ref="outputPreview"
          :preview="previewData"
          :error="previewError"
          :loading="isEvaluating"
          :inputPreview="inputPreviewData"
          :inputError="inputPreviewError"
          :inputLoading="isLoadingInput"
          :compact="true"
          @evaluate="evaluateExpressions"
          @loadInput="loadInputTable"
        />
      </div>
    </template>
  </div>
</template>

<script>
import { ref, reactive, onMounted, onUnmounted } from 'vue'
import { initKnimeService, setApplyListener, callRpc, registerModelSetting, markDirty } from './knimeService.js'
import InputColumns from './components/InputColumns.vue'
import FlowVariables from './components/FlowVariables.vue'
import FunctionCatalog from './components/FunctionCatalog.vue'
import ExpressionEditors from './components/ExpressionEditors.vue'
import OutputPreview from './components/OutputPreview.vue'

export default {
  name: 'SparkExpressionApp',
  components: { InputColumns, FlowVariables, FunctionCatalog, ExpressionEditors, OutputPreview },

  setup() {
    const editors = ref(null)
    const outputPreview = ref(null)
    const columns = ref([])
    const flowVariables = ref([])
    const functionCatalog = ref([])
    const previewData = ref('')
    const previewError = ref('')
    const isEvaluating = ref(false)
    const inputPreviewData = ref('')
    const inputPreviewError = ref('')
    const isLoadingInput = ref(false)
    const isCompact = ref(window.innerWidth < 600)

    const settings = reactive({
      expressions: [''],
      outputModes: ['APPEND'],
      columnNames: ['new_column']
    })

    const onResize = () => {
      isCompact.value = window.innerWidth < 600
    }

    onMounted(async () => {
      window.addEventListener('resize', onResize)

      // Initialize KNIME services (JsonDataService + DialogService via postMessage)
      // and load initial data from the backend
      const data = await initKnimeService()

      // Load settings from backend (ScriptingNodeSettingsService.fromNodeSettings)
      if (data?.settings) {
        const s = data.settings
        if (s.expressions?.length) settings.expressions = [...s.expressions]
        if (s.outputModes?.length) settings.outputModes = [...s.outputModes]
        if (s.columnNames?.length) settings.columnNames = [...s.columnNames]
      }

      // Load initial data (columns, flow variables, function catalog)
      const init = data?.initialData || {}
      if (init.columnNamesAndTypes) columns.value = init.columnNamesAndTypes
      if (init.flowVariables) flowVariables.value = init.flowVariables
      if (init.functionCatalog) functionCatalog.value = init.functionCatalog

      // Register model setting for dirty-state tracking (enables Apply button on change)
      registerModelSetting({
        expressions: [...settings.expressions],
        outputModes: [...settings.outputModes],
        columnNames: [...settings.columnNames]
      })

      // Set up the apply listener (handles OK/Apply button clicks).
      // When the user clicks OK/Apply, KNIME sends an "ApplyDataEvent" push event,
      // and this callback sends current settings to the backend via applyData().
      setApplyListener(() => ({
        expressions: [...settings.expressions],
        outputModes: [...settings.outputModes],
        columnNames: [...settings.columnNames]
      }))
    })

    onUnmounted(() => {
      window.removeEventListener('resize', onResize)
    })

    function onSettingsUpdate(updated) {
      settings.expressions = updated.expressions
      settings.outputModes = updated.outputModes
      settings.columnNames = updated.columnNames
      markDirty({
        expressions: [...settings.expressions],
        outputModes: [...settings.outputModes],
        columnNames: [...settings.columnNames]
      })
    }

    function insertTextIntoEditor(text) {
      if (editors.value) {
        editors.value.insertText(text)
      }
    }

    async function evaluateExpressions() {
      // Guard against concurrent evaluations (Ctrl+Enter spam)
      if (isEvaluating.value) return

      // Auto-switch to output tab
      if (outputPreview.value) {
        outputPreview.value.switchToOutput()
      }

      // Frontend validation
      const emptyIdx = settings.expressions.findIndex(e => !e || !e.trim())
      if (emptyIdx >= 0) {
        previewError.value = `Expression ${emptyIdx + 1} is empty.`
        previewData.value = ''
        return
      }
      const emptyNameIdx = settings.columnNames.findIndex(n => !n || !n.trim())
      if (emptyNameIdx >= 0) {
        previewError.value = `Output column name for Expression ${emptyNameIdx + 1} is empty.`
        previewData.value = ''
        return
      }

      // Check for duplicate column names
      const seen = new Set()
      for (let i = 0; i < settings.columnNames.length; i++) {
        const name = settings.columnNames[i].trim()
        if (seen.has(name)) {
          previewError.value = `Duplicate output column name "${name}" in Expression ${i + 1}.`
          previewData.value = ''
          return
        }
        seen.add(name)
      }

      isEvaluating.value = true
      previewData.value = ''
      previewError.value = ''

      try {
        // Spread reactive proxy arrays to plain JS arrays for RPC serialization
        const result = await callRpc('SparkExpressionService', 'evaluateExpressions', [
          [...settings.expressions],
          [...settings.outputModes],
          [...settings.columnNames],
          10
        ])

        if (result.success) {
          previewData.value = result.preview || 'Evaluation successful (no preview data).'
          previewError.value = ''
        } else {
          previewData.value = ''
          previewError.value = result.error || 'Unknown error'
        }
      } catch (e) {
        previewError.value = e.message || 'Evaluation failed'
      } finally {
        isEvaluating.value = false
      }
    }

    async function loadInputTable() {
      if (inputPreviewData.value || isLoadingInput.value) return // already loaded or loading

      isLoadingInput.value = true
      inputPreviewData.value = ''
      inputPreviewError.value = ''

      try {
        const result = await callRpc('SparkExpressionService', 'previewInputTable', [])
        if (result.success) {
          inputPreviewData.value = result.preview || 'No data available.'
        } else {
          inputPreviewError.value = result.error || 'Failed to load input data.'
        }
      } catch (e) {
        inputPreviewError.value = e.message || 'Failed to load input data.'
      } finally {
        isLoadingInput.value = false
      }
    }

    return {
      editors, outputPreview, columns, flowVariables, functionCatalog, settings,
      previewData, previewError, isEvaluating, isCompact,
      inputPreviewData, inputPreviewError, isLoadingInput,
      onSettingsUpdate, insertTextIntoEditor, evaluateExpressions, loadInputTable
    }
  }
}
</script>

<style>
* {
  margin: 0;
  padding: 0;
  box-sizing: border-box;
}
html, body, #app {
  height: 100%;
  width: 100%;
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
  font-size: 13px;
  color: #333;
  background: #fff;
}
.spark-expression {
  height: 100%;
  display: flex;
  flex-direction: column;
}

/* Enlarged (full) mode */
.main-layout {
  display: flex;
  height: 100%;
  overflow: hidden;
}
.left-panel {
  width: 200px;
  min-width: 160px;
  border-right: 1px solid #ddd;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}
.center-panel {
  flex: 1;
  display: flex;
  flex-direction: column;
  overflow: hidden;
  min-width: 0;
}
.right-panel {
  width: 220px;
  min-width: 180px;
  border-left: 1px solid #ddd;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}

/* Editor / Preview height ratio */
.center-panel .expression-editors {
  flex: 1;
  min-height: 0;
}
.center-panel .output-preview {
  flex: 1;
  min-height: 0;
}

/* Compact (side-panel) mode */
.compact-layout {
  height: 100%;
  display: flex;
  flex-direction: column;
  padding: 8px;
  overflow: hidden;
}
.compact-layout .expression-editors {
  flex: 1;
  min-height: 0;
}
.compact-layout .output-preview {
  flex: 1;
  min-height: 0;
}
</style>

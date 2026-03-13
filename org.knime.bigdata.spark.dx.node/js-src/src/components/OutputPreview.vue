<template>
  <div class="output-preview" :class="{ compact: compact }">
    <!-- Tab bar -->
    <div class="preview-tabs">
      <div
        class="preview-tab"
        :class="{ active: activeTab === 'input' }"
        role="tab"
        :aria-selected="activeTab === 'input'"
        @click="activeTab = 'input'; $emit('loadInput')"
      >Input table</div>
      <div
        class="preview-tab"
        :class="{ active: activeTab === 'output' }"
        role="tab"
        :aria-selected="activeTab === 'output'"
        @click="activeTab = 'output'"
      >Output table</div>
    </div>

    <!-- Input table tab -->
    <template v-if="activeTab === 'input'">
      <div class="preview-content" v-if="inputPreview || inputError || inputLoading">
        <div v-if="inputLoading" class="preview-loading">Loading input data from Spark...</div>
        <div v-else-if="inputError" class="preview-error-msg">{{ inputError }}</div>
        <div v-else-if="parsedInputTable" class="table-wrapper">
          <table class="data-grid">
            <thead>
              <tr>
                <th class="row-num-header">#</th>
                <th v-for="(h, i) in parsedInputTable.headers" :key="i">{{ h }}</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="(row, ri) in parsedInputTable.rows" :key="ri">
                <td class="row-num-cell">{{ ri + 1 }}</td>
                <td v-for="(cell, ci) in row" :key="ci">{{ cell }}</td>
              </tr>
            </tbody>
          </table>
        </div>
        <pre v-else class="preview-data">{{ inputPreview }}</pre>
      </div>
      <div class="preview-empty" v-else>
        Loading input data from Spark...
      </div>
    </template>

    <!-- Output table tab -->
    <template v-else-if="activeTab === 'output'">
      <div class="preview-header">
        <button class="evaluate-btn" @click="triggerEvaluate" :disabled="loading">
          <span class="play-icon">▶</span>
          {{ loading ? 'Evaluating...' : 'Evaluate first 10 rows (Ctrl+Enter)' }}
        </button>
      </div>
      <div class="preview-content" v-if="preview || error || loading">
        <div v-if="loading" class="preview-loading">Evaluating expressions on Spark...</div>
        <div v-else-if="error" class="preview-error-msg">{{ error }}</div>
        <div v-else-if="parsedOutputTable" class="table-wrapper">
          <table class="data-grid">
            <thead>
              <tr>
                <th class="row-num-header">#</th>
                <th v-for="(h, i) in parsedOutputTable.headers" :key="i">{{ h }}</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="(row, ri) in parsedOutputTable.rows" :key="ri">
                <td class="row-num-cell">{{ ri + 1 }}</td>
                <td v-for="(cell, ci) in row" :key="ci">{{ cell }}</td>
              </tr>
            </tbody>
          </table>
        </div>
        <pre v-else class="preview-data preview-success">{{ preview }}</pre>
      </div>
      <div class="preview-empty" v-else>
        Click "Evaluate first 10 rows" to preview expression results.
      </div>
    </template>
  </div>
</template>

<script>
import { ref, computed } from 'vue'

/**
 * Parse Spark's showString() output format into headers + rows.
 * Format: +---+------+\n| h1|    h2|\n+---+------+\n| v1|   v2 |\n+---+------+
 *
 * Uses the separator line (e.g. +---+------+) to determine column boundary
 * positions, so cell values containing '|' are parsed correctly.
 */
function parseSparkTable(text) {
  if (!text || typeof text !== 'string') return null
  const lines = text.split('\n').filter(l => l.trim())
  if (lines.length < 3) return null

  // Find separator line to determine column boundaries
  const sepLine = lines.find(l => l.startsWith('+') && l.endsWith('+'))
  if (!sepLine) return null

  // Extract boundary positions (positions of '+' chars)
  const boundaries = []
  for (let i = 0; i < sepLine.length; i++) {
    if (sepLine[i] === '+') boundaries.push(i)
  }
  if (boundaries.length < 2) return null

  // Parse a data line using fixed boundary positions (handles embedded '|' in cells)
  const parseLine = (line) => {
    const cells = []
    for (let b = 0; b < boundaries.length - 1; b++) {
      const start = boundaries[b] + 1
      const end = Math.min(boundaries[b + 1], line.length)
      cells.push(line.substring(start, end).trim())
    }
    return cells
  }

  const dataLines = lines.filter(l => l.startsWith('|'))
  if (dataLines.length < 1) return null

  const headers = parseLine(dataLines[0])
  const rows = dataLines.slice(1).map(parseLine)

  if (headers.length === 0) return null
  return { headers, rows }
}

export default {
  name: 'OutputPreview',
  props: {
    preview: { type: String, default: '' },
    error: { type: String, default: '' },
    loading: { type: Boolean, default: false },
    compact: { type: Boolean, default: false },
    inputPreview: { type: String, default: '' },
    inputError: { type: String, default: '' },
    inputLoading: { type: Boolean, default: false }
  },
  emits: ['evaluate', 'loadInput'],
  setup(props, { emit }) {
    const activeTab = ref('output')

    const parsedInputTable = computed(() => parseSparkTable(props.inputPreview))
    const parsedOutputTable = computed(() => parseSparkTable(props.preview))

    function triggerEvaluate() {
      activeTab.value = 'output'
      emit('evaluate')
    }

    function switchToOutput() {
      activeTab.value = 'output'
    }

    return { activeTab, parsedInputTable, parsedOutputTable, triggerEvaluate, switchToOutput }
  }
}
</script>

<style scoped>
.output-preview {
  border-top: 1px solid #ddd;
  display: flex;
  flex-direction: column;
  min-height: 120px;
  flex: 1;
}
.output-preview.compact {
  min-height: 100px;
}

/* Tabs */
.preview-tabs {
  display: flex;
  border-bottom: 1px solid #eee;
  background: #fafafa;
  flex-shrink: 0;
}
.preview-tab {
  padding: 6px 14px;
  font-size: 12px;
  color: #666;
  cursor: pointer;
  border-bottom: 2px solid transparent;
  user-select: none;
}
.preview-tab:hover {
  color: #333;
  background: #f0f0f0;
}
.preview-tab.active {
  color: #333;
  font-weight: 600;
  border-bottom-color: #f8c900;
}

/* Evaluate button */
.preview-header {
  padding: 6px 12px;
  background: #fafafa;
  border-bottom: 1px solid #eee;
  flex-shrink: 0;
}
.evaluate-btn {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 5px 14px;
  border: 1px solid #ddd;
  border-radius: 4px;
  background: #f8c900;
  font-size: 12px;
  font-weight: 600;
  cursor: pointer;
  color: #333;
}
.evaluate-btn:hover:not(:disabled) {
  background: #f0be00;
  border-color: #ccc;
}
.evaluate-btn:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}
.play-icon {
  font-size: 10px;
}

/* Content */
.preview-content {
  flex: 1;
  display: flex;
  flex-direction: column;
  overflow: hidden;
  background: #fff;
  min-height: 0;
}
.preview-data {
  font-family: "Consolas", "Monaco", monospace;
  font-size: 11px;
  line-height: 1.4;
  color: #333;
  white-space: pre;
  margin: 0;
  padding: 8px 12px;
  overflow: auto;
  flex: 1;
  min-height: 0;
}
.preview-success {
  color: #2e7d32;
}
.preview-error-msg {
  padding: 12px;
  color: #c62828;
  font-size: 12px;
  line-height: 1.5;
  white-space: pre-wrap;
  word-break: break-word;
}
.preview-loading {
  padding: 12px;
  color: #888;
  font-style: italic;
}
.preview-empty {
  padding: 16px 12px;
  color: #999;
  font-size: 12px;
  text-align: center;
}

/* Data grid table */
.table-wrapper {
  flex: 1;
  overflow: auto;
  min-height: 0;
  border: 1px solid #c8c8c8;
  margin: 4px;
  border-radius: 3px;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.08);
}
.data-grid {
  width: 100%;
  border-collapse: collapse;
  font-family: "Consolas", "Monaco", monospace;
  font-size: 11px;
  border-spacing: 0;
}
.data-grid th {
  position: sticky;
  top: 0;
  z-index: 1;
  background: linear-gradient(to bottom, #f0f0f0, #e4e4e4);
  border-bottom: 2px solid #c0c0c0;
  border-right: 1px solid #d0d0d0;
  padding: 6px 10px;
  text-align: left;
  font-weight: 700;
  font-size: 11px;
  color: #333;
  white-space: nowrap;
}
.data-grid th:last-child {
  border-right: none;
}
.data-grid td {
  padding: 4px 10px;
  border-bottom: 1px solid #e0e0e0;
  border-right: 1px solid #eee;
  color: #333;
  white-space: nowrap;
  max-width: 300px;
  overflow: hidden;
  text-overflow: ellipsis;
}
.data-grid td:last-child {
  border-right: none;
}
.data-grid tbody tr:nth-child(even) {
  background: #f7f9fc;
}
.data-grid tbody tr:nth-child(odd) {
  background: #fff;
}
.data-grid tbody tr:hover {
  background: #e8eef8;
}
.data-grid tbody tr:last-child td {
  border-bottom: none;
}
.row-num-header {
  width: 36px;
  min-width: 36px;
  text-align: center;
  color: #999;
  background: linear-gradient(to bottom, #e8e8e8, #ddd) !important;
  border-right: 2px solid #c0c0c0 !important;
}
.row-num-cell {
  width: 36px;
  min-width: 36px;
  text-align: center;
  color: #999;
  font-size: 10px;
  background: #f0f0f0 !important;
  border-right: 2px solid #d8d8d8 !important;
}
</style>

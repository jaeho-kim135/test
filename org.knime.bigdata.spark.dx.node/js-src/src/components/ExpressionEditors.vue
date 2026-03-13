<template>
  <div class="expression-editors">
    <!-- Tab bar -->
    <div class="tab-bar">
      <div
        v-for="(expr, idx) in expressions"
        :key="idx"
        class="tab"
        :class="{ active: activeTab === idx, empty: !expr || !expr.trim() }"
        role="tab"
        :aria-selected="activeTab === idx"
        :tabindex="activeTab === idx ? 0 : -1"
        @click="switchTab(idx)"
        @keydown.left.prevent="switchTab(Math.max(0, idx - 1))"
        @keydown.right.prevent="switchTab(Math.min(expressions.length - 1, idx + 1))"
      >
        <span class="tab-label">Expression {{ idx + 1 }}</span>
        <span v-if="!expr || !expr.trim()" class="tab-warning" title="Empty expression">!</span>
        <button
          v-if="expressions.length > 1"
          class="tab-close"
          @click.stop="removeExpression(idx)"
          title="Remove this expression"
          aria-label="Remove this expression"
        >&times;</button>
      </div>
      <button class="tab-add" @click="addExpression" title="Add new expression" aria-label="Add new expression">+</button>
    </div>

    <!-- Active expression editor -->
    <div class="editor-area" v-if="expressions.length > 0">
      <div class="editor-wrapper">
        <textarea
          ref="editorTextarea"
          :value="expressions[activeTab]"
          @input="onExpressionInput($event.target.value)"
          @keydown.tab.prevent="insertText('  ')"
          @keydown.ctrl.enter.prevent="$emit('evaluate')"
          @dragover.prevent="onDragOver"
          @dragleave="isDragOver = false"
          @drop.prevent="onDrop"
          class="expression-textarea"
          :class="{ 'drag-over': isDragOver }"
          :aria-label="'Spark SQL expression ' + (activeTab + 1)"
          :placeholder="'-- Write a Spark SQL expression, e.g.\n--   UPPER(`name`)\n--   CAST(`age` AS STRING)\n--   CASE WHEN `salary` > 50000 THEN \'High\' ELSE \'Low\' END'"
          spellcheck="false"
        ></textarea>
      </div>

      <!-- Output configuration -->
      <div class="output-config">
        <div class="output-controls">
          <span class="output-label">Output column:</span>
          <div class="mode-buttons">
            <button
              class="mode-btn"
              :class="{ active: outputModes[activeTab] === 'APPEND' }"
              @click="setOutputMode('APPEND')"
            >Append</button>
            <button
              class="mode-btn"
              :class="{ active: outputModes[activeTab] === 'REPLACE' }"
              @click="setOutputMode('REPLACE')"
            >Replace</button>
          </div>
          <!-- APPEND: free-text input for new column name -->
          <input
            v-if="outputModes[activeTab] === 'APPEND'"
            :value="columnNames[activeTab]"
            @input="onColumnNameInput($event.target.value)"
            class="column-name-input"
            placeholder="New column name"
          />
          <!-- REPLACE: combobox to select existing column -->
          <select
            v-else
            :value="columnNames[activeTab]"
            @change="onColumnNameInput($event.target.value)"
            class="column-name-select"
          >
            <option value="" disabled>Select column to replace</option>
            <option
              v-for="col in columns"
              :key="col.name"
              :value="col.name"
            >{{ col.name }} ({{ col.type }})</option>
          </select>
        </div>
      </div>
    </div>

    <!-- Add expression link -->
    <div class="add-expression-row" v-if="!compact">
      <button class="add-expression-btn" @click="addExpression">
        <span class="add-icon">+</span> Add expression
      </button>
    </div>
  </div>
</template>

<script>
import { ref, watch, nextTick } from 'vue'

export default {
  name: 'ExpressionEditors',
  props: {
    expressions: { type: Array, default: () => [''] },
    outputModes: { type: Array, default: () => ['APPEND'] },
    columnNames: { type: Array, default: () => ['new_column'] },
    columns: { type: Array, default: () => [] },
    compact: { type: Boolean, default: false }
  },
  emits: ['update', 'evaluate'],

  setup(props, { emit }) {
    const activeTab = ref(0)
    const editorTextarea = ref(null)
    const isDragOver = ref(false)

    watch(() => props.expressions.length, (newLen) => {
      if (activeTab.value >= newLen) {
        activeTab.value = Math.max(0, newLen - 1)
      }
    })

    function switchTab(idx) {
      activeTab.value = idx
      nextTick(() => {
        if (editorTextarea.value) {
          editorTextarea.value.focus()
        }
      })
    }

    function emitUpdate(overrides = {}) {
      emit('update', {
        expressions: overrides.expressions || [...props.expressions],
        outputModes: overrides.outputModes || [...props.outputModes],
        columnNames: overrides.columnNames || [...props.columnNames]
      })
    }

    function onExpressionInput(value) {
      const exprs = [...props.expressions]
      exprs[activeTab.value] = value
      emitUpdate({ expressions: exprs })
    }

    function onColumnNameInput(value) {
      const names = [...props.columnNames]
      names[activeTab.value] = value
      emitUpdate({ columnNames: names })
    }

    function generateUniqueName(excludeIdx) {
      // Avoid conflicts with both existing output column names and input column names
      // excludeIdx: skip this tab's name from reserved set (it will be replaced)
      const reserved = new Set()
      props.columnNames.forEach((n, i) => {
        if (i !== excludeIdx) reserved.add(n)
      })
      props.columns.forEach(c => reserved.add(c.name))
      let newName = 'new_column'
      let suffix = 2
      while (reserved.has(newName)) {
        newName = 'new_column_' + suffix++
      }
      return newName
    }

    function setOutputMode(mode) {
      // Skip if already in this mode (avoids overwriting user's custom column name)
      if (props.outputModes[activeTab.value] === mode) return

      const modes = [...props.outputModes]
      modes[activeTab.value] = mode
      const names = [...props.columnNames]
      if (mode === 'REPLACE') {
        // Auto-select first available column when switching to REPLACE
        if (props.columns.length > 0 && (!names[activeTab.value] || !props.columns.some(c => c.name === names[activeTab.value]))) {
          names[activeTab.value] = props.columns[0].name
        }
      } else {
        // Switching to APPEND — generate a unique new column name
        names[activeTab.value] = generateUniqueName(activeTab.value)
      }
      emitUpdate({ outputModes: modes, columnNames: names })
    }

    function addExpression() {
      const exprs = [...props.expressions, '']
      const modes = [...props.outputModes, 'APPEND']
      const names = [...props.columnNames, generateUniqueName()]
      emitUpdate({ expressions: exprs, outputModes: modes, columnNames: names })
      activeTab.value = exprs.length - 1
      nextTick(() => {
        if (editorTextarea.value) {
          editorTextarea.value.focus()
        }
      })
    }

    function removeExpression(idx) {
      if (props.expressions.length <= 1) return
      const exprs = props.expressions.filter((_, i) => i !== idx)
      const modes = props.outputModes.filter((_, i) => i !== idx)
      const names = props.columnNames.filter((_, i) => i !== idx)
      emitUpdate({ expressions: exprs, outputModes: modes, columnNames: names })
      if (idx < activeTab.value) {
        // Removing a tab before active: shift index to keep same expression visible
        activeTab.value--
      } else if (activeTab.value >= exprs.length) {
        // Removed the last tab (was active): go to new last
        activeTab.value = exprs.length - 1
      }
    }

    function insertText(text) {
      const textarea = editorTextarea.value
      if (!textarea) return
      const start = textarea.selectionStart
      const end = textarea.selectionEnd
      const currentValue = props.expressions[activeTab.value] || ''
      const newValue = currentValue.substring(0, start) + text + currentValue.substring(end)
      const exprs = [...props.expressions]
      exprs[activeTab.value] = newValue
      emitUpdate({ expressions: exprs })

      nextTick(() => {
        if (editorTextarea.value) {
          const newPos = start + text.length
          editorTextarea.value.focus()
          editorTextarea.value.setSelectionRange(newPos, newPos)
        }
      })
    }

    function onDragOver(event) {
      event.dataTransfer.dropEffect = 'copy'
      isDragOver.value = true
    }

    function onDrop(event) {
      isDragOver.value = false
      const text = event.dataTransfer.getData('text/plain')
      if (!text) return

      const textarea = editorTextarea.value
      if (!textarea) return

      // Insert at the drop position (caret position under the mouse)
      // Use document.caretPositionFromPoint or caretRangeFromPoint for precise placement
      let insertPos = textarea.selectionStart
      if (document.caretPositionFromPoint) {
        // Standard API (Firefox)
        const pos = document.caretPositionFromPoint(event.clientX, event.clientY)
        if (pos && pos.offsetNode === textarea) {
          insertPos = pos.offset
        }
      } else if (document.caretRangeFromPoint) {
        // WebKit/Blink (Chrome, KNIME AP CEF)
        const range = document.caretRangeFromPoint(event.clientX, event.clientY)
        if (range) {
          insertPos = range.startOffset
        }
      }

      const currentValue = props.expressions[activeTab.value] || ''
      // Clamp insertPos to valid range (caretRangeFromPoint may return invalid offset for textareas in CEF)
      insertPos = Math.max(0, Math.min(insertPos, currentValue.length))
      const newValue = currentValue.substring(0, insertPos) + text + currentValue.substring(insertPos)
      const exprs = [...props.expressions]
      exprs[activeTab.value] = newValue
      emitUpdate({ expressions: exprs })

      nextTick(() => {
        if (editorTextarea.value) {
          const newPos = insertPos + text.length
          editorTextarea.value.focus()
          editorTextarea.value.setSelectionRange(newPos, newPos)
        }
      })
    }

    return {
      activeTab, editorTextarea, isDragOver,
      switchTab, onExpressionInput, onColumnNameInput, setOutputMode,
      addExpression, removeExpression, insertText, onDragOver, onDrop
    }
  }
}
</script>

<style scoped>
.expression-editors {
  display: flex;
  flex-direction: column;
  flex: 1;
  min-height: 0;
  overflow: hidden;
}

/* Tab bar */
.tab-bar {
  display: flex;
  align-items: center;
  border-bottom: 1px solid #ddd;
  background: #fafafa;
  padding: 0 4px;
  flex-shrink: 0;
  overflow-x: auto;
  scrollbar-width: thin; /* Firefox */
}
.tab-bar::-webkit-scrollbar {
  height: 3px;
}
.tab-bar::-webkit-scrollbar-thumb {
  background: #ccc;
  border-radius: 2px;
}
.tab-bar::-webkit-scrollbar-track {
  background: transparent;
}
.tab {
  display: flex;
  align-items: center;
  padding: 6px 12px;
  cursor: pointer;
  border-bottom: 2px solid transparent;
  white-space: nowrap;
  font-size: 12px;
  color: #666;
  user-select: none;
}
.tab:hover {
  color: #333;
  background: #f0f0f0;
}
.tab.active {
  color: #333;
  font-weight: 600;
  border-bottom-color: #f8c900;
}
.tab-label {
  margin-right: 4px;
}
.tab.empty .tab-label {
  color: #e65100;
}
.tab-warning {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 14px;
  height: 14px;
  border-radius: 50%;
  background: #ff9800;
  color: #fff;
  font-size: 9px;
  font-weight: bold;
  margin-right: 2px;
  flex-shrink: 0;
}
.tab-close {
  background: none;
  border: none;
  font-size: 14px;
  color: #999;
  cursor: pointer;
  padding: 0 2px;
  line-height: 1;
}
.tab-close:hover {
  color: #e53935;
}
.tab-add {
  background: none;
  border: none;
  font-size: 16px;
  font-weight: bold;
  color: #888;
  cursor: pointer;
  padding: 4px 8px;
  margin-left: 4px;
}
.tab-add:hover {
  color: #333;
  background: #eee;
  border-radius: 4px;
}

/* Editor area */
.editor-area {
  flex: 1;
  display: flex;
  flex-direction: column;
  min-height: 0;
}
.editor-wrapper {
  flex: 1;
  min-height: 100px;
  position: relative;
}
.expression-textarea {
  width: 100%;
  height: 100%;
  padding: 10px 12px;
  font-family: "Consolas", "Monaco", "Courier New", monospace;
  font-size: 13px;
  line-height: 1.5;
  border: none;
  outline: none;
  resize: none;
  background: #fff;
  color: #333;
  tab-size: 4;
}
.expression-textarea::placeholder {
  color: #bbb;
}
.expression-textarea:focus {
  background: #fffef5;
}
.expression-textarea.drag-over {
  background: #f0f7ff;
  outline: 2px dashed #90caf9;
  outline-offset: -2px;
}

/* Output config */
.output-config {
  border-top: 1px solid #eee;
  padding: 8px 12px;
  background: #fafafa;
  flex-shrink: 0;
}
.output-label {
  font-size: 12px;
  font-weight: 600;
  color: #555;
  white-space: nowrap;
}
.output-controls {
  display: flex;
  align-items: center;
  gap: 6px 8px;
  flex-wrap: wrap;
}
.mode-buttons {
  display: flex;
  border: 1px solid #ddd;
  border-radius: 4px;
  overflow: hidden;
}
.mode-btn {
  padding: 4px 14px;
  border: none;
  background: #fff;
  font-size: 12px;
  cursor: pointer;
  color: #666;
}
.mode-btn:first-child {
  border-right: 1px solid #ddd;
}
.mode-btn.active {
  background: #f8c900;
  color: #333;
  font-weight: 600;
}
.mode-btn:hover:not(.active) {
  background: #f5f5f5;
}
.column-name-input {
  flex: 1;
  min-width: 120px;
  padding: 4px 8px;
  border: 1px solid #ddd;
  border-radius: 4px;
  font-family: "Consolas", "Monaco", monospace;
  font-size: 12px;
  outline: none;
}
.column-name-input:focus {
  border-color: #f8c900;
}
.column-name-select {
  flex: 1;
  min-width: 120px;
  padding: 4px 8px;
  border: 1px solid #e0b800;
  border-radius: 4px;
  font-family: "Consolas", "Monaco", monospace;
  font-size: 12px;
  outline: none;
  background: #fffef5;
  cursor: pointer;
}
.column-name-select:focus {
  border-color: #f8c900;
  background: #fff;
}

/* Add expression row */
.add-expression-row {
  padding: 6px 12px;
  border-top: 1px solid #eee;
  flex-shrink: 0;
}
.add-expression-btn {
  background: none;
  border: none;
  color: #1a73e8;
  font-size: 13px;
  cursor: pointer;
  padding: 4px 0;
}
.add-expression-btn:hover {
  text-decoration: underline;
}
.add-icon {
  font-weight: bold;
  margin-right: 4px;
}
</style>

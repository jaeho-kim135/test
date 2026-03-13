<template>
  <div class="input-columns">
    <div class="panel-header">
      <span class="panel-title">Input Columns</span>
      <span class="col-count" v-if="columns.length">({{ columns.length }})</span>
    </div>
    <div class="search-box" v-if="columns.length > 5">
      <input
        v-model="searchQuery"
        type="text"
        placeholder="Filter columns..."
        class="search-input"
      />
    </div>
    <div class="column-list">
      <!-- Built-in row metadata -->
      <div class="section-label" v-if="!searchQuery">Row</div>
      <div
        v-for="meta in filteredMeta"
        :key="meta.name"
        class="column-item meta-item"
        draggable="true"
        @dragstart="onDragStart($event, meta.template)"
        @dblclick="$emit('insert', meta.template)"
        :title="meta.tooltip"
      >
        <span class="col-icon meta-icon">R</span>
        <span class="col-name">{{ meta.name }}</span>
        <span class="col-type">{{ meta.type }}</span>
      </div>
      <!-- Data columns -->
      <div class="section-label" v-if="!searchQuery && columns.length">Columns</div>
      <div
        v-for="col in filteredColumns"
        :key="col.name"
        class="column-item"
        draggable="true"
        @dragstart="onDragStartColumn($event, col)"
        @dblclick="insertColumn(col)"
        :title="'Drag or double-click to insert: `' + col.name + '`'"
      >
        <span class="col-icon" :class="typeClass(col.type)">{{ typeIcon(col.type) }}</span>
        <span class="col-name">{{ col.name }}</span>
        <span class="col-type">{{ col.type }}</span>
      </div>
      <div v-if="!columns.length && !rowMeta.length" class="empty-hint">
        No columns available
      </div>
      <div v-else-if="searchQuery && !filteredColumns.length && !filteredMeta.length" class="empty-hint">
        No matching columns
      </div>
    </div>
  </div>
</template>

<script>
import { ref, computed } from 'vue'

export default {
  name: 'InputColumns',
  props: {
    columns: { type: Array, default: () => [] }
  },
  emits: ['insert'],
  setup(props, { emit }) {
    const searchQuery = ref('')

    const rowMeta = [
      { name: 'ROW_NUMBER', type: 'Long', template: 'ROW_NUMBER() OVER (ORDER BY MONOTONICALLY_INCREASING_ID())', tooltip: 'Row number (1-based, requires ORDER BY)' },
      { name: 'ROW_ID', type: 'Long', template: 'MONOTONICALLY_INCREASING_ID()', tooltip: 'Unique row ID (not consecutive, partition-local)' }
    ]

    const filteredMeta = computed(() => {
      const q = searchQuery.value.toLowerCase().trim()
      if (!q) return rowMeta
      return rowMeta.filter(m => m.name.toLowerCase().includes(q))
    })

    const filteredColumns = computed(() => {
      const q = searchQuery.value.toLowerCase().trim()
      if (!q) return props.columns
      return props.columns.filter(col =>
        col.name.toLowerCase().includes(q) || col.type.toLowerCase().includes(q)
      )
    })

    function typeIcon(type) {
      const t = (type || '').toLowerCase()
      if (t.includes('string') || t.includes('char') || t.includes('varchar')) return 'S'
      if (t.includes('int') || t.includes('short') || t.includes('byte')) return 'I'
      if (t.includes('long') || t.includes('bigint')) return 'L'
      if (t.includes('double') || t.includes('float') || t.includes('decimal') || t.includes('numeric')) return 'D'
      if (t.includes('bool')) return 'B'
      if (t.includes('date') || t.includes('timestamp')) return 'T'
      if (t.includes('array') || t.includes('map') || t.includes('struct')) return '{}'
      if (t.includes('binary')) return 'X'
      return '?'
    }

    function typeClass(type) {
      const t = (type || '').toLowerCase()
      if (t.includes('string') || t.includes('char')) return 'type-string'
      if (t.includes('int') || t.includes('short') || t.includes('byte') || t.includes('long') || t.includes('bigint')) return 'type-int'
      if (t.includes('double') || t.includes('float') || t.includes('decimal') || t.includes('numeric')) return 'type-double'
      if (t.includes('bool')) return 'type-bool'
      if (t.includes('date') || t.includes('timestamp')) return 'type-date'
      return 'type-other'
    }

    function insertColumn(col) {
      // Always backtick-wrap to distinguish columns from functions in Spark SQL
      emit('insert', '`' + col.name + '`')
    }

    function onDragStartColumn(event, col) {
      event.dataTransfer.setData('text/plain', '`' + col.name + '`')
      event.dataTransfer.effectAllowed = 'copy'
    }

    function onDragStart(event, text) {
      event.dataTransfer.setData('text/plain', text)
      event.dataTransfer.effectAllowed = 'copy'
    }

    return { searchQuery, rowMeta, filteredMeta, filteredColumns, typeIcon, typeClass, insertColumn, onDragStartColumn, onDragStart }
  }
}
</script>

<style scoped>
.input-columns {
  display: flex;
  flex-direction: column;
  flex: 1;
  min-height: 0;
  overflow: hidden;
}
.panel-header {
  padding: 8px 10px;
  border-bottom: 1px solid #e0e0e0;
  background: #fafafa;
  display: flex;
  align-items: center;
  gap: 4px;
}
.panel-title {
  font-weight: 600;
  font-size: 12px;
  color: #555;
  text-transform: uppercase;
  letter-spacing: 0.5px;
}
.col-count {
  font-size: 11px;
  color: #999;
}
.search-box {
  padding: 6px 8px;
  border-bottom: 1px solid #eee;
}
.search-input {
  width: 100%;
  padding: 4px 8px;
  border: 1px solid #ddd;
  border-radius: 4px;
  font-size: 12px;
  outline: none;
}
.search-input:focus {
  border-color: #f8c900;
}
.column-list {
  flex: 1;
  overflow-y: auto;
  padding: 2px 0;
}
.section-label {
  padding: 4px 10px 2px;
  font-size: 10px;
  font-weight: 600;
  color: #999;
  text-transform: uppercase;
  letter-spacing: 0.5px;
}
.column-item {
  display: flex;
  align-items: center;
  padding: 3px 10px;
  cursor: grab;
  user-select: none;
  gap: 5px;
}
.column-item:active {
  cursor: grabbing;
}
.column-item:hover {
  background: #f0f4ff;
}
.col-icon {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 16px;
  height: 16px;
  border-radius: 3px;
  font-size: 9px;
  font-weight: bold;
  color: #fff;
  flex-shrink: 0;
}
.col-icon.type-string { background: #4caf50; }
.col-icon.type-int { background: #2196f3; }
.col-icon.type-double { background: #ff9800; }
.col-icon.type-bool { background: #9c27b0; }
.col-icon.type-date { background: #e91e63; }
.col-icon.type-other { background: #757575; }
.col-icon.meta-icon { background: #607d8b; }
.col-name {
  font-family: "Consolas", "Monaco", monospace;
  font-size: 12px;
  color: #333;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  flex: 1;
}
.meta-item .col-name {
  color: #607d8b;
  font-style: italic;
}
.col-type {
  font-size: 10px;
  color: #999;
  flex-shrink: 0;
}
.empty-hint {
  padding: 16px;
  color: #999;
  font-style: italic;
  text-align: center;
  font-size: 12px;
}
</style>

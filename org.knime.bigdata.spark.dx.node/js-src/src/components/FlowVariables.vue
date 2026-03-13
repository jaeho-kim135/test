<template>
  <div class="flow-variables">
    <div class="panel-header" @click="expanded = !expanded">
      <span class="expand-arrow">{{ expanded ? '▾' : '▸' }}</span>
      <span class="panel-title">Flow Variables</span>
      <span class="var-count" v-if="variables.length">({{ variables.length }})</span>
    </div>
    <div class="variable-list" v-if="expanded">
      <div
        v-for="v in variables"
        :key="v.name"
        class="variable-item"
        draggable="true"
        @dragstart="onDragStart($event, v)"
        @dblclick="insertVariable(v)"
        :title="v.name + ' (' + v.type + ') — Drag or double-click to insert as $${' + v.name + '}'"
      >
        <span class="var-icon" :class="'type-' + v.type.toLowerCase()">{{ typeIcon(v.type) }}</span>
        <span class="var-name">{{ v.name }}</span>
        <span class="var-type">{{ v.type }}</span>
      </div>
      <div v-if="!variables.length" class="empty-hint">
        No flow variables available
      </div>
    </div>
  </div>
</template>

<script>
import { ref } from 'vue'

export default {
  name: 'FlowVariables',
  props: {
    variables: { type: Array, default: () => [] }
  },
  emits: ['insert'],
  setup(props, { emit }) {
    const expanded = ref(true)

    function typeIcon(type) {
      switch (type) {
        case 'STRING': return 'S'
        case 'INTEGER': return 'I'
        case 'DOUBLE': return 'D'
        case 'BOOLEAN': return 'B'
        case 'LONG': return 'L'
        default: return '?'
      }
    }

    function insertVariable(v) {
      // Insert as $${varName} — visually distinct from columns (backticks) and functions.
      emit('insert', '$${' + v.name + '}')
    }

    function onDragStart(event, v) {
      event.dataTransfer.setData('text/plain', '$${' + v.name + '}')
      event.dataTransfer.effectAllowed = 'copy'
    }

    return { expanded, typeIcon, insertVariable, onDragStart }
  }
}
</script>

<style scoped>
.flow-variables {
  display: flex;
  flex-direction: column;
  overflow: hidden;
  border-top: 1px solid #e0e0e0;
  flex-shrink: 1;
  max-height: 40%;
}
.panel-header {
  padding: 8px 10px;
  border-bottom: 1px solid #e0e0e0;
  background: #fafafa;
  cursor: pointer;
  user-select: none;
  display: flex;
  align-items: center;
  gap: 4px;
}
.panel-header:hover {
  background: #f0f0f0;
}
.expand-arrow {
  font-size: 10px;
  width: 14px;
  color: #888;
}
.panel-title {
  font-weight: 600;
  font-size: 12px;
  color: #555;
  text-transform: uppercase;
  letter-spacing: 0.5px;
}
.var-count {
  font-size: 11px;
  color: #999;
}
.variable-list {
  overflow-y: auto;
  padding: 4px 0;
}
.variable-item {
  display: flex;
  align-items: center;
  padding: 3px 10px;
  cursor: grab;
  user-select: none;
  gap: 6px;
}
.variable-item:active {
  cursor: grabbing;
}
.variable-item:hover {
  background: #f0f4ff;
}
.var-icon {
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
.var-icon.type-string { background: #4caf50; }
.var-icon.type-integer { background: #2196f3; }
.var-icon.type-double { background: #ff9800; }
.var-icon.type-boolean { background: #9c27b0; }
.var-icon.type-long { background: #00bcd4; }
.var-icon { background: #757575; }
.var-name {
  font-family: "Consolas", "Monaco", monospace;
  font-size: 12px;
  color: #333;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  flex: 1;
}
.var-type {
  font-size: 10px;
  color: #999;
  flex-shrink: 0;
}
.empty-hint {
  padding: 12px;
  color: #999;
  font-style: italic;
  text-align: center;
  font-size: 12px;
}
</style>

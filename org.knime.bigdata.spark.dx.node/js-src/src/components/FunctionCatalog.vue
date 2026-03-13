<template>
  <div class="function-catalog">
    <div class="panel-header">
      <span class="panel-title">Function Catalog</span>
    </div>
    <div class="search-box">
      <input
        v-model="searchQuery"
        type="text"
        placeholder="Search functions..."
        class="search-input"
      />
    </div>
    <div class="catalog-body">
      <div class="catalog-list" :class="{ 'has-detail': selectedFn }">
        <template v-for="category in filteredCatalog" :key="category.name">
          <div class="category-header" @click="toggleCategory(category.name)">
            <span class="category-arrow">{{ expandedCategories[category.name] ? '▾' : '▸' }}</span>
            {{ category.name }}
            <span class="fn-count">({{ category.functions.length }})</span>
          </div>
          <template v-if="expandedCategories[category.name]">
            <div
              v-for="fn in category.functions"
              :key="fn.label"
              class="function-item"
              :class="{ selected: selectedFn && selectedFn.label === fn.label }"
              draggable="true"
              @dragstart="onDragStart($event, fn.template)"
              @click="selectFunction(fn)"
              @dblclick="insertFunction(fn.template)"
              :title="'Drag or double-click to insert'"
            >
              {{ fn.label }}
            </div>
          </template>
        </template>
      </div>
      <!-- Function detail panel -->
      <div class="fn-detail" v-if="selectedFn">
        <div class="fn-detail-header">{{ selectedFn.label }}</div>
        <div class="fn-detail-desc" v-if="selectedFn.description">{{ selectedFn.description }}</div>
        <div class="fn-detail-desc" v-else>No description available.</div>
        <div class="fn-detail-usage" v-if="selectedFn.template">
          <span class="usage-label">Usage:</span>
          <code class="usage-code">{{ selectedFn.template }}</code>
        </div>
        <button class="fn-insert-btn" @click="insertFunction(selectedFn.template)">Insert</button>
      </div>
    </div>
  </div>
</template>

<script>
import { ref, reactive, computed, watch } from 'vue'

export default {
  name: 'FunctionCatalog',
  props: {
    catalog: { type: Array, default: () => [] }
  },
  emits: ['insert'],
  setup(props, { emit }) {
    const searchQuery = ref('')
    const expandedCategories = reactive({})
    const selectedFn = ref(null)

    watch(() => props.catalog, (newCatalog) => {
      if (newCatalog) {
        newCatalog.forEach(c => {
          if (!(c.name in expandedCategories)) {
            expandedCategories[c.name] = false
          }
        })
      }
    }, { immediate: true })

    const filteredCatalog = computed(() => {
      const q = searchQuery.value.toLowerCase().trim()
      if (!q) return props.catalog

      return props.catalog
        .map(cat => ({
          ...cat,
          functions: cat.functions.filter(fn =>
            fn.label.toLowerCase().includes(q) ||
            (fn.description && fn.description.toLowerCase().includes(q))
          )
        }))
        .filter(cat => cat.functions.length > 0)
    })

    // Auto-expand categories that match search, and clear selection if filtered away
    watch(filteredCatalog, (cats) => {
      if (searchQuery.value.trim()) {
        cats.forEach(cat => { expandedCategories[cat.name] = true })
        // Clear selection if the selected function is no longer visible
        if (selectedFn.value) {
          const stillVisible = cats.some(cat =>
            cat.functions.some(fn => fn.label === selectedFn.value.label)
          )
          if (!stillVisible) selectedFn.value = null
        }
      }
    })

    function toggleCategory(name) {
      expandedCategories[name] = !expandedCategories[name]
    }

    function selectFunction(fn) {
      // Toggle: clicking the same function deselects it
      if (selectedFn.value && selectedFn.value.label === fn.label) {
        selectedFn.value = null
      } else {
        selectedFn.value = fn
      }
    }

    function insertFunction(template) {
      emit('insert', template)
    }

    function onDragStart(event, template) {
      event.dataTransfer.setData('text/plain', template)
      event.dataTransfer.effectAllowed = 'copy'
    }

    return { searchQuery, expandedCategories, filteredCatalog, selectedFn, toggleCategory, selectFunction, insertFunction, onDragStart }
  }
}
</script>

<style scoped>
.function-catalog {
  display: flex;
  flex-direction: column;
  height: 100%;
  overflow: hidden;
}
.panel-header {
  padding: 8px 10px;
  border-bottom: 1px solid #e0e0e0;
  background: #fafafa;
}
.panel-title {
  font-weight: 600;
  font-size: 12px;
  color: #555;
  text-transform: uppercase;
  letter-spacing: 0.5px;
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
.catalog-body {
  flex: 1;
  display: flex;
  flex-direction: column;
  min-height: 0;
  overflow: hidden;
}
.catalog-list {
  flex: 1;
  overflow-y: auto;
  padding: 4px 0;
}
.catalog-list.has-detail {
  flex: 1;
  min-height: 100px;
}
.category-header {
  padding: 6px 10px;
  font-weight: 600;
  font-size: 12px;
  color: #555;
  background: #f5f5f5;
  cursor: pointer;
  user-select: none;
}
.category-header:hover {
  background: #eee;
}
.fn-count {
  font-weight: 400;
  font-size: 11px;
  color: #999;
  margin-left: 2px;
}
.category-arrow {
  display: inline-block;
  width: 14px;
  font-size: 10px;
}
.function-item {
  padding: 3px 10px 3px 24px;
  font-family: "Consolas", "Monaco", monospace;
  font-size: 11px;
  cursor: grab;
  user-select: none;
  color: #444;
}
.function-item:active {
  cursor: grabbing;
}
.function-item:hover {
  background: #f0f4ff;
  color: #1a73e8;
}
.function-item.selected {
  background: #e3f2fd;
  color: #1565c0;
  font-weight: 600;
}

/* Detail panel */
.fn-detail {
  border-top: 1px solid #ddd;
  padding: 12px;
  background: #fafafa;
  flex-shrink: 0;
  max-height: 300px;
  overflow-y: auto;
}
.fn-detail-header {
  font-family: "Consolas", "Monaco", monospace;
  font-size: 12px;
  font-weight: 700;
  color: #1565c0;
  margin-bottom: 6px;
}
.fn-detail-desc {
  font-size: 12px;
  line-height: 1.6;
  color: #444;
  margin-bottom: 10px;
  padding: 6px 8px;
  background: #fff;
  border-left: 3px solid #e3f2fd;
  border-radius: 2px;
}
.fn-detail-usage {
  margin-bottom: 8px;
}
.usage-label {
  font-size: 11px;
  font-weight: 600;
  color: #777;
  display: block;
  margin-bottom: 2px;
}
.usage-code {
  display: block;
  font-family: "Consolas", "Monaco", monospace;
  font-size: 11px;
  color: #333;
  background: #fff;
  border: 1px solid #e0e0e0;
  border-radius: 3px;
  padding: 4px 8px;
}
.fn-insert-btn {
  display: inline-flex;
  align-items: center;
  padding: 3px 12px;
  border: 1px solid #ddd;
  border-radius: 4px;
  background: #fff;
  font-size: 11px;
  font-weight: 600;
  cursor: pointer;
  color: #1565c0;
}
.fn-insert-btn:hover {
  background: #e3f2fd;
  border-color: #90caf9;
}
</style>

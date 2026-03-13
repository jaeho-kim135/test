/**
 * KNIME WebUI communication bridge for Node Dialogs.
 *
 * Uses @knime/ui-extension-service which communicates with KNIME AP
 * via postMessage protocol (not window.jsonDataService).
 *
 * Key services:
 *   JsonDataService  — initialData(), data(), applyData()
 *   DialogService    — setApplyListener(), registerSettings()
 *
 * Falls back to mock data when running outside KNIME (dev mode).
 */

const LOG_PREFIX = '[KNIME]'

let jsonDataService = null
let dialogService = null
let isKnimeMode = false
let modelSettingState = null

// ── Public API ──────────────────────────────────────────────────────────

/**
 * Initialize the KNIME service bridge and load initial data.
 * Uses @knime/ui-extension-service for proper postMessage-based communication.
 *
 * @returns {Promise<Object>} parsed initial data with {settings, initialData}
 */
export async function initKnimeService() {
  try {
    // Dynamic import so Vite can tree-shake it in dev mode if needed
    const { JsonDataService, DialogService } = await import('@knime/ui-extension-service')

    jsonDataService = await JsonDataService.getInstance()
    dialogService = await DialogService.getInstance()
    isKnimeMode = true
    console.log(LOG_PREFIX, 'Services initialized (JsonDataService + DialogService)')

    // Load initial data from ScriptingNodeSettingsService.fromNodeSettings()
    const data = await jsonDataService.initialData()
    console.log(LOG_PREFIX, 'Initial data loaded:', data ? Object.keys(data) : '(empty)')
    return data || {}
  } catch (e) {
    console.info(LOG_PREFIX, '[Dev Mode] Not in KNIME AP, using mock data.', e?.message || '')
    isKnimeMode = false
    return getMockInitialData()
  }
}

/**
 * Register a model setting with the DialogService for dirty-state tracking.
 * Must be called after initKnimeService() and before the user edits settings.
 *
 * @param {Object} initialSettings - the initial settings loaded from backend
 */
export function registerModelSetting(initialSettings) {
  if (!dialogService) return
  try {
    const createSetting = dialogService.registerSettings("model")
    modelSettingState = createSetting({
      initialValue: JSON.parse(JSON.stringify(initialSettings))
    })
    console.log(LOG_PREFIX, 'Model setting registered for dirty-state tracking')
  } catch (e) {
    console.warn(LOG_PREFIX, 'Failed to register model setting:', e?.message)
  }
}

/**
 * Signal to KNIME that the dialog settings have been modified.
 * This enables the Apply / OK buttons by updating the tracked SettingState.
 *
 * @param {Object} currentSettings - the current (modified) settings
 */
export function markDirty(currentSettings) {
  if (modelSettingState) {
    modelSettingState.setValue(JSON.parse(JSON.stringify(currentSettings)))
  }
}

/**
 * Set up the apply listener for OK/Apply buttons.
 * When the user clicks OK or Apply in the KNIME dialog, this callback is invoked.
 * The callback should return the current settings object.
 *
 * Internally uses DialogService.setApplyListener() which listens for
 * "ApplyDataEvent" push events from the KNIME AP embedder.
 *
 * @param {Function} getSettingsCallback - returns current settings object (plain JS, not reactive)
 */
export function setApplyListener(getSettingsCallback) {
  if (dialogService && jsonDataService) {
    dialogService.setApplyListener(async () => {
      const settings = getSettingsCallback()
      console.log(LOG_PREFIX, 'Apply triggered, sending settings to backend')
      return jsonDataService.applyData(settings)
    })
    console.log(LOG_PREFIX, 'Apply listener registered')
  } else {
    console.log(LOG_PREFIX, 'Apply listener skipped (dev mode)')
  }
}

/**
 * Call an RPC service method on the KNIME backend.
 * Uses JSON-RPC protocol over the KNIME data service bridge.
 *
 * @param {string} service - Service name (e.g., 'SparkExpressionService')
 * @param {string} method - Method name (e.g., 'evaluateExpressions')
 * @param {Array} params - Method parameters (must be plain JS values, not Vue reactive proxies)
 * @returns {Promise<any>} RPC result
 */
export async function callRpc(service, method, params = []) {
  if (jsonDataService) {
    try {
      const result = await jsonDataService.data({
        method: `${service}.${method}`,
        options: params
      })
      return result || { success: false, error: 'Empty response from backend' }
    } catch (e) {
      console.error(LOG_PREFIX, 'RPC call failed:', `${service}.${method}`, e)
      return { success: false, error: e?.message || 'RPC call failed' }
    }
  }
  console.warn(LOG_PREFIX, 'RPC call in dev mode:', `${service}.${method}`)
  return mockRpcCall(service, method, params)
}

/**
 * @returns {boolean} true if running inside KNIME AP
 */
export function isInKnime() {
  return isKnimeMode
}

// ── Mock data for development ───────────────────────────────────────────

function getMockInitialData() {
  return {
    settings: {
      expressions: [''],
      outputModes: ['APPEND'],
      columnNames: ['new_column']
    },
    initialData: {
      columnNamesAndTypes: [
        { name: 'id', type: 'Integer' },
        { name: 'name', type: 'String' },
        { name: 'age', type: 'Integer' },
        { name: 'salary', type: 'Double' },
        { name: 'dept', type: 'String' },
        { name: 'hire_date', type: 'Date' },
        { name: 'is_active', type: 'Boolean' }
      ],
      flowVariables: [
        { name: 'knime.workspace', type: 'STRING' }
      ],
      functionCatalog: [
        { name: 'String', functions: [
          { label: 'UPPER(col)', template: 'UPPER()', description: 'Converts a string to uppercase.' },
          { label: 'LOWER(col)', template: 'LOWER()', description: 'Converts a string to lowercase.' },
          { label: 'TRIM(col)', template: 'TRIM()', description: 'Removes leading and trailing whitespace.' },
          { label: 'LENGTH(col)', template: 'LENGTH()', description: 'Returns the character length of a string.' },
          { label: 'CONCAT(a, b, ...)', template: 'CONCAT(, )', description: 'Concatenates multiple strings together.' },
          { label: 'SUBSTRING(col, pos, len)', template: 'SUBSTRING(, 1, )', description: 'Extracts a substring.' },
          { label: 'REPLACE(col, old, new)', template: "REPLACE(, '', '')", description: 'Replaces all occurrences.' },
          { label: 'INITCAP(col)', template: 'INITCAP()', description: 'Capitalizes the first letter of each word.' }
        ]},
        { name: 'Math', functions: [
          { label: 'ABS(col)', template: 'ABS()', description: 'Returns the absolute value.' },
          { label: 'ROUND(col, scale)', template: 'ROUND(, 2)', description: 'Rounds a number.' },
          { label: 'CEIL(col)', template: 'CEIL()', description: 'Rounds up to nearest integer.' },
          { label: 'FLOOR(col)', template: 'FLOOR()', description: 'Rounds down to nearest integer.' }
        ]},
        { name: 'Date/Time', functions: [
          { label: 'CURRENT_DATE()', template: 'CURRENT_DATE()', description: 'Returns the current date.' },
          { label: 'TO_DATE(col, fmt)', template: "TO_DATE(, 'yyyy-MM-dd')", description: 'Parses a string to date.' },
          { label: 'YEAR(col)', template: 'YEAR()', description: 'Extracts the year.' }
        ]},
        { name: 'Type Cast', functions: [
          { label: 'CAST(col AS type)', template: 'CAST( AS STRING)', description: 'Converts to the specified type.' },
          { label: 'STRING(col)', template: 'STRING()', description: 'Casts to string.' },
          { label: 'INT(col)', template: 'INT()', description: 'Casts to integer.' }
        ]},
        { name: 'Conditional', functions: [
          { label: 'CASE WHEN ... THEN ... END', template: 'CASE WHEN  THEN  ELSE  END', description: 'Conditional expression.' },
          { label: 'IF(cond, true, false)', template: 'IF(, , )', description: 'If-then-else.' },
          { label: 'COALESCE(a, b, ...)', template: 'COALESCE(, )', description: 'Returns the first non-null.' }
        ]}
      ]
    }
  }
}

function mockRpcCall(service, method, params) {
  console.info(`[Dev Mock] RPC: ${service}.${method}`, params)
  if (method === 'previewInputTable') {
    return {
      success: true,
      preview: '+---+-------+---+-------+-----+----------+---------+\n| id|   name|age| salary| dept| hire_date|is_active|\n+---+-------+---+-------+-----+----------+---------+\n|  1|  Alice| 30|50000.0|   IT|2020-01-15|     true|\n|  2|    Bob| 25|45000.0|   HR|2021-03-20|     true|\n|  3|Charlie| 35|60000.0|   IT|2019-06-10|     true|\n|  4|  Diana| 28|48000.0|   HR|2022-01-05|    false|\n|  5|    Eve| 32|55000.0|   IT|2020-09-12|     true|\n|  6|  Frank| 29|47000.0|Sales|2021-07-22|     true|\n|  7|  Grace| 40|70000.0|   IT|2018-03-01|     true|\n|  8|  Henry| 27|43000.0|   HR|2022-11-15|    false|\n|  9|   Iris| 33|58000.0|Sales|2019-12-01|     true|\n| 10|   Jack| 31|52000.0|   IT|2020-05-18|     true|\n+---+-------+---+-------+-----+----------+---------+'
    }
  }
  return {
    success: true,
    preview: '+---+-------+---+-------+-----+----------+---------+----------+\n| id|   name|age| salary| dept| hire_date|is_active|new_column|\n+---+-------+---+-------+-----+----------+---------+----------+\n|  1|  Alice| 30|50000.0|   IT|2020-01-15|     true|     ALICE|\n|  2|    Bob| 25|45000.0|   HR|2021-03-20|     true|       BOB|\n|  3|Charlie| 35|60000.0|   IT|2019-06-10|     true|   CHARLIE|\n|  4|  Diana| 28|48000.0|   HR|2022-01-05|    false|     DIANA|\n|  5|    Eve| 32|55000.0|   IT|2020-09-12|     true|       EVE|\n|  6|  Frank| 29|47000.0|Sales|2021-07-22|     true|     FRANK|\n|  7|  Grace| 40|70000.0|   IT|2018-03-01|     true|     GRACE|\n|  8|  Henry| 27|43000.0|   HR|2022-11-15|    false|     HENRY|\n|  9|   Iris| 33|58000.0|Sales|2019-12-01|     true|      IRIS|\n| 10|   Jack| 31|52000.0|   IT|2020-05-18|     true|      JACK|\n+---+-------+---+-------+-----+----------+---------+----------+',
    expressionCount: params[0] ? params[0].length : 1
  }
}

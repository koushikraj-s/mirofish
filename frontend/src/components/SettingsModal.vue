<template>
  <div class="settings-trigger-wrap">
    <button class="settings-trigger" @click="openModal" :title="$t('settings.triggerLabel')">
      <span class="gear-icon">⚙</span>
      <span class="trigger-text">{{ $t('settings.triggerLabel') }}</span>
    </button>

    <Teleport to="body">
      <Transition name="modal">
        <div v-if="open" class="settings-modal-overlay" @click.self="closeModal">
          <div class="settings-modal">
            <div class="modal-header">
              <span class="modal-title">{{ $t('settings.title') }}</span>
              <button class="close-btn" @click="closeModal">×</button>
            </div>

            <div class="modal-body">
              <!-- Loading -->
              <div v-if="loading" class="state-panel">
                <span class="loading-spinner"></span>
                <span>{{ $t('settings.loading') }}</span>
              </div>

              <!-- API unavailable -->
              <div v-else-if="apiUnavailable" class="state-panel unavailable">
                <span class="state-icon">⚠</span>
                <p class="state-title">{{ $t('settings.apiUnavailableTitle') }}</p>
                <p class="state-desc">{{ $t('settings.apiUnavailableDesc') }}</p>
                <button class="secondary-btn" @click="fetchSettings">{{ $t('settings.retry') }}</button>
              </div>

              <!-- Load error (non-404) -->
              <div v-else-if="loadError" class="state-panel error">
                <span class="state-icon">⚠</span>
                <p class="state-desc">{{ loadError }}</p>
                <button class="secondary-btn" @click="fetchSettings">{{ $t('settings.retry') }}</button>
              </div>

              <!-- Form -->
              <div v-else class="settings-form">
                <p class="apply-note">{{ $t('settings.applyNote') }}</p>

                <!-- LLM Section -->
                <div class="settings-section">
                  <div class="section-header">
                    <span class="section-title">{{ $t('settings.sectionLlm') }}</span>
                  </div>

                  <div class="field-group" v-for="field in llmFields" :key="field.key">
                    <label class="field-label">
                      {{ $t(field.labelKey) }}
                      <span class="source-badge" :class="'source-' + sourceFor(field.key)">
                        {{ $t('settings.source.' + sourceFor(field.key)) }}
                      </span>
                    </label>

                    <select
                      v-if="field.type === 'select'"
                      v-model="form[field.key]"
                      class="field-input"
                    >
                      <option v-for="opt in reasoningOptions" :key="opt" :value="opt">
                        {{ opt === '' ? t('settings.reasoningUnset') : opt }}
                      </option>
                    </select>
                    <input
                      v-else
                      class="field-input"
                      :type="field.secret ? 'password' : 'text'"
                      v-model="form[field.key]"
                      @input="field.secret && (touched[field.key] = true)"
                      :placeholder="fieldPlaceholder(field)"
                      autocomplete="off"
                    />
                    <span v-if="field.secret" class="field-hint">{{ $t('settings.secretHint') }}</span>
                  </div>

                  <div class="test-row">
                    <button class="secondary-btn" :disabled="testingLlm" @click="runLlmTest">
                      {{ testingLlm ? $t('settings.testing') : $t('settings.testButton') }}
                    </button>
                    <div v-if="llmTestResult" class="test-result" :class="llmTestResult.ok ? 'ok' : 'fail'">
                      <span class="result-badge">{{ llmTestResult.ok ? $t('settings.testOk') : $t('settings.testFail') }}</span>
                      <span class="result-category">{{ $t('settings.testCategory.' + (llmTestResult.category || 'unknown')) }}</span>
                      <span v-if="llmTestResult.latency_ms != null" class="result-latency">{{ $t('settings.latencyMs', { ms: llmTestResult.latency_ms }) }}</span>
                      <p class="result-detail">{{ llmTestResult.detail }}</p>
                    </div>
                  </div>
                </div>

                <!-- Neo4j Section -->
                <div class="settings-section">
                  <div class="section-header">
                    <span class="section-title">{{ $t('settings.sectionNeo4j') }}</span>
                  </div>

                  <div class="field-group" v-for="field in neo4jFields" :key="field.key">
                    <label class="field-label">
                      {{ $t(field.labelKey) }}
                      <span class="source-badge" :class="'source-' + sourceFor(field.key)">
                        {{ $t('settings.source.' + sourceFor(field.key)) }}
                      </span>
                    </label>
                    <input
                      class="field-input"
                      :type="field.secret ? 'password' : 'text'"
                      v-model="form[field.key]"
                      @input="field.secret && (touched[field.key] = true)"
                      :placeholder="fieldPlaceholder(field)"
                      autocomplete="off"
                    />
                    <span v-if="field.secret" class="field-hint">{{ $t('settings.secretHint') }}</span>
                  </div>

                  <div class="test-row">
                    <button class="secondary-btn" :disabled="testingNeo4j" @click="runNeo4jTest">
                      {{ testingNeo4j ? $t('settings.testing') : $t('settings.testButton') }}
                    </button>
                    <div v-if="neo4jTestResult" class="test-result" :class="neo4jTestResult.ok ? 'ok' : 'fail'">
                      <span class="result-badge">{{ neo4jTestResult.ok ? $t('settings.testOk') : $t('settings.testFail') }}</span>
                      <span class="result-category">{{ $t('settings.testCategory.' + (neo4jTestResult.category || 'unknown')) }}</span>
                      <span v-if="neo4jTestResult.latency_ms != null" class="result-latency">{{ $t('settings.latencyMs', { ms: neo4jTestResult.latency_ms }) }}</span>
                      <p class="result-detail">{{ neo4jTestResult.detail }}</p>
                    </div>
                  </div>
                </div>

                <!-- Save area -->
                <div v-if="saveWarnings.length" class="warnings-panel">
                  <span class="warnings-title">{{ $t('settings.warningsTitle') }}</span>
                  <ul>
                    <li v-for="(w, idx) in saveWarnings" :key="idx">{{ w }}</li>
                  </ul>
                </div>

                <p v-if="saveError" class="save-error">{{ saveError }}</p>
                <p v-if="saveSuccess" class="save-success">{{ $t('settings.saveSuccess') }}</p>
                <p v-if="noChangesNotice" class="save-info">{{ $t('settings.noChanges') }}</p>
              </div>
            </div>

            <div v-if="!loading && !apiUnavailable && !loadError" class="modal-footer">
              <button class="secondary-btn" @click="closeModal">{{ $t('common.cancel') }}</button>
              <button class="primary-btn" :disabled="saving" @click="handleSave">
                {{ saving ? $t('settings.saving') : $t('settings.saveButton') }}
              </button>
            </div>
          </div>
        </div>
      </Transition>
    </Teleport>
  </div>
</template>

<script setup>
import { ref, reactive, computed, watch, onUnmounted } from 'vue'
import { useI18n } from 'vue-i18n'
import { getSettings, updateSettings, testLlmCredentials, testNeo4jCredentials } from '../api/settings'
import settingsPanelState from '../store/settingsPanel'

const { t } = useI18n()

const FIELDS = [
  { key: 'llm_api_key', labelKey: 'settings.fields.llmApiKey', secret: true, group: 'llm' },
  { key: 'llm_base_url', labelKey: 'settings.fields.llmBaseUrl', secret: false, group: 'llm' },
  { key: 'llm_model_name', labelKey: 'settings.fields.llmModelName', secret: false, group: 'llm' },
  { key: 'llm_reasoning_effort', labelKey: 'settings.fields.llmReasoningEffort', secret: false, group: 'llm', type: 'select' },
  { key: 'neo4j_uri', labelKey: 'settings.fields.neo4jUri', secret: false, group: 'neo4j' },
  { key: 'neo4j_user', labelKey: 'settings.fields.neo4jUser', secret: false, group: 'neo4j' },
  { key: 'neo4j_password', labelKey: 'settings.fields.neo4jPassword', secret: true, group: 'neo4j' }
]

const llmFields = FIELDS.filter(f => f.group === 'llm')
const neo4jFields = FIELDS.filter(f => f.group === 'neo4j')

const open = ref(false)
const loading = ref(false)
const apiUnavailable = ref(false)
const loadError = ref('')

// Raw settings payload as returned by GET /api/settings
const settingsData = ref({})

// Editable form state. Secret fields start blank so an untouched field
// never carries the masked placeholder value into a save/test request.
const form = reactive({})
const touched = reactive({})
FIELDS.forEach(f => {
  form[f.key] = ''
  touched[f.key] = false
})

const testingLlm = ref(false)
const testingNeo4j = ref(false)
const llmTestResult = ref(null)
const neo4jTestResult = ref(null)

const saving = ref(false)
const saveError = ref('')
const saveWarnings = ref([])
const saveSuccess = ref(false)
const noChangesNotice = ref(false)

// OpenAI-standard reasoning_effort values. Deliberately excludes "minimal":
// the CommandCode proxy this project points at rejects it outright, so
// offering it would hand the user a value that fails every request. The empty
// option means "don't send the parameter at all", which is a valid state --
// utils/openai_chat_compat.py only attaches reasoning_effort when truthy.
const reasoningOptions = computed(() => {
  const base = ['', 'low', 'medium', 'high', 'xhigh', 'max']
  const current = form.llm_reasoning_effort
  if (current && !base.includes(current)) {
    return [current, ...base]
  }
  return base
})

const sourceFor = (key) => {
  return settingsData.value.source?.[key] || 'default'
}

const fieldPlaceholder = (field) => {
  if (!field.secret) return ''
  const isSet = settingsData.value[field.key + '_set']
  if (!isSet) return t('settings.notSet')
  return settingsData.value[field.key] || t('settings.notSet')
}

const resetFormFromSettings = () => {
  FIELDS.forEach(f => {
    if (f.secret) {
      form[f.key] = ''
      touched[f.key] = false
    } else {
      form[f.key] = settingsData.value[f.key] ?? ''
    }
  })
}

const fetchSettings = async () => {
  loading.value = true
  apiUnavailable.value = false
  loadError.value = ''
  llmTestResult.value = null
  neo4jTestResult.value = null
  saveError.value = ''
  saveWarnings.value = []
  saveSuccess.value = false
  noChangesNotice.value = false

  try {
    const res = await getSettings()
    settingsData.value = res.data || {}
    resetFormFromSettings()
  } catch (err) {
    if (err.response?.status === 404) {
      apiUnavailable.value = true
    } else {
      loadError.value = err.message || t('settings.loadError')
    }
  } finally {
    loading.value = false
  }
}

const openModal = () => {
  open.value = true
  fetchSettings()
}

const closeModal = () => {
  open.value = false
}

// Other parts of the app (e.g. the stall/graph-ingestion recovery banners in
// Step3Simulation) cannot reach this component's own `open` ref directly, so
// they signal "please open Settings" through a shared counter instead.
watch(() => settingsPanelState.openRequestId, (val, oldVal) => {
  if (val !== oldVal && val > 0) {
    openModal()
  }
})

const buildLlmTestPayload = () => {
  const payload = {}
  if (form.llm_api_key) payload.llm_api_key = form.llm_api_key
  if (form.llm_base_url) payload.llm_base_url = form.llm_base_url
  if (form.llm_model_name) payload.llm_model_name = form.llm_model_name
  return payload
}

const buildNeo4jTestPayload = () => {
  const payload = {}
  if (form.neo4j_uri) payload.neo4j_uri = form.neo4j_uri
  if (form.neo4j_user) payload.neo4j_user = form.neo4j_user
  if (form.neo4j_password) payload.neo4j_password = form.neo4j_password
  return payload
}

const runLlmTest = async () => {
  testingLlm.value = true
  llmTestResult.value = null
  try {
    const res = await testLlmCredentials(buildLlmTestPayload())
    llmTestResult.value = res.data
  } catch (err) {
    if (err.response?.status === 404) {
      apiUnavailable.value = true
    } else {
      llmTestResult.value = { ok: false, latency_ms: null, detail: err.message || t('settings.testError'), category: 'unknown' }
    }
  } finally {
    testingLlm.value = false
  }
}

const runNeo4jTest = async () => {
  testingNeo4j.value = true
  neo4jTestResult.value = null
  try {
    const res = await testNeo4jCredentials(buildNeo4jTestPayload())
    neo4jTestResult.value = res.data
  } catch (err) {
    if (err.response?.status === 404) {
      apiUnavailable.value = true
    } else {
      neo4jTestResult.value = { ok: false, latency_ms: null, detail: err.message || t('settings.testError'), category: 'unknown' }
    }
  } finally {
    testingNeo4j.value = false
  }
}

const buildSavePayload = () => {
  const payload = {}
  FIELDS.forEach(f => {
    if (f.secret) {
      // Only send a secret if the user actually typed something into it.
      // An untouched secret field must never overwrite the stored value.
      if (touched[f.key] && form[f.key]) {
        payload[f.key] = form[f.key]
      }
    } else {
      const current = settingsData.value[f.key] ?? ''
      if (form[f.key] !== current) {
        payload[f.key] = form[f.key]
      }
    }
  })
  return payload
}

const handleSave = async () => {
  const payload = buildSavePayload()
  saveError.value = ''
  saveWarnings.value = []
  saveSuccess.value = false
  noChangesNotice.value = false

  if (Object.keys(payload).length === 0) {
    noChangesNotice.value = true
    return
  }

  saving.value = true
  try {
    const res = await updateSettings(payload)
    saveWarnings.value = res.data?.warnings || []
    saveSuccess.value = true
    // Refresh masked values / source map / *_set flags and clear secret inputs
    await fetchSettings()
    saveSuccess.value = true
  } catch (err) {
    if (err.response?.status === 404) {
      apiUnavailable.value = true
    } else {
      saveError.value = err.message || t('settings.saveError')
    }
  } finally {
    saving.value = false
  }
}

const onKeydown = (e) => {
  if (e.key === 'Escape') {
    closeModal()
  }
}

watch(open, (isOpen) => {
  if (isOpen) {
    document.addEventListener('keydown', onKeydown)
  } else {
    document.removeEventListener('keydown', onKeydown)
  }
})

onUnmounted(() => {
  document.removeEventListener('keydown', onKeydown)
})
</script>

<style scoped>
.settings-trigger-wrap {
  display: inline-block;
}

.settings-trigger {
  background: transparent;
  color: #333;
  border: 1px solid #CCC;
  padding: 4px 12px;
  font-family: 'JetBrains Mono', monospace;
  font-size: 0.8rem;
  cursor: pointer;
  display: flex;
  align-items: center;
  gap: 6px;
  transition: border-color 0.2s, opacity 0.2s;
}

.settings-trigger:hover {
  border-color: #999;
}

.gear-icon {
  font-size: 0.85rem;
}

/* Modal */
.settings-modal-overlay {
  position: fixed;
  inset: 0;
  background: rgba(0, 0, 0, 0.5);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 1000;
  backdrop-filter: blur(4px);
}

.settings-modal {
  background: #FFF;
  border-radius: 12px;
  width: 92%;
  max-width: 560px;
  max-height: 88vh;
  overflow: hidden;
  display: flex;
  flex-direction: column;
  box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
  font-family: 'Space Grotesk', 'Noto Sans SC', system-ui, sans-serif;
}

.modal-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 18px 24px;
  border-bottom: 1px solid #F0F0F0;
}

.modal-title {
  font-size: 16px;
  font-weight: 700;
  color: #000;
}

.close-btn {
  width: 28px;
  height: 28px;
  border: none;
  background: none;
  color: #999;
  border-radius: 50%;
  font-size: 22px;
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  line-height: 1;
  transition: color 0.2s;
  padding: 0;
}

.close-btn:hover {
  color: #333;
}

.modal-body {
  padding: 20px 24px;
  overflow-y: auto;
  flex: 1;
}

.state-panel {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 10px;
  padding: 32px 16px;
  text-align: center;
  color: #666;
  font-size: 13px;
}

.state-icon {
  font-size: 24px;
}

.state-title {
  font-weight: 700;
  color: #333;
  margin: 0;
}

.state-desc {
  margin: 0;
  color: #666;
  font-size: 12px;
  line-height: 1.6;
}

.loading-spinner {
  width: 22px;
  height: 22px;
  border: 2px solid #E5E7EB;
  border-top-color: #FF5722;
  border-radius: 50%;
  animation: spin 0.8s linear infinite;
}

@keyframes spin {
  to { transform: rotate(360deg); }
}

.apply-note {
  font-size: 11px;
  color: #666;
  background: #F9F9F9;
  border: 1px solid #EAEAEA;
  border-radius: 6px;
  padding: 10px 12px;
  line-height: 1.6;
  margin-bottom: 18px;
}

.settings-section {
  margin-bottom: 22px;
}

.section-header {
  margin-bottom: 10px;
}

.section-title {
  font-size: 12px;
  font-weight: 700;
  color: #666;
  text-transform: uppercase;
  letter-spacing: 0.5px;
}

.field-group {
  margin-bottom: 12px;
}

.field-label {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 12px;
  color: #444;
  margin-bottom: 4px;
  font-weight: 500;
}

.source-badge {
  font-size: 9px;
  padding: 1px 6px;
  border-radius: 8px;
  text-transform: uppercase;
  letter-spacing: 0.4px;
  font-weight: 600;
}

.source-badge.source-override { background: #E3F2FD; color: #1565C0; }
.source-badge.source-env { background: #FFF3E0; color: #E65100; }
.source-badge.source-default { background: #F5F5F5; color: #999; }

.field-input {
  width: 100%;
  padding: 8px 10px;
  border: 1px solid #DDD;
  border-radius: 6px;
  font-size: 13px;
  font-family: 'JetBrains Mono', monospace;
  color: #222;
  background: #FFF;
}

.field-input:focus {
  outline: none;
  border-color: #FF5722;
}

.field-hint {
  display: block;
  font-size: 10px;
  color: #999;
  margin-top: 3px;
}

.test-row {
  margin-top: 10px;
}

.test-result {
  margin-top: 10px;
  padding: 10px 12px;
  border-radius: 6px;
  font-size: 12px;
}

.test-result.ok {
  background: #E8F5E9;
  border: 1px solid #C8E6C9;
}

.test-result.fail {
  background: #FFEBEE;
  border: 1px solid #FFCDD2;
}

.result-badge {
  font-weight: 700;
  margin-right: 10px;
}

.test-result.ok .result-badge { color: #2E7D32; }
.test-result.fail .result-badge { color: #C62828; }

.result-category {
  font-family: 'JetBrains Mono', monospace;
  font-size: 10px;
  background: rgba(0,0,0,0.06);
  padding: 2px 6px;
  border-radius: 4px;
  margin-right: 8px;
  text-transform: uppercase;
}

.result-latency {
  font-family: 'JetBrains Mono', monospace;
  font-size: 11px;
  color: #555;
}

.result-detail {
  margin: 6px 0 0 0;
  color: #444;
  line-height: 1.5;
  word-break: break-word;
}

.warnings-panel {
  background: #FFF8E1;
  border: 1px solid #FFECB3;
  border-radius: 6px;
  padding: 10px 14px;
  margin-bottom: 14px;
}

.warnings-title {
  display: block;
  font-size: 11px;
  font-weight: 700;
  color: #E65100;
  text-transform: uppercase;
  letter-spacing: 0.4px;
  margin-bottom: 6px;
}

.warnings-panel ul {
  margin: 0;
  padding-left: 18px;
}

.warnings-panel li {
  font-size: 12px;
  color: #795548;
  line-height: 1.6;
}

.save-error {
  color: #C62828;
  font-size: 12px;
  margin-bottom: 10px;
}

.save-success {
  color: #2E7D32;
  font-size: 12px;
  margin-bottom: 10px;
}

.save-info {
  color: #666;
  font-size: 12px;
  margin-bottom: 10px;
}

.modal-footer {
  display: flex;
  justify-content: flex-end;
  gap: 10px;
  padding: 16px 24px;
  border-top: 1px solid #F0F0F0;
}

.secondary-btn {
  background: #F5F5F5;
  color: #333;
  border: none;
  padding: 8px 16px;
  border-radius: 6px;
  font-size: 12px;
  font-weight: 600;
  cursor: pointer;
  transition: background 0.2s;
}

.secondary-btn:hover:not(:disabled) {
  background: #E5E5E5;
}

.secondary-btn:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.primary-btn {
  background: #000;
  color: #FFF;
  border: none;
  padding: 8px 18px;
  border-radius: 6px;
  font-size: 12px;
  font-weight: 600;
  cursor: pointer;
  transition: opacity 0.2s;
}

.primary-btn:hover:not(:disabled) {
  opacity: 0.8;
}

.primary-btn:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

/* Modal transition */
.modal-enter-active,
.modal-leave-active {
  transition: opacity 0.3s ease;
}

.modal-enter-from,
.modal-leave-to {
  opacity: 0;
}

.modal-enter-active .settings-modal {
  transition: all 0.3s cubic-bezier(0.34, 1.56, 0.64, 1);
}

.modal-leave-active .settings-modal {
  transition: all 0.2s ease-in;
}

.modal-enter-from .settings-modal {
  transform: scale(0.95) translateY(10px);
  opacity: 0;
}

.modal-leave-to .settings-modal {
  transform: scale(0.95) translateY(10px);
  opacity: 0;
}
</style>

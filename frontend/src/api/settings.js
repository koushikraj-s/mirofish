import service from './index'

// The backend keys settings by their `Config` attribute name (uppercase) and
// nests each one as {set, source, value}; the UI works in flat lowercase
// fields. Translating here keeps that seam in one place instead of spreading
// SCREAMING_CASE through the components.
const toBackendKey = (key) => key.toUpperCase()
const toUiKey = (key) => key.toLowerCase()

/**
 * Normalize the backend's `{KEY: {set, source, value}}` payload into the flat
 * shape the UI binds to: `{llm_api_key, llm_api_key_set, source: {...}}`.
 */
const adaptSettingsResponse = (data) => {
  const flat = { source: {} }
  Object.entries(data || {}).forEach(([key, info]) => {
    const uiKey = toUiKey(key)
    flat[uiKey] = info?.value ?? ''
    flat[`${uiKey}_set`] = Boolean(info?.set)
    flat.source[uiKey] = info?.source || 'default'
  })
  return flat
}

/**
 * 获取当前设置（LLM/Neo4j 凭据等），敏感字段以掩码形式返回
 * @returns {Promise}
 */
export const getSettings = async () => {
  const res = await service.get('/api/settings')
  return { ...res, data: adaptSettingsResponse(res.data) }
}

/**
 * 更新设置（仅需传入要修改的字段子集）
 * @param {Object} data - 可包含 llm_api_key, llm_base_url, llm_model_name,
 *   llm_reasoning_effort, neo4j_uri, neo4j_user, neo4j_password 中的任意子集
 */
export const updateSettings = async (data) => {
  const payload = {}
  Object.entries(data || {}).forEach(([key, value]) => {
    payload[toBackendKey(key)] = value
  })
  const res = await service.put('/api/settings', payload)
  return { ...res, data: adaptSettingsResponse(res.data) }
}

/**
 * 在保存前测试 LLM 凭据是否可用
 * @param {Object} data - { llm_api_key?, llm_base_url?, llm_model_name? }
 *   省略的字段会回退到已保存的值
 */
export const testLlmCredentials = (data) => {
  return service.post('/api/settings/test', data)
}

/**
 * 在保存前测试 Neo4j 凭据是否可用
 * @param {Object} data - { neo4j_uri?, neo4j_user?, neo4j_password? }
 *   省略的字段会回退到已保存的值
 */
export const testNeo4jCredentials = (data) => {
  return service.post('/api/settings/test-neo4j', data)
}

<template>
  <div class="simulation-panel">
    <!-- Top Control Bar -->
    <div class="control-bar">
      <div class="status-group">
        <!-- Twitter 平台进度 -->
        <div class="platform-status twitter" :class="{ active: runStatus.twitter_running, completed: runStatus.twitter_completed }">
          <div class="platform-header">
            <svg class="platform-icon" viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" stroke-width="2">
              <circle cx="12" cy="12" r="10"></circle><line x1="2" y1="12" x2="22" y2="12"></line><path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"></path>
            </svg>
            <span class="platform-name">Info Plaza</span>
            <span v-if="runStatus.twitter_completed" class="status-badge">
              <svg viewBox="0 0 24 24" width="12" height="12" fill="none" stroke="currentColor" stroke-width="3">
                <polyline points="20 6 9 17 4 12"></polyline>
              </svg>
            </span>
          </div>
          <div class="platform-stats">
            <span class="stat">
              <span class="stat-label">ROUND</span>
              <span class="stat-value mono">{{ runStatus.twitter_current_round || 0 }}<span class="stat-total">/{{ runStatus.total_rounds || maxRounds || '-' }}</span></span>
            </span>
            <span class="stat">
              <span class="stat-label">TIME</span>
              <span class="stat-value mono">{{ twitterElapsedTime }}</span>
            </span>
            <span class="stat">
              <span class="stat-label">ACTS</span>
              <span class="stat-value mono">{{ runStatus.twitter_actions_count || 0 }}</span>
            </span>
          </div>
          <!-- 可用动作提示 -->
          <div class="actions-tooltip">
            <div class="tooltip-title">Available Actions</div>
            <div class="tooltip-actions">
              <span class="tooltip-action">POST</span>
              <span class="tooltip-action">LIKE</span>
              <span class="tooltip-action">REPOST</span>
              <span class="tooltip-action">QUOTE</span>
              <span class="tooltip-action">FOLLOW</span>
              <span class="tooltip-action">IDLE</span>
            </div>
          </div>
        </div>
        
        <!-- Reddit 平台进度 -->
        <div class="platform-status reddit" :class="{ active: runStatus.reddit_running, completed: runStatus.reddit_completed }">
          <div class="platform-header">
            <svg class="platform-icon" viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" stroke-width="2">
              <path d="M21 11.5a8.38 8.38 0 0 1-.9 3.8 8.5 8.5 0 0 1-7.6 4.7 8.38 8.38 0 0 1-3.8-.9L3 21l1.9-5.7a8.38 8.38 0 0 1-.9-3.8 8.5 8.5 0 0 1 4.7-7.6 8.38 8.38 0 0 1 3.8-.9h.5a8.48 8.48 0 0 1 8 8v.5z"></path>
            </svg>
            <span class="platform-name">Topic Community</span>
            <span v-if="runStatus.reddit_completed" class="status-badge">
              <svg viewBox="0 0 24 24" width="12" height="12" fill="none" stroke="currentColor" stroke-width="3">
                <polyline points="20 6 9 17 4 12"></polyline>
              </svg>
            </span>
          </div>
          <div class="platform-stats">
            <span class="stat">
              <span class="stat-label">ROUND</span>
              <span class="stat-value mono">{{ runStatus.reddit_current_round || 0 }}<span class="stat-total">/{{ runStatus.total_rounds || maxRounds || '-' }}</span></span>
            </span>
            <span class="stat">
              <span class="stat-label">TIME</span>
              <span class="stat-value mono">{{ redditElapsedTime }}</span>
            </span>
            <span class="stat">
              <span class="stat-label">ACTS</span>
              <span class="stat-value mono">{{ runStatus.reddit_actions_count || 0 }}</span>
            </span>
          </div>
          <!-- 可用动作提示 -->
          <div class="actions-tooltip">
            <div class="tooltip-title">Available Actions</div>
            <div class="tooltip-actions">
              <span class="tooltip-action">POST</span>
              <span class="tooltip-action">COMMENT</span>
              <span class="tooltip-action">LIKE</span>
              <span class="tooltip-action">DISLIKE</span>
              <span class="tooltip-action">SEARCH</span>
              <span class="tooltip-action">TREND</span>
              <span class="tooltip-action">FOLLOW</span>
              <span class="tooltip-action">MUTE</span>
              <span class="tooltip-action">REFRESH</span>
              <span class="tooltip-action">IDLE</span>
            </div>
          </div>
        </div>
      </div>

      <div class="action-controls">
        <span v-if="reportCaveatActive && phase === 2" class="report-caveat-hint" :title="t('log.reportPartialDataCaveatHint')">
          ⚠ {{ t('log.reportPartialDataCaveatHint') }}
        </span>
        <button
          class="action-btn primary"
          :disabled="phase !== 2 || isGeneratingReport"
          @click="handleNextStep"
        >
          <span v-if="isGeneratingReport" class="loading-spinner-small"></span>
          {{ isGeneratingReport ? $t('step3.generatingReportBtn') : $t('step3.startGenerateReportBtn') }}
          <span v-if="!isGeneratingReport" class="arrow-icon">→</span>
        </button>
      </div>
    </div>

    <!-- Non-blocking recovery banners: stall + pending graph ingestion.
         Both read straight off the run-status poll, so they appear and
         clear on their own as the underlying condition changes. -->
    <div class="recovery-banners" v-if="showStallBanner || showIngestionBanner">
      <div v-if="showStallBanner" class="recovery-banner stall-banner">
        <span class="banner-icon">⏱</span>
        <div class="banner-text">
          <strong>{{ t('log.stallBannerTitle', { minutes: stallMinutes }) }}</strong>
          <span>{{ t('log.stallBannerBody') }}</span>
        </div>
        <button class="banner-action" @click="handleOpenSettings">{{ t('log.openSettings') }}</button>
      </div>

      <div v-if="showIngestionBanner" class="recovery-banner ingestion-banner">
        <span class="banner-icon">⧗</span>
        <div class="banner-text">
          <strong>{{ t('log.ingestionPendingBannerTitle', { count: pendingIngestionCount }) }}</strong>
          <span>{{ t('log.ingestionPendingBannerBody') }}</span>
        </div>
        <button class="banner-action banner-action-ghost" @click="handleOpenSettings">{{ t('log.openSettings') }}</button>
        <button class="banner-action" :disabled="isRetryingIngestion" @click="handleRetryIngestion">
          {{ isRetryingIngestion ? t('log.retryingIngestion') : t('log.retryIngestion') }}
        </button>
      </div>
    </div>

    <!-- Main Content: Dual Timeline -->
    <div class="main-content-area" ref="scrollContainer">
      <!-- Timeline Header -->
      <div class="timeline-header" v-if="allActions.length > 0">
        <div class="timeline-stats">
          <span class="total-count">TOTAL EVENTS: <span class="mono">{{ allActions.length }}</span></span>
          <span class="platform-breakdown">
            <span class="breakdown-item twitter">
              <svg class="mini-icon" viewBox="0 0 24 24" width="12" height="12" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"></circle><line x1="2" y1="12" x2="22" y2="12"></line><path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"></path></svg>
              <span class="mono">{{ twitterActionsCount }}</span>
            </span>
            <span class="breakdown-divider">/</span>
            <span class="breakdown-item reddit">
              <svg class="mini-icon" viewBox="0 0 24 24" width="12" height="12" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 11.5a8.38 8.38 0 0 1-.9 3.8 8.5 8.5 0 0 1-7.6 4.7 8.38 8.38 0 0 1-3.8-.9L3 21l1.9-5.7a8.38 8.38 0 0 1-.9-3.8 8.5 8.5 0 0 1 4.7-7.6 8.38 8.38 0 0 1 3.8-.9h.5a8.48 8.48 0 0 1 8 8v.5z"></path></svg>
              <span class="mono">{{ redditActionsCount }}</span>
            </span>
          </span>
        </div>
      </div>
      
      <!-- Timeline Feed -->
      <div class="timeline-feed">
        <div class="timeline-axis"></div>
        
        <TransitionGroup name="timeline-item">
          <div 
            v-for="action in chronologicalActions" 
            :key="action._uniqueId || action.id || `${action.timestamp}-${action.agent_id}`" 
            class="timeline-item"
            :class="action.platform"
          >
            <div class="timeline-marker">
              <div class="marker-dot"></div>
            </div>
            
            <div class="timeline-card">
              <div class="card-header">
                <div class="agent-info">
                  <div class="avatar-placeholder">{{ (action.agent_name || 'A')[0] }}</div>
                  <span class="agent-name">{{ action.agent_name }}</span>
                </div>
                
                <div class="header-meta">
                  <div class="platform-indicator">
                    <svg v-if="action.platform === 'twitter'" viewBox="0 0 24 24" width="12" height="12" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"></circle><line x1="2" y1="12" x2="22" y2="12"></line><path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"></path></svg>
                    <svg v-else viewBox="0 0 24 24" width="12" height="12" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 11.5a8.38 8.38 0 0 1-.9 3.8 8.5 8.5 0 0 1-7.6 4.7 8.38 8.38 0 0 1-3.8-.9L3 21l1.9-5.7a8.38 8.38 0 0 1-.9-3.8 8.5 8.5 0 0 1 4.7-7.6 8.38 8.38 0 0 1 3.8-.9h.5a8.48 8.48 0 0 1 8 8v.5z"></path></svg>
                  </div>
                  <div class="action-badge" :class="getActionTypeClass(action.action_type)">
                    {{ getActionTypeLabel(action.action_type) }}
                  </div>
                </div>
              </div>
              
              <div class="card-body">
                <!-- CREATE_POST: 发布帖子 -->
                <div v-if="action.action_type === 'CREATE_POST' && action.action_args?.content" class="content-text main-text">
                  {{ action.action_args.content }}
                </div>

                <!-- QUOTE_POST: 引用帖子 -->
                <template v-if="action.action_type === 'QUOTE_POST'">
                  <div v-if="action.action_args?.quote_content" class="content-text">
                    {{ action.action_args.quote_content }}
                  </div>
                  <div v-if="action.action_args?.original_content" class="quoted-block">
                    <div class="quote-header">
                      <svg class="icon-small" viewBox="0 0 24 24" width="12" height="12" fill="none" stroke="currentColor" stroke-width="2"><path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71"></path><path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71"></path></svg>
                      <span class="quote-label">@{{ action.action_args.original_author_name || 'User' }}</span>
                    </div>
                    <div class="quote-text">
                      {{ truncateContent(action.action_args.original_content, 150) }}
                    </div>
                  </div>
                </template>

                <!-- REPOST: 转发帖子 -->
                <template v-if="action.action_type === 'REPOST'">
                  <div class="repost-info">
                    <svg class="icon-small" viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" stroke-width="2"><polyline points="17 1 21 5 17 9"></polyline><path d="M3 11V9a4 4 0 0 1 4-4h14"></path><polyline points="7 23 3 19 7 15"></polyline><path d="M21 13v2a4 4 0 0 1-4 4H3"></path></svg>
                    <span class="repost-label">Reposted from @{{ action.action_args?.original_author_name || 'User' }}</span>
                  </div>
                  <div v-if="action.action_args?.original_content" class="repost-content">
                    {{ truncateContent(action.action_args.original_content, 200) }}
                  </div>
                </template>

                <!-- LIKE_POST: 点赞帖子 -->
                <template v-if="action.action_type === 'LIKE_POST'">
                  <div class="like-info">
                    <svg class="icon-small filled" viewBox="0 0 24 24" width="14" height="14" fill="currentColor"><path d="M20.84 4.61a5.5 5.5 0 0 0-7.78 0L12 5.67l-1.06-1.06a5.5 5.5 0 0 0-7.78 7.78l1.06 1.06L12 21.23l7.78-7.78 1.06-1.06a5.5 5.5 0 0 0 0-7.78z"></path></svg>
                    <span class="like-label">Liked @{{ action.action_args?.post_author_name || 'User' }}'s post</span>
                  </div>
                  <div v-if="action.action_args?.post_content" class="liked-content">
                    "{{ truncateContent(action.action_args.post_content, 120) }}"
                  </div>
                </template>

                <!-- CREATE_COMMENT: 发表评论 -->
                <template v-if="action.action_type === 'CREATE_COMMENT'">
                  <div v-if="action.action_args?.content" class="content-text">
                    {{ action.action_args.content }}
                  </div>
                  <div v-if="action.action_args?.post_id" class="comment-context">
                    <svg class="icon-small" viewBox="0 0 24 24" width="12" height="12" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 11.5a8.38 8.38 0 0 1-.9 3.8 8.5 8.5 0 0 1-7.6 4.7 8.38 8.38 0 0 1-3.8-.9L3 21l1.9-5.7a8.38 8.38 0 0 1-.9-3.8 8.5 8.5 0 0 1 4.7-7.6 8.38 8.38 0 0 1 3.8-.9h.5a8.48 8.48 0 0 1 8 8v.5z"></path></svg>
                    <span>Reply to post #{{ action.action_args.post_id }}</span>
                  </div>
                </template>

                <!-- SEARCH_POSTS: 搜索帖子 -->
                <template v-if="action.action_type === 'SEARCH_POSTS'">
                  <div class="search-info">
                    <svg class="icon-small" viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="8"></circle><line x1="21" y1="21" x2="16.65" y2="16.65"></line></svg>
                    <span class="search-label">Search Query:</span>
                    <span class="search-query">"{{ action.action_args?.query || '' }}"</span>
                  </div>
                </template>

                <!-- FOLLOW: 关注用户 -->
                <template v-if="action.action_type === 'FOLLOW'">
                  <div class="follow-info">
                    <svg class="icon-small" viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" stroke-width="2"><path d="M16 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"></path><circle cx="8.5" cy="7" r="4"></circle><line x1="20" y1="8" x2="20" y2="14"></line><line x1="23" y1="11" x2="17" y2="11"></line></svg>
                    <span class="follow-label">Followed @{{ action.action_args?.target_user || action.action_args?.user_id || 'User' }}</span>
                  </div>
                </template>

                <!-- UPVOTE / DOWNVOTE -->
                <template v-if="action.action_type === 'UPVOTE_POST' || action.action_type === 'DOWNVOTE_POST'">
                  <div class="vote-info">
                    <svg v-if="action.action_type === 'UPVOTE_POST'" class="icon-small" viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" stroke-width="2"><polyline points="18 15 12 9 6 15"></polyline></svg>
                    <svg v-else class="icon-small" viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" stroke-width="2"><polyline points="6 9 12 15 18 9"></polyline></svg>
                    <span class="vote-label">{{ action.action_type === 'UPVOTE_POST' ? 'Upvoted' : 'Downvoted' }} Post</span>
                  </div>
                  <div v-if="action.action_args?.post_content" class="voted-content">
                    "{{ truncateContent(action.action_args.post_content, 120) }}"
                  </div>
                </template>

                <!-- DO_NOTHING: 无操作（静默） -->
                <template v-if="action.action_type === 'DO_NOTHING'">
                  <div class="idle-info">
                    <svg class="icon-small" viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"></circle><line x1="12" y1="8" x2="12" y2="12"></line><line x1="12" y1="16" x2="12.01" y2="16"></line></svg>
                    <span class="idle-label">Action Skipped</span>
                  </div>
                </template>

                <!-- 通用回退：未知类型或有 content 但未被上述处理 -->
                <div v-if="!['CREATE_POST', 'QUOTE_POST', 'REPOST', 'LIKE_POST', 'CREATE_COMMENT', 'SEARCH_POSTS', 'FOLLOW', 'UPVOTE_POST', 'DOWNVOTE_POST', 'DO_NOTHING'].includes(action.action_type) && action.action_args?.content" class="content-text">
                  {{ action.action_args.content }}
                </div>
              </div>

              <div class="card-footer">
                <span class="time-tag">R{{ action.round_num }} • {{ formatActionTime(action.timestamp) }}</span>
                <!-- Platform tag removed as it is in header now -->
              </div>
            </div>
          </div>
        </TransitionGroup>

        <div v-if="allActions.length === 0" class="waiting-state">
          <div class="pulse-ring"></div>
          <span>Waiting for agent actions...</span>
        </div>
      </div>
    </div>

    <!-- Bottom Info / Logs -->
    <div class="system-logs">
      <div class="log-header">
        <span class="log-title">SIMULATION MONITOR</span>
        <span class="log-id">{{ simulationId || 'NO_SIMULATION' }}</span>
      </div>
      <div class="log-content" ref="logContent">
        <div class="log-line" v-for="(log, idx) in systemLogs" :key="idx">
          <span class="log-time">{{ log.time }}</span>
          <span class="log-msg">{{ log.msg }}</span>
        </div>
      </div>
    </div>

    <!-- Resume-vs-restart confirmation, shown when this component mounts
         and a run for this simulation already exists on the backend. -->
    <Transition name="modal">
      <div v-if="showResumeDialog" class="profile-modal-overlay resume-dialog-overlay">
        <div class="profile-modal resume-dialog">
          <div class="modal-header">
            <div class="modal-header-info">
              <span class="modal-realname">{{ t('log.existingRunFoundTitle') }}</span>
            </div>
          </div>
          <div class="modal-body resume-dialog-body">
            <p>{{ resumeDialogSummary }}</p>
            <p v-if="dialogIngestionPending > 0" class="resume-dialog-subnote">
              {{ t('log.resumeDialogIngestionNote', { count: dialogIngestionPending }) }}
            </p>
            <p v-if="dialogStallDetected" class="resume-dialog-subnote">
              {{ t('log.resumeDialogStallNote') }}
            </p>

            <!-- Step 1: the three-way choice. Reattach and Continue are both
                 non-destructive and styled identically as "safe"; Abandon is
                 visually separated below a divider and styled as a warning,
                 never a plain button alongside the other two. -->
            <div class="resume-options" v-if="!abandonConfirmStep">
              <button class="resume-option resume-option-safe" @click="handleResumeExisting">
                <span class="option-title">{{ t('log.reattachExistingRun') }}</span>
                <span class="option-desc">{{ t('log.reattachExistingRunDesc') }}</span>
              </button>

              <button v-if="resumeAvailable" class="resume-option resume-option-safe" @click="handleContinueFromCheckpoint">
                <span class="option-title">{{ t('log.continueFromRound', { round: resumeFromRound }) }}</span>
                <span class="option-desc">{{ t('log.continueFromRoundDesc', { round: resumeFromRound }) }}</span>
              </button>

              <div class="resume-options-divider"></div>

              <button class="resume-option resume-option-danger" @click="handleAbandonAndRestart">
                <span class="option-title">⚠ {{ t('log.abandonAndRestart') }}</span>
                <span class="option-desc">{{ t('log.abandonAndRestartDesc', {
                  twitterActs: pendingRunData?.twitter_actions_count || 0,
                  redditActs: pendingRunData?.reddit_actions_count || 0,
                  round: pendingRunData?.current_round || 0
                }) }}</span>
              </button>
            </div>

            <!-- Step 2: explicit confirmation naming exactly what gets
                 erased. This is the only path that can actually destroy the
                 run's progress. -->
            <div class="abandon-confirm-panel" v-else>
              <p class="abandon-confirm-title">⚠ {{ t('log.abandonConfirmTitle') }}</p>
              <p class="abandon-confirm-body">{{ t('log.abandonConfirmBody', {
                twitterActs: pendingRunData?.twitter_actions_count || 0,
                redditActs: pendingRunData?.reddit_actions_count || 0,
                round: pendingRunData?.current_round || 0
              }) }}</p>
              <div class="resume-dialog-actions">
                <button class="resume-btn resume-btn-ghost" @click="cancelAbandonConfirm">
                  {{ t('log.abandonConfirmCancel') }}
                </button>
                <button class="resume-btn resume-btn-danger-solid" @click="confirmAbandonAndRestart">
                  {{ t('log.abandonConfirmYes') }}
                </button>
              </div>
            </div>
          </div>
        </div>
      </div>
    </Transition>
  </div>
</template>

<script setup>
import { ref, computed, watch, onMounted, onUnmounted, nextTick } from 'vue'
import { useRouter } from 'vue-router'
import { useI18n } from 'vue-i18n'
import {
  startSimulation,
  stopSimulation,
  getRunStatus,
  getRunStatusDetail,
  resumeSimulation,
  retryGraphIngestion
} from '../api/simulation'
import { generateReport } from '../api/report'
import { requestOpenSettings } from '../store/settingsPanel'

const { t } = useI18n()

const props = defineProps({
  simulationId: String,
  maxRounds: Number, // 从Step2传入的最大轮数
  minutesPerRound: {
    type: Number,
    default: 30 // 默认每轮30分钟
  },
  projectData: Object,
  graphData: Object,
  systemLogs: Array
})

const emit = defineEmits(['go-back', 'next-step', 'add-log', 'update-status'])

const router = useRouter()

// State
const isGeneratingReport = ref(false)
const phase = ref(0) // 0: 未开始, 1: 运行中, 2: 已完成
const isStarting = ref(false)
const isStopping = ref(false)
const startError = ref(null)
const runStatus = ref({})
const allActions = ref([]) // 所有动作（增量累积）
const actionIds = ref(new Set()) // 用于去重的动作ID集合
const scrollContainer = ref(null)
const isContinuing = ref(false) // 是否正在从round断点续跑
const isRetryingIngestion = ref(false) // 是否正在重试图谱写入

// Computed
// 按时间顺序显示动作（最新的在最后面，即底部）
const chronologicalActions = computed(() => {
  return allActions.value
})

// 各平台动作计数
const twitterActionsCount = computed(() => {
  return allActions.value.filter(a => a.platform === 'twitter').length
})

const redditActionsCount = computed(() => {
  return allActions.value.filter(a => a.platform === 'reddit').length
})

// --- Recovery affordances (surfaced by GET .../run-status) ---

// 图谱写入待确认数量：仅在没有平台仍在运行时展示重试入口——updater存活时
// 后端 retry-ingestion 接口会直接返回409，此时展示按钮只会造成困惑。
const pendingIngestionCount = computed(() => runStatus.value.graph_ingestion_pending || 0)
const showIngestionBanner = computed(() => {
  return pendingIngestionCount.value > 0 && !runStatus.value.twitter_running && !runStatus.value.reddit_running
})

// 停滞检测：后端只在runner_status===RUNNING时才可能返回true，因此这里无需
// 额外按phase过滤——一旦轮询停止（进入终态），stall_detected自然不再为true。
const showStallBanner = computed(() => !!runStatus.value.stall_detected)
const stallMinutes = computed(() => {
  const since = runStatus.value.stalled_since
  if (!since) return 0
  const elapsedMs = Date.now() - new Date(since).getTime()
  return Math.max(0, Math.round(elapsedMs / 60000))
})

// 报告caveat：运行失败，或图谱记忆写入未被确认完整完成时，提醒用户报告
// 可能基于不完整的数据——但不阻塞“生成报告”操作本身。
const reportCaveatActive = computed(() => {
  return runStatus.value.runner_status === 'failed' || runStatus.value.graph_ingestion_complete === false
})

// 格式化模拟流逝时间（根据轮次和每轮分钟数计算）
const formatElapsedTime = (currentRound) => {
  if (!currentRound || currentRound <= 0) return '0h 0m'
  const totalMinutes = currentRound * props.minutesPerRound
  const hours = Math.floor(totalMinutes / 60)
  const minutes = totalMinutes % 60
  return `${hours}h ${minutes}m`
}

// Twitter平台的模拟流逝时间
const twitterElapsedTime = computed(() => {
  return formatElapsedTime(runStatus.value.twitter_current_round || 0)
})

// Reddit平台的模拟流逝时间
const redditElapsedTime = computed(() => {
  return formatElapsedTime(runStatus.value.reddit_current_round || 0)
})

// Methods
const addLog = (msg) => {
  emit('add-log', msg)
}

// 重置所有状态（用于重新启动模拟）
const resetAllState = () => {
  phase.value = 0
  runStatus.value = {}
  allActions.value = []
  actionIds.value = new Set()
  prevTwitterRound.value = 0
  prevRedditRound.value = 0
  startError.value = null
  isStarting.value = false
  isStopping.value = false
  stopPolling()  // 停止之前可能存在的轮询
}

// A force-restart of a simulation that just failed/is finishing can hit a
// backend 409 with `pending: true` -- the previous run's graph-memory
// updater is still draining its last activity batches (this can take tens
// of seconds against a local LLM+Neo4j pipeline) and the backend correctly
// refuses to restart mid-drain rather than risk corrupting it. That's a
// transient "not yet", not a real failure, so retry a few times with a
// short delay instead of surfacing it as a generic error immediately.
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

const startSimulationWithPendingRetry = async (params, maxAttempts = 6, delayMs = 5000) => {
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      return await startSimulation(params)
    } catch (err) {
      const pending = err.response?.data?.pending === true
      if (!pending || attempt === maxAttempts) {
        throw err
      }
      addLog(t('log.previousRunStillFinalizing', { attempt, maxAttempts }))
      await sleep(delayMs)
    }
  }
}

// 启动模拟
const doStartSimulation = async () => {
  if (!props.simulationId) {
    addLog(t('log.errorMissingSimId'))
    return
  }

  // 先重置所有状态，确保不会受到上一次模拟的影响
  resetAllState()
  
  isStarting.value = true
  startError.value = null
  addLog(t('log.startingDualSim'))
  emit('update-status', 'processing')
  
  try {
    const params = {
      simulation_id: props.simulationId,
      platform: 'parallel',
      force: true,  // 强制重新开始
      enable_graph_memory_update: true  // 开启动态图谱更新
    }
    
    if (props.maxRounds) {
      params.max_rounds = props.maxRounds
      addLog(t('log.setMaxRounds', { rounds: props.maxRounds }))
    }
    
    addLog(t('log.graphMemoryUpdateEnabled'))

    const res = await startSimulationWithPendingRetry(params)

    if (res.success && res.data) {
      if (res.data.force_restarted) {
        addLog(t('log.oldSimCleared'))
      }
      addLog(t('log.engineStarted'))
      addLog(`  ├─ PID: ${res.data.process_pid || '-'}`)
      
      phase.value = 1
      runStatus.value = res.data
      
      startStatusPolling()
      startDetailPolling()
    } else {
      startError.value = res.error || '启动失败'
      addLog(t('log.startFailed', { error: res.error || t('common.unknownError') }))
      emit('update-status', 'error')
    }
  } catch (err) {
    startError.value = err.message
    addLog(t('log.startException', { error: err.message }))
    emit('update-status', 'error')
  } finally {
    isStarting.value = false
  }
}

// 停止模拟
const handleStopSimulation = async () => {
  if (!props.simulationId) return
  
  isStopping.value = true
  addLog(t('log.stoppingSim'))
  
  try {
    const res = await stopSimulation({ simulation_id: props.simulationId })
    
    if (res.success) {
      addLog(t('log.simStoppedSuccess'))
      phase.value = 2
      stopPolling()
      emit('update-status', 'completed')
    } else {
      addLog(t('log.stopFailed', { error: res.error || t('common.unknownError') }))
    }
  } catch (err) {
    addLog(t('log.stopException', { error: err.message }))
  } finally {
    isStopping.value = false
  }
}

// 打开设置面板（停滞/图谱写入卡住时，多半是LLM凭据的问题）
const handleOpenSettings = () => {
  requestOpenSettings()
}

// 重试图谱写入：完成一次被中断的图谱drain。不需要模拟子进程仍在运行——
// 所有需要的数据都在 actions.jsonl 与 graph_ingestion journal 中，因此这里
// 不依赖 phase/runStatus 是否处于某个特定状态，只依赖 simulationId。
const handleRetryIngestion = async () => {
  if (!props.simulationId || isRetryingIngestion.value) return

  isRetryingIngestion.value = true
  addLog(t('log.retryingIngestion'))

  try {
    const res = await retryGraphIngestion(props.simulationId)
    if (res.success && res.data) {
      const d = res.data
      addLog(t('log.retryIngestionResult', {
        resent: d.resent ?? 0,
        already_committed: d.already_committed ?? 0,
        still_failed: d.still_failed ?? 0
      }))
    } else {
      addLog(t('log.retryIngestionFailed', { error: res.error || t('common.unknownError') }))
    }
  } catch (err) {
    addLog(t('log.retryIngestionFailed', { error: err.message }))
  } finally {
    isRetryingIngestion.value = false
    // 无论成功与否都刷新一次run-status，让pending计数/stall等字段反映
    // 磁盘上的最新状态，而不是重试前的旧快照。
    await fetchRunStatus()
  }
}

// 轮询状态
let statusTimer = null
let detailTimer = null

const startStatusPolling = () => {
  statusTimer = setInterval(fetchRunStatus, 2000)
}

const startDetailPolling = () => {
  detailTimer = setInterval(fetchRunStatusDetail, 3000)
}

const stopPolling = () => {
  if (statusTimer) {
    clearInterval(statusTimer)
    statusTimer = null
  }
  if (detailTimer) {
    clearInterval(detailTimer)
    detailTimer = null
  }
}

// 追踪各平台的上一次轮次，用于检测变化并输出日志
const prevTwitterRound = ref(0)
const prevRedditRound = ref(0)

const fetchRunStatus = async () => {
  if (!props.simulationId) return
  
  try {
    const res = await getRunStatus(props.simulationId)
    
    if (res.success && res.data) {
      const data = res.data
      
      runStatus.value = data
      
      // 分别检测各平台的轮次变化并输出日志
      if (data.twitter_current_round > prevTwitterRound.value) {
        addLog(`[Plaza] R${data.twitter_current_round}/${data.total_rounds} | T:${data.twitter_simulated_hours || 0}h | A:${data.twitter_actions_count}`)
        prevTwitterRound.value = data.twitter_current_round
      }
      
      if (data.reddit_current_round > prevRedditRound.value) {
        addLog(`[Community] R${data.reddit_current_round}/${data.total_rounds} | T:${data.reddit_simulated_hours || 0}h | A:${data.reddit_actions_count}`)
        prevRedditRound.value = data.reddit_current_round
      }
      
      // 检测模拟是否已完成（通过 runner_status 或平台完成状态判断）
      const isCompleted = data.runner_status === 'completed' || data.runner_status === 'stopped'
      const isFailed = data.runner_status === 'failed'
      
      // runner_status is authoritative because the backend only publishes a
      // terminal state after the graph ingestion barrier has completed.
      if (isFailed) {
        addLog(t('log.simFailed') + (data.error ? `: ${data.error}` : ''))
        phase.value = 2
        stopPolling()
        emit('update-status', 'error')
      } else if (isCompleted) {
        addLog(t('log.simCompleted'))
        phase.value = 2
        stopPolling()
        emit('update-status', 'completed')
      }
    }
  } catch (err) {
    console.warn('获取运行状态失败:', err)
  }
}

// 检查所有启用的平台是否已完成
const checkPlatformsCompleted = (data) => {
  // 如果没有任何平台数据，返回 false
  if (!data) return false
  
  // 检查各平台的完成状态
  const twitterCompleted = data.twitter_completed === true
  const redditCompleted = data.reddit_completed === true
  
  // 如果至少有一个平台完成了，检查是否所有启用的平台都完成了
  // 通过 actions_count 判断平台是否被启用（如果 count > 0 或 running 曾为 true）
  const twitterEnabled = (data.twitter_actions_count > 0) || data.twitter_running || twitterCompleted
  const redditEnabled = (data.reddit_actions_count > 0) || data.reddit_running || redditCompleted
  
  // 如果没有任何平台被启用，返回 false
  if (!twitterEnabled && !redditEnabled) return false
  
  // 检查所有启用的平台是否都已完成
  if (twitterEnabled && !twitterCompleted) return false
  if (redditEnabled && !redditCompleted) return false
  
  return true
}

const fetchRunStatusDetail = async () => {
  if (!props.simulationId) return
  
  try {
    const res = await getRunStatusDetail(props.simulationId)
    
    if (res.success && res.data) {
      // 使用 all_actions 获取完整的动作列表
      const serverActions = res.data.all_actions || []
      
      // 增量添加新动作（去重）
      let newActionsAdded = 0
      serverActions.forEach(action => {
        // 生成唯一ID
        const actionId = action.id || `${action.timestamp}-${action.platform}-${action.agent_id}-${action.action_type}`
        
        if (!actionIds.value.has(actionId)) {
          actionIds.value.add(actionId)
          allActions.value.push({
            ...action,
            _uniqueId: actionId
          })
          newActionsAdded++
        }
      })
      
      // 不自动滚动，让用户自由查看时间轴
      // 新动作会在底部追加
    }
  } catch (err) {
    console.warn('获取详细状态失败:', err)
  }
}

// Helpers
const getActionTypeLabel = (type) => {
  const labels = {
    'CREATE_POST': 'POST',
    'REPOST': 'REPOST',
    'LIKE_POST': 'LIKE',
    'CREATE_COMMENT': 'COMMENT',
    'LIKE_COMMENT': 'LIKE',
    'DO_NOTHING': 'IDLE',
    'FOLLOW': 'FOLLOW',
    'SEARCH_POSTS': 'SEARCH',
    'QUOTE_POST': 'QUOTE',
    'UPVOTE_POST': 'UPVOTE',
    'DOWNVOTE_POST': 'DOWNVOTE'
  }
  return labels[type] || type || 'UNKNOWN'
}

const getActionTypeClass = (type) => {
  const classes = {
    'CREATE_POST': 'badge-post',
    'REPOST': 'badge-action',
    'LIKE_POST': 'badge-action',
    'CREATE_COMMENT': 'badge-comment',
    'LIKE_COMMENT': 'badge-action',
    'QUOTE_POST': 'badge-post',
    'FOLLOW': 'badge-meta',
    'SEARCH_POSTS': 'badge-meta',
    'UPVOTE_POST': 'badge-action',
    'DOWNVOTE_POST': 'badge-action',
    'DO_NOTHING': 'badge-idle'
  }
  return classes[type] || 'badge-default'
}

const truncateContent = (content, maxLength = 100) => {
  if (!content) return ''
  if (content.length > maxLength) return content.substring(0, maxLength) + '...'
  return content
}

const formatActionTime = (timestamp) => {
  if (!timestamp) return ''
  try {
    return new Date(timestamp).toLocaleTimeString('en-US', { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' })
  } catch {
    return ''
  }
}

const handleNextStep = async () => {
  if (!props.simulationId) {
    addLog(t('log.errorMissingSimId'))
    return
  }

  if (isGeneratingReport.value) {
    addLog(t('log.reportRequestSent'))
    return
  }
  
  isGeneratingReport.value = true
  addLog(t('log.startingReportGen'))
  
  try {
    const res = await generateReport({
      simulation_id: props.simulationId,
      force_regenerate: true
    })
    
    if (res.success && res.data) {
      const reportId = res.data.report_id
      // graph_possibly_incomplete来自后端 _build_report_coverage 的实时判断
      // （运行失败，或图谱记忆写入未被确认完整完成），而不是前端猜测——
      // 据此提醒用户，但不阻止报告已经开始生成。
      if (res.data.graph_possibly_incomplete) {
        addLog(t('log.reportPartialDataCaveat', {
          status: res.data.coverage?.runner_status || runStatus.value.runner_status || '?'
        }))
      }
      addLog(t('log.reportGenTaskStarted', { reportId }))

      // 跳转到报告页面
      router.push({ name: 'Report', params: { reportId } })
    } else {
      addLog(t('log.reportGenFailed', { error: res.error || t('common.unknownError') }))
      isGeneratingReport.value = false
    }
  } catch (err) {
    addLog(t('log.reportGenException', { error: err.message }))
    isGeneratingReport.value = false
  }
}

// Scroll log to bottom
const logContent = ref(null)
watch(() => props.systemLogs?.length, () => {
  nextTick(() => {
    if (logContent.value) {
      logContent.value.scrollTop = logContent.value.scrollHeight
    }
  })
})

// Detect an already-existing run on mount instead of blindly restarting it.
// `doStartSimulation()` force-restarts the backend simulation (wiping its
// round/action history) -- calling it unconditionally on every mount meant
// every page refresh, or even just navigating back to this step, silently
// threw away an in-progress (or already-completed) run and started a brand
// new one from round 0, with no way to say "no, keep the one that's running".
// Conversely, always silently resuming would remove the only way a user has
// to intentionally restart (there is no separate "Restart" button elsewhere
// in this wizard -- mounting this step *is* the start action). So: when a
// run already exists, ask, rather than guessing either way.
const showResumeDialog = ref(false)
const pendingRunData = ref(null)
// Two-step guard on the destructive option: clicking "Abandon & restart"
// never destroys anything by itself -- it only reveals an explicit second
// confirmation naming exactly what will be erased. A single mis-click can no
// longer wipe a run (this UI previously cost the user a completed 72-round
// run to exactly that mistake).
const abandonConfirmStep = ref(false)

const resumeDialogSummary = computed(() => {
  const data = pendingRunData.value
  if (!data) return ''
  return t('log.existingRunSummary', {
    status: data.runner_status,
    round: data.current_round || 0,
    total: data.total_rounds || props.maxRounds || '-',
    twitterActs: data.twitter_actions_count || 0,
    redditActs: data.reddit_actions_count || 0,
  })
})

const isActiveRunnerStatus = (status) => ['starting', 'running', 'paused', 'stopping'].includes(status)

// "Continue from round N" is only meaningful -- and only accepted by the
// backend -- when a round checkpoint exists AND nothing is currently active
// for this simulation (resume_available reflects the checkpoint's presence,
// but not whether a run is already live). A failed run that predates
// checkpointing (resume_available: false) correctly never shows this.
const resumeAvailable = computed(() => {
  const data = pendingRunData.value
  return !!data?.resume_available && !isActiveRunnerStatus(data?.runner_status)
})
const resumeFromRound = computed(() => pendingRunData.value?.resume_from_round)
const dialogIngestionPending = computed(() => pendingRunData.value?.graph_ingestion_pending || 0)
const dialogStallDetected = computed(() => !!pendingRunData.value?.stall_detected)

const applyResumedStatus = (data) => {
  const runnerStatus = data.runner_status
  runStatus.value = data
  prevTwitterRound.value = data.twitter_current_round || 0
  prevRedditRound.value = data.reddit_current_round || 0

  const isActive = ['starting', 'running', 'paused', 'stopping'].includes(runnerStatus)
  if (isActive) {
    phase.value = 1
    startStatusPolling()
    startDetailPolling()
  } else if (runnerStatus === 'failed') {
    phase.value = 2
    addLog(t('log.simFailed') + (data.error ? `: ${data.error}` : ''))
    emit('update-status', 'error')
  } else {
    // completed / stopped
    phase.value = 2
    emit('update-status', 'completed')
  }
}

const handleResumeExisting = async () => {
  const data = pendingRunData.value
  showResumeDialog.value = false
  abandonConfirmStep.value = false
  pendingRunData.value = null
  if (!data) return
  addLog(t('log.resumingExistingRun'))
  applyResumedStatus(data)
  // Backfill the action feed immediately rather than waiting for the next
  // poll tick, so resuming doesn't look stuck even briefly.
  await fetchRunStatusDetail()
}

// Restart the simulation subprocess from the on-disk round checkpoint. This
// is the opposite of both other options: unlike "reattach" it actually
// starts a new subprocess, and unlike "abandon" it keeps every round already
// completed -- it calls POST /resume, which the backend rejects outright if
// `force` were ever added here, precisely so this path can never turn into a
// wipe-and-restart by accident.
const handleContinueFromCheckpoint = async () => {
  const data = pendingRunData.value
  const round = data?.resume_from_round
  showResumeDialog.value = false
  abandonConfirmStep.value = false
  pendingRunData.value = null
  if (!props.simulationId) return

  resetAllState()
  isContinuing.value = true
  addLog(t('log.continuingFromRound', { round }))
  emit('update-status', 'processing')

  try {
    const params = {
      simulation_id: props.simulationId,
      platform: 'parallel',
      enable_graph_memory_update: true
    }
    if (props.maxRounds) {
      params.max_rounds = props.maxRounds
    }

    const res = await resumeSimulation(params)

    if (res.success && res.data) {
      addLog(t('log.continueSuccess', { round }))
      phase.value = 1
      runStatus.value = res.data
      startStatusPolling()
      startDetailPolling()
      // Backfill the accumulated action feed (prior rounds included) right
      // away instead of waiting for the next 3s detail-poll tick.
      await fetchRunStatusDetail()
    } else {
      addLog(t('log.continueFailed', { error: res.error || t('common.unknownError') }))
      emit('update-status', 'error')
    }
  } catch (err) {
    addLog(t('log.continueFailed', { error: err.message }))
    emit('update-status', 'error')
  } finally {
    isContinuing.value = false
  }
}

// Step 1 of the destructive path: reveal the confirmation panel. Nothing is
// deleted yet.
const handleAbandonAndRestart = () => {
  abandonConfirmStep.value = true
}

const cancelAbandonConfirm = () => {
  abandonConfirmStep.value = false
}

// Step 2: the user has seen exactly what will be erased and confirmed it
// explicitly. Only now does the destructive force-restart actually run.
const confirmAbandonAndRestart = async () => {
  showResumeDialog.value = false
  abandonConfirmStep.value = false
  pendingRunData.value = null
  addLog(t('log.abandoningExistingRun'))
  await doStartSimulation()
}

const resumeOrStartSimulation = async () => {
  if (!props.simulationId) return

  addLog(t('log.step3Init'))

  try {
    const res = await getRunStatus(props.simulationId)
    const data = res.success ? res.data : null
    const runnerStatus = data?.runner_status

    if (!data || runnerStatus === 'idle') {
      // Nothing running yet for this simulation -- nothing to abandon, this
      // mount is the actual "start" action (e.g. navigating here for the
      // first time from step 2). No need to prompt.
      await doStartSimulation()
      return
    }

    // A run already exists (starting/running/paused/stopping/completed/
    // stopped/failed). Ask the user whether to reconnect to it or abandon
    // it and start over, rather than deciding silently either way.
    pendingRunData.value = data
    showResumeDialog.value = true
  } catch (err) {
    console.warn('检查现有模拟状态失败，回退为启动新模拟:', err)
    await doStartSimulation()
  }
}

onMounted(() => {
  resumeOrStartSimulation()
})

onUnmounted(() => {
  stopPolling()
})
</script>

<style scoped>
.simulation-panel {
  height: 100%;
  display: flex;
  flex-direction: column;
  background: #FFFFFF;
  font-family: 'Space Grotesk', 'Noto Sans SC', system-ui, sans-serif;
  overflow: hidden;
}

/* --- Control Bar --- */
.control-bar {
  background: #FFF;
  padding: 12px 24px;
  display: flex;
  justify-content: space-between;
  align-items: center;
  border-bottom: 1px solid #EAEAEA;
  z-index: 10;
  height: 64px;
}

.status-group {
  display: flex;
  gap: 12px;
}

/* Platform Status Cards */
.platform-status {
  display: flex;
  flex-direction: column;
  gap: 4px;
  padding: 6px 12px;
  border-radius: 4px;
  background: #FAFAFA;
  border: 1px solid #EAEAEA;
  opacity: 0.7;
  transition: all 0.3s;
  min-width: 140px;
  position: relative;
  cursor: pointer;
}

.platform-status.active {
  opacity: 1;
  border-color: #333;
  background: #FFF;
}

.platform-status.completed {
  opacity: 1;
  border-color: #1A936F;
  background: #F2FAF6;
}

/* Actions Tooltip */
.actions-tooltip {
  position: absolute;
  top: 100%;
  left: 50%;
  transform: translateX(-50%);
  margin-top: 8px;
  padding: 10px 14px;
  background: #000;
  color: #FFF;
  border-radius: 4px;
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
  opacity: 0;
  visibility: hidden;
  transition: all 0.2s ease;
  z-index: 100;
  min-width: 180px;
  pointer-events: none;
}

.actions-tooltip::before {
  content: '';
  position: absolute;
  top: -6px;
  left: 50%;
  transform: translateX(-50%);
  border-left: 6px solid transparent;
  border-right: 6px solid transparent;
  border-bottom: 6px solid #000;
}

.platform-status:hover .actions-tooltip {
  opacity: 1;
  visibility: visible;
}

.tooltip-title {
  font-size: 10px;
  font-weight: 600;
  color: #999;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  margin-bottom: 8px;
}

.tooltip-actions {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
}

.tooltip-action {
  font-size: 10px;
  font-weight: 600;
  padding: 3px 8px;
  background: rgba(255, 255, 255, 0.15);
  border-radius: 2px;
  color: #FFF;
  letter-spacing: 0.03em;
}

.platform-header {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 2px;
}

.platform-name {
  font-size: 11px;
  font-weight: 700;
  color: #000;
  text-transform: uppercase;
  letter-spacing: 0.05em;
}

.platform-status.twitter .platform-icon { color: #000; }
.platform-status.reddit .platform-icon { color: #000; }

.platform-stats {
  display: flex;
  gap: 10px;
}

.stat {
  display: flex;
  align-items: baseline;
  gap: 3px;
}

.stat-label {
  font-size: 8px;
  color: #999;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.05em;
}

.stat-value {
  font-size: 11px;
  font-weight: 600;
  color: #333;
}

.stat-total, .stat-unit {
  font-size: 9px;
  color: #999;
  font-weight: 400;
}

.status-badge {
  margin-left: auto;
  color: #1A936F;
  display: flex;
  align-items: center;
}

.action-controls {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: 14px;
  flex-wrap: nowrap;
  min-width: 0;
}

/* Action Button */
.action-btn {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  padding: 10px 20px;
  font-size: 13px;
  font-weight: 600;
  border: none;
  border-radius: 4px;
  cursor: pointer;
  transition: all 0.2s ease;
  text-transform: uppercase;
  letter-spacing: 0.05em;
}

.action-btn.primary {
  background: #000;
  color: #FFF;
  flex-shrink: 0;
  white-space: nowrap;
}

.action-btn.primary:hover:not(:disabled) {
  background: #333;
}

.action-btn:disabled {
  opacity: 0.3;
  cursor: not-allowed;
}

/* --- Main Content Area --- */
.main-content-area {
  flex: 1;
  overflow-y: auto;
  position: relative;
  background: #FFF;
}

/* Timeline Header */
.timeline-header {
  position: sticky;
  top: 0;
  background: rgba(255, 255, 255, 0.9);
  backdrop-filter: blur(8px);
  padding: 12px 24px;
  border-bottom: 1px solid #EAEAEA;
  z-index: 5;
  display: flex;
  justify-content: center;
}

.timeline-stats {
  display: flex;
  align-items: center;
  gap: 16px;
  font-size: 11px;
  color: #666;
  background: #F5F5F5;
  padding: 4px 12px;
  border-radius: 20px;
}

.total-count {
  font-weight: 600;
  color: #333;
}

.platform-breakdown {
  display: flex;
  align-items: center;
  gap: 8px;
}

.breakdown-item {
  display: flex;
  align-items: center;
  gap: 4px;
}

.breakdown-divider { color: #DDD; }
.breakdown-item.twitter { color: #000; }
.breakdown-item.reddit { color: #000; }

/* --- Timeline Feed --- */
.timeline-feed {
  padding: 24px 0;
  position: relative;
  min-height: 100%;
  max-width: 900px;
  margin: 0 auto;
}

.timeline-axis {
  position: absolute;
  left: 50%;
  top: 0;
  bottom: 0;
  width: 1px;
  background: #EAEAEA; /* Cleaner line */
  transform: translateX(-50%);
}

.timeline-item {
  display: flex;
  justify-content: center;
  margin-bottom: 32px;
  position: relative;
  width: 100%;
}

.timeline-marker {
  position: absolute;
  left: 50%;
  top: 24px;
  width: 10px;
  height: 10px;
  background: #FFF;
  border: 1px solid #CCC;
  border-radius: 50%;
  transform: translateX(-50%);
  z-index: 2;
  display: flex;
  align-items: center;
  justify-content: center;
}

.marker-dot {
  width: 4px;
  height: 4px;
  background: #CCC;
  border-radius: 50%;
}

.timeline-item.twitter .marker-dot { background: #000; }
.timeline-item.reddit .marker-dot { background: #000; }
.timeline-item.twitter .timeline-marker { border-color: #000; }
.timeline-item.reddit .timeline-marker { border-color: #000; }

/* Card Layout */
.timeline-card {
  width: calc(100% - 48px);
  background: #FFF;
  border-radius: 2px;
  padding: 16px 20px;
  border: 1px solid #EAEAEA;
  box-shadow: 0 2px 10px rgba(0,0,0,0.02);
  position: relative;
  transition: all 0.2s;
}

.timeline-card:hover {
  box-shadow: 0 4px 12px rgba(0,0,0,0.05);
  border-color: #DDD;
}

/* Left side (Twitter) */
.timeline-item.twitter {
  justify-content: flex-start;
  padding-right: 50%;
}
.timeline-item.twitter .timeline-card {
  margin-left: auto;
  margin-right: 32px; /* Gap from axis */
}

/* Right side (Reddit) */
.timeline-item.reddit {
  justify-content: flex-end;
  padding-left: 50%;
}
.timeline-item.reddit .timeline-card {
  margin-right: auto;
  margin-left: 32px; /* Gap from axis */
}

/* Card Content Styles */
.card-header {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  margin-bottom: 12px;
  padding-bottom: 12px;
  border-bottom: 1px solid #F5F5F5;
}

.agent-info {
  display: flex;
  align-items: center;
  gap: 10px;
}

.avatar-placeholder {
  width: 24px;
  height: 24px;
  background: #000;
  color: #FFF;
  border-radius: 50%;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 12px;
  font-weight: 700;
  text-transform: uppercase;
}

.agent-name {
  font-size: 13px;
  font-weight: 600;
  color: #000;
}

.header-meta {
  display: flex;
  align-items: center;
  gap: 8px;
}

.platform-indicator {
  color: #999;
  display: flex;
  align-items: center;
}

.action-badge {
  font-size: 9px;
  padding: 2px 6px;
  border-radius: 2px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  border: 1px solid transparent;
}

/* Monochromatic Badges */
.badge-post { background: #F0F0F0; color: #333; border-color: #E0E0E0; }
.badge-comment { background: #F0F0F0; color: #666; border-color: #E0E0E0; }
.badge-action { background: #FFF; color: #666; border: 1px solid #E0E0E0; }
.badge-meta { background: #FAFAFA; color: #999; border: 1px dashed #DDD; }
.badge-idle { opacity: 0.5; }

.content-text {
  font-size: 13px;
  line-height: 1.6;
  color: #333;
  margin-bottom: 10px;
}

.content-text.main-text {
  font-size: 14px;
  color: #000;
}

/* Info Blocks (Quote, Repost, etc) */
.quoted-block, .repost-content {
  background: #F9F9F9;
  border: 1px solid #EEE;
  padding: 10px 12px;
  border-radius: 2px;
  margin-top: 8px;
  font-size: 12px;
  color: #555;
}

.quote-header, .repost-info, .like-info, .search-info, .follow-info, .vote-info, .idle-info, .comment-context {
  display: flex;
  align-items: center;
  gap: 6px;
  margin-bottom: 6px;
  font-size: 11px;
  color: #666;
}

.icon-small {
  color: #999;
}
.icon-small.filled {
  color: #999; /* Keep icons neutral unless highlighted */
}

.search-query {
  font-family: 'JetBrains Mono', monospace;
  background: #F0F0F0;
  padding: 0 4px;
  border-radius: 2px;
}

.card-footer {
  margin-top: 12px;
  display: flex;
  justify-content: flex-end;
  font-size: 10px;
  color: #BBB;
  font-family: 'JetBrains Mono', monospace;
}

/* Waiting State */
.waiting-state {
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 16px;
  color: #CCC;
  font-size: 12px;
  text-transform: uppercase;
  letter-spacing: 0.1em;
}

.pulse-ring {
  width: 32px;
  height: 32px;
  border-radius: 50%;
  border: 1px solid #EAEAEA;
  animation: ripple 2s infinite;
}

@keyframes ripple {
  0% { transform: scale(0.8); opacity: 1; border-color: #CCC; }
  100% { transform: scale(2.5); opacity: 0; border-color: #EAEAEA; }
}

/* Animation */
.timeline-item-enter-active,
.timeline-item-leave-active {
  transition: all 0.4s cubic-bezier(0.165, 0.84, 0.44, 1);
}

.timeline-item-enter-from {
  opacity: 0;
  transform: translateY(20px);
}

.timeline-item-leave-to {
  opacity: 0;
}

/* Logs */
.system-logs {
  background: #000;
  color: #DDD;
  padding: 16px;
  font-family: 'JetBrains Mono', monospace;
  border-top: 1px solid #222;
  flex-shrink: 0;
}

.log-header {
  display: flex;
  justify-content: space-between;
  border-bottom: 1px solid #333;
  padding-bottom: 8px;
  margin-bottom: 8px;
  font-size: 10px;
  color: #666;
}

.log-content {
  display: flex;
  flex-direction: column;
  gap: 4px;
  height: 100px;
  overflow-y: auto;
  padding-right: 4px;
}

.log-content::-webkit-scrollbar { width: 4px; }
.log-content::-webkit-scrollbar-thumb { background: #333; border-radius: 2px; }

.log-line {
  font-size: 11px;
  display: flex;
  gap: 12px;
  line-height: 1.5;
}

.log-time { color: #555; min-width: 75px; }
.log-msg { color: #BBB; word-break: break-all; }
.mono { font-family: 'JetBrains Mono', monospace; }

/* Loading spinner for button */
.loading-spinner-small {
  display: inline-block;
  width: 14px;
  height: 14px;
  border: 2px solid rgba(255, 255, 255, 0.3);
  border-top-color: #FFF;
  border-radius: 50%;
  animation: spin 0.8s linear infinite;
  margin-right: 6px;
}

/* Resume-vs-restart confirmation dialog */
.profile-modal-overlay {
  position: fixed;
  inset: 0;
  background: rgba(0, 0, 0, 0.5);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 1000;
}

.resume-dialog {
  background: #FFF;
  border-radius: 8px;
  width: 540px;
  max-width: 90vw;
  box-shadow: 0 12px 40px rgba(0, 0, 0, 0.25);
}

.resume-dialog .modal-header {
  padding: 20px 24px 0;
}

.resume-dialog .modal-realname {
  font-size: 16px;
  font-weight: 700;
  color: #111;
}

.resume-dialog-body {
  padding: 16px 24px 24px;
}

.resume-dialog-body p {
  margin: 0 0 20px;
  color: #444;
  font-size: 14px;
  line-height: 1.6;
}

.resume-dialog-actions {
  display: flex;
  justify-content: flex-end;
  gap: 12px;
}

.resume-btn {
  padding: 9px 18px;
  border-radius: 6px;
  font-size: 14px;
  font-weight: 600;
  border: 1px solid transparent;
  cursor: pointer;
  transition: opacity 0.15s;
}

.resume-btn:hover {
  opacity: 0.85;
}

.resume-btn-primary {
  background: #111;
  color: #FFF;
}

.resume-btn-danger {
  background: #FFF;
  color: #C0392B;
  border-color: #C0392B;
}

.resume-btn-ghost {
  background: #FFF;
  color: #444;
  border-color: #DDD;
}

.resume-btn-danger-solid {
  background: #C0392B;
  color: #FFF;
}

.resume-dialog-subnote {
  margin: -10px 0 16px !important;
  padding: 8px 12px;
  background: #FAFAFA;
  border: 1px dashed #DDD;
  border-radius: 4px;
  font-size: 12px !important;
  color: #777 !important;
  line-height: 1.5 !important;
}

/* --- Three-way recovery choice --- */
.resume-options {
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.resume-option {
  display: flex;
  flex-direction: column;
  align-items: stretch;
  gap: 4px;
  text-align: left;
  padding: 12px 14px;
  border-radius: 6px;
  border: 1.5px solid transparent;
  cursor: pointer;
  font-family: inherit;
  transition: all 0.15s;
}

.resume-option-safe {
  background: #F7F7F7;
  border-color: #E5E5E5;
}

.resume-option-safe:hover {
  background: #111;
  border-color: #111;
}

.resume-option-safe:hover .option-title,
.resume-option-safe:hover .option-desc {
  color: #FFF;
}

.resume-option .option-title {
  font-size: 14px;
  font-weight: 700;
  color: #111;
}

.resume-option .option-desc {
  font-size: 12px;
  color: #777;
  line-height: 1.5;
}

.resume-options-divider {
  height: 1px;
  background: #EEE;
  margin: 4px 0;
}

/* The destructive option is intentionally the odd one out: outlined in red,
   never adjacent-styled the same as the two safe options above. */
.resume-option-danger {
  background: #FFF;
  border-color: #F0C4BC;
}

.resume-option-danger:hover {
  background: #FDF2F0;
  border-color: #C0392B;
}

.resume-option-danger .option-title {
  color: #C0392B;
}

.resume-option-danger .option-desc {
  color: #B0554A;
}

/* Step-2 explicit confirmation for the destructive path */
.abandon-confirm-panel {
  background: #FDF2F0;
  border: 1.5px solid #C0392B;
  border-radius: 6px;
  padding: 16px;
}

.abandon-confirm-title {
  margin: 0 0 8px !important;
  font-size: 15px !important;
  font-weight: 700 !important;
  color: #C0392B !important;
}

.abandon-confirm-body {
  margin: 0 0 16px !important;
  font-size: 13px !important;
  color: #7A2D24 !important;
  line-height: 1.6 !important;
}

/* --- Non-blocking recovery banners (stall / graph ingestion pending) --- */
.recovery-banners {
  display: flex;
  flex-direction: column;
  gap: 1px;
  flex-shrink: 0;
}

.recovery-banner {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 10px 24px;
  font-size: 12px;
  line-height: 1.5;
}

.banner-icon {
  font-size: 16px;
  flex-shrink: 0;
}

.banner-text {
  display: flex;
  flex-direction: column;
  gap: 2px;
  flex: 1;
  min-width: 0;
}

.banner-text strong {
  font-size: 12px;
}

.banner-text span {
  color: inherit;
  opacity: 0.85;
}

.banner-action {
  flex-shrink: 0;
  border: 1px solid currentColor;
  background: transparent;
  color: inherit;
  padding: 6px 14px;
  border-radius: 4px;
  font-size: 12px;
  font-weight: 600;
  cursor: pointer;
  white-space: nowrap;
  transition: opacity 0.15s;
}

.banner-action:hover:not(:disabled) {
  opacity: 0.7;
}

.banner-action:disabled {
  opacity: 0.4;
  cursor: not-allowed;
}

.banner-action-ghost {
  border-color: transparent;
  text-decoration: underline;
}

/* Amber / informational -- explicitly NOT alarming. Stalled means "probably
   wedged", not "dead", and the copy + color both need to say so. */
.stall-banner {
  background: #FFF8E1;
  border-bottom: 1px solid #F0DFA0;
  color: #8A6D1D;
}

.ingestion-banner {
  background: #F5F7FF;
  border-bottom: 1px solid #D6DEFF;
  color: #33415C;
}

.report-caveat-hint {
  font-size: 11px;
  color: #B8860B;
  line-height: 1.4;
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  display: -webkit-box;
  -webkit-line-clamp: 2;
  -webkit-box-orient: vertical;
}

.modal-enter-active, .modal-leave-active {
  transition: opacity 0.2s ease;
}

.modal-enter-from, .modal-leave-to {
  opacity: 0;
}
</style>

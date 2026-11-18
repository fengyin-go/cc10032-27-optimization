<template>
  <section class="page" data-module="failover">
    <header class="page-head">
      <div>
        <h2>作业切换</h2>
        <p class="page-desc">
          切换备用拓扑时继承当前有效限流状态，不再回退默认最大QPS；切换完成后故障转移清单才标记可用。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" data-testid="refresh" type="button" @click="reload">刷新</button>
      </div>
    </header>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">故障转移清单</span>
        <strong class="stat-value">{{ manifests.length }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">可用清单</span>
        <strong class="stat-value">{{ manifests.filter((m) => m.status === '可用').length }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">活动限流键</span>
        <strong class="stat-value">{{ activeInfo?.total_active ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">活动配置冲突</span>
        <strong class="stat-value" :class="{ 'error-text': (activeInfo?.violations.length ?? 0) > 0 }">
          {{ activeInfo?.violations.length ?? 0 }}
        </strong>
      </article>
    </div>

    <form class="analysis-form" data-testid="switch-form" @submit.prevent="submitSwitch">
      <label>
        <span>流式作业</span>
        <select v-model.number="form.job_id" data-testid="switch-job" required @change="previewEffective">
          <option :value="0" disabled>选择要切换的作业</option>
          <option v-for="job in jobs" :key="job.id" :value="Number(job.id)">
            #{{ job.id }} {{ job.作业名称 }}（当前拓扑：{{ job.计算拓扑 }}）
          </option>
        </select>
      </label>
      <label>
        <span>目标备用拓扑</span>
        <select v-model="form.target_topology" data-testid="switch-target" required>
          <option value="" disabled>选择备用拓扑</option>
          <option v-for="topo in topologies" :key="String(topo.id)" :value="String(topo.拓扑名称)">
            {{ topo.拓扑名称 }}（{{ topo.status }}）
          </option>
        </select>
      </label>
      <label>
        <span>限流继承模式</span>
        <select v-model="form.mode" data-testid="switch-mode">
          <option value="inherit">沿用旧限流值直到稳定</option>
          <option value="conservative">先降级到保守阈值再逐步恢复</option>
        </select>
      </label>
      <label>
        <span>幂等键（重试不产生第二组结果）</span>
        <input v-model="form.idempotency_key" data-testid="switch-key" placeholder="留空自动生成" />
      </label>
      <button class="btn primary" data-testid="switch-submit" type="submit" :disabled="switching">
        {{ switching ? '切换中…' : '执行拓扑切换' }}
      </button>
    </form>

    <p v-if="effectivePreview" class="ops-note" data-testid="effective-preview">
      当前生效限流值：<strong>{{ effectivePreview.effective_limit }}</strong>
      （配置上限 {{ effectivePreview.max_qps }}，来源 {{ effectivePreview.origin }}，
      {{ effectivePreview.throttled ? '背压中' : '未触发背压' }}）——切换将继承该值而非默认上限。
    </p>

    <div v-if="switchResult" class="ops-panel" data-testid="switch-result">
      <h3>切换结果{{ switchResult.deduplicated ? '（幂等命中，返回已有清单）' : '' }}</h3>
      <template v-if="switchResult.ok && switchResult.manifest">
        <p>
          清单 {{ switchResult.manifest.清单编号 }}：{{ switchResult.manifest.源拓扑 }} →
          {{ switchResult.manifest.目标拓扑 }}，限流继承值
          <strong>{{ switchResult.manifest.继承限流值 }}</strong>
          （切换前生效 {{ switchResult.manifest.生效限流值 }}，配置上限 {{ switchResult.manifest.配置上限 }}），
          作业版本 v{{ switchResult.manifest.来源版本 }} → v{{ switchResult.manifest.作业版本 }}，
          清单状态「{{ switchResult.manifest.status }}」。
        </p>
        <p v-if="switchResult.manifest.恢复计划.length">
          恢复计划：
          <span v-for="step in switchResult.manifest.恢复计划" :key="step.step" class="legend-item">
            第{{ step.step }}步 → {{ step.target_limit }}
          </span>
        </p>
      </template>
      <p v-else class="error-text">{{ switchResult.error || '切换失败，已整组回滚' }}</p>
    </div>

    <h3 class="ops-title">故障转移清单（切换完成后才标记可用）</h3>
    <table class="data-table" data-testid="manifest-table">
      <thead>
        <tr>
          <th>清单编号</th><th>作业</th><th>源拓扑 → 目标拓扑</th><th>继承模式</th>
          <th>生效限流值 → 继承限流值</th><th>来源版本 → 作业版本</th><th>状态</th><th>完成时间</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="m in manifests" :key="m.清单编号" :data-testid="`manifest-${m.id}`" :data-status="m.status">
          <td>{{ m.清单编号 }}</td>
          <td>#{{ m.作业ID }} {{ m.作业名称 }}</td>
          <td>{{ m.源拓扑 }} → {{ m.目标拓扑 }}</td>
          <td>{{ m.继承模式 === 'inherit' ? '沿用旧限流值' : '保守降级恢复' }}</td>
          <td>{{ m.生效限流值 }} → <strong>{{ m.继承限流值 }}</strong></td>
          <td>v{{ m.来源版本 }} → v{{ m.作业版本 }}</td>
          <td>{{ m.status }}<span v-if="m.回滚原因" class="error-text">（{{ m.回滚原因 }}）</span></td>
          <td>{{ m.完成时间 || '—' }}</td>
        </tr>
        <tr v-if="!manifests.length">
          <td colspan="8" class="empty-state">暂无故障转移清单，执行一次拓扑切换后在此查看</td>
        </tr>
      </tbody>
    </table>

    <h3 class="ops-title">活动限流配置核查（切换期间不应出现两个活动配置）</h3>
    <table class="data-table" data-testid="active-throttle-table">
      <thead>
        <tr><th>数据源</th><th>活动状态键数</th><th>结论</th></tr>
      </thead>
      <tbody>
        <tr v-for="(count, source) in activeInfo?.by_source ?? {}" :key="source">
          <td>{{ source }}</td>
          <td>{{ count }}</td>
          <td :class="{ 'error-text': count > 1 }">{{ count > 1 ? '异常：存在多个活动配置' : '正常：单一活动配置' }}</td>
        </tr>
        <tr v-if="!(activeInfo?.total_active ?? 0)">
          <td colspan="3" class="empty-state">当前没有活动限流状态键</td>
        </tr>
      </tbody>
    </table>

    <h3 class="ops-title">整组处理台账（另一个入口联动生成）</h3>
    <form class="analysis-form" @submit.prevent="submitLedger">
      <label>
        <span>批量组</span>
        <select v-model.number="ledgerGroupId" data-testid="ledger-group" required>
          <option :value="0" disabled>选择批量组</option>
          <option v-for="g in groups" :key="g.id" :value="g.id">
            {{ g.组编号 }}（{{ g.操作 }} / {{ g.status }}）
          </option>
        </select>
      </label>
      <button class="btn" data-testid="ledger-submit" type="submit">生成整组台账</button>
      <span v-if="ledgerMessage" class="ops-note">{{ ledgerMessage }}</span>
    </form>
    <table class="data-table" data-testid="ledger-table">
      <thead>
        <tr><th>台账编号</th><th>组编号</th><th>来源入口</th><th>条目数</th><th>成功 / 失败</th><th>生成时间</th></tr>
      </thead>
      <tbody>
        <tr v-for="l in ledgers" :key="l.台账编号">
          <td>{{ l.台账编号 }}</td>
          <td>{{ l.组编号 }}</td>
          <td>{{ l.来源入口 }}</td>
          <td>{{ l.条目数 }}</td>
          <td>{{ l.成功数 }} / {{ l.失败数 }}</td>
          <td>{{ l.生成时间 }}</td>
        </tr>
        <tr v-if="!ledgers.length">
          <td colspan="6" class="empty-state">暂无台账，选择批量组后生成</td>
        </tr>
      </tbody>
    </table>

    <footer class="page-foot">
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import { listEntries } from '@/api/backend-service'
import {
  generateLedger,
  getEffectiveLimit,
  listActiveThrottles,
  listBatchGroups,
  listLedgers,
  listManifests,
  switchTopology,
} from '@/api/ops-service'
import type {
  ActiveThrottleInfo,
  BatchGroup,
  BatchLedger,
  EffectiveLimit,
  FailoverManifest,
  SwitchResult,
} from '@/api/ops-service'
import type { EntryRow } from '@/data/types'

const jobs = ref<EntryRow[]>([])
const topologies = ref<EntryRow[]>([])
const manifests = ref<FailoverManifest[]>([])
const groups = ref<BatchGroup[]>([])
const ledgers = ref<BatchLedger[]>([])
const activeInfo = ref<ActiveThrottleInfo | null>(null)
const effectivePreview = ref<EffectiveLimit | null>(null)
const switchResult = ref<SwitchResult | null>(null)
const errorMessage = ref('')
const ledgerMessage = ref('')
const ledgerGroupId = ref(0)
const switching = ref(false)

const form = ref({
  job_id: 0,
  target_topology: '',
  mode: 'inherit',
  idempotency_key: '',
})

async function previewEffective() {
  const job = jobs.value.find((j) => Number(j.id) === form.value.job_id)
  if (!job) {
    effectivePreview.value = null
    return
  }
  try {
    effectivePreview.value = await getEffectiveLimit(String(job.源端配置))
  } catch {
    effectivePreview.value = null
  }
}

async function submitSwitch() {
  switching.value = true
  errorMessage.value = ''
  switchResult.value = null
  try {
    const result = await switchTopology({
      job_id: form.value.job_id,
      target_topology: form.value.target_topology,
      mode: form.value.mode,
      idempotency_key: form.value.idempotency_key || undefined,
    })
    switchResult.value = result
    if (!result.ok && !result.manifest) errorMessage.value = result.error || '切换失败'
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '切换请求失败'
  } finally {
    switching.value = false
  }
}

async function submitLedger() {
  ledgerMessage.value = ''
  try {
    const result = await generateLedger(ledgerGroupId.value, 'failover_page')
    ledgerMessage.value = result.deduplicated
      ? `台账已存在（幂等命中）：${result.ledger?.台账编号}`
      : `台账已生成：${result.ledger?.台账编号}`
    await reload()
  } catch (error) {
    ledgerMessage.value = error instanceof Error ? error.message : '台账生成失败'
  }
}

async function reload() {
  try {
    const [jobPage, topoPage, manifestPage, groupPage, ledgerPage, active] = await Promise.all([
      listEntries('stream_job'),
      listEntries('task_topology'),
      listManifests(),
      listBatchGroups(),
      listLedgers(),
      listActiveThrottles(),
    ])
    jobs.value = jobPage.items
    topologies.value = topoPage.items
    manifests.value = manifestPage.items
    groups.value = groupPage.items
    ledgers.value = ledgerPage.items
    activeInfo.value = active
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '数据读取失败'
  }
}

onMounted(reload)
</script>

<style scoped>
.ops-title {
  margin: 18px 0 8px;
  font-size: 14px;
}
.ops-panel {
  background: #fff;
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 12px 14px;
  margin: 10px 0;
}
.ops-panel h3 {
  margin: 0 0 8px;
  font-size: 14px;
}
.ops-note {
  color: var(--muted);
  font-size: 13px;
}
.legend-item {
  margin-right: 10px;
}
</style>

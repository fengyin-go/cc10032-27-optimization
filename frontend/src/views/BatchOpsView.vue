<template>
  <section class="page" data-module="batch-ops">
    <header class="page-head">
      <div>
        <h2>批量编排</h2>
        <p class="page-desc">
          批量选择多条作业编排数据整组提交：逐条记录成功失败，事务异常回滚整组，并发重试只接收一组版本。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" data-testid="refresh" type="button" @click="reload">刷新</button>
      </div>
    </header>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">批量组</span>
        <strong class="stat-value">{{ groups.length }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">已完成</span>
        <strong class="stat-value">{{ groups.filter((g) => g.status === '已完成').length }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">已回滚</span>
        <strong class="stat-value">{{ groups.filter((g) => g.status === '已回滚').length }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">台账</span>
        <strong class="stat-value">{{ ledgers.length }}</strong>
      </article>
    </div>

    <h3 class="ops-title">批量选择作业编排数据</h3>
    <form class="analysis-form" data-testid="batch-form" @submit.prevent="submit">
      <label>
        <span>整组操作</span>
        <select v-model="operation" data-testid="batch-operation">
          <option value="启动作业">启动作业</option>
          <option value="暂停作业">暂停作业</option>
          <option value="停止作业">停止作业</option>
        </select>
      </label>
      <label>
        <span>事务模式</span>
        <select v-model="atomic" data-testid="batch-atomic">
          <option :value="true">原子提交（异常回滚整组）</option>
          <option :value="false">逐条独立（允许部分失败）</option>
        </select>
      </label>
      <label>
        <span>幂等键</span>
        <input v-model="idempotencyKey" data-testid="batch-key" placeholder="留空自动生成" />
      </label>
      <button class="btn primary" data-testid="batch-submit" type="submit" :disabled="submitting || !selected.size">
        {{ submitting ? '提交中…' : `整组提交（已选 ${selected.size} 条）` }}
      </button>
    </form>

    <table class="data-table" data-testid="job-select-table">
      <thead>
        <tr>
          <th>选择</th><th>ID</th><th>作业名称</th><th>源端配置</th><th>计算拓扑</th>
          <th>当前状态</th><th>编排版本</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="job in jobs" :key="String(job.id)" :data-testid="`job-row-${job.id}`">
          <td>
            <input
              type="checkbox"
              :checked="selected.has(Number(job.id))"
              :data-testid="`select-${job.id}`"
              @change="toggle(Number(job.id))"
            />
          </td>
          <td>{{ job.id }}</td>
          <td>{{ job.作业名称 }}</td>
          <td>{{ job.源端配置 }}</td>
          <td>{{ job.计算拓扑 }}</td>
          <td>{{ job.status }}</td>
          <td>v{{ job.version ?? 1 }}</td>
        </tr>
        <tr v-if="!jobs.length">
          <td colspan="7" class="empty-state">暂无流式作业数据</td>
        </tr>
      </tbody>
    </table>

    <h3 class="ops-title">批量结果组（一次提交保留整组结果）</h3>
    <table class="data-table" data-testid="group-table">
      <thead>
        <tr>
          <th>组编号</th><th>操作</th><th>条目</th><th>成功 / 失败</th><th>组版本</th>
          <th>事务</th><th>状态</th><th>台账</th><th>创建时间</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="g in groups"
          :key="g.组编号"
          :data-testid="`group-${g.id}`"
          :data-status="g.status"
          class="group-row"
          @click="selectGroup(g)"
        >
          <td>{{ g.组编号 }}</td>
          <td>{{ g.操作 }}</td>
          <td>{{ g.total }}</td>
          <td>{{ g.success_count }} / {{ g.fail_count }}</td>
          <td>v{{ g.version }}</td>
          <td>{{ g.原子提交 ? '原子' : '逐条' }}</td>
          <td>{{ g.status }}</td>
          <td>{{ g.ledger_id ? `台账#${g.ledger_id}` : '—' }}</td>
          <td>{{ g.创建时间 }}</td>
        </tr>
        <tr v-if="!groups.length">
          <td colspan="9" class="empty-state">暂无批量组，勾选作业后整组提交</td>
        </tr>
      </tbody>
    </table>

    <div v-if="currentGroup" class="ops-panel" data-testid="group-detail">
      <h3>
        组详情 {{ currentGroup.组编号 }}：逐条结果
        <span class="ops-note">（组版本 v{{ currentGroup.version }}，状态「{{ currentGroup.status }}」）</span>
      </h3>
      <table class="data-table" data-testid="group-items">
        <thead>
          <tr>
            <th>逐条版本</th><th>作业</th><th>结果</th><th>状态流转</th>
            <th>编排版本</th><th>回滚</th><th>对照故障转移清单（唯一继承限流配置）</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in currentGroup.items" :key="item.seq" :data-testid="`item-${item.seq}`">
            <td>#{{ item.item_version }}</td>
            <td>#{{ item.job_id }} {{ item.作业名称 }}</td>
            <td :class="{ 'error-text': !item.ok }">{{ item.ok ? '成功' : '失败' }}：{{ item.message }}</td>
            <td>{{ item.before_status ?? '—' }} → {{ item.after_status ?? '—' }}</td>
            <td>v{{ item.job_version_before ?? '—' }} → v{{ item.job_version_after ?? '—' }}</td>
            <td>{{ item.rolled_back ? '已回滚' : '—' }}</td>
            <td>
              <template v-if="manifestFor(item.job_id)">
                {{ manifestFor(item.job_id)?.清单编号 }}：继承限流值
                <strong>{{ manifestFor(item.job_id)?.继承限流值 }}</strong>
                （作业版本 v{{ manifestFor(item.job_id)?.作业版本 }}，{{ manifestFor(item.job_id)?.status }}）
              </template>
              <span v-else class="ops-note">无切换记录</span>
            </td>
          </tr>
        </tbody>
      </table>
      <div class="page-actions ops-actions">
        <button class="btn" data-testid="ledger-submit" type="button" @click="submitLedger">
          生成整组台账
        </button>
        <span v-if="ledgerMessage" class="ops-note">{{ ledgerMessage }}</span>
      </div>
    </div>

    <h3 class="ops-title">整组处理台账</h3>
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
          <td colspan="6" class="empty-state">暂无台账，可在组详情或作业切换页生成</td>
        </tr>
      </tbody>
    </table>

    <footer class="page-foot">
      <span v-if="submitMessage" class="ops-note">{{ submitMessage }}</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import { listEntries } from '@/api/backend-service'
import {
  generateLedger,
  listBatchGroups,
  listLedgers,
  listManifests,
  submitBatch,
} from '@/api/ops-service'
import type { BatchGroup, BatchLedger, FailoverManifest } from '@/api/ops-service'
import type { EntryRow } from '@/data/types'

const jobs = ref<EntryRow[]>([])
const groups = ref<BatchGroup[]>([])
const ledgers = ref<BatchLedger[]>([])
const manifests = ref<FailoverManifest[]>([])
const selected = ref<Set<number>>(new Set())
const operation = ref('启动作业')
const atomic = ref(true)
const idempotencyKey = ref('')
const submitting = ref(false)
const submitMessage = ref('')
const errorMessage = ref('')
const ledgerMessage = ref('')
const currentGroup = ref<BatchGroup | null>(null)

function toggle(id: number) {
  const next = new Set(selected.value)
  if (next.has(id)) next.delete(id)
  else next.add(id)
  selected.value = next
}

function selectGroup(group: BatchGroup) {
  currentGroup.value = group
  ledgerMessage.value = ''
}

function manifestFor(jobId: number): FailoverManifest | undefined {
  // 对照故障转移清单：每个作业只取最新一条唯一继承的限流配置
  const matched = manifests.value.filter((m) => Number(m.作业ID) === jobId)
  return matched.length ? matched[matched.length - 1] : undefined
}

async function submit() {
  submitting.value = true
  submitMessage.value = ''
  errorMessage.value = ''
  try {
    const result = await submitBatch({
      job_ids: [...selected.value],
      operation: operation.value,
      idempotency_key: idempotencyKey.value || undefined,
      atomic: atomic.value,
    })
    if (result.group) {
      submitMessage.value = result.deduplicated
        ? `幂等命中，返回已有组结果：${result.group.组编号}`
        : `整组提交完成：${result.group.组编号}（${result.group.status}）`
      currentGroup.value = result.group
    } else {
      errorMessage.value = result.error || '批量提交失败'
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '批量提交请求失败'
  } finally {
    submitting.value = false
  }
}

async function submitLedger() {
  if (!currentGroup.value) return
  ledgerMessage.value = ''
  try {
    const result = await generateLedger(currentGroup.value.id, 'batch_page')
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
    const [jobPage, groupPage, ledgerPage, manifestPage] = await Promise.all([
      listEntries('stream_job'),
      listBatchGroups(),
      listLedgers(),
      listManifests(),
    ])
    jobs.value = jobPage.items
    groups.value = groupPage.items
    ledgers.value = ledgerPage.items
    manifests.value = manifestPage.items
    if (currentGroup.value) {
      const fresh = groupPage.items.find((g) => g.id === currentGroup.value?.id)
      if (fresh) currentGroup.value = fresh
    }
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
.group-row {
  cursor: pointer;
}
.group-row:hover {
  background: #f0f6ff;
}
.ops-actions {
  margin-top: 10px;
  display: flex;
  gap: 10px;
  align-items: center;
}
</style>

/** 拓扑切换与批量编排接口封装（序号27）：与后端 /api/ops/* 对应。 */
import { request } from './client'

// ---- 类型 ----

export type FailoverManifest = {
  id: number
  清单编号: string
  作业ID: number
  作业名称: string
  数据源: string
  源拓扑: string
  目标拓扑: string
  继承模式: string
  生效限流值: number
  继承限流值: number
  配置上限: number
  来源版本: number
  作业版本: number
  幂等键: string
  恢复计划: { step: number; ratio: number; target_limit: number }[]
  status: string
  创建时间: string
  完成时间: string
  回滚原因?: string
}

export type SwitchResult = {
  ok: boolean
  deduplicated?: boolean
  error?: string
  manifest?: FailoverManifest
  inherited_limit?: number
  effective_before?: number
  job_version?: number
  source_version?: number
}

export type ActiveThrottleInfo = {
  ok: boolean
  total_active: number
  by_source: Record<string, number>
  violations: string[]
  keys: Record<string, unknown>[]
}

export type EffectiveLimit = {
  source: string
  max_qps: number
  effective_limit: number
  throttled: boolean
  strategy: string
  origin: string
  active_key_count: number
}

export type BatchItem = {
  seq: number
  job_id: number
  作业名称: string
  ok: boolean
  message: string
  item_version: number
  rolled_back: boolean
  before_status?: string
  after_status?: string
  job_version_before?: number
  job_version_after?: number
}

export type BatchGroup = {
  id: number
  组编号: string
  操作: string
  幂等键: string
  原子提交: boolean
  status: string
  version: number
  total: number
  success_count: number
  fail_count: number
  items: BatchItem[]
  ledger_id: number | null
  创建时间: string
}

export type BatchLedger = {
  id: number
  台账编号: string
  group_id: number
  组编号: string
  来源入口: string
  操作: string
  组状态: string
  组版本: number
  条目数: number
  成功数: number
  失败数: number
  作业版本快照: {
    job_id: number
    作业名称: string
    job_version: number
    结果: string
    已回滚: boolean
  }[]
  生成时间: string
}

// ---- 拓扑切换 ----

export async function switchTopology(payload: {
  job_id: number
  target_topology: string
  mode: string
  idempotency_key?: string
}): Promise<SwitchResult> {
  const response = await request('/api/ops/failover/switch', {
    method: 'POST',
    body: JSON.stringify(payload),
  })
  return await response.json()
}

export async function listManifests(): Promise<{ items: FailoverManifest[]; total: number }> {
  const response = await request('/api/ops/failover/manifests')
  return await response.json()
}

export async function listActiveThrottles(): Promise<ActiveThrottleInfo> {
  const response = await request('/api/ops/throughput/active')
  return await response.json()
}

export async function getEffectiveLimit(source: string): Promise<EffectiveLimit> {
  const response = await request(`/api/ops/throughput/effective?source=${encodeURIComponent(source)}`)
  return await response.json()
}

// ---- 批量编排 ----

export async function submitBatch(payload: {
  job_ids: number[]
  operation: string
  idempotency_key?: string
  atomic?: boolean
}): Promise<{ ok: boolean; deduplicated?: boolean; error?: string; group?: BatchGroup }> {
  const response = await request('/api/ops/batch/submit', {
    method: 'POST',
    body: JSON.stringify(payload),
  })
  return await response.json()
}

export async function listBatchGroups(): Promise<{ items: BatchGroup[]; total: number }> {
  const response = await request('/api/ops/batch/groups')
  return await response.json()
}

export async function generateLedger(
  groupId: number,
  entry: string,
): Promise<{ ok: boolean; deduplicated?: boolean; ledger?: BatchLedger }> {
  const response = await request(`/api/ops/batch/groups/${groupId}/ledger`, {
    method: 'POST',
    body: JSON.stringify({ entry }),
  })
  return await response.json()
}

export async function listLedgers(): Promise<{ items: BatchLedger[]; total: number }> {
  const response = await request('/api/ops/batch/ledgers')
  return await response.json()
}

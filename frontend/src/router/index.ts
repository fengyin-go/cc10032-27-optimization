import { createRouter, createWebHistory } from 'vue-router'

import BatchOpsView from '@/views/BatchOpsView.vue'
import FailoverView from '@/views/FailoverView.vue'
import Overview from '@/views/Overview.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    // 固定路径须先于 /:moduleKey 动态段注册
    { path: '/ops/failover', name: 'failover', component: FailoverView },
    { path: '/ops/batch', name: 'batch-ops', component: BatchOpsView },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router

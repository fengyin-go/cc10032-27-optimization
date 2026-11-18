"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import ops, resource

# ops 路由（/api/ops/...）须先于通用资源路由注册，
# 避免 /api/{module} 动态段抢先匹配固定前缀。
ROUTERS = [ops, resource]

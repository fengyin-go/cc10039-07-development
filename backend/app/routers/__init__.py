"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import resource, cross_module

# cross_module 必须先注册：/api/cross/events 等固定路径若排在
# /api/{module}/{entry_id} 之后，会被动态段抢先匹配而 422。
ROUTERS = [cross_module, resource]
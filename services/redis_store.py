import json
from datetime import date
import asyncio
from typing import Any, Dict, Optional


from django_redis import get_redis_connection


class RedisStore:
    """针对投放快照、预算、频控等 RTB 需求的 Redis 客户端"""
    placement_prefix = "rtb:placement:"
    placement_channel = "rtb:placement:updated"
    budget_prefix = "rtb:budget:"
    daily_budget_prefix = "rtb:budget_daily:"
    freqcfg_prefix = "rtb:freqcfg:"
    userfreq_prefix = "rtb:userfreq:"

    def __init__(self, alias: Optional[str] = "default") -> None:
        self.alias = alias 
        self._conn = None

    @property
    def connection(self):
        """惰性获取原生 Redis 连接。"""
        if self._conn is None:
            self._conn = get_redis_connection(self.alias)
        return self._conn

    async def close(self) -> None:
        """关闭 Redis 连接。"""
        if self._conn is not None:
            self._conn = None

    async def refresh(self) -> None:
        """重新获取连接，适用于长时间空闲后的重连。"""
        self._conn = get_redis_connection(self.alias)

    async def _call(self, func, *args, **kwargs):
        """异步执行 Redis 命令，确保在事件循环中运行。"""
        return await asyncio.to_thread(func, *args, **kwargs)

    @staticmethod
    async def _serialize(value: Dict[str, Any]) -> str:
        """异步序列化字典为 JSON 字符串。"""
        return await asyncio.to_thread(json.dumps, value, ensure_ascii=False)

    @staticmethod
    async def _deserialize(raw: str) -> Dict[str, Any]:
        """异步反序列化 JSON 字符串为字典。"""
        return await asyncio.to_thread(json.loads, raw)

    def placement_key(self, placement_id: int) -> str:
        """生成投放快照的 Redis 键。"""
        return f"{self.placement_prefix}{placement_id}"

    async def write_snapshot(self, placement_id: int, payload: Dict[str, Any]) -> None:
        """异步写入投放快照到 Redis。"""
        key = self.placement_key(placement_id)
        record = {"payload": payload}
        serialized = await self._serialize(record)
        await self._call(self.connection.set, key, serialized)

    async def publish_snapshot(self, placement_id: int) -> None:
        """异步发布投放快照更新到 Redis 频道。"""
        key = self.placement_key(placement_id)
        await self._call(self.connection.publish, self.placement_channel, key)

    async def read_snapshot(self, placement_id: int) -> Optional[Dict[str, Any]]:
        """异步读取投放快照从 Redis。"""
        raw = await self._call(self.connection.get, self.placement_key(placement_id))
        if not raw:
            return None
        try:
            record = await self._deserialize(raw)
        except json.JSONDecodeError:
            return None
        return record.get("payload") or None

    async def ensure_budget_bucket(self, placement_id: int, total: float, daily_cap: float | None, alert_threshold: float | None = None) -> None:
        """异步确保预算桶存在，不存在则创建。"""
        key = f"{self.budget_prefix}{placement_id}"
        mapping = {
            "total": str(total),
            "spent": "0",
            "daily_cap": str(daily_cap or 0),
            "alert_threshold": str(alert_threshold or 0),
        }
        if not await self._call(self.connection.exists, key):
            await self._call(self.connection.hset, key, mapping=mapping)
        else:
            await self._call(self.connection.hset, key, mapping={
                "total": mapping["total"],
                "daily_cap": mapping["daily_cap"],
                "alert_threshold": mapping["alert_threshold"],
            })

    async def get_budget_bucket(self, placement_id: int) -> Dict[str, str]:
        """异步获取预算桶信息。"""
        return await self._call(self.connection.hgetall, f"{self.budget_prefix}{placement_id}")

    async def incr_spent(self, placement_id: int, amount: float) -> float:
        """异步增加预算已花费金额。"""
        key = f"{self.budget_prefix}{placement_id}"
        return float(await self._call(self.connection.hincrbyfloat, key, "spent", amount))

    async def get_daily_spent(self, placement_id: int, day: date) -> float:
        """异步获取指定日期的预算已花费金额。"""
        key = f"{self.daily_budget_prefix}{placement_id}:{day.isoformat()}"
        value = await self._call(self.connection.get, key)
        if value is None:
            return 0.0
        try:
            return float(value)
        except ValueError:
            return 0.0

    async def incr_daily_spent(self, placement_id: int, amount: float, day: date) -> float:
        """异步增加指定日期的预算已花费金额。"""
        key = f"{self.daily_budget_prefix}{placement_id}:{day.isoformat()}"     
        def _pipe():
            pipe = self.connection.pipeline()
            pipe.incrbyfloat(key, amount)
            pipe.expire(key, 172800)
            return pipe.execute()
        spent, _ = await self._call(_pipe)
        return float(spent)

    async def cache_frequency_rule(self, placement_id: int, limit_per_user: int, window_hours: int) -> None:
        """异步缓存频率规则到 Redis。"""
        key = f"{self.freqcfg_prefix}{placement_id}"
        await self._call(self.connection.hset, key, mapping={
            "limit_per_user": str(limit_per_user),
            "window_hours": str(window_hours),
        })

    async def get_frequency_rule(self, placement_id: int) -> Dict[str, str]:
        """异步获取缓存的频率规则。"""
        return await self._call(self.connection.hgetall, f"{self.freqcfg_prefix}{placement_id}")

    async def check_and_increment_freq(self, placement_id: int, user_id: str, limit_per_user: int, window_hours: int) -> bool:
        """异步检查并增加用户在频率窗口内的请求次数。"""
        key = f"{self.userfreq_prefix}{placement_id}:{user_id}"
        def _pipe():
            pipe = self.connection.pipeline()
            pipe.incr(key, 1)
            pipe.expire(key, window_hours * 3600)
            return pipe.execute()
        count, _ = await self._call(_pipe)
        return int(count) <= limit_per_user

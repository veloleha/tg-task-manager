import redis.asyncio as redis
from redis.exceptions import RedisError
import json
from datetime import datetime
from config import settings
import logging

logger = logging.getLogger(__name__)

class RedisClient:
    def __init__(self):
        self.conn = redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            password=settings.REDIS_PASSWORD,
            decode_responses=True
        )
        self.task_counter = 0

    async def save_task(self, task_data: dict) -> str:
        task_id = f"task:{datetime.now().strftime('%Y%m%d')}:{self.task_counter}"
        self.task_counter += 1
        task_data["task_number"] = task_id
        await self.conn.hset(task_id, mapping=task_data)
        await self.conn.expire(task_id, settings.TASK_TTL)
        return task_id

    async def update_task_status(self, task_id: str, status: str):
        await self.conn.hset(task_id, "status", status)
        await self.conn.hset(task_id, "updated_at", datetime.utcnow().isoformat())

    async def set_assignee(self, task_id: str, username: str):
        await self.conn.hset(task_id, "assignee", username)

    async def add_reply(self, task_id: str, reply: str):
        await self.conn.hset(task_id, "reply", reply)

    async def get_task(self, task_id: str) -> dict:
        return await self.conn.hgetall(task_id)

    async def get_tasks_by_assignee(self, username: str) -> list:
        tasks = []
        async for key in self.conn.scan_iter("task:*"):
            task = await self.conn.hgetall(key)
            if task.get("assignee") == username:
                tasks.append(task)
        return tasks

    async def publish_event(self, channel: str, data: dict):
        await self.conn.publish(channel, json.dumps(data))

    async def increment_counter(self, counter_type: str):
        await self.conn.incr(f"counter:{counter_type}")

    async def is_connected(self) -> bool:
        try:
            return await self.conn.ping()
        except RedisError:
            return False
        
    async def get_executors_stats(self) -> dict:
        """Возвращает статистику по исполнителям"""
        pipe = self.conn.pipeline()
        
        # 1. Получаем всех исполнителей
        executors = set()
        async for key in self.conn.scan_iter("task:*"):
            task = await self.conn.hgetall(key)
            if assignee := task.get("assignee"):
                executors.add(assignee)
        
        # 2. Собираем статистику для каждого
        stats = {}
        for executor in executors:
            pipe.get(f"stats:in_progress:{executor}")
            pipe.get(f"stats:completed:{executor}")
            counts = await pipe.execute()
            stats[executor] = {
                "in_progress": int(counts[0] or 0),
                "completed": int(counts[1] or 0)
            }
        
        return stats   
    async def increment_executor_counter(self, executor: str, counter_type: str):
        """Увеличивает счётчик задач исполнителя"""
        await self.conn.incr(f"stats:{counter_type}:{executor}")     

redis_client = RedisClient()
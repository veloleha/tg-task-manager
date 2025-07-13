import redis.asyncio as redis
from redis.exceptions import RedisError
import json
from datetime import datetime
from config.settings import settings
import logging
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)

class RedisClient:
    def __init__(self):
        self.conn = redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            password=settings.REDIS_PASSWORD,
            decode_responses=False,  # Важно для корректной работы с разными типами данных
            socket_connect_timeout=5,
            socket_timeout=5
        )
        self.task_counter = 0

    async def _ensure_connection(self):
        """Проверяет подключение к Redis"""
        try:
            if not await self.conn.ping():
                raise ConnectionError("Redis connection failed")
        except Exception as e:
            logger.error(f"Redis connection error: {e}")
            raise

    async def save_task(self, task_data: Dict[str, Any]) -> str:
        """Сохраняет задачу в Redis с гарантированной совместимостью"""
        try:
            await self._ensure_connection()
            
            # Генерируем ID задачи
            task_id = f"task:{datetime.now().strftime('%Y%m%d')}:{self.task_counter}"
            self.task_counter += 1
            
            # Подготавливаем данные для сохранения
            pipeline = self.conn.pipeline()
            
            # Сохраняем как строку JSON (альтернатива хэшу)
            task_json = json.dumps({
                k: str(v) if v is not None else ""
                for k, v in task_data.items()
            })
            pipeline.set(task_id, task_json)
            pipeline.expire(task_id, 604800)
            
            # Сохраняем индекс для быстрого поиска
            pipeline.sadd(f"tasks:index", task_id)
            
            await pipeline.execute()
            return task_id
            
        except Exception as e:
            logger.error(f"Ошибка сохранения задачи: {e}")
            raise

    async def get_task(self, task_id: str) -> Dict[str, Any]:
        """Получает задачу по ID"""
        try:
            await self._ensure_connection()
            task_json = await self.conn.get(task_id)
            if not task_json:
                return {}
                
            if isinstance(task_json, bytes):
                task_json = task_json.decode('utf-8')
                
            return json.loads(task_json)
        except Exception as e:
            logger.error(f"Error getting task {task_id}: {e}", exc_info=True)
            return {}

    async def update_task(self, task_id: str, **fields):
        """Обновляет поля задачи"""
        try:
            await self._ensure_connection()
            task = await self.get_task(task_id)
            if not task:
                return False
                
            task.update(fields)
            await self.conn.set(task_id, json.dumps(task))
            return True
        except Exception as e:
            logger.error(f"Error updating task {task_id}: {e}")
            return False

    async def update_task_status(self, task_id: str, status: str):
        """Обновляет статус задачи"""
        try:
            await self._ensure_connection()
            task = await self.get_task(task_id)
            if not task:
                return False
                
            task['status'] = status
            task['updated_at'] = datetime.utcnow().isoformat()
            await self.conn.set(task_id, json.dumps(task))
            return True
        except Exception as e:
            logger.error(f"Error updating task status: {e}")
            return False

    async def set_assignee(self, task_id: str, username: str):
        """Устанавливает исполнителя задачи"""
        try:
            await self._ensure_connection()
            await self.conn.hset(task_id, "assignee", username)
        except Exception as e:
            logger.error(f"Error setting assignee: {e}")
            raise

    async def get_tasks_by_status(self, status: str) -> List[Dict[str, Any]]:
        """Получает все задачи с указанным статусом"""
        try:
            await self._ensure_connection()
            tasks = []
            async for key in self.conn.scan_iter("task:*"):
                task = await self.get_task(key)
                if task.get("status") == status:
                    tasks.append(task)
            return tasks
        except Exception as e:
            logger.error(f"Error getting tasks by status: {e}")
            return []

    async def get_global_stats(self) -> Dict[str, Any]:
        """Возвращает глобальную статистику по задачам"""
        try:
            await self._ensure_connection()
            return {
                "unreacted": int(await self.conn.get("counter:unreacted") or 0),
                "waiting": int(await self.conn.get("counter:waiting") or 0),
                "in_progress": int(await self.conn.get("counter:in_progress") or 0),
                "completed": int(await self.conn.get("counter:completed") or 0),
                "executors": await self.get_executors_stats()
            }
        except Exception as e:
            logger.error(f"Error getting global stats: {e}")
            return {
                "unreacted": 0,
                "waiting": 0,
                "in_progress": 0,
                "completed": 0,
                "executors": {}
            }

    async def get_executors_stats(self) -> Dict[str, Dict[str, int]]:
        """Статистика по исполнителям"""
        try:
            await self._ensure_connection()
            executors = set()
            
            # Собираем всех исполнителей
            async for key in self.conn.scan_iter("task:*"):
                task = await self.get_task(key)
                if assignee := task.get("assignee"):
                    executors.add(assignee)
            
            # Собираем статистику
            stats = {}
            for executor in executors:
                stats[executor] = {
                    "in_progress": int(await self.conn.get(f"stats:in_progress:{executor}") or 0),
                    "completed": int(await self.conn.get(f"stats:completed:{executor}") or 0)
                }
            
            return stats
        except Exception as e:
            logger.error(f"Error getting executors stats: {e}")
            return {}

    async def increment_counter(self, counter_type: str):
        """Увеличивает счетчик задач"""
        try:
            await self._ensure_connection()
            await self.conn.incr(f"counter:{counter_type}")
        except Exception as e:
            logger.error(f"Error incrementing counter: {e}")
            raise

    async def publish_event(self, channel: str, data: Dict[str, Any]):
        """Публикует событие в Redis Pub/Sub"""
        try:
            await self._ensure_connection()
            await self.conn.publish(channel, json.dumps(data))
        except Exception as e:
            logger.error(f"Error publishing event: {e}")
            raise

    async def is_connected(self) -> bool:
        """Проверяет подключение к Redis"""
        try:
            return await self.conn.ping()
        except RedisError:
            return False

    async def close(self):
        """Закрывает соединение с Redis"""
        await self.conn.close()


# Глобальный экземпляр клиента Redis
redis_client = RedisClient()
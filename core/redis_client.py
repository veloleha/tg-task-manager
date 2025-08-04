import redis.asyncio as redis
from redis.exceptions import RedisError
import json
from datetime import datetime
from config.settings import settings
import logging
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)

class RedisManager:
    """Менеджер для работы с Redis"""
    def __init__(self):
        self.conn = None
        self.pubsub_conn = None  # Отдельное соединение для PubSub
        self.task_counter = 0

    async def _ensure_connection(self):
        """Проверяет подключение к Redis"""
        try:
            if self.conn is None:
                logger.info("Establishing Redis connection")
                self.conn = redis.Redis(
                    host=settings.REDIS_HOST,
                    port=settings.REDIS_PORT,
                    db=settings.REDIS_DB,
                    password=settings.REDIS_PASSWORD,
                    decode_responses=False,
                    socket_connect_timeout=5,
                    socket_timeout=5
                )
            
            # Проверяем соединение через PING
            if not await self.conn.ping():
                raise ConnectionError("Redis connection failed")
        except Exception as e:
            logger.error(f"Redis connection error: {e}")
            # Закрываем соединение при ошибке
            if self.conn:
                await self.conn.close()
            self.conn = None
            raise

    async def connect(self):
        """Устанавливает соединение с Redis"""
        logger.info("Calling Redis connect method")
        await self._ensure_connection()

    async def save_task(self, task_data: Dict[str, Any]) -> str:
        """Сохраняет задачу в Redis с гарантированной совместимостью"""
        try:
            logger.info(f"[DB][SAVE_TASK] Starting task save operation...")
            await self._ensure_connection()
            logger.info(f"[DB][SAVE_TASK] Redis connection ensured")
            
            # Генерируем уникальный ID задачи используя UUID
            import uuid
            task_id = str(uuid.uuid4())
            full_key = f"task:{task_id}"
            logger.info(f"[DB][SAVE_TASK] Generated task ID: {task_id}, key: {full_key}")
            
            # Подготавливаем данные для сохранения
            pipeline = self.conn.pipeline()
            
            # Сохраняем как строку JSON
            task_json = json.dumps({
                k: str(v) if v is not None else ""
                for k, v in task_data.items()
            })
            logger.info(f"[DB][SAVE_TASK] Task data serialized to JSON: {len(task_json)} chars")
            
            pipeline.set(full_key, task_json)
            pipeline.expire(full_key, 604800)  # TTL 7 дней
            logger.info(f"[DB][SAVE_TASK] Added SET and EXPIRE commands to pipeline")
            
            # Сохраняем индекс для быстрого поиска
            pipeline.sadd("tasks:index", full_key)
            logger.info(f"[DB][SAVE_TASK] Added SADD command to pipeline for index")
            
            result = await pipeline.execute()
            logger.info(f"[DB][SAVE_TASK] Pipeline executed successfully: {result}")
            
            logger.info(f"[DB][SAVE_TASK] ✅ Task saved with ID: {task_id} (key: {full_key})")
            return task_id  # Возвращаем только UUID, без префикса
            
        except Exception as e:
            logger.error(f"[DB][SAVE_TASK] ❌ Ошибка сохранения задачи: {e}", exc_info=True)
            raise

    async def get_task(self, task_id: str) -> Dict[str, Any]:
        """Получает задачу по ID"""
        try:
            logger.info(f"[DB][GET_TASK] Starting task retrieval for ID: {task_id}")
            await self._ensure_connection()
            logger.info(f"[DB][GET_TASK] Redis connection ensured")
            
            # Добавляем префикс если его нет
            if not task_id.startswith("task:"):
                full_key = f"task:{task_id}"
            else:
                full_key = task_id
            logger.info(f"[DB][GET_TASK] Using key: {full_key}")
            
            task_json = await self.conn.get(full_key)
            logger.info(f"[DB][GET_TASK] Redis GET result: {task_json is not None} (length: {len(task_json) if task_json else 0})")
            
            if not task_json:
                logger.warning(f"[DB][GET_TASK] ⚠️ Task not found: {task_id} (key: {full_key})")
                return {}
                
            if isinstance(task_json, bytes):
                task_json = task_json.decode('utf-8')
                logger.info(f"[DB][GET_TASK] Decoded bytes to string")
                
            task_data = json.loads(task_json)
            logger.info(f"[DB][GET_TASK] ✅ Task retrieved successfully: {task_id} (key: {full_key}) - {len(task_data)} fields")
            return task_data
            
        except Exception as e:
            logger.error(f"[DB][GET_TASK] ❌ Error getting task {task_id}: {e}", exc_info=True)
            return {}

    async def update_task(self, task_id: str, **fields):
        """Обновляет поля задачи"""
        try:
            await self._ensure_connection()
            task = await self.get_task(task_id)
            if not task:
                return False
            
            # Добавляем префикс если его нет
            if not task_id.startswith("task:"):
                full_key = f"task:{task_id}"
            else:
                full_key = task_id
                
            task.update(fields)
            await self.conn.set(full_key, json.dumps(task))
            logger.debug(f"Task updated: {task_id} (key: {full_key})")
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
            task = await self.get_task(task_id)
            if not task:
                return False
                
            task['assignee'] = username
            await self.conn.set(f"task:{task_id}", json.dumps(task))
            return True
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

    async def get_tasks_by_assignee(self, assignee: str) -> List[Dict[str, Any]]:
        """Получает все задачи с указанным исполнителем"""
        try:
            await self._ensure_connection()
            tasks = []
            async for key in self.conn.scan_iter("task:*"):
                task = await self.get_task(key)
                if task.get("assignee") == assignee:
                    tasks.append(task)
            return tasks
        except Exception as e:
            logger.error(f"Error getting tasks by assignee: {e}")
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
                # Извлекаем task_id из ключа
                if isinstance(key, bytes):
                    key = key.decode('utf-8')
                task_id = key.split(':', 1)[1] if ':' in key else key
                task = await self.get_task(task_id)
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
            logger.info(f"[USERBOT][STEP 2] Published event to {channel}: {data}")
        except Exception as e:
            logger.error(f"Error publishing event: {e}")
            raise

    async def get_pubsub(self, fresh: bool = False):
        """Возвращает объект Pub/Sub с отдельным соединением"""
        try:
            if fresh or self.pubsub_conn is None:
                # Закрываем старое соединение если было
                if self.pubsub_conn:
                    await self.pubsub_conn.close()
                
                # Создаем новое выделенное соединение для PubSub
                self.pubsub_conn = redis.Redis(
                    host=settings.REDIS_HOST,
                    port=settings.REDIS_PORT,
                    db=settings.REDIS_DB,
                    password=settings.REDIS_PASSWORD,
                    socket_connect_timeout=5,
                    socket_timeout=30,  # Увеличенный таймаут для PubSub
                    max_connections=10,
                    health_check_interval=30
                )
            
            return self.pubsub_conn.pubsub()
        except Exception as e:
            logger.error(f"PubSub connection error: {e}")
            raise

    async def is_connected(self) -> bool:
        """Проверяет подключение к Redis"""
        try:
            if self.conn is None:
                return False
            return await self.conn.ping()
        except (RedisError, Exception):
            return False

    async def close(self):
        """Закрывает соединение с Redis"""
        if self.conn:
            await self.conn.close()
            self.conn = None
    
    async def get_next_task_number(self) -> int:
        """Генерирует уникальный номер задачи"""
        return await self.conn.incr("global_task_counter")

    async def get_task(self, task_id: str) -> Optional[dict]:
        """Получает задачу по ID"""
        task_data = await self.conn.get(f"task:{task_id}")
        if task_data:
            return json.loads(task_data)
        return None
    
    async def update_task_data(self, task_id: str, task_data: dict):
        """Обновляет задачу"""
        await self.conn.set(f"task:{task_id}", json.dumps(task_data))

    async def get(self, key: str) -> Optional[str]:
        """Получает значение по ключу"""
        if self.conn is None:
            await self._ensure_connection()
        return await self.conn.get(key)
    
    async def set(self, key: str, value: str):
        """Устанавливает значение по ключу"""
        if self.conn is None:
            await self._ensure_connection()
        await self.conn.set(key, value)
    
    async def set_pinned_message_id(self, message_id: int):
        """Сохраняет ID закрепленного сообщения статистики"""
        await self._ensure_connection()
        await self.conn.set("pinned_stats_message_id", str(message_id))
        logger.info(f"Saved pinned message ID: {message_id}")
    
    async def get_pinned_message_id(self) -> Optional[int]:
        """Получает ID закрепленного сообщения статистики"""
        await self._ensure_connection()
        message_id = await self.conn.get("pinned_stats_message_id")
        if message_id:
            return int(message_id.decode() if isinstance(message_id, bytes) else message_id)
        return None
    
    async def clear_pinned_message_id(self):
        """Удаляет сохраненный ID закрепленного сообщения"""
        await self._ensure_connection()
        await self.conn.delete("pinned_stats_message_id")
        logger.info("Cleared pinned message ID")
    
    async def delete_task(self, task_id: str):
        """Удаляет задачу из Redis"""
        try:
            await self._ensure_connection()
            
            # Удаляем основную запись задачи
            task_key = f"task:{task_id}"
            pipeline = self.conn.pipeline()
            
            # Удаляем задачу
            pipeline.delete(task_key)
            
            # Удаляем из индекса
            pipeline.srem("tasks:index", task_key)
            
            result = await pipeline.execute()
            logger.info(f"Task {task_id} deleted from Redis: {result}")
            
        except Exception as e:
            logger.error(f"Error deleting task {task_id}: {e}")
            raise


# Глобальный экземпляр клиента Redis
redis_client = RedisManager()

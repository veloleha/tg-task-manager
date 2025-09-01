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
        self._enhanced_stats = None  # Lazy initialization

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
                
            # Initialize enhanced statistics if not already done
            if self._enhanced_stats is None:
                from .enhanced_statistics import EnhancedStatistics
                self._enhanced_stats = EnhancedStatistics(self)
                
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
            
            # Сохраняем как строку JSON с правильной обработкой списков и объектов
            def serialize_value(value):
                if value is None:
                    return ""
                elif isinstance(value, (list, dict)):
                    # Списки и словари сохраняем как JSON
                    return value
                else:
                    # Остальные значения конвертируем в строку
                    return str(value)
        
            task_json = json.dumps({
                k: serialize_value(v)
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

    async def update_task_status(self, task_id: str, status: str, executor: str = None) -> bool:
        """Обновляет статус задачи с обновлением счётчиков"""
        try:
            await self._ensure_connection()
            
            # Ensure enhanced stats is initialized
            if self._enhanced_stats is None:
                from .enhanced_statistics import EnhancedStatistics
                self._enhanced_stats = EnhancedStatistics(self)
            
            task = await self.get_task(task_id)
            if not task:
                return False
            
            old_status = task.get('status')
            old_executor = task.get('assignee')
            
            # Update task data
            task['status'] = status
            if executor:
                task['assignee'] = executor
            task['updated_at'] = datetime.utcnow().isoformat()
            
            await self.conn.set(f"task:{task_id}", json.dumps(task))
            
            # Update statistics counters
            current_executor = executor or old_executor
            await self._enhanced_stats.update_task_status_counters(
                old_status, status, current_executor
            )
            
            logger.debug(f"Task status updated: {task_id} -> {status} (executor: {current_executor})")
            return True
        except Exception as e:
            logger.error(f"Error updating task status: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
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
                # Извлекаем task_id из ключа (убираем префикс "task:")
                task_id = key.decode('utf-8').replace('task:', '')
                task = await self.get_task(task_id)
                if task and task.get("status") == status:
                    # Добавляем task_id в данные задачи
                    task['task_id'] = task_id
                    tasks.append(task)
            return tasks
        except Exception as e:
            logger.error(f"Error getting tasks by status: {e}")
            return []

    async def get_tasks_by_assignee(self, assignee: str) -> List[Dict[str, Any]]:
        """Получает все задачи с указанным исполнителем"""
        try:
            await self._ensure_connection()
            keys = await self.conn.smembers("tasks:index")
            tasks = []
            for key in keys:
                task_id = key.decode().split(":")[1]
                task_data = await self.get_task(task_id)
                if task_data and task_data.get("assignee") == assignee:
                    task_data['task_id'] = task_id  # Add missing field
                    tasks.append(task_data)
            return tasks
        except Exception as e:
            logger.error(f"Error getting tasks by assignee {assignee}: {e}")
            return []
    
    async def get_user_tasks(self, user_id: int) -> List[Dict[str, Any]]:
        """Получает все задачи пользователя"""
        try:
            await self._ensure_connection()
            keys = await self.conn.smembers("tasks:index")
            tasks = []
            deleted_keys = []  # Список ключей удаленных задач для очистки индекса
            
            for key in keys:
                task_id = key.decode().split(":")[1]
                logger.info(f"[DB][GET_USER_TASKS] Checking task {task_id} for user {user_id}")
                task_data = await self.get_task(task_id)
                logger.info(f"[DB][GET_USER_TASKS] Task {task_id} data: {task_data}")
                # ИСПРАВЛЕНИЕ: get_task возвращает {} для несуществующих задач, а не None
                task_exists = task_data and len(task_data) > 0 and 'user_id' in task_data and 'status' in task_data
                logger.info(f"[DB][GET_USER_TASKS] Task {task_id} exists: {task_exists}")
            
                if task_exists and str(task_data.get("user_id")) == str(user_id):
                    logger.info(f"[DB][GET_USER_TASKS] Task {task_id} belongs to user {user_id}")
                    task_data['task_id'] = task_id  # Add missing field
                    tasks.append(task_data)
                elif not task_exists:
                    logger.info(f"[DB][GET_USER_TASKS] Task {task_id} marked for cleanup (deleted)")
                    # Задача удалена, но ключ остался в индексе - добавляем в список для очистки
                    deleted_keys.append(key)
            
            # Очищаем индекс от удаленных задач
            if deleted_keys:
                logger.info(f"Cleaning up {len(deleted_keys)} deleted task keys from index")
                pipeline = self.conn.pipeline()
                for deleted_key in deleted_keys:
                    pipeline.srem("tasks:index", deleted_key)
                await pipeline.execute()
                logger.info(f"Successfully cleaned up {len(deleted_keys)} deleted task keys from index")
                
            return tasks
        except Exception as e:
            logger.error(f"Error getting tasks for user {user_id}: {e}")
            return []

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
            await self.conn.incr(f"stats:{counter_type}")
        except Exception as e:
            logger.error(f"Error incrementing counter {counter_type}: {e}")

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
        logger.info(f"[DB][GET_TASK] Attempting to get task {task_id}")
        task_data = await self.conn.get(f"task:{task_id}")
        logger.info(f"[DB][GET_TASK] Raw data for task {task_id}: {task_data}")
        if task_data:
            result = json.loads(task_data)
            logger.info(f"[DB][GET_TASK] Parsed task {task_id} data: {result}")
            return result
        logger.info(f"[DB][GET_TASK] Task {task_id} not found, returning None")
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
    
    async def set_pinned_message_id(self, message_id: Optional[int]):
        """Сохраняет ID закрепленного сообщения статистики"""
        await self._ensure_connection()
        if message_id is None:
            await self.conn.delete("pinned_stats_message_id")
            logger.info("Cleared pinned message ID")
        else:
            await self.conn.set("pinned_stats_message_id", str(message_id))
            logger.info(f"Saved pinned message ID: {message_id}")
    
    async def get_pinned_message_id(self) -> Optional[int]:
        """Получает ID закрепленного сообщения статистики"""
        await self._ensure_connection()
        message_id = await self.conn.get("pinned_stats_message_id")
        if message_id:
            try:
                message_id_str = message_id.decode() if isinstance(message_id, bytes) else message_id
                # Проверяем, что это не строка 'None'
                if message_id_str == 'None' or message_id_str == 'null':
                    logger.warning(f"Found invalid pinned message ID: {message_id_str}, clearing it")
                    await self.conn.delete("pinned_stats_message_id")
                    return None
                return int(message_id_str)
            except (ValueError, TypeError) as e:
                logger.error(f"Invalid pinned message ID format: {message_id}, error: {e}")
                # Очищаем некорректное значение
                await self.conn.delete("pinned_stats_message_id")
                return None
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
            logger.info(f"[DB][DELETE_TASK] Attempting to delete task {task_id} with key {task_key}")
            
            # Получаем задачу перед удалением для получения user_id
            task_data = await self.get_task(task_id)
            user_id = task_data.get('user_id') if task_data else None
            logger.info(f"[DB][DELETE_TASK] Task user_id: {user_id}")
            
            # Проверяем, существует ли задача до удаления
            task_exists_before = await self.conn.exists(task_key)
            logger.info(f"[DB][DELETE_TASK] Task {task_id} exists before deletion: {task_exists_before}")
            
            # Дополнительная проверка: удаляем задачу из всех возможных индексов
            pipeline = self.conn.pipeline()
            
            # Удаляем задачу
            pipeline.delete(task_key)
            
            # Удаляем из основного индекса
            pipeline.srem("tasks:index", task_key)
            
            # Если есть user_id, удаляем из пользовательского индекса (если такой существует)
            if user_id:
                user_index_key = f"user_tasks:{user_id}"
                pipeline.srem(user_index_key, task_key)
                logger.info(f"[DB][DELETE_TASK] Added removal from user index {user_index_key}")
            
            # Дополнительная очистка: удаляем из всех пользовательских индексов
            # Это необходимо для предотвращения утечек в случае, если user_id изменился
            all_user_keys = await self.conn.keys("user_tasks:*")
            for user_key in all_user_keys:
                pipeline.srem(user_key, task_key)
            
            result = await pipeline.execute()
            logger.info(f"[DB][DELETE_TASK] Task {task_id} deletion result: {result}")
            
            # Проверяем, существует ли задача после удаления
            task_exists_after = await self.conn.exists(task_key)
            logger.info(f"[DB][DELETE_TASK] Task {task_id} exists after deletion: {task_exists_after}")
            
            # Проверяем, есть ли задача в индексе после удаления
            in_index_after = await self.conn.sismember("tasks:index", task_key)
            logger.info(f"[DB][DELETE_TASK] Task {task_id} in index after deletion: {in_index_after}")
            
            # Дополнительная проверка: убеждаемся, что задача удалена из всех индексов
            if task_exists_after:
                logger.warning(f"[DB][DELETE_TASK] Task {task_id} still exists after deletion, forcing additional cleanup")
                await self.conn.delete(task_key)
                
            if in_index_after:
                logger.warning(f"[DB][DELETE_TASK] Task {task_id} still in index after deletion, forcing additional cleanup")
                await self.conn.srem("tasks:index", task_key)
                
        except Exception as e:
            logger.error(f"[DB][DELETE_TASK] Error deleting task {task_id}: {e}")
            import traceback
            logger.error(f"[DB][DELETE_TASK] Traceback: {traceback.format_exc()}")
            raise
    
    # ==================== ENHANCED STATISTICS METHODS ====================
    
    async def get_period_stats(self, period: str) -> Dict[str, Dict[str, int]]:
        """Get statistics for a specific period (day/week/month)"""
        try:
            await self._ensure_connection()
            
            # Ensure enhanced stats is initialized
            if self._enhanced_stats is None:
                from .enhanced_statistics import EnhancedStatistics
                self._enhanced_stats = EnhancedStatistics(self)
                
            return await self._enhanced_stats.get_period_stats(period)
        except Exception as e:
            logger.error(f"Error getting {period} stats: {e}")
            return {}
    
    async def format_pinned_message(self, stats: Dict[str, Any] = None) -> str:
        """Format enhanced pinned message with executor statistics"""
        try:
            await self._ensure_connection()
            
            # Ensure enhanced stats is initialized
            if self._enhanced_stats is None:
                from .enhanced_statistics import EnhancedStatistics
                self._enhanced_stats = EnhancedStatistics(self)
            
            if stats is None:
                stats = await self._enhanced_stats.get_global_stats()
            return self._enhanced_stats.format_pinned_message(stats)
        except Exception as e:
            logger.error(f"Error formatting pinned message: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return "📊 Ошибка получения статистики"
    
    async def format_period_stats_message(self, period: str, stats: Dict[str, Dict[str, int]] = None) -> str:
        """Format period statistics message"""
        try:
            await self._ensure_connection()
            
            # Ensure enhanced stats is initialized
            if self._enhanced_stats is None:
                from .enhanced_statistics import EnhancedStatistics
                self._enhanced_stats = EnhancedStatistics(self)
                
            if stats is None:
                stats = await self.get_period_stats(period)
            return self._enhanced_stats.format_period_stats_message(period, stats)
        except Exception as e:
            logger.error(f"Error formatting period stats: {e}")
            return f"📈 Ошибка получения статистики за {period}"
    
    async def get_global_stats(self) -> Dict[str, Any]:
        """Get comprehensive global statistics"""
        try:
            await self._ensure_connection()
            
            # Ensure enhanced stats is initialized
            if self._enhanced_stats is None:
                from .enhanced_statistics import EnhancedStatistics
                self._enhanced_stats = EnhancedStatistics(self)
                
            return await self._enhanced_stats.get_global_stats()
        except Exception as e:
            logger.error(f"Error getting global stats: {e}")
            return {
                "unreacted": 0,
                "in_progress": 0,
                "completed": 0,
                "executors": {}
            }
    
    async def reset_all_counters(self):
        """Reset all statistics counters (for testing/debugging)"""
        try:
            await self._ensure_connection()
            
            # Ensure enhanced stats is initialized
            if self._enhanced_stats is None:
                from .enhanced_statistics import EnhancedStatistics
                self._enhanced_stats = EnhancedStatistics(self)
                
            await self._enhanced_stats.reset_all_counters()
        except Exception as e:
            logger.error(f"Error resetting counters: {e}")
            raise


# Глобальный экземпляр клиента Redis
redis_client = RedisManager()

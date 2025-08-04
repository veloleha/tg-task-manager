import asyncio
import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import json
from core.redis_client import redis_client
from config.settings import settings

logger = logging.getLogger(__name__)

class MessageAggregator:
    """Менеджер для автообъединения сообщений пользователей"""
    
    def __init__(self):
        self.redis = redis_client
        self.aggregation_timeout = 60  # 1 минута для обновления задачи
        self.pending_tasks: Dict[int, asyncio.Task] = {}
        self.created_tasks: Dict[int, str] = {}  # user_id -> task_id для созданных задач
    
    async def add_message(self, user_id: int, message_data: dict) -> bool:
        """
        Добавляет сообщение в очередь агрегации
        Создает задачу немедленно при первом сообщении, обновляет при последующих
        """
        try:
            logger.info(f"[AGGREGATOR][START] add_message called for user {user_id}")
            logger.info(f"[AGGREGATOR][START] Message data keys: {list(message_data.keys())}")
            logger.info(f"[AGGREGATOR][START] Current created_tasks: {list(self.created_tasks.keys())}")
            
            # Проверяем, есть ли уже созданная задача для этого пользователя
            if user_id in self.created_tasks:
                logger.info(f"[AGGREGATOR][UPDATE] Found existing task for user {user_id}")
                # Задача уже создана, добавляем сообщение к существующей
                logger.info(f"[AGGREGATOR][UPDATE] Getting pending messages...")
                existing_messages = await self._get_pending_messages(user_id)
                logger.info(f"[AGGREGATOR][UPDATE] Got {len(existing_messages)} existing messages")
                
                existing_messages.append(message_data)
                logger.info(f"[AGGREGATOR][UPDATE] Appended new message, total: {len(existing_messages)}")
                
                logger.info(f"[AGGREGATOR][UPDATE] Saving pending messages...")
                await self._save_pending_messages(user_id, existing_messages)
                logger.info(f"[AGGREGATOR][UPDATE] Pending messages saved")
                
                # Обновляем существующую задачу
                logger.info(f"[AGGREGATOR][UPDATE] Updating existing task...")
                await self._update_existing_task(user_id, existing_messages)
                logger.info(f"[AGGREGATOR][UPDATE] Task updated")
                
                # Сбрасываем таймер
                logger.info(f"[AGGREGATOR][UPDATE] Resetting timer...")
                await self._reset_aggregation_timer(user_id)
                logger.info(f"[AGGREGATOR][UPDATE] Timer reset")
                
                logger.info(f"[AGGREGATOR][UPDATE] ✅ Updated existing task for user {user_id}. Total messages: {len(existing_messages)}")
                return True
            else:
                logger.info(f"[AGGREGATOR][CREATE] No existing task for user {user_id}, creating new one")
                # Первое сообщение - создаем задачу немедленно
                logger.info(f"[AGGREGATOR][CREATE] Saving pending messages...")
                await self._save_pending_messages(user_id, [message_data])
                logger.info(f"[AGGREGATOR][CREATE] Pending messages saved")
                
                logger.info(f"[AGGREGATOR][CREATE] Creating aggregated task...")
                task_id = await self._create_aggregated_task(user_id)
                logger.info(f"[AGGREGATOR][CREATE] Task creation result: {task_id}")
                
                if task_id:
                    logger.info(f"[AGGREGATOR][CREATE] Adding task to created_tasks dict")
                    self.created_tasks[user_id] = task_id
                    logger.info(f"[AGGREGATOR][CREATE] Starting aggregation timer...")
                    # Запускаем таймер для возможных обновлений
                    await self._start_aggregation_timer(user_id)
                    logger.info(f"[AGGREGATOR][CREATE] Timer started")
                    
                    logger.info(f"[AGGREGATOR][CREATE] ✅ Created task immediately for user {user_id}: {task_id}")
                    return True
                else:
                    logger.error(f"[AGGREGATOR][CREATE] ❌ Failed to create task for user {user_id}")
                    return False
                
        except Exception as e:
            logger.error(f"[AGGREGATOR][ERROR] ❌ Error adding message to aggregation: {e}", exc_info=True)
            return False
    
    async def _get_pending_messages(self, user_id: int) -> List[dict]:
        """Получает ожидающие сообщения пользователя"""
        try:
            await self.redis._ensure_connection()
            messages_data = await self.redis.conn.get(f"pending_messages:{user_id}")
            if not messages_data:
                return []
            
            if isinstance(messages_data, bytes):
                messages_data = messages_data.decode('utf-8')
            
            return json.loads(messages_data)
            
        except Exception as e:
            logger.error(f"Error getting pending messages: {e}")
            return []
    
    async def _save_pending_messages(self, user_id: int, messages: List[dict]):
        """Сохраняет ожидающие сообщения"""
        try:
            await self.redis._ensure_connection()
            await self.redis.conn.setex(
                f"pending_messages:{user_id}",
                self.aggregation_timeout + 60,  # TTL чуть больше таймаута
                json.dumps(messages)
            )
        except Exception as e:
            logger.error(f"Error saving pending messages: {e}")
    
    async def _start_aggregation_timer(self, user_id: int):
        """Запускает таймер агрегации для пользователя"""
        # Отменяем существующий таймер если есть
        if user_id in self.pending_tasks:
            self.pending_tasks[user_id].cancel()
        
        # Создаем новый таймер
        task = asyncio.create_task(self._aggregation_timer(user_id))
        self.pending_tasks[user_id] = task
    
    async def _reset_aggregation_timer(self, user_id: int):
        """Сбрасывает таймер агрегации (продлевает ожидание)"""
        await self._start_aggregation_timer(user_id)
    
    async def _aggregation_timer(self, user_id: int):
        """Таймер агрегации сообщений (теперь для очистки после обновлений)"""
        try:
            await asyncio.sleep(self.aggregation_timeout)
            
            # Время истекло - очищаем данные агрегации
            await self._cleanup_aggregation_data(user_id)
            
        except asyncio.CancelledError:
            logger.debug(f"Aggregation timer cancelled for user {user_id}")
        except Exception as e:
            logger.error(f"Error in aggregation timer: {e}")
        finally:
            # Убираем задачу из списка активных
            if user_id in self.pending_tasks:
                del self.pending_tasks[user_id]
            if user_id in self.created_tasks:
                del self.created_tasks[user_id]
    
    async def _create_aggregated_task(self, user_id: int):
        """Создает задачу из агрегированных сообщений"""
        try:
            messages = await self._get_pending_messages(user_id)
            if not messages:
                logger.warning(f"No messages found for aggregation for user {user_id}")
                return
            
            # Объединяем тексты сообщений
            combined_text = self._combine_messages(messages)
            
            # Берем данные из первого сообщения как основу
            base_message = messages[0]
            
            # Создаем агрегированную задачу
            aggregated_task = {
                "message_id": base_message["message_id"],
                "chat_id": base_message["chat_id"],
                "chat_title": base_message["chat_title"],
                "chat_type": base_message["chat_type"],
                "user_id": base_message["user_id"],
                "first_name": base_message["first_name"],
                "last_name": base_message["last_name"],
                "username": base_message["username"],
                "language_code": base_message["language_code"],
                "is_bot": base_message["is_bot"],
                "text": combined_text,
                "status": "unreacted",
                "task_number": None,
                "assignee": None,
                "task_link": None,
                "reply": None,
                "created_at": base_message["created_at"],
                "updated_at": None,
                "aggregated": True,
                "message_count": len(messages),
                "aggregation_period": self.aggregation_timeout
            }
            
            # Сохраняем задачу
            logger.info(f"🔄 ЭТАП 1: Начинаем сохранение задачи в Redis...")
            task_id = await self._save_task_to_redis(aggregated_task)
            logger.info(f"✅ ЭТАП 1 ЗАВЕРШЕН: Задача {task_id} сохранена в Redis")
            
            # Небольшая задержка для гарантии сохранения
            await asyncio.sleep(0.1)
            
            # Публикуем событие о новой задаче
            logger.info(f"🔄 ЭТАП 2: Отправляем сигнал в PubSub канал 'new_tasks'...")
            await self._publish_task_event(task_id)
            logger.info(f"[USERBOT][STEP 2] Отправлен сигнал по Pub/Sub о новой задаче: {task_id}")
            
            # Обновляем счетчики
            await self._increment_counter("unreacted")
            
            # Очищаем ожидающие сообщения
            await self.redis._ensure_connection()
            await self.redis.conn.delete(f"pending_messages:{user_id}")
            
            logger.info(f"Created aggregated task {task_id} from {len(messages)} messages for user {user_id}")
            
            return task_id
            
        except Exception as e:
            logger.error(f"Error creating aggregated task: {e}")
            return None
    
    async def _update_existing_task(self, user_id: int, messages: list):
        """Обновляет существующую задачу новыми сообщениями"""
        try:
            task_id = self.created_tasks.get(user_id)
            if not task_id:
                logger.error(f"No task ID found for user {user_id}")
                return False
            
            # Объединяем тексты сообщений
            combined_text = self._combine_messages(messages)
            
            # Обновляем задачу в Redis
            await self.redis._ensure_connection()
            task_key = f"task:{task_id}"
            task_data = await self.redis.get_task(task_id)
            
            if task_data:
                task_data['text'] = combined_text
                task_data['message_count'] = len(messages)
                task_data['updated_at'] = datetime.utcnow().isoformat()
                
                # Сохраняем обновленную задачу
                await self.redis.conn.set(task_key, json.dumps(task_data))
                
                # Публикуем событие об обновлении задачи
                await self._publish_task_update_event(task_id, combined_text)
                
                logger.info(f"Updated task {task_id} with {len(messages)} messages")
                return True
            else:
                logger.error(f"Task {task_id} not found in Redis")
                return False
                
        except Exception as e:
            logger.error(f"Error updating existing task: {e}")
            return False
    
    async def _publish_task_event(self, task_id: str):
        """Публикует событие о новой задаче"""
        try:
            logger.info(f"[PUBSUB][PUBLISH] Starting task event publication for task: {task_id}")
            await self.redis._ensure_connection()
            logger.info(f"[PUBSUB][PUBLISH] Redis connection ensured")
            
            # Получаем данные задачи для события
            task_data = await self.redis.get_task(task_id)
            if not task_data:
                logger.error(f"[PUBSUB][PUBLISH] ❌ Cannot publish event for non-existent task: {task_id}")
                return
            logger.info(f"[PUBSUB][PUBLISH] Task data retrieved for event: {len(task_data)} fields")
            
            event_data = {
                'task_id': task_id,
                'type': 'new_task',
                'user_id': int(task_data.get('user_id', 0)),
                'username': task_data.get('username', ''),
                'text': task_data.get('text', '')
            }
            logger.info(f"[PUBSUB][PUBLISH] Event data prepared: {event_data}")
            
            event_json = json.dumps(event_data)
            logger.info(f"[PUBSUB][PUBLISH] Event serialized to JSON: {len(event_json)} chars")
            
            result = await self.redis.conn.publish('new_tasks', event_json)
            logger.info(f"[PUBSUB][PUBLISH] ✅ Published to 'new_tasks' channel, subscribers notified: {result}")
            logger.info(f"[USERBOT][STEP 2] Published event to new_tasks: {event_data}")
            
        except Exception as e:
            logger.error(f"[PUBSUB][PUBLISH] ❌ Error publishing task event: {e}", exc_info=True)
    
    async def _publish_task_update_event(self, task_id: str, updated_text: str):
        """Публикует событие об обновлении задачи"""
        try:
            logger.info(f"[PUBSUB][PUBLISH] Starting task update event publication for task: {task_id}")
            await self.redis._ensure_connection()
            logger.info(f"[PUBSUB][PUBLISH] Redis connection ensured")
            
            # Получаем данные задачи для события
            task_data = await self.redis.get_task(task_id)
            if not task_data:
                logger.error(f"[PUBSUB][PUBLISH] ❌ Cannot publish event for non-existent task: {task_id}")
                return
            logger.info(f"[PUBSUB][PUBLISH] Task data retrieved for event: {len(task_data)} fields")
            
            event_data = {
                'task_id': task_id,
                'type': 'task_update',
                'user_id': int(task_data.get('user_id', 0)),
                'username': task_data.get('username', ''),
                'updated_text': updated_text
            }
            logger.info(f"[PUBSUB][PUBLISH] Event data prepared: {event_data}")
            
            event_json = json.dumps(event_data)
            logger.info(f"[PUBSUB][PUBLISH] Event serialized to JSON: {len(event_json)} chars")
            
            result = await self.redis.conn.publish('task_updates', event_json)
            logger.info(f"[PUBSUB][PUBLISH] ✅ Published to 'task_updates' channel, subscribers notified: {result}")
            logger.info(f"[USERBOT][STEP 2.1] Published task update event for task {task_id}")
            
        except Exception as e:
            logger.error(f"[PUBSUB][PUBLISH] ❌ Error publishing task update event: {e}", exc_info=True)
    
    async def _cleanup_aggregation_data(self, user_id: int):
        """Очищает данные агрегации после завершения периода обновлений"""
        try:
            await self.redis._ensure_connection()
            # Очищаем ожидающие сообщения
            await self.redis.conn.delete(f"pending_messages:{user_id}")
            logger.info(f"Cleaned up aggregation data for user {user_id}")
        except Exception as e:
            logger.error(f"Error cleaning up aggregation data: {e}")
    
    def _combine_messages(self, messages: List[dict]) -> str:
        """Объединяет тексты сообщений в один"""
        try:
            texts = []
            for i, msg in enumerate(messages, 1):
                text = msg.get("text", "").strip()
                if text:
                    if len(messages) > 1:
                        # Добавляем номер сообщения если их несколько
                        texts.append(f"[{i}] {text}")
                    else:
                        texts.append(text)
            
            combined = "\n\n".join(texts)
            
            # Добавляем информацию об агрегации
            if len(messages) > 1:
                timestamp_info = f"\n\n📝 Объединено {len(messages)} сообщений за {self.aggregation_timeout // 60} минут"
                combined += timestamp_info
            
            return combined
            
        except Exception as e:
            logger.error(f"Error combining messages: {e}")
            return "Ошибка объединения сообщений"
    
    async def force_create_task(self, user_id: int) -> Optional[str]:
        """Принудительно создает задачу из ожидающих сообщений"""
        try:
            # Отменяем таймер если есть
            if user_id in self.pending_tasks:
                self.pending_tasks[user_id].cancel()
                del self.pending_tasks[user_id]
            
            # Создаем задачу
            return await self._create_aggregated_task(user_id)
            
        except Exception as e:
            logger.error(f"Error force creating task: {e}")
            return None
    
    async def cancel_aggregation(self, user_id: int):
        """Отменяет агрегацию для пользователя"""
        try:
            # Отменяем таймер
            if user_id in self.pending_tasks:
                self.pending_tasks[user_id].cancel()
                del self.pending_tasks[user_id]
            
            # Очищаем ожидающие сообщения
            await self.redis._ensure_connection()
            await self.redis.conn.delete(f"pending_messages:{user_id}")
            
            logger.info(f"Cancelled aggregation for user {user_id}")
            
        except Exception as e:
            logger.error(f"Error cancelling aggregation: {e}")
    
    async def get_aggregation_status(self, user_id: int) -> dict:
        """Получает статус агрегации для пользователя"""
        try:
            messages = await self._get_pending_messages(user_id)
            has_timer = user_id in self.pending_tasks
            
            return {
                "user_id": user_id,
                "pending_messages": len(messages),
                "has_active_timer": has_timer,
                "timeout_seconds": self.aggregation_timeout
            }
            
        except Exception as e:
            logger.error(f"Error getting aggregation status: {e}")
            return {"error": str(e)}
    
    async def cleanup_expired_aggregations(self):
        """Очищает истекшие агрегации (периодическая задача)"""
        try:
            await self.redis._ensure_connection()
            
            # Получаем все ключи ожидающих сообщений
            keys = []
            async for key in self.redis.conn.scan_iter("pending_messages:*"):
                keys.append(key)
            
            for key in keys:
                # Проверяем TTL
                ttl = await self.redis.conn.ttl(key)
                if ttl <= 0:  # Ключ истек или не существует
                    await self.redis.conn.delete(key)
                    logger.debug(f"Cleaned up expired aggregation: {key}")
            
        except Exception as e:
            logger.error(f"Error cleaning up aggregations: {e}")
    
    def set_aggregation_timeout(self, seconds: int):
        """Устанавливает таймаут агрегации"""
        if seconds > 0:
            self.aggregation_timeout = seconds
            logger.info(f"Aggregation timeout set to {seconds} seconds")

    async def _save_task_to_redis(self, task_data: dict) -> str:
        """Сохраняет задачу в Redis используя стандартный метод"""
        try:
            # Используем стандартный метод из redis_client
            task_id = await self.redis.save_task(task_data)
            logger.info(f"Task {task_id} saved to Redis successfully")
            return task_id
            
        except Exception as e:
            logger.error(f"Error saving task to Redis: {e}")
            raise

    async def _publish_task_event(self, task_id: str):
        """Публикует событие о новой задаче"""
        try:
            await self.redis._ensure_connection()
            
            event_data = {
                "type": "new_task",
                "task_id": task_id
            }
            
            await self.redis.conn.publish("new_tasks", json.dumps(event_data))
            
        except Exception as e:
            logger.error(f"Error publishing task event: {e}")

    async def _increment_counter(self, counter_name: str):
        """Увеличивает счетчик"""
        try:
            await self.redis._ensure_connection()
            await self.redis.conn.incr(f"counter:{counter_name}")
            
        except Exception as e:
            logger.error(f"Error incrementing counter {counter_name}: {e}")

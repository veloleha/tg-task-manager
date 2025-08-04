import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from core.redis_client import redis_client

logger = logging.getLogger(__name__)

class RedisManager:
    """Менеджер для работы с Redis в контексте task_bot"""
    
    def __init__(self):
        self.redis = redis_client
    
    async def get_task(self, task_id: str) -> Dict[str, Any]:
        """Получает задачу по ID"""
        return await self.redis.get_task(task_id)
    
    async def update_task(self, task_id: str, **fields) -> bool:
        """Обновляет поля задачи"""
        return await self.redis.update_task(task_id, **fields)
    
    async def delete_task(self, task_id: str) -> bool:
        """Удаляет задачу"""
        try:
            await self.redis._ensure_connection()
            
            # Получаем задачу для обновления счетчиков
            task = await self.get_task(task_id)
            if task:
                status = task.get('status', 'unreacted')
                await self._decrement_status_counter(status)
                
                # Если есть исполнитель, обновляем его статистику
                if assignee := task.get('assignee'):
                    await self._decrement_executor_counter(assignee, status)
            
            # Удаляем задачу
            result = await self.redis.conn.delete(task_id)
            
            # Удаляем из индекса
            await self.redis.conn.srem("tasks:index", task_id)
            
            logger.info(f"Task {task_id} deleted successfully")
            return bool(result)
            
        except Exception as e:
            logger.error(f"Error deleting task {task_id}: {e}")
            return False
    
    async def get_next_task_number(self) -> int:
        """Генерирует следующий номер задачи"""
        return await self.redis.get_next_task_number()
    
    async def get_tasks_by_status(self, status: str) -> List[Dict[str, Any]]:
        """Получает все задачи с указанным статусом"""
        return await self.redis.get_tasks_by_status(status)
    
    async def get_tasks_by_assignee(self, assignee: str) -> List[Dict[str, Any]]:
        """Получает все задачи назначенные указанному исполнителю"""
        try:
            await self.redis._ensure_connection()
            tasks = []
            
            async for key in self.redis.conn.scan_iter("task:*"):
                task = await self.get_task(key)
                if task.get("assignee") == assignee:
                    tasks.append(task)
            
            return tasks
        except Exception as e:
            logger.error(f"Error getting tasks by assignee {assignee}: {e}")
            return []
    
    async def change_task_status(self, task_id: str, new_status: str, assignee: Optional[str] = None) -> bool:
        """Изменяет статус задачи с обновлением счетчиков"""
        try:
            task = await self.get_task(task_id)
            if not task:
                logger.error(f"Task {task_id} not found")
                return False
            
            old_status = task.get('status', 'unreacted')
            
            # Обновляем счетчики статусов
            await self._decrement_status_counter(old_status)
            await self._increment_status_counter(new_status)
            
            # Обновляем статистику исполнителей
            old_assignee = task.get('assignee')
            
            if old_assignee and old_status in ['in_progress', 'completed']:
                await self._decrement_executor_counter(old_assignee, old_status)
            
            if assignee and new_status in ['in_progress', 'completed']:
                await self._increment_executor_counter(assignee, new_status)
            
            # Обновляем задачу
            updates = {
                'status': new_status,
                'updated_at': datetime.utcnow().isoformat()
            }
            
            if assignee:
                updates['assignee'] = assignee
            
            return await self.update_task(task_id, **updates)
            
        except Exception as e:
            logger.error(f"Error changing task status: {e}")
            return False
    
    async def add_task_reply(self, task_id: str, reply_text: str, reply_author: str) -> bool:
        """Добавляет ответ к задаче"""
        try:
            updates = {
                'reply': reply_text,
                'reply_author': reply_author,
                'reply_at': datetime.utcnow().isoformat()
            }
            
            return await self.update_task(task_id, **updates)
            
        except Exception as e:
            logger.error(f"Error adding reply to task {task_id}: {e}")
            return False
    
    async def get_global_stats(self) -> Dict[str, Any]:
        """Получает глобальную статистику"""
        return await self.redis.get_global_stats()
    
    async def get_executor_stats(self, executor: str) -> Dict[str, int]:
        """Получает статистику конкретного исполнителя"""
        try:
            await self.redis._ensure_connection()
            
            return {
                'in_progress': int(await self.redis.conn.get(f"stats:in_progress:{executor}") or 0),
                'completed': int(await self.redis.conn.get(f"stats:completed:{executor}") or 0)
            }
        except Exception as e:
            logger.error(f"Error getting executor stats for {executor}: {e}")
            return {'in_progress': 0, 'completed': 0}
    
    async def _increment_status_counter(self, status: str):
        """Увеличивает счетчик статуса"""
        try:
            await self.redis._ensure_connection()
            await self.redis.conn.incr(f"counter:{status}")
        except Exception as e:
            logger.error(f"Error incrementing counter for {status}: {e}")
    
    async def _decrement_status_counter(self, status: str):
        """Уменьшает счетчик статуса"""
        try:
            await self.redis._ensure_connection()
            current = int(await self.redis.conn.get(f"counter:{status}") or 0)
            if current > 0:
                await self.redis.conn.decr(f"counter:{status}")
        except Exception as e:
            logger.error(f"Error decrementing counter for {status}: {e}")
    
    async def _increment_executor_counter(self, executor: str, status: str):
        """Увеличивает счетчик исполнителя"""
        try:
            await self.redis._ensure_connection()
            await self.redis.conn.incr(f"stats:{status}:{executor}")
        except Exception as e:
            logger.error(f"Error incrementing executor counter: {e}")
    
    async def _decrement_executor_counter(self, executor: str, status: str):
        """Уменьшает счетчик исполнителя"""
        try:
            await self.redis._ensure_connection()
            current = int(await self.redis.conn.get(f"stats:{status}:{executor}") or 0)
            if current > 0:
                await self.redis.conn.decr(f"stats:{status}:{executor}")
        except Exception as e:
            logger.error(f"Error decrementing executor counter: {e}")
    
    async def set_reminder(self, task_id: str, hours: int) -> bool:
        """Устанавливает напоминание для задачи"""
        try:
            await self.redis._ensure_connection()
            
            reminder_data = {
                'task_id': task_id,
                'hours': hours,
                'created_at': datetime.utcnow().isoformat()
            }
            
            reminder_key = f"reminder:{task_id}:{hours}"
            await self.redis.conn.setex(
                reminder_key, 
                hours * 3600,  # TTL в секундах
                json.dumps(reminder_data)
            )
            
            logger.info(f"Reminder set for task {task_id} in {hours} hours")
            return True
            
        except Exception as e:
            logger.error(f"Error setting reminder: {e}")
            return False
    
    async def get_active_reminders(self) -> List[Dict[str, Any]]:
        """Получает все активные напоминания"""
        try:
            await self.redis._ensure_connection()
            reminders = []
            
            async for key in self.redis.conn.scan_iter("reminder:*"):
                reminder_data = await self.redis.conn.get(key)
                if reminder_data:
                    if isinstance(reminder_data, bytes):
                        reminder_data = reminder_data.decode('utf-8')
                    reminders.append(json.loads(reminder_data))
            
            return reminders
        except Exception as e:
            logger.error(f"Error getting active reminders: {e}")
            return []
    
    async def cleanup_expired_tasks(self, days: int = 30) -> int:
        """Очищает старые завершенные задачи"""
        try:
            await self.redis._ensure_connection()
            cleaned = 0
            cutoff_date = datetime.utcnow().timestamp() - (days * 24 * 3600)
            
            async for key in self.redis.conn.scan_iter("task:*"):
                task = await self.get_task(key)
                if task.get('status') == 'completed':
                    try:
                        created_at = datetime.fromisoformat(task.get('created_at', ''))
                        if created_at.timestamp() < cutoff_date:
                            await self.delete_task(key)
                            cleaned += 1
                    except:
                        continue
            
            logger.info(f"Cleaned up {cleaned} expired tasks")
            return cleaned
            
        except Exception as e:
            logger.error(f"Error cleaning up expired tasks: {e}")
            return 0
    
    async def get_task_history(self, task_id: str) -> List[Dict[str, Any]]:
        """Получает историю изменений задачи"""
        try:
            await self.redis._ensure_connection()
            history_key = f"history:{task_id}"
            history_data = await self.redis.conn.lrange(history_key, 0, -1)
            
            history = []
            for item in history_data:
                if isinstance(item, bytes):
                    item = item.decode('utf-8')
                history.append(json.loads(item))
            
            return history
        except Exception as e:
            logger.error(f"Error getting task history: {e}")
            return []
    
    async def add_task_history_entry(self, task_id: str, action: str, details: Dict[str, Any]):
        """Добавляет запись в историю задачи"""
        try:
            await self.redis._ensure_connection()
            
            history_entry = {
                'action': action,
                'timestamp': datetime.utcnow().isoformat(),
                'details': details
            }
            
            history_key = f"history:{task_id}"
            await self.redis.conn.lpush(history_key, json.dumps(history_entry))
            
            # Ограничиваем историю 50 записями
            await self.redis.conn.ltrim(history_key, 0, 49)
            
        except Exception as e:
            logger.error(f"Error adding task history entry: {e}")
    
    async def get_statistics(self) -> Dict[str, int]:
        """Получает общую статистику задач по статусам"""
        try:
            await self.redis._ensure_connection()
            
            stats = {}
            statuses = ['unreacted', 'in_progress', 'completed']
            
            for status in statuses:
                count = await self.redis.conn.get(f"stats:status:{status}")
                stats[status] = int(count) if count else 0
            
            return stats
        except Exception as e:
            logger.error(f"Error getting statistics: {e}")
            return {'unreacted': 0, 'in_progress': 0, 'completed': 0}
    
    async def get_executor_statistics(self) -> Dict[str, int]:
        """Получает статистику по исполнителям (количество задач в работе)"""
        try:
            await self.redis._ensure_connection()
            
            executor_stats = {}
            
            # Получаем всех исполнителей из ключей статистики
            async for key in self.redis.conn.scan_iter("stats:executor:*:in_progress"):
                # Извлекаем имя исполнителя из ключа (декодируем байты в строку)
                key_str = key.decode('utf-8') if isinstance(key, bytes) else key
                executor = key_str.split(':')[2]  # stats:executor:username:in_progress
                count = await self.redis.conn.get(key)
                if count and int(count) > 0:
                    executor_stats[executor] = int(count)
            
            return executor_stats
        except Exception as e:
            logger.error(f"Error getting executor statistics: {e}")
            return {}
    
    async def get_pinned_message_id(self) -> Optional[int]:
        """Получает ID закрепленного сообщения со статистикой"""
        try:
            await self.redis._ensure_connection()
            msg_id = await self.redis.conn.get("pinned_stats_message_id")
            return int(msg_id) if msg_id else None
        except Exception as e:
            logger.error(f"Error getting pinned message ID: {e}")
            return None
    
    async def set_pinned_message_id(self, message_id: int = None):
        """Сохраняет ID закрепленного сообщения со статистикой"""
        try:
            await self.redis._ensure_connection()
            if message_id is None:
                # Удаляем ключ из Redis
                await self.redis.conn.delete("pinned_stats_message_id")
                logger.info("Pinned message ID cleared from Redis")
            else:
                await self.redis.conn.set("pinned_stats_message_id", str(message_id))
                logger.info(f"Pinned message ID set to {message_id}")
        except Exception as e:
            logger.error(f"Error setting pinned message ID: {e}")
    
    async def publish_event(self, channel: str, data: Dict[str, Any]):
        """Публикует событие в Redis Pub/Sub"""
        return await self.redis.publish_event(channel, data)

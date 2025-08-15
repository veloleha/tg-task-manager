import asyncio
import logging
from typing import Dict, Optional
from datetime import datetime, timedelta
from aiogram import Bot
from core.redis_client import redis_client

logger = logging.getLogger(__name__)

class TopicManager:
    """Менеджер для управления темами пользователей в форумных чатах"""
    
    def __init__(self, bot: Bot):
        self.bot = bot
        self.redis = redis_client
        
    async def get_or_create_user_topic(self, chat_id: int, user_id: int, username: str = None, first_name: str = None) -> Optional[int]:
        """Получает существующую тему пользователя в данном чате или создает новую"""
        try:
            # Проверяем, является ли чат форумом
            chat_info = await self.bot.get_chat(chat_id)
            if not chat_info.is_forum:
                logger.info(f"Chat {chat_id} is not a forum, skipping topic creation")
                return None
            
            # Проверяем существующую активную тему
            existing_topic = await self._get_active_user_topic(chat_id, user_id)
            if existing_topic:
                logger.info(f"Found existing topic {existing_topic} for user {user_id} in chat {chat_id}")
                return existing_topic
            
            # Создаем новую тему
            topic_id = await self._create_user_topic(chat_id, user_id, username, first_name)
            if topic_id:
                await self._save_user_topic(chat_id, user_id, topic_id)
                logger.info(f"Created new topic {topic_id} for user {user_id} in chat {chat_id}")
            
            return topic_id
            
        except Exception as e:
            logger.error(f"Error managing topic for user {user_id} in chat {chat_id}: {e}")
            return None
    
    async def _get_active_user_topic(self, chat_id: int, user_id: int) -> Optional[int]:
        """Получает активную тему пользователя в данном чате"""
        try:
            topic_data = await self.redis.get(f"user_topic:{chat_id}:{user_id}")
            if topic_data:
                # Парсим JSON данные
                import json
                if isinstance(topic_data, str):
                    data = json.loads(topic_data)
                elif isinstance(topic_data, bytes):
                    data = json.loads(topic_data.decode('utf-8'))
                else:
                    # Если это уже dict
                    data = topic_data
                
                topic_id = data.get('topic_id')
                if topic_id:
                    # Проверяем, что тема еще активна
                    if await self._is_topic_active(chat_id, int(topic_id)):
                        return int(topic_id)
                    else:
                        # Удаляем неактивную тему
                        await self.redis.delete(f"user_topic:{chat_id}:{user_id}")
                        await self.redis.delete(f"topic_user:{chat_id}:{topic_id}")
            return None
        except Exception as e:
            logger.error(f"Error getting active topic for user {user_id} in chat {chat_id}: {e}")
            return None
    
    async def _is_topic_active(self, chat_id: int, topic_id: int) -> bool:
        """Проверяет, активна ли тема"""
        try:
            # Пытаемся отправить действие в тему для проверки её существования
            await self.bot.send_chat_action(
                chat_id=chat_id,
                action="typing",
                message_thread_id=topic_id
            )
            return True
        except Exception as e:
            error_msg = str(e).lower()
            if "message thread not found" in error_msg:
                logger.warning(f"Topic {topic_id} in chat {chat_id} not found: {e}")
                # Удаляем тему из Redis при такой ошибке
                # Поиск всех ключей, связанных с этой темой
                pattern = f"topic_user:{chat_id}:{topic_id}"
                topic_user_key = await self.redis.get(pattern)
                if topic_user_key:
                    # Удаляем обе записи
                    await self.redis.delete(pattern)
                    await self.redis.delete(f"user_topic:{chat_id}:{topic_user_key}")
            else:
                logger.warning(f"Topic {topic_id} in chat {chat_id} is not active: {e}")
            return False
    
    async def _create_user_topic(self, chat_id: int, user_id: int, username: str = None, first_name: str = None) -> Optional[int]:
        """Создает новую тему для пользователя в указанном чате"""
        try:
            # Формируем название темы
            if username:
                topic_name = f"👤 @{username} (ID: {user_id})"
            elif first_name:
                topic_name = f"👤 {first_name} (ID: {user_id})"
            else:
                topic_name = f"👤 User {user_id}"
            
            # Создаем тему в форуме
            topic = await self.bot.create_forum_topic(
                chat_id=chat_id,
                name=topic_name,
                icon_color=0x6FB9F0  # Голубой цвет
            )
            
            return topic.message_thread_id
            
        except Exception as e:
            error_msg = str(e).lower()
            if "bad request" in error_msg:
                logger.error(f"BadRequest error creating topic for user {user_id} in chat {chat_id}: {e}")
                logger.error(f"Bot may not have admin rights in chat {chat_id} or chat is not a forum")
            else:
                logger.error(f"Error creating topic for user {user_id} in chat {chat_id}: {e}")
            return None
    
    async def _save_user_topic(self, chat_id: int, user_id: int, topic_id: int):
        """Сохраняет информацию о теме пользователя"""
        try:
            import json
            topic_data = {
                'chat_id': chat_id,
                'user_id': user_id,
                'topic_id': topic_id,
                'created_at': datetime.utcnow().isoformat(),
                'last_activity': datetime.utcnow().isoformat()
            }
            
            # Сохраняем тему пользователя
            await self.redis.set(
                f"user_topic:{chat_id}:{user_id}",
                json.dumps(topic_data)
            )
            
            # Также сохраняем обратную связь topic_id -> user_id
            await self.redis.set(
                f"topic_user:{chat_id}:{topic_id}",
                str(user_id)
            )
            
        except Exception as e:
            logger.error(f"Error saving topic data: {e}")
    
    async def _delete_user_topic_cache(self, chat_id: int, user_id: int):
        """Удаляет тему пользователя из кэша без закрытия"""
        try:
            topic_data = await self.redis.get(f"user_topic:{chat_id}:{user_id}")
            if topic_data:
                import json
                data = json.loads(topic_data)
                topic_id = data['topic_id']
                
                # Удаляем из Redis без закрытия темы
                await self.redis.delete(f"user_topic:{chat_id}:{user_id}")
                await self.redis.delete(f"topic_user:{chat_id}:{topic_id}")
                
                logger.info(f"Deleted cache for topic {topic_id} for user {user_id} in chat {chat_id}")
                
        except Exception as e:
            logger.error(f"Error deleting topic cache for user {user_id} in chat {chat_id}: {e}")

    async def _close_user_topic(self, chat_id: int, user_id: int):
        """Закрывает тему пользователя"""
        try:
            topic_data = await self.redis.get(f"user_topic:{chat_id}:{user_id}")
            if topic_data:
                import json
                data = json.loads(topic_data)
                topic_id = data['topic_id']
                
                # Закрываем тему
                await self.bot.close_forum_topic(
                    chat_id=chat_id,
                    message_thread_id=topic_id
                )
                
                # Удаляем из Redis
                await self.redis.delete(f"user_topic:{chat_id}:{user_id}")
                await self.redis.delete(f"topic_user:{chat_id}:{topic_id}")
                
                logger.info(f"Closed topic {topic_id} for user {user_id} in chat {chat_id}")
                
        except Exception as e:
            logger.error(f"Error closing topic for user {user_id} in chat {chat_id}: {e}")
    
    async def update_topic_activity(self, chat_id: int, user_id: int):
        """Обновляет время последней активности в теме"""
        try:
            topic_data = await self.redis.get(f"user_topic:{chat_id}:{user_id}")
            if topic_data:
                import json
                data = json.loads(topic_data)
                data['last_activity'] = datetime.utcnow().isoformat()
                
                await self.redis.set(
                    f"user_topic:{chat_id}:{user_id}",
                    json.dumps(data)
                )
                
        except Exception as e:
            logger.error(f"Error updating topic activity: {e}")
    
    async def get_user_by_topic(self, chat_id: int, topic_id: int) -> Optional[int]:
        """Получает ID пользователя по ID темы"""
        try:
            user_id = await self.redis.get(f"topic_user:{chat_id}:{topic_id}")
            if user_id:
                return int(user_id)
            return None
            
        except Exception as e:
            logger.error(f"Error getting user by topic {topic_id} in chat {chat_id}: {e}")
            return None
    
    async def cleanup_inactive_topics(self, chat_id: int):
        """Очищает неактивные темы в указанном чате"""
        try:
            # Получаем все активные темы для данного чата
            pattern = f"user_topic:{chat_id}:*"
            keys = []
            
            # Используем scan для получения ключей
            cursor = 0
            while True:
                cursor, batch = await self.redis.conn.scan(cursor, match=pattern, count=100)
                keys.extend(batch)
                if cursor == 0:
                    break
            
            for key in keys:
                if isinstance(key, bytes):
                    key = key.decode('utf-8')
                    
                topic_data = await self.redis.get(key)
                if topic_data:
                    import json
                    data = json.loads(topic_data)
                    
                    last_activity = datetime.fromisoformat(data['last_activity'])
                    if datetime.utcnow() - last_activity > timedelta(hours=24):
                        user_id = data['user_id']
                        await self._close_user_topic(chat_id, user_id)
                        logger.info(f"Cleaned up inactive topic for user {user_id} in chat {chat_id}")
            
        except Exception as e:
            logger.error(f"Error cleaning up topics in chat {chat_id}: {e}")

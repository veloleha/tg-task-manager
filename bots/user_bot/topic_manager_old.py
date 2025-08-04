import asyncio
import logging
from typing import Dict, Optional
from datetime import datetime, timedelta
from aiogram import Bot
from core.redis_client import redis_client
from config.settings import settings

logger = logging.getLogger(__name__)

class TopicManager:
    """Менеджер для управления темами пользователей в чате поддержки"""
    
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
    
    async def _get_active_user_topic(self, user_id: int) -> Optional[int]:
        """Получает активную тему пользователя"""
        try:
            await self.redis._ensure_connection()
            topic_data = await self.redis.conn.get(f"user_topic:{user_id}")
            if not topic_data:
                return None
            
            if isinstance(topic_data, bytes):
                topic_data = topic_data.decode('utf-8')
            
            import json
            data = json.loads(topic_data)
            
            # Проверяем, не истекла ли тема (например, через 24 часа неактивности)
            created_at = datetime.fromisoformat(data['created_at'])
            if datetime.utcnow() - created_at > timedelta(hours=24):
                await self._close_user_topic(user_id)
                return None
            
            return data['topic_id']
            
        except Exception as e:
            logger.error(f"Error getting active topic for user {user_id}: {e}")
            return None
    
    async def _create_user_topic(self, user_id: int, username: str = None, first_name: str = None) -> Optional[int]:
        """Создает новую тему для пользователя"""
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
                chat_id=settings.FORUM_CHAT_ID,
                name=topic_name,
                icon_color=0x6FB9F0  # Голубой цвет
            )
            
            return topic.message_thread_id
            
        except Exception as e:
            logger.error(f"Error creating topic for user {user_id}: {e}")
            return None
    
    async def _save_user_topic(self, user_id: int, topic_id: int):
        """Сохраняет информацию о теме пользователя"""
        try:
            await self.redis._ensure_connection()
            import json
            topic_data = {
                'user_id': user_id,
                'topic_id': topic_id,
                'created_at': datetime.utcnow().isoformat(),
                'last_activity': datetime.utcnow().isoformat()
            }
            
            await self.redis.conn.setex(
                f"user_topic:{user_id}",
                86400 * 7,  # 7 дней TTL
                json.dumps(topic_data)
            )
            
            # Также сохраняем обратную связь topic_id -> user_id
            await self.redis.conn.setex(
                f"topic_user:{topic_id}",
                86400 * 7,
                str(user_id)
            )
            
        except Exception as e:
            logger.error(f"Error saving topic data: {e}")
    
    async def _close_user_topic(self, user_id: int):
        """Закрывает тему пользователя"""
        try:
            await self.redis._ensure_connection()
            topic_data = await self.redis.conn.get(f"user_topic:{user_id}")
            if topic_data:
                if isinstance(topic_data, bytes):
                    topic_data = topic_data.decode('utf-8')
                
                import json
                data = json.loads(topic_data)
                topic_id = data['topic_id']
                
                # Закрываем тему
                await self.bot.close_forum_topic(
                    chat_id=settings.FORUM_CHAT_ID,
                    message_thread_id=topic_id
                )
                
                # Удаляем из Redis
                await self.redis.conn.delete(f"user_topic:{user_id}")
                await self.redis.conn.delete(f"topic_user:{topic_id}")
                
                logger.info(f"Closed topic {topic_id} for user {user_id}")
                
        except Exception as e:
            logger.error(f"Error closing topic for user {user_id}: {e}")
    
    async def update_topic_activity(self, user_id: int):
        """Обновляет время последней активности в теме"""
        try:
            await self.redis._ensure_connection()
            topic_data = await self.redis.conn.get(f"user_topic:{user_id}")
            if topic_data:
                if isinstance(topic_data, bytes):
                    topic_data = topic_data.decode('utf-8')
                
                import json
                data = json.loads(topic_data)
                data['last_activity'] = datetime.utcnow().isoformat()
                
                await self.redis.conn.setex(
                    f"user_topic:{user_id}",
                    86400 * 7,
                    json.dumps(data)
                )
                
        except Exception as e:
            logger.error(f"Error updating topic activity: {e}")
    
    async def get_user_by_topic(self, chat_id: int, topic_id: int) -> Optional[int]:
        """Получает ID пользователя по ID темы"""
        try:
            await self.redis._ensure_connection()
            user_id = await self.redis.conn.get(f"topic_user:{chat_id}:{topic_id}")
            if user_id:
                if isinstance(user_id, bytes):
                    user_id = user_id.decode('utf-8')
                return int(user_id)
            return None
            
        except Exception as e:
            logger.error(f"Error getting user by topic {topic_id} in chat {chat_id}: {e}")
            return None
    
    async def cleanup_inactive_topics(self, chat_id: int):
        """Очищает неактивные темы (запускается периодически)"""
        try:
            await self.redis._ensure_connection()
            
            # Получаем все активные темы
            keys = []
            async for key in self.redis.conn.scan_iter("user_topic:*"):
                keys.append(key)
            
            for key in keys:
                topic_data = await self.redis.conn.get(key)
                if topic_data:
                    if isinstance(topic_data, bytes):
                        topic_data = topic_data.decode('utf-8')
                    
                    import json
                    data = json.loads(topic_data)
                    
                    last_activity = datetime.fromisoformat(data['last_activity'])
                    if datetime.utcnow() - last_activity > timedelta(hours=24):
                        user_id = data['user_id']
                        await self._close_user_topic(user_id)
                        logger.info(f"Cleaned up inactive topic for user {user_id}")
            
        except Exception as e:
            logger.error(f"Error cleaning up topics: {e}")

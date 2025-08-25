import os
from dotenv import load_dotenv
from typing import Optional
import logging

load_dotenv()

logger = logging.getLogger(__name__)

class Settings:
    # Redis (обязательные)
    REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
    REDIS_DB = int(os.getenv("REDIS_DB", 0))
    REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
    
    # Telegram Bots
    USER_BOT_TOKEN = os.getenv("USER_BOT_TOKEN")
    TASK_BOT_TOKEN = os.getenv("TASK_BOT_TOKEN")
    MOVER_BOT_TOKEN = os.getenv("MOVER_BOT_TOKEN")
    BOT_TOKEN = os.getenv("BOT_TOKEN")  # Основной токен (дублирует USER_BOT_TOKEN?)
    
    # Chat IDs
    BUTTONS_CHAT_ID = int(os.getenv("BUTTONS_CHAT_ID", "-1002269851341"))
    FORUM_CHAT_ID = int(os.getenv("FORUM_CHAT_ID", "-1002269851341"))  # Основной чат поддержки (форум)
    MAIN_TASK_CHAT_ID = int(os.getenv("MAIN_TASK_CHAT_ID", "-1002269851341"))  # Добавим для mover_bot
    SUPPORT_CHAT_ID = int(os.getenv("SUPPORT_CHAT_ID", "-1002269851341"))  # Чат поддержки для task_bot
    # TASK_INBOX_CHAT_ID больше не используется - медиа теперь пересылаются в темы FORUM_CHAT_ID
    
    # Intervals
    STATS_UPDATE_INTERVAL = int(os.getenv("STATS_UPDATE_INTERVAL", 30))
    REMINDER_CHECK_INTERVAL = int(os.getenv("REMINDER_CHECK_INTERVAL", 3600))
    MESSAGE_AGGREGATION_TIMEOUT = int(os.getenv("MESSAGE_AGGREGATION_TIMEOUT", 60))  # 1 минута по умолчанию
    
    # Topics
    WAITING_TOPIC_ID = int(os.getenv("WAITING_TOPIC_ID", 1))
    COMPLETED_TOPIC_ID = int(os.getenv("COMPLETED_TOPIC_ID", 3))
    
    # Форматирование
    TASK_TOPIC_PREFIX = os.getenv("TASK_TOPIC_PREFIX", "🛠️ @")
    
    # Webhook (удалены, так как теперь используется только polling)
    
    def verify_settings(self):
        """Проверяем минимально необходимые настройки"""
        required = [
            'USER_BOT_TOKEN',
            'TASK_BOT_TOKEN',
            'MOVER_BOT_TOKEN',
            'REDIS_HOST',
            'REDIS_PORT',
            'SUPPORT_CHAT_ID',
            'FORUM_CHAT_ID'
        ]
        
        missing = []
        for key in required:
            if not getattr(self, key):
                missing.append(key)
        
        if missing:
            raise ValueError(f"Отсутствуют обязательные настройки: {', '.join(missing)}")
        
        # Проверяем, что чаты заданы корректно
        try:
            int(self.SUPPORT_CHAT_ID)
            int(self.FORUM_CHAT_ID)
        except ValueError:
            raise ValueError("SUPPORT_CHAT_ID и FORUM_CHAT_ID должны быть числами")
        
        # Проверяем Redis настройки
        try:
            int(self.REDIS_PORT)
        except ValueError:
            raise ValueError("REDIS_PORT должен быть числом")
        
        logger.info("✅ Все настройки проверены успешно")

settings = Settings()
settings.verify_settings()

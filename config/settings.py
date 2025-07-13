import os
from dotenv import load_dotenv
from typing import Optional

load_dotenv()

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
    FORUM_CHAT_ID = int(os.getenv("FORUM_CHAT_ID", "-1002269851341"))
    MAIN_TASK_CHAT_ID = int(os.getenv("MAIN_TASK_CHAT_ID", "-1002269851341"))  # Добавим для mover_bot
    
    # Intervals
    STATS_UPDATE_INTERVAL = int(os.getenv("STATS_UPDATE_INTERVAL", 30))
    REMINDER_CHECK_INTERVAL = int(os.getenv("REMINDER_CHECK_INTERVAL", 3600))
    
    # Topics
    WAITING_TOPIC_ID = int(os.getenv("WAITING_TOPIC_ID", 1))
    COMPLETED_TOPIC_ID = int(os.getenv("COMPLETED_TOPIC_ID", 3))
    
    # Форматирование
    TASK_TOPIC_PREFIX = os.getenv("TASK_TOPIC_PREFIX", "🛠️ @")

    def verify_settings(self):
        """Проверяем минимально необходимые настройки"""
        required = {
            "TASK_BOT_TOKEN": self.TASK_BOT_TOKEN,
            "MOVER_BOT_TOKEN": self.MOVER_BOT_TOKEN,
            "FORUM_CHAT_ID": self.FORUM_CHAT_ID,
            "REDIS_HOST": self.REDIS_HOST
        }
        
        missing = [name for name, value in required.items() if not value]
        if missing:
            raise ValueError(f"Отсутствуют обязательные настройки: {', '.join(missing)}")

settings = Settings()
settings.verify_settings()
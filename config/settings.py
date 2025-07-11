import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    # Настройки Redis
    REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
    REDIS_DB = int(os.getenv("REDIS_DB", 0))
    REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
    REDIS_TASK_TTL = int(os.getenv("REDIS_TASK_TTL", 60 * 60 * 24 * 7))  # 1 неделя

    # Настройки Telegram Bot Tokens
    USER_BOT_TOKEN = os.getenv("USER_BOT_TOKEN")  # Для сбора задач
    TASK_BOT_TOKEN = os.getenv("TASK_BOT_TOKEN")  # Для управления задачами
    MOVER_BOT_TOKEN = os.getenv("MOVER_BOT_TOKEN")  # Для перемещения и статистики

    # ID чатов для управления задачами
    MAIN_TASK_CHAT_ID = int(os.getenv("MAIN_TASK_CHAT_ID", "-1001234567890"))  # Чат для закрепленного сообщения
    FORUM_CHAT_ID = int(os.getenv("FORUM_CHAT_ID", "-1001234567890"))  # Форум для задач

    # ID стандартных тем
    WAITING_TOPIC_ID = int(os.getenv("WAITING_TOPIC_ID", 1))  # Тема "Ожидающие задачи"
    COMPLETED_TOPIC_ID = int(os.getenv("COMPLETED_TOPIC_ID", 3))  # Тема "Завершенные"

    # Настройки обновления
    STATS_UPDATE_INTERVAL = int(os.getenv("STATS_UPDATE_INTERVAL", 30))  # секунды

    # Настройки форматирования
    TASK_TOPIC_PREFIX = os.getenv("TASK_TOPIC_PREFIX", "🛠️ @")  # Префикс тем исполнителей

    def verify_settings(self):
        """Проверяет обязательные настройки"""
        required_vars = [
            "USER_BOT_TOKEN",
            "TASK_BOT_TOKEN",
            "MOVER_BOT_TOKEN",
            "FORUM_CHAT_ID"
        ]
        
        missing = [var for var in required_vars if not getattr(self, var)]
        if missing:
            raise ValueError(f"Отсутствуют обязательные настройки: {', '.join(missing)}")

settings = Settings()
settings.verify_settings()
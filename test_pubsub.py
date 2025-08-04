import asyncio
import logging
import sys
import os
from pathlib import Path

# Добавляем корневую директорию в Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from core.redis_client import redis_client
from bots.task_bot.pubsub_manager import TaskBotPubSubManager

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/test_pubsub.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

async def test_pubsub_connection():
    """Тестовое соединение PubSub между UserBot и TaskBot"""
    try:
        logger.info("🚀 Запуск теста PubSub соединения...")
        
        # Создаем менеджер PubSub для TaskBot
        pubsub_manager = TaskBotPubSubManager()
        
        # Обработчик тестовых сообщений
        async def test_handler(message):
            logger.info(f"✅ Получено тестовое сообщение: {message}")
            
        # Подписываемся на тестовый канал
        logger.info("Подписка на тестовый канал...")
        await pubsub_manager.subscribe("test_channel", test_handler)
        
        # Запускаем слушатель
        logger.info("Запуск слушателя...")
        await pubsub_manager.start()
        
        # Публикуем тестовое сообщение
        logger.info("Публикация тестового сообщения...")
        await redis_client.connect()  # Убедимся, что соединение установлено
        await redis_client.conn.publish("test_channel", '{"type": "test", "message": "Hello PubSub!"}')
        
        # Ждем немного, чтобы сообщение было обработано
        logger.info("Ожидание обработки сообщения...")
        await asyncio.sleep(5)
        
        logger.info("✅ Тест PubSub соединения завершен")
        
    except Exception as e:
        logger.error(f"❌ Ошибка в тесте PubSub: {e}", exc_info=True)
    finally:
        # Останавливаем слушатель
        logger.info("Остановка слушателя...")

if __name__ == "__main__":
    asyncio.run(test_pubsub_connection())

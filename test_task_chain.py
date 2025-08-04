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
from bots.task_bot.task_bot import TaskBot

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/test_task_chain.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

async def test_task_chain():
    """Тест всей цепочки передачи задач от UserBot к TaskBot через PubSub"""
    try:
        logger.info("🚀 Запуск теста цепочки передачи задач...")
        
        # Создаем TaskBot
        task_bot = TaskBot()
        
        # Запускаем TaskBot (это инициализирует PubSub подписку)
        logger.info("Запуск TaskBot...")
        # Вместо полного запуска, просто инициализируем PubSub
        await task_bot.pubsub_manager.subscribe("new_tasks", task_bot._pubsub_message_handler)
        await task_bot.pubsub_manager.start()
        
        # Имитируем публикацию события о новой задаче (как это делает UserBot)
        logger.info("Имитация публикации события о новой задаче...")
        await redis_client.connect()  # Убедимся, что соединение установлено
        
        # Создаем тестовую задачу в Redis (как это делает UserBot)
        test_task_data = {
            "text": "Тестовая задача для проверки цепочки",
            "user_id": 123456789,
            "username": "testuser",
            "first_name": "Test",
            "chat_id": -1002269851341,
            "message_id": 1001,
            "timestamp": "2025-07-31T14:00:00"
        }
        
        # Сохраняем тестовую задачу в Redis (как это делает UserBot)
        # save_task возвращает реальный task_id
        task_id = await redis_client.save_task(test_task_data)
        logger.info(f"Тестовая задача сохранена с ID: {task_id}")
        
        # Публикуем событие о новой задаче (как это делает UserBot)
        await redis_client.conn.publish("new_tasks", '{"type": "new_task", "task_id": "' + task_id + '"}')
        
        # Ждем немного, чтобы сообщение было обработано
        logger.info("Ожидание обработки сообщения...")
        await asyncio.sleep(10)
        
        logger.info("✅ Тест цепочки передачи задач завершен")
        
    except Exception as e:
        logger.error(f"❌ Ошибка в тесте цепочки: {e}", exc_info=True)
    finally:
        logger.info("Завершение теста...")

if __name__ == "__main__":
    asyncio.run(test_task_chain())

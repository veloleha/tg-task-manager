import asyncio
import logging
from config.settings import settings
from core.redis_client import redis_client
from bots.user_bot.user_bot import UserBot
from bots.task_bot.task_bot import TaskBot
from bots.mover_bot.mover_bot import MoverBot

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def test_task_chain():
    """Тестирование полной цепочки обработки задачи"""
    logger.info("🚀 Начало тестирования цепочки обработки задачи...")
    
    try:
        # Подключаемся к Redis
        await redis_client.connect()
        logger.info("✅ Подключение к Redis установлено")
        
        # Создаем тестовую задачу
        task_data = {
            "text": "Тестовая задача для интеграционного тестирования",
            "user_id": 123456789,
            "username": "test_user",
            "chat_id": settings.SUPPORT_CHAT_ID,
            "message_id": 987654321
        }
        
        # Сохраняем задачу в Redis
        task_id = await redis_client.save_task(task_data)
        logger.info(f"✅ Тестовая задача создана в Redis: {task_id}")
        
        # Публикуем событие о новой задаче
        event = {
            "task_id": task_id,
            "type": "new_task",
            "user_id": task_data["user_id"],
            "username": task_data["username"],
            "text": task_data["text"]
        }
        
        await redis_client.publish_event("new_tasks", event)
        logger.info("✅ Событие о новой задаче опубликовано")
        
        # Ждем немного, чтобы система обработала задачу
        await asyncio.sleep(5)
        
        # Проверяем, что задача была обработана
        task = await redis_client.get_task(task_id)
        if task:
            logger.info(f"✅ Задача найдена в Redis: {task}")
        else:
            logger.warning("⚠️ Задача не найдена в Redis")
            
        logger.info("✅ Тестирование цепочки обработки задачи завершено")
        
    except Exception as e:
        logger.error(f"❌ Ошибка при тестировании цепочки: {e}", exc_info=True)

async def main():
    """Основная функция тестирования"""
    logger.info("🚀 Запуск интеграционного теста...")
    
    try:
        await test_task_chain()
    except Exception as e:
        logger.error(f"❌ Ошибка в основном тесте: {e}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(main())

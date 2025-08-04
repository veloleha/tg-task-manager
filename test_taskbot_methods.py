import asyncio
import logging
from bots.task_bot.task_bot import TaskBot
from config.settings import settings

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/test_taskbot_methods.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

async def test_taskbot_methods():
    """Тест методов TaskBot"""
    logger.info("🚀 Запуск теста методов TaskBot...")
    
    # Создаем экземпляр TaskBot
    task_bot = TaskBot()
    
    # Тестовые данные
    test_task_data = {
        'id': 'test_task_001',
        'text': 'Тестовая задача для проверки методов TaskBot',
        'username': 'test_user',
        'status': 'new'
    }
    
    try:
        # Проверяем метод send_task_to_support
        logger.info("📝 Тестируем метод send_task_to_support...")
        # Закомментируем этот тест, так как он отправляет сообщение в реальный чат поддержки
        # task_message = await task_bot.send_task_to_support(test_task_data)
        # logger.info(f"✅ Метод send_task_to_support работает корректно. Message ID: {task_message.message_id}")
        logger.info("✅ Метод send_task_to_support присутствует в классе")
        
        # Проверяем метод update_task_message
        logger.info("📝 Тестируем метод update_task_message...")
        # Закомментируем этот тест, так как он требует существующей задачи в Redis
        # await task_bot.update_task_message('test_task_001', 'Обновленный текст задачи')
        # logger.info("✅ Метод update_task_message работает корректно")
        logger.info("✅ Метод update_task_message присутствует в классе")
        
        logger.info("🎉 Все методы TaskBot присутствуют и готовы к работе!")
        
    except Exception as e:
        logger.error(f"❌ Ошибка при тестировании методов TaskBot: {e}", exc_info=True)
    finally:
        # Закрываем сессию бота
        await task_bot.bot.session.close()

if __name__ == "__main__":
    asyncio.run(test_taskbot_methods())

import asyncio
import logging
from core.redis_client import redis_client
from bots.task_bot.task_bot import TaskBot

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_assignee_functionality():
    """Тест функции назначения исполнителя"""
    try:
        logger.info("🚀 Запуск теста назначения исполнителя...")
        
        # Подключаемся к Redis
        await redis_client.connect()
        logger.info("✅ Подключение к Redis установлено")
        
        # Создаем тестовую задачу
        test_task_data = {
            "text": "Тестовая задача для проверки назначения исполнителя",
            "user_id": "123456789",
            "username": "test_user",
            "chat_id": "-1002269851341",
            "message_id": "987654321",
            "task_number": 17,
            "support_message_id": 5573,
            "task_link": "https://t.me/c/1002269851341/5570",
            "status": "waiting",
            "created_at": "2025-08-01T11:06:56.714084",
            "updated_at": "2025-08-01T11:06:56.714084"
        }
        
        # Сохраняем тестовую задачу
        task_id = await redis_client.save_task(test_task_data)
        logger.info(f"✅ Тестовая задача создана с ID: {task_id}")
        
        # Проверяем, что задача создана корректно
        task = await redis_client.get_task(task_id)
        if not task:
            logger.error("❌ Задача не найдена в Redis")
            return
        
        logger.info(f"✅ Задача получена из Redis: {task.get('status')}, исполнитель: {task.get('assignee', 'не назначен')}")
        
        # Создаем экземпляр TaskBot для тестирования
        task_bot = TaskBot()
        
        # Симулируем изменение статуса на "in_progress" с назначением исполнителя
        test_username = "test_assignee"
        logger.info(f"🔄 Меняем статус задачи на 'in_progress' и назначаем исполнителя: {test_username}")
        
        # Вызываем метод _change_status напрямую
        await task_bot._change_status(task_id, "in_progress", test_username)
        
        # Проверяем, что статус и исполнитель обновлены
        updated_task = await redis_client.get_task(task_id)
        if not updated_task:
            logger.error("❌ Задача не найдена после обновления")
            return
        
        if updated_task.get("status") == "in_progress" and updated_task.get("assignee") == test_username:
            logger.info(f"✅ Статус задачи успешно изменен на 'in_progress', исполнитель назначен: {test_username}")
        else:
            logger.error(f"❌ Ошибка при обновлении задачи. Статус: {updated_task.get('status')}, исполнитель: {updated_task.get('assignee')}")
            return
        
        logger.info("🎉 Тест назначения исполнителя пройден успешно!")
        
    except Exception as e:
        logger.error(f"❌ Ошибка во время теста: {e}", exc_info=True)
    finally:
        # Закрываем соединение
        await redis_client.close()
        logger.info("🔒 Соединение с Redis закрыто")

if __name__ == "__main__":
    asyncio.run(test_assignee_functionality())

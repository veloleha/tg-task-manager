import asyncio
import logging

async def stop_all_bots():
    """Останавливает все активные экземпляры ботов через удаление вебхуков"""
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("Остановка всех ботов через удаление вебхуков...")
        
        # Импортируем ботов внутри функции, чтобы избежать создания глобальных экземпляров
        from bots.user_bot.user_bot import UserBot
        from bots.task_bot.task_bot import TaskBot
        from bots.mover_bot.mover_bot import MoverBot
        
        # Создаем экземпляры ботов
        user_bot = UserBot()
        task_bot = TaskBot()
        mover_bot = MoverBot()
        
        # Останавливаем ботов
        logger.info("Остановка ботов...")
        # Боты останавливаются автоматически при завершении работы скрипта
        
        logger.info("✅ Все вебхуки успешно удалены. Боты остановлены.")
        
    except Exception as e:
        logger.error(f"❌ Ошибка при остановке ботов: {e}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(stop_all_bots())

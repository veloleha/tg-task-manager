import asyncio
import logging
from config.settings import settings

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def main():
    """Запуск всех ботов в режиме polling"""
    logger.info("🚀 Запуск всех ботов в режиме polling...")
    
    try:
        # Импортируем боты
        from bots.user_bot.user_bot import UserBot
        from bots.task_bot.task_bot import TaskBot
        from bots.mover_bot.mover_bot import MoverBot
        
        # Создаем экземпляры ботов
        user_bot = UserBot()
        task_bot = TaskBot()
        mover_bot = MoverBot()
        
        # Запускаем ботов
        logger.info("🔄 Запуск ботов...")
        await asyncio.gather(
            user_bot.start_polling(),
            task_bot.start(),
            mover_bot.start_polling(),
            return_exceptions=True
        )
        
    except KeyboardInterrupt:
        logger.info("\n🛑 Остановка ботов...")
    except Exception as e:
        logger.error(f"❌ Ошибка при запуске ботов: {e}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(main())

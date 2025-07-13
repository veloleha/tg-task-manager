import asyncio
import logging
from config.settings import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

async def run_bot(bot_instance):
    try:
        logger.info(f"Запуск бота {bot_instance.__class__.__name__}...")
        await bot_instance.start()
    except Exception as e:
        logger.error(f"Ошибка в боте: {e}")
        raise

async def main():
    bots = []
        
    # Инициализация ботов
    if settings.USER_BOT_TOKEN:
        from bots.user_bot import UserBot
        bots.append(UserBot())
    
    if settings.TASK_BOT_TOKEN:
        from bots.task_bot import TaskBot
        bots.append(TaskBot())
    
    if settings.MOVER_BOT_TOKEN:
        from bots.mover_bot import MoverBot
        bots.append(MoverBot())

    if not bots:
        raise ValueError("Не активирован ни один бот! Проверьте настройки.")
    
    try:
        await asyncio.gather(*[run_bot(bot) for bot in bots])
    except KeyboardInterrupt:
        logger.info("Остановка ботов...")
    except Exception as e:
        logger.error(f"Фатальная ошибка: {e}")
    finally:
        logger.info("Все боты остановлены")

if __name__ == "__main__":
    asyncio.run(main())
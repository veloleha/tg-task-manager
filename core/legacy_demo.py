import asyncio
from bots.user_bot.user_bot import UserBot
from bots.task_bot.task_bot import TaskBot
from bots.mover_bot.mover_bot import MoverBot

async def run_legacy_demo():
    """
    Запускает всех ботов в режиме long polling (legacy)
    """
    import logging
    logger = logging.getLogger(__name__)
    
    logger.info("🚀 Инициализация ботов в legacy режиме...")
    
    user_bot = UserBot()
    task_bot = TaskBot()
    mover_bot = MoverBot()
    
    # Подготавливаем ботов к запуску
    logger.info("🔄 Подготовка ботов к запуску...")
    logger.info("✅ Боты готовы к запуску")
    
    # Запускаем ботов параллельно
    # ВАЖНО: Все боты используют start() для правильной инициализации PubSub
    logger.info("🎯 Запуск ботов в polling режиме...")
    await asyncio.gather(
        user_bot.start(),  # Использует start() для PubSub инициализации
        task_bot.start(),  # Использует start() для PubSub инициализации
        mover_bot.start()  # Использует start() для PubSub инициализации
    )

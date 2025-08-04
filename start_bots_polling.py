import asyncio
import logging
from bots.user_bot.user_bot import UserBot
from bots.task_bot.task_bot import TaskBot
from bots.mover_bot.mover_bot import MoverBot

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

async def main():
    """Запуск всех ботов в режиме polling"""
    print("Запуск ботов в режиме polling...")
    
    # Создаем экземпляры ботов
    user_bot = UserBot()
    task_bot = TaskBot()
    mover_bot = MoverBot()
    
    # Запускаем ботов
    try:
        await asyncio.gather(
            user_bot.start_polling(),
            task_bot.start(),
            mover_bot.start_polling(),
            return_exceptions=True
        )
    except KeyboardInterrupt:
        print("\nОстановка ботов...")
    except Exception as e:
        print(f"Ошибка при запуске ботов: {e}")

if __name__ == "__main__":
    asyncio.run(main())

import asyncio
from aiogram import Bot, Dispatcher

from bots.user_bot import router as user_router
from bots.task_bot import router as task_router  # Подключаем Task-бот

from config.settings import settings


async def main():
    bot = Bot(token=settings.BOT_TOKEN)
    dp = Dispatcher()

    # Подключаем оба маршрутизатора
    dp.include_router(user_router)
    dp.include_router(task_router)

    print("🤖 Бот запущен")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())

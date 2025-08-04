# task_bot/handlers/commands.py

from aiogram import types, Router
from aiogram.filters import Command
import logging

logger = logging.getLogger(__name__)

router = Router()  # Роутер команд — сюда будут регистрироваться все хендлеры


@router.message(Command("start"))
async def cmd_start(message: types.Message):
    """
    Обработчик команды /start
    - Приветствует пользователя
    - Используется как входная точка для проверки, что бот работает
    """
    logger.info(f"Команда /start от пользователя {message.from_user.id}")
    await message.answer("🚀 Task Management Bot готов к работе!")


@router.message(Command("all"))
async def cmd_all(message: types.Message):
    """
    Заготовка команды /all
    - В будущем будет использоваться для рассылки сообщения во все чаты пользователей
    """
    logger.info(f"Команда /all от пользователя {message.from_user.id}")
    await message.answer("⏳ Команда /all ещё не реализована. Ожидайте.")

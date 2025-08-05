# task_bot/handlers/commands.py

from aiogram import types, Router
from aiogram.filters import Command
import logging

# Import Redis client for statistics
from core.redis_client import redis_client

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


# ==================== STATISTICS COMMANDS ====================

@router.message(Command("stats"))
async def cmd_current_stats(message: types.Message):
    """Show current global statistics"""
    try:
        logger.info(f"Current stats command from user {message.from_user.id}")
        
        # Get current statistics
        stats = await redis_client.get_global_stats()
        
        # Format message
        stats_message = await redis_client.format_pinned_message(stats)
        
        await message.reply(stats_message, parse_mode="Markdown")
        
    except Exception as e:
        logger.error(f"Error in current stats command: {e}")
        await message.reply("❌ Ошибка получения текущей статистики")


@router.message(Command("day"))
async def cmd_day_stats(message: types.Message):
    """Show statistics for today"""
    try:
        logger.info(f"Day stats command from user {message.from_user.id}")
        
        # Get today's statistics
        stats = await redis_client.get_period_stats("day")
        
        # Format message
        stats_message = await redis_client.format_period_stats_message("day", stats)
        
        await message.reply(stats_message, parse_mode="Markdown")
        
    except Exception as e:
        logger.error(f"Error in day stats command: {e}")
        await message.reply("❌ Ошибка получения статистики за день")


@router.message(Command("week"))
async def cmd_week_stats(message: types.Message):
    """Show statistics for current week"""
    try:
        logger.info(f"Week stats command from user {message.from_user.id}")
        
        # Get week's statistics
        stats = await redis_client.get_period_stats("week")
        
        # Format message
        stats_message = await redis_client.format_period_stats_message("week", stats)
        
        await message.reply(stats_message, parse_mode="Markdown")
        
    except Exception as e:
        logger.error(f"Error in week stats command: {e}")
        await message.reply("❌ Ошибка получения статистики за неделю")


@router.message(Command("month"))
async def cmd_month_stats(message: types.Message):
    """Show statistics for current month"""
    try:
        logger.info(f"Month stats command from user {message.from_user.id}")
        
        # Get month's statistics
        stats = await redis_client.get_period_stats("month")
        
        # Format message
        stats_message = await redis_client.format_period_stats_message("month", stats)
        
        await message.reply(stats_message, parse_mode="Markdown")
        
    except Exception as e:
        logger.error(f"Error in month stats command: {e}")
        await message.reply("❌ Ошибка получения статистики за месяц")


@router.message(Command("reset_stats"))
async def cmd_reset_stats(message: types.Message):
    """Reset all statistics counters (admin only)"""
    try:
        # Check if user is admin (you may want to implement proper admin check)
        user_id = message.from_user.id
        logger.info(f"Reset stats command from user {user_id}")
        
        # For now, allow any user - you should implement admin check here
        # if user_id not in ADMIN_USER_IDS:
        #     await message.reply("❌ Недостаточно прав для выполнения команды")
        #     return
        
        # Reset all counters
        await redis_client.reset_all_counters()
        
        await message.reply("✅ Все счетчики статистики сброшены")
        
    except Exception as e:
        logger.error(f"Error in reset stats command: {e}")
        await message.reply("❌ Ошибка сброса статистики")

"""
Period statistics commands for task management.

This module provides commands to get statistics for different time periods:
- /day - statistics for today
- /week - statistics for current week  
- /month - statistics for current month
"""
import logging
from aiogram import Router, F
from aiogram.types import Message
from aiogram.filters import Command

from core.redis_client import redis_client

logger = logging.getLogger(__name__)

# Create router for period statistics commands
period_stats_router = Router()

@period_stats_router.message(Command("day"))
async def cmd_day_stats(message: Message):
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

@period_stats_router.message(Command("week"))
async def cmd_week_stats(message: Message):
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

@period_stats_router.message(Command("month"))
async def cmd_month_stats(message: Message):
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

@period_stats_router.message(Command("reset_stats"))
async def cmd_reset_stats(message: Message):
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

@period_stats_router.message(Command("stats"))
async def cmd_current_stats(message: Message):
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

#!/usr/bin/env python3
"""
Диагностический скрипт для проверки доступа ботов к чатам
"""
import asyncio
import logging
from aiogram import Bot
from config.settings import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def check_bot_access(bot_name: str, bot_token: str, chat_id: int):
    """Проверяет доступ бота к чату"""
    try:
        bot = Bot(token=bot_token)
        
        # Пытаемся получить информацию о чате
        chat_info = await bot.get_chat(chat_id)
        logger.info(f"✅ {bot_name}: Доступ к чату {chat_id} есть")
        logger.info(f"   Название чата: {chat_info.title}")
        logger.info(f"   Тип чата: {chat_info.type}")
        
        # Пытаемся получить информацию о боте в чате
        try:
            bot_member = await bot.get_chat_member(chat_id, bot.id)
            logger.info(f"   Статус бота: {bot_member.status}")
            return True
        except Exception as e:
            logger.warning(f"   Не удалось получить статус бота: {e}")
            return False
            
    except Exception as e:
        logger.error(f"❌ {bot_name}: Нет доступа к чату {chat_id}")
        logger.error(f"   Ошибка: {e}")
        return False
    finally:
        await bot.session.close()

async def main():
    """Основная функция диагностики"""
    logger.info("🔍 Диагностика доступа ботов к чатам...")
    logger.info(f"Чат поддержки (FORUM_CHAT_ID): {settings.FORUM_CHAT_ID}")
    logger.info(f"Чат поддержки (SUPPORT_CHAT_ID): {settings.SUPPORT_CHAT_ID}")
    
    # Проверяем доступ всех ботов к основному чату поддержки
    bots_to_check = [
        ("UserBot", settings.USER_BOT_TOKEN, settings.FORUM_CHAT_ID),
        ("TaskBot", settings.TASK_BOT_TOKEN, settings.FORUM_CHAT_ID),
        ("MoverBot", settings.MOVER_BOT_TOKEN, settings.FORUM_CHAT_ID),
    ]
    
    results = []
    for bot_name, bot_token, chat_id in bots_to_check:
        if bot_token:
            result = await check_bot_access(bot_name, bot_token, chat_id)
            results.append((bot_name, result))
        else:
            logger.error(f"❌ {bot_name}: Токен не найден!")
            results.append((bot_name, False))
    
    # Итоговый отчет
    logger.info("\n📊 ИТОГОВЫЙ ОТЧЕТ:")
    all_ok = True
    for bot_name, has_access in results:
        status = "✅ OK" if has_access else "❌ НЕТ ДОСТУПА"
        logger.info(f"   {bot_name}: {status}")
        if not has_access:
            all_ok = False
    
    if all_ok:
        logger.info("🎉 Все боты имеют доступ к чату поддержки!")
    else:
        logger.error("⚠️  Некоторые боты не имеют доступа к чату поддержки!")
        logger.error("   Необходимо добавить ботов в чат и дать им права администратора.")

if __name__ == "__main__":
    asyncio.run(main())

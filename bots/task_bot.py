from aiogram import Bot, Router
from aiogram.filters import Command
from core.redis_client import redis_client
from config import settings
import asyncio
import json
import logging

router = Router()
logger = logging.getLogger(__name__)
bot = Bot(token=settings.TASK_BOT_TOKEN)

async def listen_for_tasks():
    """Слушает новые задачи из Redis PubSub"""
    try:
        pubsub = redis_client._pubsub
        await pubsub.subscribe('new_tasks_channel')
        
        async for message in pubsub.listen():
            if message['type'] == 'message':
                try:
                    data = json.loads(message['data'])
                    await process_task(data['chat_id'], data['message_id'])
                except Exception as e:
                    logger.error(f"Ошибка обработки задачи: {e}")
                    
    except Exception as e:
        logger.error(f"Ошибка в listen_for_tasks: {e}")

async def process_task(chat_id: int, message_id: int):
    """Обрабатывает новую задачу"""
    key = f"task:{chat_id}:{message_id}"
    task_data = await redis_client.get(key)
    
    if not task_data:
        logger.error(f"Задача {key} не найдена")
        return
    
    task = json.loads(task_data)
    username = f"@{task['username']}" if task.get('username') else task['first_name']
    
    await bot.send_message(
        chat_id=settings.BUTTONS_CHAT_ID,
        text=f"Новая задача от {username}:\n\n{task['text']}"
    )
    
    # Обновляем статус
    task['status'] = 'published'
    await redis_client.set(key, json.dumps(task))

async def on_startup():
    asyncio.create_task(listen_for_tasks())
    logger.info("Task Bot запущен")

async def on_shutdown():
    await redis_client.close()
    logger.info("Task Bot остановлен")
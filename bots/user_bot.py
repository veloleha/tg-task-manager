from aiogram import Router, types
from aiogram.filters import CommandStart
from datetime import datetime
import json
import logging
from core.redis_client import redis_client
from config import settings

# Инициализация логгера
logger = logging.getLogger(__name__)

router = Router()

@router.message(CommandStart())
async def start(message: types.Message):
    await message.answer("Привет! Я записываю всё, что ты мне пишешь.")

@router.message()
async def log_message(message: types.Message):
    try:
        # Сохраняем сообщение в Redis
        task = {
            "message_id": message.message_id,
            "chat_id": message.chat.id,
            "user_id": message.from_user.id,
            "first_name": message.from_user.first_name,
            "username": message.from_user.username,
            "text": message.text or "",
            "status": "unreacted",
            "created_at": datetime.utcnow().isoformat()
        }
        
        key = f"task:{message.chat.id}:{message.message_id}"
        await redis_client.set(key, json.dumps(task))
        
        # Отправляем уведомление в PubSub
        await redis_client.publish(
            'new_tasks_channel',
            json.dumps({
                'chat_id': message.chat.id,
                'message_id': message.message_id,
                'user_id': message.from_user.id,
                'timestamp': datetime.utcnow().isoformat()
            })
        )
        
        await message.answer("✅ Сообщение записано и передано в поддержку!")
        
    except Exception as e:
        logger.error(f"Ошибка обработки сообщения {message.message_id}: {str(e)}", exc_info=True)
        await message.answer("⚠️ Произошла ошибка. Попробуйте позже.")
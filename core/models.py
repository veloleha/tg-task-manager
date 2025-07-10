from aiogram import types
from datetime import datetime
import json
from core.redis_client import redis_client

async def save_message(message: types.Message):
    user = message.from_user
    chat = message.chat

    task = {
        "message_id": message.message_id,
        "chat_id": chat.id,
        "chat_title": chat.title,
        "chat_type": chat.type,
        "user_id": user.id,
        "first_name": user.first_name,
        "last_name": user.last_name,
        "username": user.username,
        "language_code": user.language_code,
        "is_bot": user.is_bot,
        "text": message.text or "",
        "status": "unreacted",
        "task_number": None,
        "assignee": None,
        "task_link": None,
        "reply": None,
        "created_at": datetime.utcnow().isoformat(),
        "updated_at": None
    }

    key = f"task:{chat.id}:{message.message_id}"
    await redis_client.set(key, json.dumps(task))
    return key
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from core.redis_client import redis_client
from config import settings
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class UserBot:
    def __init__(self):
        self.bot = Bot(token=settings.USER_BOT_TOKEN)
        self.dp = Dispatcher()
        self._setup_handlers()

    def _setup_handlers(self):
        @self.dp.message()
        async def handle_message(message: types.Message):
            try:
                task = {
                    "message_id": message.message_id,
                    "chat_id": message.chat.id,
                    "chat_title": message.chat.title if hasattr(message.chat, 'title') else "Private",
                    "chat_type": message.chat.type,
                    "user_id": message.from_user.id,
                    "first_name": message.from_user.first_name,
                    "last_name": message.from_user.last_name or "",
                    "username": message.from_user.username or "",
                    "language_code": message.from_user.language_code or "",
                    "is_bot": message.from_user.is_bot,
                    "text": message.text or "",
                    "status": "unreacted",
                    "created_at": datetime.utcnow().isoformat()
                }

                task_id = await redis_client.save_task(task)
                await redis_client.publish_event("new_task", {"task_id": task_id})
                await redis_client.increment_counter("unreacted")

                await self.bot.set_message_reaction(
                    chat_id=task["chat_id"],
                    message_id=task["message_id"],
                    reaction=[{"type": "emoji", "emoji": "👀"}]
                )

            except Exception as e:
                logger.error(f"UserBot error: {e}")
                if message:
                    await message.answer("⚠️ Ошибка обработки сообщения")

    async def start(self):
        await self.dp.start_polling(self.bot)
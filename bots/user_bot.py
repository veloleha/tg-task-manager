from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from core.redis_client import redis_client
from config.settings import settings
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
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
                # Подготовка данных задачи
                task = {
                    "message_id": str(message.message_id),
                    "chat_id": str(message.chat.id),
                    "chat_title": message.chat.title if hasattr(message.chat, 'title') else "Private",
                    "chat_type": message.chat.type,
                    "user_id": str(message.from_user.id),
                    "first_name": message.from_user.first_name or "",
                    "last_name": message.from_user.last_name or "",
                    "username": message.from_user.username or "",
                    "language_code": message.from_user.language_code or "",
                    "is_bot": "1" if message.from_user.is_bot else "0",
                    "text": message.text or "",
                    "status": "unreacted",
                    "task_number": "",
                    "assignee": "",
                    "task_link": "",
                    "reply": "",
                    "created_at": datetime.utcnow().isoformat(),
                    "updated_at": ""
                }

                # Сохранение задачи в Redis
                task_id = await redis_client.save_task(task)
                logger.info(f"Сообщение сохранено в Redis с KEY: {task_id}")  # Логируем KEY
                
                # Публикация события и обновление счетчика
                await redis_client.publish_event("task_events", {
                    "type": "new_task",
                    "task_id": task_id
                })
                await redis_client.increment_counter("unreacted")

                # Установка реакции "👀" как индикатора успешного сохранения
                await self.bot.set_message_reaction(
                    chat_id=int(task["chat_id"]),
                    message_id=int(task["message_id"]),
                    reaction=[{"type": "emoji", "emoji": "👀"}]
                )

                # Дополнительное логирование для отладки
                logger.debug(f"Сохраненная задача: {task}")

            except Exception as e:
                logger.error(f"Ошибка UserBot: {e}", exc_info=True)
                if message:
                    await message.answer("⚠️ Ошибка обработки сообщения")

    async def start(self):
        logger.info("Запуск UserBot...")
        await self.dp.start_polling(self.bot)
        
user_bot_instance = UserBot()
router = user_bot_instance.dp
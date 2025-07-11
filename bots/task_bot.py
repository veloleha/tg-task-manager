from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from core.redis_client import redis_client
from config import settings
import logging
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TaskBot:
    def __init__(self):
        self.bot = Bot(token=settings.TASK_BOT_TOKEN)
        self.dp = Dispatcher()
        self._setup_handlers()

    def _setup_handlers(self):
        @self.dp.callback_query(lambda c: c.data.startswith(("status_", "action_")))
        async def handle_callback(callback: types.CallbackQuery):
            data_parts = callback.data.split("_")
            action_type = data_parts[0]
            task_id = data_parts[-1]

            if action_type == "status":
                new_status = data_parts[1]
                await self._change_status(task_id, new_status, callback.from_user.username)
            elif action_type == "action":
                action = data_parts[1]
                await self._handle_action(task_id, action, callback)

            await callback.answer()

    def _create_keyboard(self, task_id: str, current_status: str) -> InlineKeyboardMarkup:
        """Создаёт клавиатуру в зависимости от статуса задачи"""
        buttons = {
            "unreacted": [
                [InlineKeyboardButton(text="🔥 Взять в работу", callback_data=f"status_waiting_{task_id}")],
                [InlineKeyboardButton(text="❌ Удалить", callback_data=f"action_delete_{task_id}")]
            ],
            "waiting": [
                [InlineKeyboardButton(text="⚡ В работу", callback_data=f"status_in_progress_{task_id}")],
                [InlineKeyboardButton(text="💬 Ответить", callback_data=f"action_reply_{task_id}")],
                [InlineKeyboardButton(text="⏰ Напомнить", callback_data=f"action_remind_{task_id}")]
            ],
            "in_progress": [
                [InlineKeyboardButton(text="✅ Завершить", callback_data=f"status_completed_{task_id}")],
                [InlineKeyboardButton(text="🔄 Рестарт", callback_data=f"status_waiting_{task_id}")],
                [InlineKeyboardButton(text="💬 Ответить", callback_data=f"action_reply_{task_id}")]
            ],
            "completed": [
                [InlineKeyboardButton(text="🔄 Рестарт", callback_data=f"status_in_progress_{task_id}")],
                [InlineKeyboardButton(text="📝 Отчёт", callback_data=f"action_report_{task_id}")]
            ]
        }
        return InlineKeyboardMarkup(inline_keyboard=buttons.get(current_status, []))

    async def _change_status(self, task_id: str, new_status: str, username: str = None):
        """Обрабатывает изменение статуса задачи"""
        task = await redis_client.get_task(task_id)
        if not task:
            return

        old_status = task["status"]
        
        # Обновляем статус в Redis
        await redis_client.update_task_status(task_id, new_status)
        
        # Если задача взята в работу - сохраняем исполнителя
        if new_status == "in_progress" and username:
            await redis_client.set_assignee(task_id, username)
            await redis_client.incr(f"stats:in_progress:{username}")

        # Уведомляем MoverBot об изменении
        await redis_client.publish_event("task_updated", {
            "task_id": task_id,
            "old_status": old_status,
            "new_status": new_status,
            "assignee": username if new_status == "in_progress" else None
        })

        # Обновляем клавиатуру в сообщении
        await self._update_task_message(task_id)

    async def _handle_action(self, task_id: str, action: str, callback: types.CallbackQuery):
        """Обрабатывает дополнительные действия"""
        if action == "delete":
            await self._delete_task(task_id)
        elif action == "reply":
            await self._request_reply(task_id, callback.from_user.id)
        elif action == "remind":
            await self._set_reminder(task_id)
        elif action == "report":
            await self._generate_report(task_id)

    async def _update_task_message(self, task_id: str):
        """Обновляет сообщение задачи в чате поддержки"""
        task = await redis_client.get_task(task_id)
        if not task or "support_message_id" not in task:
            return

        try:
            await self.bot.edit_message_reply_markup(
                chat_id=settings.SUPPORT_CHAT_ID,
                message_id=int(task["support_message_id"]),
                reply_markup=self._create_keyboard(task_id, task["status"])
            )
        except Exception as e:
            logger.error(f"Can't update task message: {e}")

    async def start(self):
        await self.dp.start_polling(self.bot)
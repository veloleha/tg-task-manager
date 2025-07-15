from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from core.redis_client import redis_client
from config.settings import settings
import logging
import json
import asyncio
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class ReplyState(StatesGroup):
    waiting_for_reply = State()

class TaskBot:
    def __init__(self):
        self.bot = Bot(token=settings.TASK_BOT_TOKEN)
        self.dp = Dispatcher()
        self._setup_handlers()
        self.task_listener_task = None
        self.waiting_replies = {}  # {user_id: task_id}

    def _setup_handlers(self):
        @self.dp.message(Command("start"))
        async def cmd_start(message: types.Message):
            await message.answer("Бот для управления задачами готов к работе!")

        @self.dp.callback_query(lambda c: c.data.startswith(("status_", "action_")))
        async def handle_callback(callback: types.CallbackQuery, state: FSMContext):
            data_parts = callback.data.split("_")
            action_type = data_parts[0]
            task_id = data_parts[-1]

            if action_type == "status":
                new_status = data_parts[1]
                await self._change_status(task_id, new_status, callback.from_user.username)
            elif action_type == "action":
                action = data_parts[1]
                if action == "reply":
                    await callback.answer("Введите ваш ответ:")
                    self.waiting_replies[callback.from_user.id] = task_id
                    await state.set_state(ReplyState.waiting_for_reply)
                else:
                    await self._handle_action(task_id, action, callback)

            await callback.answer()

        @self.dp.message(ReplyState.waiting_for_reply)
        async def handle_reply(message: types.Message, state: FSMContext):
            user_id = message.from_user.id
            if user_id in self.waiting_replies:
                task_id = self.waiting_replies.pop(user_id)
                await self._save_reply(task_id, message.text, message.from_user.username)
                await message.reply("✅ Ваш ответ сохранён в задаче!")
                await state.clear()

    async def _save_reply(self, task_id: str, reply_text: str, username: str):
        """Сохраняет ответ к задаче"""
        await redis_client.update_task(
            task_id,
            reply=reply_text,
            reply_author=username,
            reply_at=datetime.utcnow().isoformat()
        )
        await self._update_task_message(task_id)
        logger.info(f"Reply saved for task {task_id}")

    async def _listen_for_new_tasks(self):
        """Слушает новые задачи из Redis"""
        pubsub = redis_client.conn.pubsub()
        await pubsub.subscribe("task_events")
        
        async for message in pubsub.listen():
            if message["type"] == "message":
                try:
                    data = json.loads(message["data"])
                    if data.get("type") == "new_task":
                        await self._process_new_task(data["task_id"])
                except Exception as e:
                    logger.error(f"Error processing message: {e}")

    async def _process_new_task(self, task_id: str):
        """Обрабатывает новую задачу"""
        task = await redis_client.get_task(task_id)
        if not task:
            logger.error(f"Task {task_id} not found")
            return

        if task.get('support_message_id'):
            logger.warning(f"Task {task_id} already has message")
            return

        # Генерируем уникальный номер задачи
        if not task.get('task_number'):
            task_number = await redis_client.get_next_task_number()
            await redis_client.update_task(task_id, task_number=task_number)
            task['task_number'] = task_number

        # Отправляем сообщение с кнопками
        try:
            message = await self.bot.send_message(
                chat_id=settings.BUTTONS_CHAT_ID,
                text=self._format_task_message(task),
                reply_markup=self._create_keyboard(task_id, task.get("status", "unreacted")),
                parse_mode="HTML"
            )
            
            # Формируем и сохраняем ссылку на задачу
            task_link = f"https://t.me/c/{str(abs(settings.BUTTONS_CHAT_ID))}/{message.message_id}"
            await redis_client.update_task(
                task_id,
                support_message_id=str(message.message_id),
                task_link=task_link
            )
            logger.info(f"Task {task_id} message created with link: {task_link}")

        except Exception as e:
            logger.error(f"Error sending task message: {e}")

    def _format_task_message(self, task: dict) -> str:
        """Форматирует текст задачи для сообщения"""
        status_icons = {
            "unreacted": "⚠️",
            "waiting": "⏳",
            "in_progress": "⚡",
            "completed": "✅"
        }
        
        reply_section = ""
        if task.get("reply"):
            reply_section = (
                f"\n\n💬 <b>Ответ:</b> {task['reply']}\n"
                f"👤 <b>От:</b> @{task.get('reply_author', 'N/A')}\n"
                f"⏱️ <b>Время ответа:</b> {task.get('reply_at', 'N/A')}"
            )

        return (
            f"{status_icons.get(task.get('status'), '📌')} <b>Задача #{task.get('task_number', 'N/A')}</b>\n"
            f"👤 <b>Автор:</b> @{task.get('username', 'N/A')}\n"
            f"📝 <b>Текст:</b> {task.get('text', '')}\n"
            f"🔄 <b>Статус:</b> {task.get('status', 'unreacted')}\n"
            f"⏱️ <b>Создана:</b> {task.get('created_at', 'N/A')}"
            f"{reply_section}"
        )

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
            logger.error(f"Task {task_id} not found")
            return

        updates = {
            'status': new_status,
            'updated_at': datetime.utcnow().isoformat()
        }

        if new_status == "in_progress" and username:
            updates['assignee'] = username

        await redis_client.update_task(task_id, **updates)
        await self._update_task_message(task_id)

        await redis_client.publish_event("task_events", {
            "type": "status_change",
            "task_id": task_id,
            "new_status": new_status,
            "executor": username
        })

    async def _update_task_message(self, task_id: str):
        """Обновляет сообщение задачи в чате"""
        task = await redis_client.get_task(task_id)
        if not task or not task.get('support_message_id'):
            return

        try:
            await self.bot.edit_message_text(
                chat_id=settings.BUTTONS_CHAT_ID,
                message_id=int(task['support_message_id']),
                text=self._format_task_message(task),
                reply_markup=self._create_keyboard(task_id, task['status']),
                parse_mode="HTML"
            )
        except Exception as e:
            logger.error(f"Error updating message: {e}")

    async def _handle_action(self, task_id: str, action: str, callback: types.CallbackQuery):
        """Обрабатывает дополнительные действия"""
        if action == "delete":
            await self._delete_task(task_id)
        elif action == "remind":
            await self._set_reminder(task_id)
        elif action == "report":
            await self._generate_report(task_id)

    async def _delete_task(self, task_id: str):
        """Удаляет задачу"""
        task = await redis_client.get_task(task_id)
        if task and "support_message_id" in task:
            try:
                await self.bot.delete_message(
                    chat_id=settings.BUTTONS_CHAT_ID,
                    message_id=int(task["support_message_id"])
                )
            except Exception as e:
                logger.error(f"Error deleting message: {e}")
        await redis_client.delete_task(task_id)

    async def _set_reminder(self, task_id: str):
        """Устанавливает напоминание для задачи"""
        await redis_client.publish_event("task_events", {
            "type": "reminder_set",
            "task_id": task_id,
            "hours": 24  # Напоминание через 24 часа
        })

    async def _generate_report(self, task_id: str):
        """Генерирует отчет по задаче"""
        task = await redis_client.get_task(task_id)
        if task:
            report_text = (
                f"📊 <b>Отчет по задаче #{task.get('task_number')}</b>\n\n"
                f"📝 <b>Описание:</b> {task.get('text')}\n"
                f"👤 <b>Автор:</b> @{task.get('username')}\n"
                f"🔄 <b>Статус:</b> {task.get('status')}\n"
                f"⏱️ <b>Время создания:</b> {task.get('created_at')}"
            )
            await self.bot.send_message(
                chat_id=settings.BUTTONS_CHAT_ID,
                text=report_text,
                parse_mode="HTML"
            )

    async def start(self):
        """Запускает бота"""
        try:
            logger.info("Starting TaskBot...")
            self.task_listener_task = asyncio.create_task(self._listen_for_new_tasks())
            await self.dp.start_polling(self.bot)
        except Exception as e:
            logger.error(f"Failed to start TaskBot: {e}")
        finally:
            if self.task_listener_task:
                self.task_listener_task.cancel()

task_bot_instance = TaskBot()
router = task_bot_instance.dp
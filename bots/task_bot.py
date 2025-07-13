from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from core.redis_client import redis_client
from config.settings import settings
import logging
import json
import asyncio

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class TaskBot:
    def __init__(self):
        self.bot = Bot(token=settings.TASK_BOT_TOKEN)
        self.dp = Dispatcher()
        self._setup_handlers()
        self.task_listener_task = None

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

    async def _listen_for_new_tasks(self):
        """Слушает новые задачи из Redis"""
        logger.info("Запуск слушателя новых задач...")
        pubsub = redis_client.conn.pubsub()
        
        try:
            await pubsub.subscribe("task_events")
            logger.info("Успешно подписались на канал 'task_events'")
            
            while True:
                try:
                    message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                    if message is None:
                        continue
                        
                    logger.debug(f"Получено сообщение из Redis: {message}")
                    
                    try:
                        data = json.loads(message['data'])
                        task_id = data['task_id']
                        logger.info(f"Обработка новой задачи: {task_id}")
                        await self._process_new_task(task_id)
                    except json.JSONDecodeError as e:
                        logger.error(f"Ошибка декодирования JSON: {e}")
                    except KeyError as e:
                        logger.error(f"Отсутствует ключ в данных: {e}")
                        
                except Exception as e:
                    logger.error(f"Ошибка в слушателе задач: {e}", exc_info=True)
                    await asyncio.sleep(5)  # Пауза при ошибке
                    
        except Exception as e:
            logger.error(f"Критическая ошибка слушателя: {e}", exc_info=True)
        finally:
            await pubsub.close()
    
    async def _process_new_task(self, task_id: str):
        """Обрабатывает новую задачу"""
        logger.info(f"Начало обработки задачи {task_id}")
        
        try:
            task = await redis_client.get_task(task_id)
            if not task:
                logger.error(f"Задача {task_id} не найдена в Redis")
                return

            logger.debug(f"Данные задачи: {task}")
            
            # Проверяем, не было ли уже создано сообщение
            if task.get('support_message_id'):
                logger.warning(f"Для задачи {task_id} уже есть сообщение")
                return

            try:
                logger.info(f"Попытка отправить сообщение в чат {settings.BUTTONS_CHAT_ID}")
                message = await self.bot.send_message(
                    chat_id=settings.BUTTONS_CHAT_ID,
                    text=self._format_task_message(task),
                    reply_markup=self._create_keyboard(task_id, "unreacted"),
                    parse_mode="HTML"
                )
                logger.info(f"Сообщение отправлено, ID: {message.message_id}")
                
                # Обновляем задачу
                update_data = {
                    'support_message_id': str(message.message_id),
                    'task_link': f"t.me/c/{str(settings.BUTTONS_CHAT_ID)[4:]}/{message.message_id}"
                }
                await redis_client.update_task(task_id, **update_data)
                
            except Exception as e:
                logger.error(f"Ошибка при отправке сообщения: {e}", exc_info=True)
                
        except Exception as e:
            logger.error(f"Ошибка обработки задачи {task_id}: {e}", exc_info=True)

    def _format_task_message(self, task: dict) -> str:
        """Форматирует текст задачи для сообщения"""
        return (
            f"📌 <b>Новая задача #{task.get('task_number', 'N/A')}</b>\n"
            f"👤 От: @{task.get('username', 'N/A')}\n"
            f"📝 Текст: {task.get('text', '')}\n"
            f"🔄 Статус: {task.get('status', 'unreacted')}\n"
            f"⏱️ Создана: {task.get('created_at', 'N/A')}"
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
            logger.error(f"Задача {task_id} не найдена")
            return

        old_status = task.get("status")
        
        # Обновляем статус в Redis
        success = await redis_client.update_task_status(task_id, new_status)
        if not success:
            logger.error(f"Не удалось обновить статус задачи {task_id}")
            return
        
        # Если задача взята в работу - сохраняем исполнителя
        if new_status == "in_progress" and username:
            await redis_client.update_task(task_id, assignee=username)
            await redis_client.incr(f"stats:in_progress:{username}")

        # Уведомляем MoverBot об изменении
        await redis_client.publish_event("task_events", {
            "type": "status_change",
            "task_id": task_id,
            "old_status": old_status,
            "new_status": new_status,
            "executor": username if new_status == "in_progress" else None
        })

        # Обновляем клавиатуру в сообщении
        await self._update_task_message(task_id)

    async def _update_task_message(self, task_id: str):
        """Обновляет сообщение задачи в чате поддержки"""
        task = await redis_client.get_task(task_id)
        if not task or "support_message_id" not in task:
            return

        try:
            await self.bot.edit_message_text(
                chat_id=settings.BUTTONS_CHAT_ID,
                message_id=int(task["support_message_id"]),
                text=self._format_task_message(task),
                reply_markup=self._create_keyboard(task_id, task["status"])
            )
        except Exception as e:
            logger.error(f"Can't update task message: {e}")

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
                logger.error(f"Ошибка удаления сообщения задачи: {e}")
        await redis_client.conn.delete(task_id)

    async def start(self):
        # Запускаем слушателя новых задач
        self.task_listener_task = asyncio.create_task(self._listen_for_new_tasks())
        logger.info("Запущен слушатель новых задач")
        await self.dp.start_polling(self.bot)

    async def stop(self):
        if self.task_listener_task:
            self.task_listener_task.cancel()
            try:
                await self.task_listener_task
            except asyncio.CancelledError:
                pass
       
task_bot_instance = TaskBot()
router = task_bot_instance.dp
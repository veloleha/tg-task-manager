import os
import json
import asyncio
import logging
from datetime import datetime
from typing import Dict, Optional, Callable

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

# Локальные импорты
from core.redis_client import redis_client
from .pubsub_manager import TaskBotPubSubManager
from bots.task_bot.formatters import format_task_message
from bots.task_bot.keyboards import create_task_keyboard
from config.settings import settings

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/taskbot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ReplyState(StatesGroup):
    waiting_for_reply = State()

class TaskBot:
    def __init__(self):
        self.bot = Bot(token=settings.TASK_BOT_TOKEN)
        self.dp = Dispatcher()
        self.redis = redis_client
        self.pubsub_manager = TaskBotPubSubManager()
        self.waiting_replies: Dict[int, str] = {}  # {user_id: task_id}
        self._setup_handlers()

    async def start(self):
        """Запускает бота"""
        try:
            logger.info("[TASKBOT][STEP 1] Starting TaskBot...")
            logger.info("[TASKBOT][STEP 1.1] Subscribing to new_tasks channel...")
            await self.pubsub_manager.subscribe("new_tasks", self._pubsub_message_handler)
            logger.info("[TASKBOT][STEP 1.2] Subscribing to task_updates channel...")
            await self.pubsub_manager.subscribe("task_updates", self._pubsub_message_handler)
            logger.info("[TASKBOT][STEP 1.3] TaskBot subscribed to 'new_tasks' and 'task_updates' channels")
            logger.info("[TASKBOT][STEP 1.4] Starting PubSub listener...")
            await self.pubsub_manager.start()  # слушатель запускается после всех подписок
            logger.info("[TASKBOT][STEP 1.5] PubSub listener started, starting polling...")
            await self.dp.start_polling(self.bot)
        except Exception as e:
            logger.error(f"[TASKBOT][ERROR] Failed to start TaskBot: {e}", exc_info=True)
        finally:
            await self.bot.session.close()

    def _setup_handlers(self):
        @self.dp.message(Command("start"))
        async def cmd_start(message: types.Message):
            logger.info(f"Start command from user {message.from_user.id}")
            await message.answer("🚀 Task Management Bot готов к работе!")

        @self.dp.message(Command("menu"))
        async def cmd_menu(message: types.Message):
            """Показывает главное меню с административными функциями"""
            logger.info(f"Menu command from user {message.from_user.id}")
            
            # Создаем инлайн-клавиатуру с кнопками управления
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    text="🔄 Завершить все активные задачи",
                    callback_data="admin_complete_all"
                )],
                [InlineKeyboardButton(
                    text="📊 Показать статистику",
                    callback_data="admin_show_stats"
                )],
                [InlineKeyboardButton(
                    text="🗑️ Очистить завершенные задачи",
                    callback_data="admin_cleanup_completed"
                )],
                [InlineKeyboardButton(
                    text="⚠️ ПОЛНАЯ ОЧИСТКА БД",
                    callback_data="admin_full_reset_confirm"
                )]
            ])
            
            await message.answer(
                "🎛️ <b>Панель управления TaskBot</b>\n\n"
                "Выберите действие:",
                parse_mode="HTML",
                reply_markup=keyboard
            )

        @self.dp.callback_query(lambda c: c.data.startswith("admin_"))
        async def handle_admin_callback(callback: types.CallbackQuery):
            """Обработчик административных команд"""
            try:
                action = callback.data.replace("admin_", "")
                logger.info(f"Admin action from {callback.from_user.username}: {action}")
                
                if action == "complete_all":
                    await self._complete_all_active_tasks(callback)
                elif action == "show_stats":
                    await self._show_detailed_stats(callback)
                elif action == "cleanup_completed":
                    await self._cleanup_completed_tasks(callback)
                elif action == "full_reset_confirm":
                    await self._full_reset_confirmation(callback)
                elif action == "full_reset_execute":
                    await self._full_reset_database(callback)
                elif action == "full_reset_cancel":
                    await self._full_reset_cancel(callback)
                else:
                    await callback.answer("❌ Неизвестная команда")
                    
            except Exception as e:
                logger.error(f"Error handling admin callback: {e}")
                await callback.answer("❌ Ошибка выполнения команды")

        @self.dp.callback_query(lambda c: c.data.startswith(("status_", "action_")))
        async def handle_callback(callback: types.CallbackQuery, state: FSMContext):
            try:
                data_parts = callback.data.split("_")
                action_type = data_parts[0]
                task_id = data_parts[-1]

                logger.info(f"Callback from {callback.from_user.id} for task {task_id}: {callback.data}")

                if action_type == "status":
                    new_status = data_parts[1]
                    await self._change_status(task_id, new_status, callback.from_user.username)
                    # update_task_message уже вызывается в _change_status
                elif action_type == "action":
                    action = data_parts[1]
                    if action == "reply":
                        await callback.answer("Введите ваш ответ:")
                        self.waiting_replies[callback.from_user.id] = task_id
                        await state.set_state(ReplyState.waiting_for_reply)
                        logger.info(f"Waiting reply for task {task_id} from {callback.from_user.username}")
                    else:
                        await self._handle_action(task_id, action, callback)
                        # Обновляем сообщение с новой клавиатурой
                        task = await self.redis.get_task(task_id)
                        if task:
                            await self.update_task_message(task_id, task.get('text', ''))

                await callback.answer()
            except Exception as e:
                logger.error(f"Error handling callback: {e}", exc_info=True)
                await callback.answer("Произошла ошибка при обработке запроса")

        @self.dp.message(ReplyState.waiting_for_reply)
        async def handle_reply(message: types.Message, state: FSMContext):
            user_id = message.from_user.id
            if user_id in self.waiting_replies:
                task_id = self.waiting_replies.pop(user_id)
                logger.info(f"Received reply for task {task_id} from {message.from_user.username}")
                await self._save_reply(task_id, message.text, message.from_user.username)
                await message.reply("✅ Ответ сохранён!")
                await state.clear()



    async def _listen_for_new_tasks(self):
        """
        Запускает PubSub слушатель для новых задач (больше не нужен, запуск через start)
        """
        logger.info("_listen_for_new_tasks больше не используется. Слушатель запускается в start().")
        # Метод оставлен для совместимости, ничего не делает
        return

    async def _pubsub_message_handler(self, channel: str, message: dict):
        """
        Обработчик сообщений от PubSub менеджера
        """
        try:
            logger.info(f"[TASKBOT][STEP 3] Received PubSub message on channel {channel}: {message}")
            if channel == "new_tasks" and message.get("type") == "new_task":
                task_id = message["task_id"]
                logger.info(f"[TASKBOT][STEP 3.1] Processing new task event for task_id: {task_id}")
                await self._process_new_task(task_id)
                logger.info(f"[TASKBOT][STEP 3.2] Finished processing new task event for task_id: {task_id}")
            elif channel == "task_updates":
                # Обрабатываем события изменения статуса для обновления закрепленного сообщения
                event_type = message.get("type")
                if event_type in ["status_change", "task_assigned", "task_completed", "task_deleted"]:
                    task_id = message.get("task_id")
                    logger.info(f"[TASKBOT] Received {event_type} event for task {task_id}, updating pinned stats")
                    await self._update_pinned_stats()
            elif channel == "task_updates" and message.get("type") == "task_update":
                task_id = message["task_id"]
                new_text = message.get("text", "")
                logger.info(f"[TASKBOT][STEP 3.4] Processing task update event for task_id: {task_id}")
                await self._update_task_text(task_id, new_text)
                logger.info(f"[TASKBOT][STEP 3.5] Finished processing task update event for task_id: {task_id}")
            elif channel == "task_updates" and message.get("type") == "status_change":
                # Обработка изменения статуса задачи
                task_id = message["task_id"]
                new_status = message["new_status"]
                assignee = message.get("assignee")
                logger.info(f"[TASKBOT][STEP 3.7] Processing status change for task {task_id} to {new_status}")
                # Обновляем сообщение в чате поддержки
                task = await self.redis.get_task(task_id)
                if task:
                    await self.update_task_message(task_id, task.get('text', ''))
            else:
                logger.info(f"[TASKBOT][STEP 3.6] Ignoring message on channel {channel} with type {message.get('type')}")
        except Exception as e:
            logger.error(f"[TASKBOT][STEP 3.ERROR] Error handling PubSub message from {channel}: {e}", exc_info=True)

    async def _process_new_task(self, task_id: str):
        """
        Обрабатывает новую задачу
        """
        try:
            logger.info(f"[TASKBOT][STEP 4] Начинаем обработку задачи: {task_id}")
            # Небольшая задержка для гарантии сохранения
            await asyncio.sleep(0.1)

            logger.info(f"[TASKBOT][STEP 5] Читаем задачу из Redis: {task_id}")
            task = await self.redis.get_task(task_id)
            if not task:
                logger.error(f"[TASKBOT][STEP 5 ERROR] Задача не найдена: {task_id}")
                return

            logger.info(f"[TASKBOT][STEP 6] Проверяем номер задачи...")
            if not task.get('task_number'):
                task_number = await self.redis.get_next_task_number()
                await self.redis.update_task(task_id, task_number=task_number)
                task['task_number'] = task_number
                logger.info(f"✅ Присвоен номер задачи: {task_number}")

            # Инкрементируем счетчик неотреагированных задач
            logger.info(f"[TASKBOT][STEP 6.5] Инкрементируем счетчик неотреагированных задач")
            from core.redis_client import redis_client
            await redis_client.conn.incr("counter:unreacted")
            logger.info(f"[TASKBOT][STEP 6.5] ✅ Счетчик неотреагированных задач увеличен")

            # Отправляем сообщение в MoverBot для перемещения в тему "неотреагированные"
            logger.info(f"[TASKBOT][STEP 7] Отправляем сообщение в MoverBot для перемещения в тему 'неотреагированные'")
            await redis_client.publish_event("task_updates", {
                "type": "new_task",
                "task_id": task_id
            })
            
            # Обновляем закреплённое сообщение со статистикой
            await self._update_pinned_stats()

        except Exception as e:
            logger.error(f"[TASKBOT][CRITICAL] Ошибка при обработке задачи {task_id}: {e}", exc_info=True)

    async def _change_status(self, task_id: str, new_status: str, username: Optional[str] = None):
        """Обновляет статус задачи"""
        try:
            logger.info(f"Updating task {task_id} status to {new_status}")
            
            updates = {
                'status': new_status,
                'updated_at': datetime.now().isoformat()
            }
            
            if new_status == "in_progress" and username:
                updates['assignee'] = username

            await self.redis.update_task(task_id, **updates)
            await self.update_task_message(task_id, f"Статус изменен на {new_status}")
            
            from core.redis_client import redis_client
            await redis_client.publish_event("task_updates", {
                "type": "status_change",
                "task_id": task_id,
                "new_status": new_status,
                "changed_by": username
            })
            
        except Exception as e:
            logger.error(f"Error changing status for task {task_id}: {e}", exc_info=True)

    async def _update_task_text(self, task_id: str, new_text: str):
        """Обновляет только текст сообщения задачи в чате"""
        try:
            task = await self.redis.get_task(task_id)
            if not task or not task.get('support_message_id'):
                return

            # Обновляем текст в задаче
            task['text'] = new_text
            
            # Формируем клавиатуру в зависимости от статуса
            status = task.get('status', 'unreacted')
            # Используем правильную функцию создания клавиатуры с учетом статуса задачи
            keyboard = create_task_keyboard(task_id, status, task.get('assignee'))
                
            await self.bot.edit_message_text(
                chat_id=settings.SUPPORT_CHAT_ID,
                message_id=int(task['support_message_id']),
                text=format_task_message(task),
                reply_markup=keyboard,
                parse_mode="HTML"
            )
            logger.info(f"Updated text for task {task_id}")
        except Exception as e:
            logger.error(f"Error updating text for task {task_id}: {e}")

    async def _save_reply(self, task_id: str, reply_text: str, username: str):
        """Сохраняет ответ к задаче"""
        try:
            logger.info(f"Saving reply for task {task_id} from @{username}")
            await self.redis.update_task(
                task_id,
                reply=reply_text,
                reply_author=username,
                reply_at=datetime.now().isoformat()
            )
            
            await self.update_task_message(task_id, reply_text)
            
            from core.redis_client import redis_client
            await redis_client.publish_event("task_updates", {
                "type": "new_reply",
                "task_id": task_id,
                "author": username
            })
        except Exception as e:
            logger.error(f"Error saving reply for task {task_id}: {e}", exc_info=True)

    async def _handle_action(self, task_id: str, action: str, callback: types.CallbackQuery):
        """Обрабатывает дополнительные действия"""
        try:
            if action == "delete":
                await self._delete_task(task_id)
            elif action == "remind":
                await self._set_reminder(task_id)
            elif action == "report":
                await self._generate_report(task_id)
        except Exception as e:
            logger.error(f"Error handling action {action} for task {task_id}: {e}", exc_info=True)

    async def _delete_task(self, task_id: str):
        """Удаляет задачу"""
        logger.info(f"Deleting task {task_id}")
        task = await self.redis.get_task(task_id)
        if task and "support_message_id" in task:
            try:
                await self.bot.delete_message(
                    chat_id=settings.SUPPORT_CHAT_ID,
                    message_id=int(task["support_message_id"])
                )
            except Exception as e:
                logger.error(f"Error deleting message: {e}")
        await self.redis.delete_task(task_id)
        from core.redis_client import redis_client
        await redis_client.publish_event("task_updates", {
            "type": "task_deleted",
            "task_id": task_id
        })

    async def _set_reminder(self, task_id: str, hours: int = 24):
        """Устанавливает напоминание"""
        logger.info(f"Setting reminder for task {task_id} in {hours} hours")
        
        # Сохраняем напоминание в Redis
        await self.redis.set_reminder(task_id, hours)
        
        # Публикуем событие для других компонентов
        from core.redis_client import redis_client
        await redis_client.publish_event("reminders", {
            "type": "reminder_request",
            "task_id": task_id,
            "hours": hours
        })
        
        # Запускаем асинхронную задачу напоминания
        asyncio.create_task(self._schedule_reminder(task_id, hours))
    
    async def _schedule_reminder(self, task_id: str, hours: int):
        """Планирует отправку напоминания"""
        try:
            # Ждем указанное количество часов
            await asyncio.sleep(hours * 3600)
            
            # Проверяем, что задача все еще актуальна
            task = await self.redis.get_task(task_id)
            if not task or task.get('status') in ['completed', 'deleted']:
                logger.info(f"Task {task_id} is no longer active, skipping reminder")
                return
            
            # Отправляем напоминание
            from bots.task_bot.formatters import format_reminder_message
            reminder_text = format_reminder_message(task)
            
            await self.bot.send_message(
                chat_id=settings.SUPPORT_CHAT_ID,
                text=reminder_text,
                parse_mode="HTML"
            )
            
            logger.info(f"Reminder sent for task {task_id}")
            
        except Exception as e:
            logger.error(f"Error sending reminder for task {task_id}: {e}")

    async def _generate_report(self, task_id: str):
        """Генерирует отчет по задаче"""
        logger.info(f"Generating report for task {task_id}")
        task = await self.redis.get_task(task_id)
        if task:
            report_text = (
                f"📊 Отчет по задаче #{task.get('task_number')}\n\n"
                f"📝 Текст: {task.get('text')}\n"
                f"👤 Автор: @{task.get('username')}\n"
                f"🔄 Статус: {task.get('status')}\n"
                f"⏱️ Создана: {task.get('created_at')}\n"
                f"💬 Ответов: {1 if task.get('reply') else 0}"
            )
            await self.bot.send_message(
                chat_id=settings.SUPPORT_CHAT_ID,
                text=report_text,
                parse_mode="HTML"
            )

    async def send_task_to_support(self, task_data: dict) -> types.Message:
        """Отправляет задачу в чат поддержки"""
        text = f"🚀 Новая задача #{task_data['id']}\n\n{task_data['text']}"
        # Используем правильную функцию создания клавиатуры с учетом статуса задачи
        status = task_data.get('status', 'unreacted')
        keyboard = create_task_keyboard(task_data['id'], status, task_data.get('assignee'))
        return await self.bot.send_message(
            chat_id=settings.SUPPORT_CHAT_ID,
            text=text,
            reply_markup=keyboard
        )

    async def update_task_message(self, task_id: str, text: str):
        """Обновляет сообщение задачи в чате поддержки"""
        try:
            # Получаем свежие данные задачи
            task = await self.redis.get_task(task_id)
            if not task:
                logger.warning(f"Task {task_id} not found for message update")
                return
                
            # Проверяем наличие support_message_id
            if not task.get('support_message_id'):
                logger.warning(f"Task {task_id} has no support_message_id")
                return
                
            # Форматируем сообщение
            message_text = format_task_message(task)
            
            # Используем правильную функцию создания клавиатуры с учетом статуса задачи
            status = task.get('status', 'unreacted')
            keyboard = create_task_keyboard(task_id, status, task.get('assignee'))
            
            # Проверяем, находится ли сообщение в форумной теме
            # Если есть support_topic_id или статус не 'unreacted', значит сообщение перемещено
            if task.get('support_topic_id') or status != 'unreacted':
                # Сообщение в форумной теме - обновляем его в теме
                logger.info(f"Task {task_id} is in forum topic {task.get('support_topic_id')} or status is {status}, updating message in topic")
                
                # Получаем ID темы
                topic_id = task.get('support_topic_id')
                if not topic_id and status in ['waiting', 'in_progress', 'completed']:
                    # Если тема не указана, но статус изменился, пытаемся получить тему из MoverBot
                    # Это временное решение, в будущем нужно улучшить передачу информации о темах
                    logger.warning(f"Task {task_id} has no support_topic_id but status is {status}")
                
                if topic_id:
                    try:
                        await self.bot.edit_message_text(
                            chat_id=settings.SUPPORT_CHAT_ID,
                            message_id=task['support_message_id'],
                            text=message_text,
                            reply_markup=keyboard,
                            parse_mode="HTML"
                        )
                        logger.info(f"Successfully updated message for task {task_id} in topic {topic_id}")
                        return
                    except Exception as edit_error:
                        error_msg = str(edit_error).lower()
                        if "message to edit not found" in error_msg:
                            logger.warning(f"Message for task {task_id} not found in topic {topic_id}, possibly moved or deleted")
                        elif "message is not modified" in error_msg:
                            logger.info(f"Message for task {task_id} in topic {topic_id} already up to date")
                        else:
                            logger.error(f"Unexpected error editing message for task {task_id} in topic {topic_id}: {edit_error}")
                else:
                    logger.warning(f"Task {task_id} has no topic_id, cannot update message in topic")
                
                # Если не удалось обновить в теме, возвращаемся к основному сообщению
                logger.info(f"Falling back to updating main message for task {task_id}")
        
            # Обновляем сообщение только в основном чате поддержки
            try:
                await self.bot.edit_message_text(
                    chat_id=settings.SUPPORT_CHAT_ID,
                    message_id=task['support_message_id'],
                    text=message_text,
                    reply_markup=keyboard,
                    parse_mode="HTML"
                )
                logger.info(f"Successfully updated message for task {task_id} in support chat")
            except Exception as edit_error:
                error_msg = str(edit_error).lower()
                if "message to edit not found" in error_msg:
                    logger.warning(f"Message for task {task_id} not found in support chat, possibly moved to topic")
                    # Сообщение не найдено - вероятно, перемещено в тему
                elif "message is not modified" in error_msg:
                    logger.info(f"Message for task {task_id} already up to date")
                else:
                    logger.error(f"Unexpected error editing message for task {task_id}: {edit_error}")
            
        except Exception as e:
            logger.error(f"Error updating message for task {task_id}: {e}")
    
    async def _update_pinned_stats(self):
        """Обновляет закреплённое сообщение со статистикой в главном чате (с защитой от Flood Control)"""
        try:
            # Получаем статистику из Redis с новой системой
            stats = await self.redis.get_global_stats()
            
            # Формируем текст статистики с новым форматированием
            stats_text = await redis_client.format_pinned_message(stats)
            
            # Получаем ID закреплённого сообщения
            pinned_msg_id = await redis_client.get_pinned_message_id()
            
            if pinned_msg_id:
                # Обновляем существующее сообщение
                try:
                    await self.bot.edit_message_text(
                        chat_id=settings.MAIN_TASK_CHAT_ID,
                        message_id=pinned_msg_id,
                        text=stats_text,
                        parse_mode="HTML"
                    )
                    logger.info(f"Updated pinned stats message {pinned_msg_id}")
                    return  # Успешно обновили, выходим
                except Exception as edit_error:
                    error_text = str(edit_error)
                    # Проверяем, если сообщение не изменилось - это не ошибка
                    if "message is not modified" in error_text:
                        logger.info(f"Pinned message {pinned_msg_id} content is already up to date")
                        return  # Сообщение уже актуально
                    
                    logger.warning(f"Failed to edit pinned message {pinned_msg_id}: {edit_error}")
                    
                    # Проверяем, если сообщение не найдено или удалено
                    if any(phrase in error_text.lower() for phrase in ["message to edit not found", "message not found", "bad request"]):
                        logger.info(f"Pinned message {pinned_msg_id} not found, creating new one")
                        # Очищаем ID из Redis и создаём новое
                        await redis_client.set_pinned_message_id(None)
                        await self._create_new_pinned_stats(stats_text)
                        return
                    
                    # Для других ошибок - пытаемся удалить старое и создать новое
                    try:
                        await self.bot.delete_message(
                            chat_id=settings.MAIN_TASK_CHAT_ID,
                            message_id=pinned_msg_id
                        )
                        logger.info(f"Deleted old pinned message {pinned_msg_id}")
                    except Exception as delete_error:
                        logger.warning(f"Failed to delete old pinned message {pinned_msg_id}: {delete_error}")
                    
                    # Создаём новое закреплённое сообщение
                    await self._create_new_pinned_stats(stats_text)
            else:
                # Создаём новое закреплённое сообщение
                await self._create_new_pinned_stats(stats_text)
                
        except Exception as e:
            logger.error(f"Error updating pinned stats: {e}")
    
    async def _create_new_pinned_stats(self, stats_text: str):
        """Создаёт новое закреплённое сообщение со статистикой (с защитой от Flood Control)"""
        try:
            # Отправляем новое сообщение
            message = await self.bot.send_message(
                chat_id=settings.MAIN_TASK_CHAT_ID,
                text=stats_text,
                parse_mode="HTML"
            )
            
            # Небольшая задержка перед закреплением
            await asyncio.sleep(0.5)
            
            # Закрепляем сообщение
            await self.bot.pin_chat_message(
                chat_id=settings.MAIN_TASK_CHAT_ID,
                message_id=message.message_id,
                disable_notification=True
            )
            
            # Сохраняем ID в Redis
            await self.redis.set_pinned_message_id(message.message_id)
            
            logger.info(f"Created and pinned new stats message {message.message_id}")
            
        except Exception as e:
            error_text = str(e)
            if "flood control exceeded" in error_text.lower() or "too many requests" in error_text.lower():
                logger.warning(f"Flood control detected when creating pinned stats, skipping update: {e}")
                # Не логируем как error - это нормальное поведение при массовых операциях
            else:
                logger.error(f"Error creating new pinned stats: {e}")
    
    async def _format_pinned_stats(self, stats: dict) -> str:
        """Форматирует текст статистики для закреплённого сообщения (устаревший метод)"""
        try:
            # Используем новую систему форматирования
            return await self.redis.format_pinned_message(stats)
            
        except Exception as e:
            logger.error(f"Error formatting pinned stats: {e}")
            return "📊 Ошибка получения статистики"

    async def _complete_all_active_tasks(self, callback: types.CallbackQuery):
        """Завершает все активные задачи (unreacted и in_progress)"""
        try:
            await callback.answer("🔄 Завершаю все активные задачи...")
            
            # Получаем все задачи со статусами unreacted и in_progress
            unreacted_tasks = await self.redis.get_tasks_by_status("unreacted")
            in_progress_tasks = await self.redis.get_tasks_by_status("in_progress")
            
            total_tasks = len(unreacted_tasks) + len(in_progress_tasks)
            
            if total_tasks == 0:
                await callback.message.edit_text(
                    "✅ <b>Нет активных задач для завершения</b>\n\n"
                    "Все задачи уже завершены или закрыты.",
                    parse_mode="HTML"
                )
                return
            
            # Показываем прогресс
            await callback.message.edit_text(
                f"🔄 <b>Завершаю {total_tasks} активных задач...</b>\n\n"
                f"⏳ Это может занять некоторое время для избежания ограничений Telegram.",
                parse_mode="HTML"
            )
            
            completed_count = 0
            batch_size = 5  # Обрабатываем по 5 задач за раз
            delay_between_batches = 2.0  # 2 секунды между батчами
            
            # Объединяем все задачи в один список
            all_tasks = unreacted_tasks + in_progress_tasks
            
            # Обрабатываем задачи батчами
            for i in range(0, len(all_tasks), batch_size):
                batch = all_tasks[i:i + batch_size]
                
                # Обрабатываем текущий батч
                for task in batch:
                    try:
                        task_id = task.get('task_id')
                        if task_id:
                            await self._change_status(task_id, "completed", "admin")
                            completed_count += 1
                            logger.info(f"Completed task {task_id} ({completed_count}/{total_tasks})")
                    except Exception as e:
                        logger.error(f"Error completing task {task.get('task_id')}: {e}")
                
                # Обновляем прогресс
                progress_percent = int((completed_count / total_tasks) * 100)
                await callback.message.edit_text(
                    f"🔄 <b>Завершаю задачи... {progress_percent}%</b>\n\n"
                    f"✅ Завершено: {completed_count} из {total_tasks}\n"
                    f"⏳ Осталось: {total_tasks - completed_count}",
                    parse_mode="HTML"
                )
                
                # Задержка между батчами (кроме последнего)
                if i + batch_size < len(all_tasks):
                    await asyncio.sleep(delay_between_batches)
            
            # Финальное сообщение
            await callback.message.edit_text(
                f"✅ <b>Массовое завершение задач выполнено</b>\n\n"
                f"📊 Завершено задач: {completed_count} из {total_tasks}\n"
                f"🔄 Неотреагированных: {len(unreacted_tasks)}\n"
                f"⚡ В работе: {len(in_progress_tasks)}\n\n"
                f"⏱️ Обработка завершена с защитой от Telegram Flood Control.",
                parse_mode="HTML"
            )
            
            logger.info(f"Admin {callback.from_user.username} completed {completed_count} active tasks with rate limiting")
            
        except Exception as e:
            logger.error(f"Error in _complete_all_active_tasks: {e}")
            await callback.message.edit_text(
                "❌ <b>Ошибка при завершении задач</b>\n\n"
                f"Произошла ошибка: {str(e)}",
                parse_mode="HTML"
            )
    
    async def _show_detailed_stats(self, callback: types.CallbackQuery):
        """Показывает детальную статистику по задачам"""
        try:
            await callback.answer("📊 Получаю статистику...")
            
            # Получаем статистику по всем статусам
            stats = await self.redis.get_global_stats()
            
            # Получаем задачи по статусам для детального анализа
            unreacted = await self.redis.get_tasks_by_status("unreacted")
            in_progress = await self.redis.get_tasks_by_status("in_progress")
            completed = await self.redis.get_tasks_by_status("completed")
            
            stats_text = (
                f"📊 <b>Детальная статистика TaskBot</b>\n\n"
                f"🔄 Неотреагированные: {len(unreacted)}\n"
                f"⚡ В работе: {len(in_progress)}\n"
                f"✅ Завершенные: {len(completed)}\n"
                f"📈 Всего задач: {len(unreacted) + len(in_progress) + len(completed)}\n\n"
                f"🕐 Время обновления: {datetime.now().strftime('%H:%M:%S')}"
            )
            
            await callback.message.edit_text(
                stats_text,
                parse_mode="HTML"
            )
            
        except Exception as e:
            logger.error(f"Error in _show_detailed_stats: {e}")
            await callback.message.edit_text(
                "❌ <b>Ошибка получения статистики</b>\n\n"
                f"Произошла ошибка: {str(e)}",
                parse_mode="HTML"
            )
    
    async def _cleanup_completed_tasks(self, callback: types.CallbackQuery):
        """Очищает завершенные задачи из Redis"""
        try:
            await callback.answer("🗑️ Очищаю завершенные задачи...")
            
            # Получаем все завершенные задачи
            completed_tasks = await self.redis.get_tasks_by_status("completed")
            
            if not completed_tasks:
                await callback.message.edit_text(
                    "✅ <b>Нет завершенных задач для очистки</b>\n\n"
                    "База данных уже чистая.",
                    parse_mode="HTML"
                )
                return
            
            deleted_count = 0
            
            # Удаляем завершенные задачи
            for task in completed_tasks:
                try:
                    task_id = task.get('task_id')
                    if task_id:
                        await self.redis.delete_task(task_id)
                        deleted_count += 1
                except Exception as e:
                    logger.error(f"Error deleting completed task {task.get('task_id')}: {e}")
            
            await callback.message.edit_text(
                f"✅ <b>Очистка завершенных задач выполнена</b>\n\n"
                f"🗑️ Удалено задач: {deleted_count} из {len(completed_tasks)}",
                parse_mode="HTML"
            )
            
            logger.info(f"Admin {callback.from_user.username} deleted {deleted_count} completed tasks")
            
        except Exception as e:
            logger.error(f"Error in _cleanup_completed_tasks: {e}")
            await callback.message.edit_text(
                "❌ <b>Ошибка при очистке задач</b>\n\n"
                f"Произошла ошибка: {str(e)}",
                parse_mode="HTML"
            )

    async def _full_reset_confirmation(self, callback: types.CallbackQuery):
        """Показывает предупреждение и запрашивает подтверждение полной очистки БД"""
        try:
            await callback.answer("⚠️ ВНИМАНИЕ! Это удалит ВСЕ данные!")
            
            # Создаем клавиатуру подтверждения
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    text="🔥 ДА, УДАЛИТЬ ВСЁ",
                    callback_data="admin_full_reset_execute"
                )],
                [InlineKeyboardButton(
                    text="❌ Отмена",
                    callback_data="admin_full_reset_cancel"
                )]
            ])
            
            await callback.message.edit_text(
                "⚠️ <b>ПОЛНАЯ ОЧИСТКА БАЗЫ ДАННЫХ REDIS</b>\n\n"
                "🚨 <b>ВНИМАНИЕ! ЭТО ДЕЙСТВИЕ НЕОБРАТИМО!</b>\n\n"
                "Будут удалены:\n"
                "• 🗂️ Все задачи (активные, завершённые, в работе)\n"
                "• 📊 Вся статистика\n"
                "• 👥 Данные исполнителей\n"
                "• 🔢 Счётчики и номера задач\n"
                "• 📌 ID закреплённых сообщений\n"
                "• 🎯 Темы форума\n\n"
                "<b>ВЫ УВЕРЕНЫ, ЧТО ХОТИТЕ ПРОДОЛЖИТЬ?</b>",
                parse_mode="HTML",
                reply_markup=keyboard
            )
            
            logger.warning(f"Admin {callback.from_user.username} requested full database reset confirmation")
            
        except Exception as e:
            logger.error(f"Error in _full_reset_confirmation: {e}")
            await callback.message.edit_text(
                "❌ <b>Ошибка при отображении подтверждения</b>\n\n"
                f"Произошла ошибка: {str(e)}",
                parse_mode="HTML"
            )

    async def _full_reset_database(self, callback: types.CallbackQuery):
        """Выполняет полную очистку Redis БД с подробным логированием"""
        try:
            await callback.answer("🔥 Выполняю полную очистку БД...")
            
            # Показываем прогресс
            await callback.message.edit_text(
                "🔥 <b>ВЫПОЛНЯЕТСЯ ПОЛНАЯ ОЧИСТКА БАЗЫ ДАННЫХ</b>\n\n"
                "⏳ Пожалуйста, подождите...\n"
                "🚫 НЕ ПРЕРЫВАЙТЕ ПРОЦЕСС!",
                parse_mode="HTML"
            )
            
            logger.critical(f"FULL DATABASE RESET initiated by admin {callback.from_user.username}")
            
            # Получаем статистику перед удалением (с обработкой ошибок)
            total_tasks = 0
            try:
                stats_before = await self.redis.get_global_stats()
                total_tasks = stats_before.get('unreacted', 0) + stats_before.get('in_progress', 0) + stats_before.get('completed', 0)
                logger.info(f"Retrieved stats before clearing: {stats_before}")
            except Exception as stats_error:
                logger.error(f"Error getting stats before clearing: {stats_error}")
                # Продолжаем очистку даже если не удалось получить статистику
            
            # Выполняем полную очистку Redis
            await self.redis.conn.flushdb()
            
            # Небольшая задержка для завершения операции
            await asyncio.sleep(1.0)
            
            # Финальное сообщение
            await callback.message.edit_text(
                "✅ <b>ПОЛНАЯ ОЧИСТКА БАЗЫ ДАННЫХ ЗАВЕРШЕНА</b>\n\n"
                f"🗑️ Удалено задач: {total_tasks}\n"
                f"📊 Очищена вся статистика\n"
                f"👤 Администратор: {callback.from_user.username}\n"
                f"🕐 Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                "🔄 <b>База данных полностью очищена и готова к работе</b>\n\n"
                "⚠️ Все боты продолжают работать, но статистика сброшена.",
                parse_mode="HTML"
            )
            
            logger.critical(f"FULL DATABASE RESET completed by admin {callback.from_user.username}. Total tasks deleted: {total_tasks}")
            
        except Exception as e:
            logger.error(f"Error in _full_reset_database: {e}")
            await callback.message.edit_text(
                "❌ <b>ОШИБКА ПРИ ПОЛНОЙ ОЧИСТКЕ БД</b>\n\n"
                f"Произошла ошибка: {str(e)}\n\n"
                "⚠️ База данных может быть в неконсистентном состоянии.\n"
                "Рекомендуется перезапустить систему.",
                parse_mode="HTML"
            )

    async def _full_reset_cancel(self, callback: types.CallbackQuery):
        """Отменяет полную очистку БД и возвращает в главное меню"""
        try:
            await callback.answer("✅ Полная очистка БД отменена")
            
            # Возвращаемся к главному меню
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    text="🔄 Завершить все активные задачи",
                    callback_data="admin_complete_all"
                )],
                [InlineKeyboardButton(
                    text="📊 Показать статистику",
                    callback_data="admin_show_stats"
                )],
                [InlineKeyboardButton(
                    text="🗑️ Очистить завершенные задачи",
                    callback_data="admin_cleanup_completed"
                )],
                [InlineKeyboardButton(
                    text="⚠️ ПОЛНАЯ ОЧИСТКА БД",
                    callback_data="admin_full_reset_confirm"
                )]
            ])
            
            await callback.message.edit_text(
                "🎛️ <b>Панель управления TaskBot</b>\n\n"
                "✅ Полная очистка БД отменена.\n\n"
                "Выберите действие:",
                parse_mode="HTML",
                reply_markup=keyboard
            )
            
            logger.info(f"Admin {callback.from_user.username} cancelled full database reset")
            
        except Exception as e:
            logger.error(f"Error in _full_reset_cancel: {e}")
            await callback.message.edit_text(
                "❌ <b>Ошибка при отмене операции</b>\n\n"
                f"Произошла ошибка: {str(e)}",
                parse_mode="HTML"
            )

    async def start_polling(self):
        """Запуск бота в режиме long polling с инициализацией PubSub"""
        try:
            logger.info("[TASKBOT][STEP 1] Starting TaskBot in polling mode...")
            logger.info("[TASKBOT][STEP 1.1] Subscribing to new_tasks channel...")
            await self.pubsub_manager.subscribe("new_tasks", self._pubsub_message_handler)
            logger.info("[TASKBOT][STEP 1.2] Subscribing to task_updates channel...")
            await self.pubsub_manager.subscribe("task_updates", self._pubsub_message_handler)
            logger.info("[TASKBOT][STEP 1.3] TaskBot subscribed to 'new_tasks' and 'task_updates' channels")
            logger.info("[TASKBOT][STEP 1.4] Starting PubSub listener...")
            await self.pubsub_manager.start()  # слушатель запускается после всех подписок
            logger.info("[TASKBOT][STEP 1.5] PubSub listener started, starting polling...")
            await self.dp.start_polling(self.bot)
        except Exception as e:
            logger.error(f"[TASKBOT][ERROR] Failed to start TaskBot: {e}", exc_info=True)
        finally:
            await self.bot.session.close()

# Создание экземпляра бота
task_bot = TaskBot()

# Для интеграции с другими модулями
async def get_task_bot() -> TaskBot:
    return task_bot


if __name__ == "__main__":
    asyncio.run(task_bot.start())

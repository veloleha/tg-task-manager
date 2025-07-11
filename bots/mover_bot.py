from aiogram import Bot
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from core.redis_client import redis_client
from config import settings
import logging
import asyncio
import json
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MoverBot:
    def __init__(self):
        self.bot = Bot(token=settings.MOVER_BOT_TOKEN)
        self.pinned_msg_id = None
        self.executor_topics = {}  # {"username": topic_id}
        self.reminder_tasks = {}  # {task_id: asyncio.Task}

    async def start(self):
        """Запускает все фоновые процессы"""
        try:
            # Инициализация
            await self._load_existing_topics()
            
            # Запуск фоновых задач
            asyncio.create_task(self._stats_updater_loop())
            asyncio.create_task(self._task_event_listener())
            
            logger.info("MoverBot успешно запущен")
        except Exception as e:
            logger.error(f"Ошибка запуска MoverBot: {e}")

    async def _load_existing_topics(self):
        """Загружает существующие темы при старте"""
        try:
            topics = await self.bot.get_forum_topics(settings.FORUM_CHAT_ID)
            for topic in topics.topics:
                if topic.name.startswith("🛠️ @"):
                    username = topic.name.split("@")[-1]
                    self.executor_topics[username] = topic.message_thread_id
            logger.info(f"Загружено тем исполнителей: {len(self.executor_topics)}")
        except Exception as e:
            logger.error(f"Ошибка загрузки тем: {e}")

    async def _stats_updater_loop(self):
        """Цикл обновления статистики"""
        while True:
            try:
                await self._update_pinned_stats()
                await asyncio.sleep(30)  # Обновление каждые 30 секунд
            except Exception as e:
                logger.error(f"Ошибка обновления статистики: {e}")
                await asyncio.sleep(10)

    async def _task_event_listener(self):
        """Слушает события задач из Redis"""
        pubsub = redis_client.conn.pubsub()
        await pubsub.subscribe("task_events")
        
        async for message in pubsub.listen():
            if message["type"] == "message":
                try:
                    event = json.loads(message["data"])
                    await self._process_task_event(event)
                except Exception as e:
                    logger.error(f"Ошибка обработки события: {e}")

    async def _process_task_event(self, event: dict):
        """Обрабатывает событие задачи"""
        event_type = event.get("type")
        
        if event_type == "status_change":
            await self._handle_status_change(event)
        elif event_type == "new_task":
            await self._handle_new_task(event)
        elif event_type == "reminder_set":
            await self._setup_reminder(event)

    async def _handle_status_change(self, event: dict):
        """Обрабатывает изменение статуса задачи"""
        task_id = event["task_id"]
        new_status = event["new_status"]
        executor = event.get("executor")
        
        # Обновляем задачу в соответствующей теме
        if new_status == "in_progress" and executor:
            await self._move_task_to_topic(task_id, executor, "in_progress")
        elif new_status == "completed":
            await self._move_task_to_topic(task_id, "completed", "completed")
        elif new_status == "waiting":
            await self._move_task_to_topic(task_id, "waiting", "waiting")
        
        # Обновляем статистику
        await self._update_pinned_stats()

    async def _move_task_to_topic(self, task_id: str, topic_identifier: str, status: str):
        """Перемещает задачу в указанную тему"""
        try:
            task = await redis_client.get_task(task_id)
            if not task:
                return

            # Определяем ID темы
            if status == "in_progress":
                topic_id = await self._get_or_create_executor_topic(topic_identifier)
                topic_name = f"executor_{topic_identifier}"
            else:
                topic_id = settings.WAITING_TOPIC_ID if status == "waiting" else settings.COMPLETED_TOPIC_ID
                topic_name = status

            # Удаляем старое сообщение (если есть)
            if "support_message_id" in task:
                await self._delete_message(task["support_message_id"])

            # Отправляем в новую тему
            message = await self.bot.send_message(
                chat_id=settings.FORUM_CHAT_ID,
                message_thread_id=topic_id,
                text=self._format_task_message(task),
                reply_markup=self._create_task_keyboard(task_id, status)
            )

            # Обновляем данные задачи
            await redis_client.conn.hset(
                task_id,
                mapping={
                    "current_topic": topic_name,
                    "support_message_id": message.message_id,
                    "task_link": f"t.me/c/{str(settings.FORUM_CHAT_ID)[4:]}/{message.message_id}"
                }
            )
            
            logger.info(f"Задача {task_id} перемещена в тему {topic_name}")
        except Exception as e:
            logger.error(f"Ошибка перемещения задачи: {e}")

    async def _get_or_create_executor_topic(self, executor: str) -> int:
        """Возвращает ID темы исполнителя, создаёт при необходимости"""
        if executor not in self.executor_topics:
            try:
                topic = await self.bot.create_forum_topic(
                    chat_id=settings.FORUM_CHAT_ID,
                    name=f"🛠️ @{executor}"  # Иконка инструментов для тем исполнителей
                )
                self.executor_topics[executor] = topic.message_thread_id
                logger.info(f"Создана новая тема для исполнителя @{executor}")
            except Exception as e:
                logger.error(f"Ошибка создания темы: {e}")
                return settings.WAITING_TOPIC_ID
        return self.executor_topics[executor]

    async def _update_pinned_stats(self):
        """Обновляет закреплённое сообщение со статистикой"""
        try:
            stats = await redis_client.get_global_stats()
            text = self._format_stats_message(stats)
            
            if self.pinned_msg_id:
                try:
                    await self.bot.edit_message_text(
                        chat_id=settings.MAIN_TASK_CHAT_ID,
                        message_id=self.pinned_msg_id,
                        text=text,
                        parse_mode="HTML"
                    )
                    return
                except Exception as e:
                    logger.warning(f"Не удалось обновить сообщение: {e}")
                    self.pinned_msg_id = None  # Сброс для создания нового

            # Создаём новое сообщение
            msg = await self.bot.send_message(
                chat_id=settings.MAIN_TASK_CHAT_ID,
                text=text,
                parse_mode="HTML"
            )
            await self.bot.pin_chat_message(
                chat_id=settings.MAIN_TASK_CHAT_ID,
                message_id=msg.message_id,
                disable_notification=True
            )
            self.pinned_msg_id = msg.message_id
        except Exception as e:
            logger.error(f"Ошибка обновления статистики: {e}")

    def _format_stats_message(self, stats: dict) -> str:
        """Форматирует сообщение со статистикой в требуемом виде"""
        # Верхняя строка с основными статусами
        status_line = (
            f"📊 <b>Статистика задач</b>\n"
            f"⚠️ Не обработано: {stats['unreacted']}  "
            f"⏳ В ожидании: {stats['waiting']}  "
            f"⚡ В работе: {stats['in_progress']}  "
            f"✅ Завершено: {stats['completed']}\n"
        )
        
        # Строки с исполнителями
        executors_lines = []
        for executor, data in stats['executors'].items():
            if data['in_progress'] > 0:
                executors_lines.append(f"👤 @{executor}: {data['in_progress']}")
        
        # Собираем всё сообщение
        message = status_line
        if executors_lines:
            message += "\n" + "\n".join([f"<pre>           {line}</pre>" for line in executors_lines])
            message += f"\n<pre>           Всего в работе: {stats['in_progress']}</pre>"
        
        return message

    async def _delete_message(self, message_id: int):
        """Удаляет сообщение в чате поддержки"""
        try:
            await self.bot.delete_message(
                chat_id=settings.FORUM_CHAT_ID,
                message_id=message_id
            )
        except Exception as e:
            logger.warning(f"Не удалось удалить сообщение: {e}")

    def _format_task_message(self, task: dict) -> str:
        """Форматирует текст задачи для отправки в тему"""
        status_icons = {
            "unreacted": "⚠️",
            "waiting": "⏳",
            "in_progress": "⚡",
            "completed": "✅"
        }
        return (
            f"{status_icons.get(task['status'], '📌')} <b>Задача #{task['task_number']}</b>\n"
            f"👤 От: @{task.get('username', 'N/A')}\n"
            f"📝 Текст: {task['text']}\n"
            f"🔄 Статус: {task['status']}"
        )

    def _create_task_keyboard(self, task_id: str, status: str) -> InlineKeyboardMarkup:
        """Создаёт простую клавиатуру для сообщения задачи"""
        return InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="ℹ️ Подробности", callback_data=f"info_{task_id}")
        ]])

    async def _setup_reminder(self, event: dict):
        """Настраивает напоминание для задачи"""
        task_id = event["task_id"]
        if task_id in self.reminder_tasks:
            self.reminder_tasks[task_id].cancel()
        
        self.reminder_tasks[task_id] = asyncio.create_task(
            self._send_reminder(task_id, event["hours"])
        )

    async def _send_reminder(self, task_id: str, hours: int):
        """Отправляет напоминание через указанное время"""
        await asyncio.sleep(hours * 3600)
        try:
            task = await redis_client.get_task(task_id)
            if task and task["status"] == "in_progress":
                await self.bot.send_message(
                    chat_id=settings.FORUM_CHAT_ID,
                    message_thread_id=self.executor_topics.get(task["assignee"], settings.WAITING_TOPIC_ID),
                    text=f"⏰ Напоминание о задаче #{task_id}\n{task['text']}"
                )
        except Exception as e:
            logger.error(f"Ошибка отправки напоминания: {e}")
        finally:
            self.reminder_tasks.pop(task_id, None)
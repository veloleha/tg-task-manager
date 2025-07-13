from aiogram import Bot
from config.settings import settings
import logging
import asyncio
import json
import random
from datetime import datetime
from typing import Dict, Optional

from core.redis_client import redis_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MoverBot:
    def __init__(self):
        self.bot = Bot(token=settings.MOVER_BOT_TOKEN)
        self.pinned_msg_id = None
        self.active_topics = {
            "unreacted": None,  # ID темы для неотреагированных задач
            "waiting": None,    # ID темы для задач в ожидании
            "completed": None,  # ID темы для выполненных задач
            "executors": {}     # {"username": topic_id}
        }
        self.reminder_tasks = {}

    async def start(self):
        """Запускает все фоновые процессы"""
        try:
            # Проверяем, что чат доступен и является форумом
            chat = await self.bot.get_chat(settings.FORUM_CHAT_ID)
            if not chat.is_forum:
                logger.error("Указанный чат не является форумом! Темы не будут работать")
            
            # Запуск фоновых задач
            asyncio.create_task(self._stats_updater_loop())
            asyncio.create_task(self._task_event_listener())
            
            logger.info("MoverBot успешно запущен")
        except Exception as e:
            logger.error(f"Ошибка запуска MoverBot: {e}")

    async def _ensure_topic_exists(self, topic_type: str, topic_name: str = None) -> Optional[int]:
        """Создает тему при необходимости и возвращает её ID"""
        try:
            # Для исполнителей
            if topic_type == "executor" and topic_name:
                if topic_name in self.active_topics["executors"]:
                    return self.active_topics["executors"][topic_name]
                
                topic = await self.bot.create_forum_topic(
                    chat_id=settings.FORUM_CHAT_ID,
                    name=f"🛠️ @{topic_name}"
                )
                self.active_topics["executors"][topic_name] = topic.message_thread_id
                logger.info(f"Создана тема для исполнителя @{topic_name}")
                return topic.message_thread_id
            
            # Для системных тем
            elif topic_type in ["unreacted", "waiting", "completed"]:
                if self.active_topics[topic_type]:
                    return self.active_topics[topic_type]
                
                name_map = {
                    "unreacted": "⚠️ Неотреагированные",
                    "waiting": "⏳ В ожидании",
                    "completed": "✅ Выполненные"
                }
                
                topic = await self.bot.create_forum_topic(
                    chat_id=settings.FORUM_CHAT_ID,
                    name=name_map[topic_type]
                )
                self.active_topics[topic_type] = topic.message_thread_id
                logger.info(f"Создана системная тема: {name_map[topic_type]}")
                return topic.message_thread_id
            
            raise ValueError(f"Неизвестный тип темы: {topic_type}")
            
        except Exception as e:
            logger.error(f"Ошибка создания темы {topic_type}: {e}")
            return None

    async def _cleanup_empty_topics(self):
        """Удаляет пустые темы"""
        try:
            # Проверяем системные темы
            for topic_type in ["unreacted", "waiting", "completed"]:
                if self.active_topics[topic_type]:
                    tasks = await redis_client.get_tasks_by_status(topic_type)
                    if not tasks:
                        await self._delete_topic(self.active_topics[topic_type])
                        self.active_topics[topic_type] = None
                        logger.info(f"Удалена пустая тема {topic_type}")
            
            # Проверяем темы исполнителей
            for executor, topic_id in list(self.active_topics["executors"].items()):
                tasks = await redis_client.get_tasks_by_assignee(executor)
                if not tasks:
                    await self._delete_topic(topic_id)
                    del self.active_topics["executors"][executor]
                    logger.info(f"Удалена пустая тема исполнителя @{executor}")
                    
        except Exception as e:
            logger.error(f"Ошибка очистки тем: {e}")

    async def _delete_topic(self, topic_id: int):
        """Удаляет тему форума"""
        try:
            await self.bot.delete_forum_topic(
                chat_id=settings.FORUM_CHAT_ID,
                message_thread_id=topic_id
            )
        except Exception as e:
            logger.warning(f"Не удалось удалить тему {topic_id}: {e}")

    async def _move_task_to_topic(self, task_id: str, status: str, executor: str = None):
        """Основная логика перемещения задач"""
        try:
            task = await redis_client.get_task(task_id)
            if not task:
                logger.warning(f"Задача {task_id} не найдена")
                return

            # Определяем тип темы
            if status == "in_progress" and executor:
                topic_id = await self._ensure_topic_exists("executor", executor)
                topic_type = f"executor_{executor}"
            else:
                topic_id = await self._ensure_topic_exists(status)
                topic_type = status

            if not topic_id:
                raise ValueError("Не удалось получить ID темы")

            # Удаляем старое сообщение (если есть)
            if "support_message_id" in task:
                await self._delete_message(task["support_message_id"])

            # Отправляем в новую тему (без клавиатуры)
            message = await self.bot.send_message(
                chat_id=settings.FORUM_CHAT_ID,
                message_thread_id=topic_id,
                text=self._format_task_message(task)
            )

            # Обновляем данные задачи
            await redis_client.update_task(
                task_id,
                current_topic=topic_type,
                support_message_id=message.message_id,
                task_link=f"t.me/c/{str(settings.FORUM_CHAT_ID)[4:]}/{message.message_id}"
            )

            logger.info(f"Задача {task_id} перемещена в тему {topic_type}")
            
            # Периодическая очистка пустых тем (10% chance)
            if random.random() < 0.1:
                await self._cleanup_empty_topics()
                
        except Exception as e:
            logger.error(f"Ошибка перемещения задачи: {e}", exc_info=True)

    async def _delete_message(self, message_id: int):
        """Удаляет сообщение в чате поддержки"""
        try:
            await self.bot.delete_message(
                chat_id=settings.FORUM_CHAT_ID,
                message_id=message_id
            )
        except Exception as e:
            logger.warning(f"Не удалось удалить сообщение: {e}")

    async def _stats_updater_loop(self):
        """Цикл обновления статистики"""
        while True:
            try:
                await self._update_pinned_stats()
                await asyncio.sleep(settings.STATS_UPDATE_INTERVAL)
            except Exception as e:
                logger.error(f"Ошибка обновления статистики: {e}")
                await asyncio.sleep(10)

    async def _update_pinned_stats(self):
        """Обновляет закреплённое сообщение со статистикой"""
        try:
            if not settings.MAIN_TASK_CHAT_ID:
                return

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
                except Exception:
                    self.pinned_msg_id = None

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

    def _format_stats_message(self, stats: Dict) -> str:
        """Форматирует сообщение со статистикой"""
        return (
            "📊 <b>Статистика задач</b>\n\n"
            "🟡 <b>Не обработано:</b> {unreacted}\n"
            "🟠 <b>В ожидании:</b> {waiting}\n"
            "🔴 <b>В работе:</b> {in_progress}\n"
            "🟢 <b>Завершено:</b> {completed}\n\n"
            "👥 <b>Активные исполнители:</b>\n{executors}"
        ).format(
            unreacted=stats['unreacted'],
            waiting=stats['waiting'],
            in_progress=stats['in_progress'],
            completed=stats['completed'],
            executors="\n".join(
                f"▫️ @{executor}: {data['in_progress']}"
                for executor, data in stats['executors'].items()
                if data['in_progress'] > 0
            ) if stats['executors'] else "Нет активных исполнителей"
        )

    def _format_task_message(self, task: Dict) -> str:
        """Форматирует текст задачи (без клавиатуры)"""
        status_icons = {
            "unreacted": "⚠️",
            "waiting": "⏳",
            "in_progress": "⚡",
            "completed": "✅"
        }
        return (
            f"{status_icons.get(task['status'], '📌')} <b>Задача #{task.get('task_number', 'N/A')}</b>\n"
            f"👤 От: @{task.get('username', 'N/A')}\n"
            f"📝 Текст: {task.get('text', '')}\n"
            f"🔄 Статус: {task.get('status', 'N/A')}"
        )

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

    async def _process_task_event(self, event: Dict):
        """Обрабатывает событие задачи"""
        event_type = event.get("type")
        
        if event_type == "status_change":
            await self._handle_status_change(event)
        elif event_type == "new_task":
            await self._handle_new_task(event)
        elif event_type == "reminder_set":
            await self._setup_reminder(event)

    async def _handle_status_change(self, event: Dict):
        """Обрабатывает изменение статуса задачи"""
        task_id = event["task_id"]
        new_status = event["new_status"]
        executor = event.get("executor")

        try:
            await self._move_task_to_topic(task_id, new_status, executor)
            await self._update_pinned_stats()
        except Exception as e:
            logger.error(f"Ошибка обработки изменения статуса: {e}")

    async def _handle_new_task(self, event: Dict):
        """Обрабатывает новую задачу"""
        task_id = event["task_id"]
        await self._move_task_to_topic(task_id, "unreacted")

    async def _setup_reminder(self, event: Dict):
        """Настраивает напоминание для задачи"""
        task_id = event["task_id"]
        hours = event.get("hours", 1)  # Значение по умолчанию 1 час
        
        if task_id in self.reminder_tasks:
            self.reminder_tasks[task_id].cancel()
        
        self.reminder_tasks[task_id] = asyncio.create_task(
            self._send_reminder(task_id, hours)
        )

    async def _send_reminder(self, task_id: str, hours: int):
        """Отправляет напоминание через указанное время"""
        await asyncio.sleep(hours * 3600)
        
        try:
            task = await redis_client.get_task(task_id)
            if task and task["status"] == "in_progress":
                assignee = task.get("assignee")
                if assignee and assignee in self.active_topics["executors"]:
                    await self.bot.send_message(
                        chat_id=settings.FORUM_CHAT_ID,
                        message_thread_id=self.active_topics["executors"][assignee],
                        text=f"⏰ Напоминание о задаче #{task_id}\n{task.get('text', '')}"
                    )
        except Exception as e:
            logger.error(f"Ошибка отправки напоминания: {e}")
        finally:
            self.reminder_tasks.pop(task_id, None)
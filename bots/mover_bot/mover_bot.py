from aiogram import Bot
from config.settings import settings
import logging
import asyncio
import json
import random
from datetime import datetime
from typing import Dict, Optional, Any

from core.redis_client import redis_client
from core.pubsub_manager import MoverBotPubSubManager
from bots.task_bot.redis_manager import RedisManager
from .topic_keyboards import create_unreacted_topic_keyboard, create_executor_topic_keyboard, create_completed_topic_keyboard

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


from aiogram import Dispatcher

class MoverBot:
    async def start_polling(self):
        """Запускает polling для aiogram 3.x Dispatcher с инициализацией PubSub."""
        try:
            # Проверяем, что чат доступен и является форумом
            chat = await self.bot.get_chat(settings.FORUM_CHAT_ID)
            if not chat.is_forum:
                logger.error("Указанный чат не является форумом! Темы не будут работать")
            
            # Подписываемся на каналы
            await self.pubsub_manager.subscribe("task_updates", self._pubsub_message_handler)
            
            # Запускаем слушателя PubSub
            await self.pubsub_manager.start()
            
            # Запуск фоновых задач
            asyncio.create_task(self._stats_updater_loop())
            asyncio.create_task(self._task_event_listener())
            
            logger.info("MoverBot успешно запущен")
            
            # Запускаем polling
            await self.dp.start_polling(self.bot)
        except Exception as e:
            logger.error(f"Ошибка запуска MoverBot: {e}")



    def __init__(self):
        self.bot = Bot(token=settings.MOVER_BOT_TOKEN)
        self.dp = Dispatcher()
        self.redis = RedisManager()
        self.pinned_msg_id = None
        self.active_topics = {
            "unreacted": None,  # ID темы для неотреагированных задач
            "waiting": None,    # ID темы для задач в ожидании
            "completed": None,  # ID темы для выполненных задач
            "executors": {}     # {"username": topic_id}
        }
        self.reminder_tasks = {}
        self.pubsub_manager = MoverBotPubSubManager(bot_instance=self)
        
        # Регистрируем обработчики callback-ов для новых кнопок
        self._register_callback_handlers()
    
    def _register_callback_handlers(self):
        """Регистрирует обработчики callback-ов для новых кнопок"""
        from aiogram import F
        
        # Обработчик для кнопки "В работу"
        @self.dp.callback_query(F.data.startswith("topic_take_"))
        async def handle_take_task(callback):
            task_id = callback.data.split("_", 2)[2]
            await self._handle_take_task(callback, task_id)
        
        # Обработчик для кнопки "Удалить"
        @self.dp.callback_query(F.data.startswith("topic_delete_"))
        async def handle_delete_task(callback):
            task_id = callback.data.split("_", 2)[2]
            await self._handle_delete_task(callback, task_id)
        
        # Обработчик для кнопки "Поручить @исполнитель"
        @self.dp.callback_query(F.data.startswith("topic_assign_"))
        async def handle_assign_task(callback):
            parts = callback.data.split("_", 3)
            executor = parts[2]
            task_id = parts[3]
            await self._handle_assign_task(callback, task_id, executor)
        
        # Обработчик для кнопки "Завершить"
        @self.dp.callback_query(F.data.startswith("topic_complete_"))
        async def handle_complete_task(callback):
            task_id = callback.data.split("_", 2)[2]
            await self._handle_complete_task(callback, task_id)
        
        # Обработчик для кнопки "Ответить"
        @self.dp.callback_query(F.data.startswith("topic_reply_"))
        async def handle_reply_task(callback):
            task_id = callback.data.split("_", 2)[2]
            await self._handle_reply_task(callback, task_id)
        
        # Обработчик для кнопки "Переоткрыть"
        @self.dp.callback_query(F.data.startswith("topic_reopen_"))
        async def handle_reopen_task(callback):
            task_id = callback.data.split("_", 2)[2]
            await self._handle_reopen_task(callback, task_id)
    
    async def _handle_take_task(self, callback, task_id: str):
        """Обрабатывает нажатие кнопки 'В работу'"""
        try:
            # Получаем задачу
            task = await redis_client.get_task(task_id)
            if not task:
                await callback.answer("Задача не найдена", show_alert=True)
                return
            
            # Определяем исполнителя (тот, кто нажал кнопку)
            executor = callback.from_user.username
            if not executor:
                await callback.answer("У вас нет username! Необходимо установить username в настройках Telegram", show_alert=True)
                return
            
            # Обновляем статус задачи и назначаем исполнителя
            await redis_client.update_task_status(task_id, "in_progress", executor)
            
            # Удаляем сообщение из темы "неотреагированные"
            await callback.message.delete()
            
            # Перемещаем задачу в тему исполнителя
            await self._move_task_to_executor_topic(task_id, executor)
            
            await callback.answer(f"Задача взята в работу @{executor}")
            
        except Exception as e:
            logger.error(f"Ошибка при взятии задачи в работу: {e}")
            await callback.answer("Ошибка при взятии задачи в работу", show_alert=True)
    
    async def _handle_assign_task(self, callback, task_id: str, executor: str):
        """Обрабатывает нажатие кнопки 'Поручить @исполнитель'"""
        try:
            # Получаем задачу
            task = await redis_client.get_task(task_id)
            if not task:
                await callback.answer("Задача не найдена", show_alert=True)
                return
            
            # Обновляем статус задачи и назначаем исполнителя
            await redis_client.update_task_status(task_id, "in_progress", executor)
            
            # Удаляем сообщение из темы "неотреагированные"
            await callback.message.delete()
            
            # Перемещаем задачу в тему исполнителя
            await self._move_task_to_executor_topic(task_id, executor)
            
            await callback.answer(f"Задача поручена @{executor}")
            
        except Exception as e:
            logger.error(f"Ошибка при поручении задачи: {e}")
            await callback.answer("Ошибка при поручении задачи", show_alert=True)
    
    async def _handle_delete_task(self, callback, task_id: str):
        """Обрабатывает нажатие кнопки 'Удалить'"""
        try:
            # Удаляем задачу из Redis с правильным декрементом счетчиков
            await self.redis.delete_task(task_id)
            
            # Удаляем сообщение
            await callback.message.delete()
            
            # Отправляем событие об удалении задачи для обновления закреплённого сообщения
            await redis_client.publish_event("task_updates", {
                "type": "task_deleted",
                "task_id": task_id
            })
            
            # Обновляем статистику (только логирование)
            await self._update_pinned_stats()
            
            await callback.answer("Задача удалена")
            
        except Exception as e:
            logger.error(f"Ошибка при удалении задачи: {e}")
            await callback.answer("Ошибка при удалении задачи", show_alert=True)
    
    async def _handle_complete_task(self, callback, task_id: str):
        """Обрабатывает нажатие кнопки 'Завершить'"""
        try:
            # Получаем задачу для определения исполнителя
            task = await redis_client.get_task(task_id)
            if not task:
                await callback.answer("Задача не найдена", show_alert=True)
                return
            
            # Обновляем статус задачи
            await redis_client.update_task_status(task_id, "completed", task.get('assignee'))
            
            # Удаляем сообщение из темы исполнителя
            await callback.message.delete()
            
            # Перемещаем задачу в тему "завершённые"
            await self._move_task_to_completed_topic(task_id)
            
            await callback.answer("Задача завершена")
            
        except Exception as e:
            logger.error(f"Ошибка при завершении задачи: {e}")
            await callback.answer("Ошибка при завершении задачи", show_alert=True)
    
    async def _handle_reply_task(self, callback, task_id: str):
        """Обрабатывает нажатие кнопки 'Ответить'"""
        try:
            # Пока просто уведомляем, что функция будет реализована
            await callback.answer("Функция ответа будет реализована позже")
            
        except Exception as e:
            logger.error(f"Ошибка при ответе на задачу: {e}")
            await callback.answer("Ошибка при ответе на задачу", show_alert=True)
    
    async def _handle_reopen_task(self, callback, task_id: str):
        """Обрабатывает нажатие кнопки 'Переоткрыть'"""
        try:
            # Получаем задачу
            task = await redis_client.get_task(task_id)
            if not task:
                await callback.answer("Задача не найдена", show_alert=True)
                return
            
            # Обновляем статус задачи на "in_progress"
            executor = task.get('assignee', '')
            await redis_client.update_task_status(task_id, "in_progress", executor)
            
            # Удаляем сообщение из темы "завершённые"
            await callback.message.delete()
            
            # Перемещаем задачу обратно в тему исполнителя
            if executor:
                await self._move_task_to_executor_topic(task_id, executor)
            else:
                # Если нет исполнителя, перемещаем в "неотреагированные"
                await self._move_task_to_unreacted_topic(task_id)
            
            await callback.answer("Задача переоткрыта")
            
        except Exception as e:
            logger.error(f"Ошибка при переоткрытии задачи: {e}")
            await callback.answer("Ошибка при переоткрытии задачи", show_alert=True)
    
    async def _move_task_to_unreacted_topic(self, task_id: str):
        """Перемещает задачу в тему 'неотреагированные' с новой клавиатурой"""
        try:
            task = await redis_client.get_task(task_id)
            if not task:
                logger.warning(f"Задача {task_id} не найдена")
                return
            
            # Проверяем, не находится ли задача уже в теме "unreacted"
            current_topic = task.get('current_topic')
            if current_topic == 'unreacted':
                logger.info(f"Задача {task_id} уже находится в теме 'unreacted', пропускаем")
                return
            
            # Получаем ID темы "неотреагированные"
            topic_id = await self._ensure_topic_exists("unreacted")
            if not topic_id:
                raise ValueError("Не удалось получить ID темы 'неотреагированные'")
            
            # Получаем список исполнителей для клавиатуры
            executors = await self._get_chat_executors()
            
            # Формируем текст сообщения
            msg_text = self._format_task_message(task)
            
            # Создаем клавиатуру с кнопками исполнителей
            keyboard = create_unreacted_topic_keyboard(task_id, executors)
            
            # Отправляем сообщение в тему с клавиатурой
            message = await self.bot.send_message(
                chat_id=settings.FORUM_CHAT_ID,
                message_thread_id=topic_id,
                text=msg_text,
                reply_markup=keyboard,
                parse_mode="HTML"
            )
            
            # Создаем ссылку на сообщение в форумной теме
            task_link = f"https://t.me/c/{str(abs(settings.FORUM_CHAT_ID))[4:]}/{topic_id}/{message.message_id}"
            
            # Обновляем данные задачи
            await redis_client.update_task(
                task_id,
                current_topic="unreacted",
                support_message_id=message.message_id,
                support_topic_id=topic_id,
                status="unreacted",
                task_link=task_link
            )
            
            # Обновляем счетчики статистики
            await self._increment_status_counter("unreacted")
            
            logger.info(f"Задача {task_id} перемещена в тему 'неотреагированные'")
            
            # Публикуем событие для обновления закрепленного сообщения
            await redis_client.publish_event("task_updates", {
                "type": "status_change",
                "task_id": task_id,
                "new_status": "unreacted"
            })
            
        except Exception as e:
            logger.error(f"Ошибка перемещения задачи в тему 'неотреагированные': {e}")
    
    async def _increment_status_counter(self, status: str):
        """Увеличивает счетчик статуса"""
        try:
            await redis_client._ensure_connection()
            key = f"stats:status:{status}"
            await redis_client.conn.incr(key)
            logger.debug(f"Incremented status counter for {status}")
        except Exception as e:
            logger.error(f"Error incrementing status counter for {status}: {e}")
    
    async def _decrement_status_counter(self, status: str):
        """Уменьшает счетчик статуса"""
        try:
            await redis_client._ensure_connection()
            key = f"stats:status:{status}"
            current = await redis_client.conn.get(key)
            if current and int(current) > 0:
                await redis_client.conn.decr(key)
                logger.debug(f"Decremented status counter for {status}")
        except Exception as e:
            logger.error(f"Error decrementing status counter for {status}: {e}")
    
    async def _increment_executor_counter(self, executor: str, status: str):
        """Увеличивает счетчик исполнителя для определенного статуса"""
        try:
            await redis_client._ensure_connection()
            key = f"stats:executor:{executor}:{status}"
            await redis_client.conn.incr(key)
            logger.debug(f"Incremented executor counter for {executor} in {status}")
        except Exception as e:
            logger.error(f"Error incrementing executor counter for {executor} in {status}: {e}")
    
    async def _decrement_executor_counter(self, executor: str, status: str):
        """Уменьшает счетчик исполнителя для определенного статуса"""
        try:
            await redis_client._ensure_connection()
            key = f"stats:executor:{executor}:{status}"
            current = await redis_client.conn.get(key)
            if current and int(current) > 0:
                await redis_client.conn.decr(key)
                logger.debug(f"Decremented executor counter for {executor} in {status}")
        except Exception as e:
            logger.error(f"Error decrementing executor counter for {executor} in {status}: {e}")
    
    async def _move_task_to_executor_topic(self, task_id: str, executor: str):
        """Перемещает задачу в тему исполнителя с соответствующей клавиатурой"""
        try:
            task = await redis_client.get_task(task_id)
            if not task:
                logger.warning(f"Задача {task_id} не найдена")
                return
            
            # Проверяем, не находится ли задача уже у этого исполнителя
            current_topic = task.get('current_topic')
            current_assignee = task.get('assignee')
            if current_topic == 'executor' and current_assignee == executor:
                logger.info(f"Задача {task_id} уже находится у исполнителя {executor}, пропускаем")
                return
            
            # Получаем или создаем тему исполнителя
            topic_id = await self._ensure_topic_exists("executor", executor)
            if not topic_id:
                raise ValueError(f"Не удалось получить ID темы исполнителя {executor}")
            
            # Формируем текст сообщения
            msg_text = self._format_task_message(task)
            
            # Создаем клавиатуру для темы исполнителя
            keyboard = create_executor_topic_keyboard(task_id)
            
            # Отправляем сообщение в тему с клавиатурой
            message = await self.bot.send_message(
                chat_id=settings.FORUM_CHAT_ID,
                message_thread_id=topic_id,
                text=msg_text,
                reply_markup=keyboard,
                parse_mode="HTML"
            )
            
            # Получаем старый статус для обновления счетчиков
            old_status = task.get('status', 'unreacted')
            old_assignee = task.get('assignee')
            
            # Создаем ссылку на сообщение в форумной теме исполнителя
            task_link = f"https://t.me/c/{str(abs(settings.FORUM_CHAT_ID))[4:]}/{topic_id}/{message.message_id}"
            
            # Обновляем данные задачи
            await redis_client.update_task(
                task_id,
                current_topic="executor",
                support_message_id=message.message_id,
                support_topic_id=topic_id,
                status="in_progress",
                assignee=executor,
                task_link=task_link
            )
            
            # Обновляем счетчики статистики
            if old_status != "in_progress":
                await self._decrement_status_counter(old_status)
                await self._increment_status_counter("in_progress")
            
            # Обновляем счетчики исполнителей
            if old_assignee and old_assignee != executor:
                await self._decrement_executor_counter(old_assignee, old_status)
            await self._increment_executor_counter(executor, "in_progress")
            
            logger.info(f"Задача {task_id} перемещена в тему исполнителя @{executor}")
            
            # Публикуем событие для обновления закрепленного сообщения
            await redis_client.publish_event("task_updates", {
                "type": "status_change",
                "task_id": task_id,
                "new_status": "in_progress",
                "assignee": executor
            })
            
        except Exception as e:
            logger.error(f"Ошибка перемещения задачи в тему исполнителя: {e}")
    
    async def _move_task_to_completed_topic(self, task_id: str):
        """Перемещает задачу в тему 'завершённые' с новой клавиатурой"""
        try:
            task = await redis_client.get_task(task_id)
            if not task:
                logger.warning(f"Задача {task_id} не найдена")
                return
            
            # Проверяем, не находится ли задача уже в теме "completed"
            current_topic = task.get('current_topic')
            if current_topic == 'completed':
                logger.info(f"Задача {task_id} уже находится в теме 'completed', пропускаем")
                return
            
            # Получаем ID темы "завершённые"
            topic_id = await self._ensure_topic_exists("completed")
            if not topic_id:
                raise ValueError("Не удалось получить ID темы 'завершённые'")
            
            # Формируем текст сообщения
            msg_text = self._format_task_message(task)
            
            # Создаем клавиатуру для завершённых задач
            keyboard = create_completed_topic_keyboard(task_id)
            
            # Отправляем сообщение в тему с клавиатурой
            message = await self.bot.send_message(
                chat_id=settings.FORUM_CHAT_ID,
                message_thread_id=topic_id,
                text=msg_text,
                reply_markup=keyboard,
                parse_mode="HTML"
            )
            
            # Получаем старые данные для обновления счетчиков
            old_status = task.get('status', 'in_progress')
            old_assignee = task.get('assignee')
            
            # Создаем ссылку на сообщение в форумной теме завершенных
            task_link = f"https://t.me/c/{str(abs(settings.FORUM_CHAT_ID))[4:]}/{topic_id}/{message.message_id}"
            
            # Обновляем данные задачи
            await redis_client.update_task(
                task_id,
                current_topic="completed",
                support_message_id=message.message_id,
                support_topic_id=topic_id,
                status="completed",
                task_link=task_link
            )
            
            # Обновляем счетчики статистики
            if old_status != "completed":
                await self._decrement_status_counter(old_status)
                await self._increment_status_counter("completed")
            
            # Обновляем счетчики исполнителей
            if old_assignee and old_status in ['in_progress', 'unreacted']:
                await self._decrement_executor_counter(old_assignee, old_status)
            
            logger.info(f"Задача {task_id} перемещена в тему 'завершённые'")
            
            # Публикуем событие для обновления закрепленного сообщения
            await redis_client.publish_event("task_updates", {
                "type": "status_change",
                "task_id": task_id,
                "new_status": "completed",
                "assignee": old_assignee
            })
            
        except Exception as e:
            logger.error(f"Ошибка перемещения задачи в тему 'завершённые': {e}")
    
    async def _save_topics_to_redis(self):
        """Сохраняет активные темы в Redis для восстановления после перезапуска"""
        try:
            topics_data = {
                "system": {
                    "unreacted": self.active_topics["unreacted"],
                    "waiting": self.active_topics["waiting"],
                    "completed": self.active_topics["completed"]
                },
                "executors": self.active_topics["executors"]
            }
            await redis_client.set("mover_bot_topics", json.dumps(topics_data))
            logger.info("Активные темы сохранены в Redis")
        except Exception as e:
            logger.error(f"Ошибка сохранения тем в Redis: {e}")
    
    async def _restore_topics_from_redis(self):
        """Восстанавливает активные темы из Redis после перезапуска"""
        try:
            topics_json = await redis_client.get("mover_bot_topics")
            if topics_json:
                topics_data = json.loads(topics_json)
                
                # Восстанавливаем системные темы
                self.active_topics["unreacted"] = topics_data["system"]["unreacted"]
                self.active_topics["waiting"] = topics_data["system"]["waiting"]
                self.active_topics["completed"] = topics_data["system"]["completed"]
                
                # Восстанавливаем темы исполнителей
                self.active_topics["executors"] = topics_data["executors"]
                
                logger.info(f"Темы восстановлены из Redis: {topics_data}")
            else:
                logger.info("Нет сохраненных тем в Redis для восстановления")
        except Exception as e:
            logger.error(f"Ошибка восстановления тем из Redis: {e}")

    async def start(self):
        """Запускает все фоновые процессы и polling"""
        try:
            # Проверяем, что чат доступен и является форумом
            chat = await self.bot.get_chat(settings.FORUM_CHAT_ID)
            if not chat.is_forum:
                logger.error("Указанный чат не является форумом! Темы не будут работать")
            
            # Восстанавливаем темы из Redis
            await self._restore_topics_from_redis()
            
            # Подписываемся на каналы
            await self.pubsub_manager.subscribe("task_updates", self._pubsub_message_handler)
            
            # Запускаем слушателя PubSub
            await self.pubsub_manager.start()
            
            # Запуск фоновых задач
            asyncio.create_task(self._stats_updater_loop())
            asyncio.create_task(self._task_event_listener())
            
            logger.info("MoverBot успешно запущен")
            
            # Запускаем polling
            await self.dp.start_polling(self.bot)
        except Exception as e:
            logger.error(f"Ошибка запуска MoverBot: {e}")
    
    async def _pubsub_message_handler(self, channel: str, message: Dict[str, Any]):
        """Обработчик сообщений PubSub для MoverBot"""
        try:
            logger.info(f"[MOVERBOT][PUBSUB] Received message on channel {channel}: {message}")
            
            if channel == "task_updates":
                await self._process_task_event(message)
            else:
                logger.warning(f"[MOVERBOT][PUBSUB] Unknown channel: {channel}")
                
        except Exception as e:
            logger.error(f"[MOVERBOT][PUBSUB] Error handling message from {channel}: {e}")

    async def _find_existing_topic(self, topic_name: str) -> Optional[int]:
        """Ищет существующую тему по имени в форум-чате"""
        try:
            # Поскольку в aiogram нет прямого метода для получения списка тем форума,
            # мы используем кэширование в Redis и проверку существующих тем
            
            # Проверяем системные темы
            name_map = {
                "⚠️ Неотреагированные": "unreacted",
                "⏳ В ожидании": "waiting",
                "✅ Выполненные": "completed"
            }
            
            if topic_name in name_map:
                topic_type = name_map[topic_name]
                topic_id = self.active_topics.get(topic_type)
                if topic_id:
                    # Проверяем, что тема всё ещё существует
                    try:
                        # Пробуем отправить тестовое сообщение в тему
                        # Это более надежный способ проверки существования темы
                        await self.bot.send_chat_action(
                            chat_id=settings.FORUM_CHAT_ID,
                            action="typing",
                            message_thread_id=topic_id
                        )
                        return topic_id
                    except Exception as e:
                        # Тема не существует или произошла другая ошибка
                        logger.warning(f"Тема {topic_name} (ID: {topic_id}) не найдена или недоступна: {e}")
                        self.active_topics[topic_type] = None
                        await self._save_topics_to_redis()
            
            # Проверяем темы исполнителей
            if topic_name.startswith("🛠️ @"):
                executor_name = topic_name[5:]  # Убираем префикс "🛠️ @"
                topic_id = self.active_topics["executors"].get(executor_name)
                if topic_id:
                    # Проверяем, что тема всё ещё существует
                    try:
                        # Пробуем отправить тестовое сообщение в тему
                        await self.bot.send_chat_action(
                            chat_id=settings.FORUM_CHAT_ID,
                            action="typing",
                            message_thread_id=topic_id
                        )
                        return topic_id
                    except Exception as e:
                        # Тема не существует или произошла другая ошибка
                        logger.warning(f"Тема {topic_name} (ID: {topic_id}) не найдена или недоступна: {e}")
                        self.active_topics["executors"].pop(executor_name, None)
                        await self._save_topics_to_redis()
            
            return None
        except Exception as e:
            logger.warning(f"Не удалось найти существующую тему '{topic_name}': {e}")
            return None
    
    async def _ensure_topic_exists(self, topic_type: str, topic_name: str = None) -> Optional[int]:
        """Создает тему при необходимости и возвращает её ID"""
        try:
            # Для исполнителей
            if topic_type == "executor" and topic_name:
                # Проверяем кэш
                if topic_name in self.active_topics["executors"]:
                    logger.info(f"Найдена кэшированная тема для исполнителя @{topic_name}: {self.active_topics['executors'][topic_name]}")
                    return self.active_topics["executors"][topic_name]
                
                # Пытаемся найти существующую тему
                existing_topic_id = await self._find_existing_topic(f"🛠️ @{topic_name}")
                if existing_topic_id:
                    self.active_topics["executors"][topic_name] = existing_topic_id
                    logger.info(f"Найдена существующая тема для исполнителя @{topic_name}: {existing_topic_id}")
                    return existing_topic_id
                
                # Создаём новую тему
                topic = await self.bot.create_forum_topic(
                    chat_id=settings.FORUM_CHAT_ID,
                    name=f"🛠️ @{topic_name}"
                )
                self.active_topics["executors"][topic_name] = topic.message_thread_id
                # Сохраняем темы в Redis
                await self._save_topics_to_redis()
                logger.info(f"Создана новая тема для исполнителя @{topic_name}: {topic.message_thread_id}")
                return topic.message_thread_id
            
            # Для системных тем
            elif topic_type in ["unreacted", "waiting", "completed"]:
                # Проверяем кэш
                if self.active_topics.get(topic_type):
                    logger.info(f"Найдена кэшированная системная тема {topic_type}: {self.active_topics[topic_type]}")
                    return self.active_topics[topic_type]
                
                name_map = {
                    "unreacted": "⚠️ Неотреагированные",
                    "waiting": "⏳ В ожидании",
                    "completed": "✅ Выполненные"
                }
                
                # Пытаемся найти существующую тему
                existing_topic_id = await self._find_existing_topic(name_map[topic_type])
                if existing_topic_id:
                    self.active_topics[topic_type] = existing_topic_id
                    logger.info(f"Найдена существующая системная тема {topic_type}: {existing_topic_id}")
                    return existing_topic_id
                
                # Создаём новую тему
                topic = await self.bot.create_forum_topic(
                    chat_id=settings.FORUM_CHAT_ID,
                    name=name_map[topic_type]
                )
                self.active_topics[topic_type] = topic.message_thread_id
                # Сохраняем темы в Redis
                await self._save_topics_to_redis()
                logger.info(f"Создана новая системная тема {topic_type}: {topic.message_thread_id}")
                return topic.message_thread_id
            
            # Для статуса in_progress задачи должны перемещаться в темы исполнителей
            elif topic_type == "in_progress":
                # Этот статус не должен создавать отдельную системную тему
                # Задачи с этим статусом перемещаются в темы исполнителей
                logger.warning(f"Попытка создания системной темы для статуса {topic_type}, который должен обрабатываться как тема исполнителя")
                raise ValueError(f"Статус {topic_type} должен обрабатываться как тема исполнителя")
            
            raise ValueError(f"Неизвестный тип темы: {topic_type}")
            
        except Exception as e:
            logger.error(f"Ошибка обеспечения существования темы {topic_type}: {e}")
            return None

    async def _cleanup_empty_topics(self):
        """Удаляет темы исполнителей, если у них нет задач в работе"""
        try:
            # Проверяем темы исполнителей
            for executor, topic_id in list(self.active_topics["executors"].items()):
                tasks = await redis_client.get_tasks_by_assignee(executor)
                in_progress_tasks = [t for t in tasks if t.get("status") == "in_progress"]
                if not in_progress_tasks:
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
    
    async def _get_chat_executors(self):
        """Получает список участников чата поддержки (исключая ботов)"""
        try:
            executors = []
            # Получаем администраторов чата
            admins = await self.bot.get_chat_administrators(settings.SUPPORT_CHAT_ID)
            
            for admin in admins:
                user = admin.user
                # Исключаем ботов и добавляем только пользователей с username
                if not user.is_bot and user.username:
                    executors.append({
                        'id': user.id,
                        'username': user.username,
                        'first_name': user.first_name or user.username
                    })
            
            logger.info(f"Найдено {len(executors)} исполнителей в чате поддержки")
            return executors
            
        except Exception as e:
            logger.error(f"Ошибка получения участников чата: {e}")
            return []

    async def _move_task_to_topic(self, task_id: str, status: str, executor: str = None):
        """Основная логика перемещения задач с формированием сообщения-ссылки на задачу в главном меню."""
        try:
            task = await redis_client.get_task(task_id)
            if not task:
                logger.warning(f"Задача {task_id} не найдена")
                return

            # Маппинг статусов для совместимости
            status_mapping = {
                "in": "in_progress",
                "progress": "in_progress",
                "done": "completed",
                "finished": "completed"
            }
            
            # Применяем маппинг статуса
            mapped_status = status_mapping.get(status, status)
            logger.info(f"Status mapping: {status} -> {mapped_status}")
            
            # Защита от повторного перемещения задач со статусом in_progress
            if mapped_status == "in_progress":
                logger.warning(f"Метод _move_task_to_topic не должен обрабатывать задачи со статусом in_progress. Используйте _move_task_to_executor_topic или _move_task_to_unreacted_topic.")
                return

            # Определяем тему
            if mapped_status == "in_progress" and executor:
                topic_id = await self._ensure_topic_exists("executor", executor)
                topic_type = "executor"  # Используем общий тип для тем исполнителей
            elif mapped_status == "in_progress" and not executor:
                # Если статус in_progress, но нет исполнителя, используем тему неотреагированных
                topic_id = await self._ensure_topic_exists("unreacted")
                topic_type = "unreacted"
            else:
                # Дополнительная проверка для статуса in_progress
                if mapped_status == "in_progress":
                    logger.warning(f"Попытка создания системной темы для статуса {mapped_status}, который должен обрабатываться как тема исполнителя")
                    raise ValueError(f"Статус {mapped_status} должен обрабатываться как тема исполнителя")
                topic_id = await self._ensure_topic_exists(mapped_status)
                topic_type = mapped_status

            if not topic_id:
                raise ValueError("Не удалось получить ID темы")

            # Удаляем старое сообщение в теме (если есть)
            if "support_message_id" in task and task["support_message_id"]:
                old_topic_id = task.get("support_topic_id")
                if old_topic_id:
                    await self._delete_message(task["support_message_id"], old_topic_id)

            # Формируем ссылку на задачу в главном меню (если есть)
            main_chat_link = None
            if task.get("main_message_id"):
                main_chat_link = f"https://t.me/c/{str(settings.MAIN_TASK_CHAT_ID)[4:]}/{task['main_message_id']}"
            elif task.get("support_message_id"):
                main_chat_link = f"https://t.me/c/{str(abs(settings.SUPPORT_CHAT_ID))[4:]}/{task['support_message_id']}"
            elif task.get("task_link"):
                main_chat_link = task["task_link"]

            # Формируем текст сообщения
            msg_text = self._format_task_message(task, main_chat_link)

            # В темах форума не должно быть кнопок (клавиатур)
            # Отправляем новое сообщение в тему без клавиатуры
            message = await self.bot.send_message(
                chat_id=settings.FORUM_CHAT_ID,
                message_thread_id=topic_id,
                text=msg_text,
                parse_mode="HTML"
            )

            # Обновляем данные задачи
            await redis_client.update_task(
                task_id,
                current_topic=topic_type,
                support_message_id=message.message_id,
                support_topic_id=topic_id,
                task_link=main_chat_link or f"https://t.me/c/{str(abs(settings.MAIN_TASK_CHAT_ID))[4:]}/{message.message_id}"
            )
            
            # Публикуем событие обновления задачи, чтобы TaskBot обновил клавиатуру
            await redis_client.publish_event("task_updates", {
                "type": "task_update",
                "task_id": task_id,
                "text": task.get('text', '')
            })

            logger.info(f"Задача {task_id} перемещена в тему {topic_type}")
            
            # Периодическая очистка пустых тем (10% chance)
            if random.random() < 0.1:
                await self._cleanup_empty_topics()
        except Exception as e:
            logger.error(f"Ошибка перемещения задачи: {e}", exc_info=True)


    async def _delete_message(self, message_id: int, topic_id: int = None):
        """Удаляет сообщение в чате поддержки"""
        try:
            # Удаляем сообщение (без message_thread_id, так как он не поддерживается в delete_message)
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
        """Обновляет статистику (только счётчики, закреплённое сообщение обновляет TaskBot)"""
        try:
            # Получаем статистику для логирования
            stats = await redis_client.get_global_stats()
            logger.info(f"📊 Redis: {stats}")
            
            # MoverBot только обновляет счётчики
            # Закреплённое сообщение обновляет TaskBot через PubSub
            
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

    def _format_task_message(self, task: Dict, main_chat_link: str = None) -> str:
        """Форматирует текст задачи с ссылкой на задачу в главном меню."""
        status_icons = {
            "unreacted": "⚠️",
            "waiting": "⏳",
            "in_progress": "⚡",
            "completed": "✅"
        }
        link_part = f'\n🔗 <a href="{main_chat_link}">Открыть в главном меню</a>' if main_chat_link else ""
        return (
            f"{status_icons.get(task['status'], '📌')} <b>Задача #{task.get('task_number', 'N/A')}</b>\n"
            f"👤 От: @{task.get('username', 'N/A')}\n"
            f"📝 Текст: {task.get('text', '')}\n"
            f"🔄 Статус: {task.get('status', 'N/A')}" + link_part
        )


    async def _task_event_listener(self):
        """Слушает события задач из Redis"""
        # Этот метод больше не нужен, так как мы используем PubSubManager
        # Оставляем пустую реализацию для совместимости
        pass

    async def _process_task_event(self, event: Dict):
        """Обрабатывает событие задачи"""
        try:
            event_type = event.get("type")
            
            if event_type == "status_change":
                await self._handle_status_change(event)
            elif event_type == "new_task":
                await self._handle_new_task(event)
            elif event_type == "reminder_set":
                await self._setup_reminder(event)
            elif event_type == "task_deleted":
                # Обновляем статистику при удалении задачи
                await self._update_pinned_stats()
            elif event_type == "task_update":
                # Для события task_update не выполняем перемещение задачи,
                # так как это может привести к бесконечному циклу
                # Обновляем только статистику
                await self._update_pinned_stats()
        except Exception as e:
            logger.error(f"Ошибка обработки события задачи: {e}")

    async def _handle_status_change(self, event: Dict):
        """Обрабатывает изменение статуса задачи"""
        task_id = event["task_id"]
        new_status = event["new_status"]
        # Получаем исполнителя из разных возможных ключей
        executor = event.get("assignee") or event.get("changed_by")

        try:
            # Для статуса "unreacted" используем метод с клавиатурой
            if new_status == "unreacted":
                await self._move_task_to_unreacted_topic(task_id)
            elif new_status == "in_progress" and executor:
                await self._move_task_to_executor_topic(task_id, executor)
            elif new_status == "completed":
                await self._move_task_to_completed_topic(task_id)
            else:
                # Для остальных случаев используем общий метод
                await self._move_task_to_topic(task_id, new_status, executor)
            await self._update_pinned_stats()
        except Exception as e:
            logger.error(f"Ошибка обработки изменения статуса: {e}")

    async def _handle_new_task(self, event: Dict):
        """Обрабатывает новую задачу - сразу помещает в тему 'неотреагированные' с клавиатурой"""
        task_id = event["task_id"]
        # Используем новый метод для перемещения в тему с клавиатурой
        await self._move_task_to_unreacted_topic(task_id)

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


# Создание экземпляра бота
mover_bot = MoverBot()

if __name__ == "__main__":
    asyncio.run(mover_bot.start())

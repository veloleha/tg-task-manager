import asyncio
import json
import logging
import os
import uuid
import aiofiles
from datetime import datetime
from typing import Dict, Optional
from collections import defaultdict

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile
import aiogram.exceptions

from core.redis_client import redis_client
from core.pubsub_manager import UserBotPubSubManager
from config.settings import settings
from bots.user_bot.topic_manager import TopicManager

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/userbot.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
# Этапные логи будут видны и в терминале, и в logs/userbot.log

class CreateTaskState(StatesGroup):
    waiting_for_task = State()

class MessageAggregator:
    def __init__(self, bot: Bot, redis_client: UserBotPubSubManager, timeout=60):
        self.bot = bot
        self.redis = redis_client
        self.user_messages = defaultdict(list)
        self.timeout = timeout  # 1 минута для дополнительных сообщений
        self.lock = asyncio.Lock()
        self.processed_tasks = {}  # Хранит ID задач для каждого пользователя
    
    async def add_message(self, user_id: int, message_data: dict):
        async with self.lock:
            # Проверяем, есть ли уже созданная задача для этого пользователя
            if user_id in self.processed_tasks:
                # Добавляем сообщение к существующей задаче
                self.user_messages[user_id].append(message_data)
                # Обновляем задачу немедленно
                await self.update_existing_task(user_id)
            else:
                # Создаем новую задачу моментально
                self.user_messages[user_id].append(message_data)
                await self.create_immediate_task(user_id)
                # Запускаем таймер для сбора дополнительных сообщений
                asyncio.create_task(self.flush_user(user_id))
    
    async def create_immediate_task(self, user_id: int):
        """Создает задачу моментально при первом сообщении"""
        async with self.lock:
            if user_id in self.user_messages and self.user_messages[user_id]:
                message_data = self.user_messages[user_id][0].copy()
                try:
                    # Сохраняем задачу в Redis
                    task_id = await self.redis.save_task(message_data)
                    
                    # Сохраняем ID задачи для последующих обновлений
                    self.processed_tasks[user_id] = task_id
                    
                    # Публикуем событие о новой задаче
                    await self.redis.publish_event("new_tasks", {
                        "type": "new_task",
                        "task_id": task_id
                    })
                    
                    # Обновляем счетчики
                    await self.redis.increment_counter("unreacted")
                    
                    logger.info(f"Created immediate task {task_id} for user {message_data['user_id']}")
                    
                    # Устанавливаем реакцию '👀' на исходное сообщение
                    await self.bot.set_message_reaction(
                        chat_id=message_data['chat_id'],
                        message_id=message_data['message_id'],
                        reaction=[{"type": "emoji", "emoji": "👀"}]
                    )
                except Exception as e:
                    logger.error(f"Error creating immediate task: {e}")
    
    async def update_existing_task(self, user_id: int):
        """Обновляет существующую задачу с новыми сообщениями"""
        async with self.lock:
            if user_id in self.processed_tasks and user_id in self.user_messages:
                task_id = self.processed_tasks[user_id]
                messages = self.user_messages[user_id]
                
                # Проверяем, существует ли задача перед обновлением
                try:
                    task = await self.redis.get_task(task_id)
                    if not task or len(task) == 0 or 'user_id' not in task:
                        logger.warning(f"Task {task_id} for user {user_id} no longer exists, clearing cache")
                        # Удаляем из кэша, так как задача удалена
                        if user_id in self.processed_tasks:
                            del self.processed_tasks[user_id]
                        if user_id in self.user_messages:
                            del self.user_messages[user_id]
                        return
                except Exception as check_error:
                    logger.error(f"Error checking task {task_id} existence: {check_error}")
                    return
                
                # Объединяем все сообщения
                combined_text = "\n".join(msg.get('text', '') for msg in messages if msg.get('text', ''))
                
                if combined_text:
                    try:
                        # Обновляем задачу в Redis
                        await self.redis.update_task(task_id, text=combined_text)
                        
                        # Публикуем событие об обновлении задачи
                        await self.redis.publish_event("task_updates", {
                            "type": "task_update",
                            "task_id": task_id,
                            "text": combined_text
                        })
                        
                        logger.info(f"Updated task {task_id} for user {user_id} with {len(messages)} messages")
                    except Exception as e:
                        logger.error(f"Error updating task {task_id}: {e}")
    
    async def flush_user(self, user_id: int):
        """Завершает обработку сообщений пользователя через 1 минуту"""
        await asyncio.sleep(self.timeout)
        async with self.lock:
            if user_id in self.user_messages:
                # Удаляем пользователя из обработки
                if user_id in self.processed_tasks:
                    del self.processed_tasks[user_id]
                del self.user_messages[user_id]
                logger.info(f"Finished processing messages for user {user_id}")
    
    async def save_and_process(self, message_data: dict):
        """Сохраняет и обрабатывает сообщение (сохранено для совместимости)"""
        try:
            # Сохраняем задачу в Redis
            task_id = await self.redis.save_task(message_data)
            
            # Публикуем событие о новой задаче
            await self.redis.publish_event("new_tasks", {
                "type": "new_task",
                "task_id": task_id
            })
            
            # Обновляем счетчики
            await self.redis.increment_counter("unreacted")
            
            logger.info(f"Created aggregated task {task_id} for user {message_data['user_id']}")
            
            # Устанавливаем реакцию '👀' на исходное сообщение
            await self.bot.set_message_reaction(
                chat_id=message_data['chat_id'],
                message_id=message_data['message_id'],
                reaction=[{"type": "emoji", "emoji": "👀"}]
            )
        except Exception as e:
            logger.error(f"Error creating aggregated task: {e}")


class UserBot:
    """
    User Bot - собирает сообщения пользователей и создает задачи
    
    Новые функции:
    - Создание отдельных тем для каждого пользователя
    - Автообъединение сообщений в течение заданного времени
    - Отправка реакций для обратной связи
    - Обработка ответов от поддержки
    """
    
    def __init__(self):
        self.bot = Bot(token=settings.USER_BOT_TOKEN)
        self.dp = Dispatcher()
        self.redis = redis_client
        
        # Инициализация менеджеров
        self.topic_manager = TopicManager(self.bot)
        self.pubsub_manager = UserBotPubSubManager()
        
        # Инициализация агрегатора сообщений
        self.message_aggregator = MessageAggregator(bot=self.bot, redis_client=self.redis)
        
        # Настройка таймаута агрегации (из настроек или по умолчанию 5 минут)
        aggregation_timeout = getattr(settings, 'MESSAGE_AGGREGATION_TIMEOUT', 300)
        self.message_aggregator.timeout = aggregation_timeout
        
        self._setup_handlers()

    def _setup_handlers(self):
        """Настройка обработчиков сообщений"""
        
        @self.dp.message(Command("start"))
        async def cmd_start(message: types.Message):
            """Обработчик команды /start"""
            logger.info(f"Start command from user {message.from_user.id}")
            
            # Добавляем кнопку "создать задачу" во всех чатах
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    text="🎯 Создать задачу", 
                    callback_data="create_task"
                )]
            ])
            
            if message.chat.type == "private":
                # В личных сообщениях
                await message.answer(
                    "👋 Привет! Я бот для сбора задач.\n\n"
                    "📝 Можете просто написать мне сообщение, и я создам из него задачу.\n"
                    "⚡ Или нажмите кнопку 'Создать задачу', чтобы сразу перейти в статус 'в ожидании'.",
                    reply_markup=keyboard
                )
            else:
                # В групповых чатах тоже добавляем кнопку
                await message.answer(
                    "👋 Привет! Я бот для сбора задач.\n\n"
                    "📝 Можете просто написать сообщение, и я создам из него задачу.\n"
                    "⚡ Или нажмите кнопку 'Создать задачу', чтобы сразу перейти в статус 'в ожидании'.",
                    reply_markup=keyboard
                )

        @self.dp.callback_query(F.data == "create_task")
        async def handle_create_task_button(callback: types.CallbackQuery, state: FSMContext):
            """Обработчик кнопки 'Создать задачу'"""
            logger.info(f"Create task button pressed by user {callback.from_user.id}")
            await callback.answer()
            await callback.message.answer(
                "📝 Напишите текст задачи, которую хотите создать:"
            )
            await state.set_state(CreateTaskState.waiting_for_task)

        @self.dp.message(CreateTaskState.waiting_for_task)
        async def handle_task_text(message: types.Message, state: FSMContext):
            """Обработчик текста задачи для быстрого создания"""
            try:
                logger.info(f"Received task text from user {message.from_user.id}: {message.text[:100] if message.text else 'No text'}...")
                
                # Подготавливаем данные сообщения
                message_data = await self._prepare_message_data(message)
                
                # Создаем задачу напрямую со статусом 'waiting' (минуя 'unreacted')
                task_id = await self._create_task_directly(message_data, status='waiting')
                
                # Убираем уведомление о создании задачи, чтобы не засорять чат
                # await message.answer(
                #     "✅ Задача создана и сразу переведена в статус 'В ожидании'!\n"
                #     f"🆔 ID задачи: {task_id}"
                # )
                
                # Очищаем состояние
                await state.clear()
                
                logger.info(f"✅ Fast task created: {task_id} with status 'waiting'")
                
            except Exception as e:
                logger.error(f"Error creating fast task: {e}", exc_info=True)
                await message.answer("❌ Произошла ошибка при создании задачи. Попробуйте еще раз.")
                await state.clear()

        @self.dp.message()
        async def handle_message(message: types.Message):
            """Основной обработчик сообщений"""
            try:
                # Игнорируем сообщения от ботов
                if message.from_user.is_bot:
                    return
                
                # Игнорируем команды
                if message.text and message.text.startswith('/'):
                    return
                
                # Игнорируем сообщения из чата поддержки (форумного чата)
                if message.chat.id == settings.FORUM_CHAT_ID:
                    logger.info(f"[USERBOT][MSG] Ignoring message from support chat {message.chat.id}")
                    return
                
                logger.info(f"[USERBOT][MSG] Processing message from user {message.from_user.id} in chat {message.chat.id}")
                logger.info(f"[USERBOT][MSG] Message text: {message.text[:100] if message.text else 'No text'}...")
                
                user_id = message.from_user.id
                chat_id = message.chat.id
                
                # Проверяем, пишет ли пользователь в своей теме (инициализируем как False)
                user_in_own_topic = False
                
                # Создаем или получаем пользовательскую тему в текущем чате (если это форум)
                logger.info(f"[USERBOT][MSG] Getting or creating user topic for user {user_id} in chat {chat_id}...")
                user_topic_id = await self.topic_manager.get_or_create_user_topic(
                    chat_id=chat_id,
                    user_id=user_id,
                    username=message.from_user.username,
                    first_name=message.from_user.first_name
                )
                
                if user_topic_id:
                    logger.info(f"[USERBOT][MSG] User topic ID: {user_topic_id}")
                    
                    # Проверяем, не пишет ли пользователь уже в своей теме
                    current_thread_id = getattr(message, 'message_thread_id', None)
                    user_in_own_topic = current_thread_id and current_thread_id == user_topic_id
                    if user_in_own_topic:
                        logger.info(f"[USERBOT][MSG] User is already writing in their own topic {user_topic_id}, will process as additional message")
                    # Обновляем активность темы
                    await self.topic_manager.update_topic_activity(chat_id, user_id)
                else:
                    logger.info(f"[USERBOT][MSG] Chat {chat_id} is not a forum or topic creation failed")
                
                # ЭТАП 1: Проверяем, есть ли у пользователя активная задача
                logger.info(f"[USERBOT][GROUPING] === ЭТАП 1: Поиск активной задачи ===")
                active_task_id = await self._find_user_active_task(user_id)
                
                if active_task_id:
                    logger.info(f"[USERBOT][GROUPING] ✅ ЭТАП 1 РЕЗУЛЬТАТ: Найдена активная задача {active_task_id}")
                    # ЭТАП 3: Добавляем сообщение к существующей задаче
                    append_success = await self._append_to_existing_task(active_task_id, message)
                    if append_success:
                        logger.info(f"[USERBOT][GROUPING] ✅ Сообщение успешно добавлено к задаче {active_task_id}")
                        return  # Завершаем обработку - сообщение добавлено к существующей задаче
                    else:
                        logger.warning(f"[USERBOT][GROUPING] ⚠️ Не удалось добавить к задаче {active_task_id}, создаем новую")
                else:
                    logger.info(f"[USERBOT][GROUPING] ✅ ЭТАП 1 РЕЗУЛЬТАТ: Активных задач не найдено")
                    logger.info(f"[USERBOT][GROUPING] === ЭТАП 2: Создание новой задачи ===")
                
                # Подготавливаем данные сообщения
                logger.info(f"[USERBOT][MSG] Preparing message data...")
                message_data = await self._prepare_message_data(message)
                logger.info(f"[USERBOT][MSG] Message data prepared: {len(message_data)} fields")
                logger.info(f"[USERBOT][MSG] support_media_message_id in message_data: {message_data.get('support_media_message_id')}")
                
                # Создаем задачу напрямую (без агрегатора)
                logger.info(f"[USERBOT][MSG] Creating task directly...")
                task_id = await self._create_task_directly(message_data)
                logger.info(f"[USERBOT][MSG] ✅ Task created directly: {task_id}")
                
                # Пересылка сообщения в тему пользователя (если он не в своей теме)
                if user_topic_id and task_id and not user_in_own_topic:
                    try:
                        # Пересылаем оригинальное сообщение в тему пользователя
                        forwarded_msg = await message.forward(chat_id, message_thread_id=user_topic_id)
                        logger.info(f"[USERBOT][MSG] ✅ Переслано сообщение {message.message_id} в тему пользователя {user_topic_id} (forwarded as {forwarded_msg.message_id})")
                    except Exception as forward_error:
                        logger.error(f"[USERBOT][MSG] ❌ Ошибка пересылки сообщения в тему: {forward_error}")
                elif user_in_own_topic:
                    logger.info(f"[USERBOT][MSG] Пользователь уже пишет в своей теме {user_topic_id}, пересылка не нужна")
                
                # Добавляем reply-клавиатуру "Создать задачу" во всех чатах
                reply_keyboard = types.ReplyKeyboardMarkup(
                    keyboard=[
                        [types.KeyboardButton(text="🎯 Создать задачу")]
                    ],
                    resize_keyboard=True
                )
                
                # Убираем уведомление о создании задачи, чтобы не засорять чат
                # await message.answer("✅ Задача создана!", reply_markup=reply_keyboard)
                
            except Exception as e:
                logger.error(f"Error handling message: {e}", exc_info=True)
                try:
                    await self._set_error_reaction(message)
                except:
                    pass
    
    async def _forward_to_user_topic(self, message: types.Message, topic_id: int, chat_id: int):
        """Пересылает сообщение в пользовательскую тему"""
        try:
            # Пересылаем оригинальное сообщение в пользовательскую тему
            await self.bot.forward_message(
                chat_id=chat_id,
                from_chat_id=chat_id,
                message_id=message.message_id,
                message_thread_id=topic_id
            )
            
            logger.info(f"[USERBOT] Forwarded message to user topic {topic_id} in chat {chat_id}")
            
        except Exception as e:
            error_msg = str(e).lower()
            if "message thread not found" in error_msg:
                logger.warning(f"[USERBOT] Topic {topic_id} not found, creating new topic for user {message.from_user.id} in chat {chat_id}")
                # Удаляем старую тему из Redis
                await self.topic_manager.redis.conn.delete(f"user_topic:{chat_id}:{message.from_user.id}")
                await self.topic_manager.redis.conn.delete(f"topic_user:{chat_id}:{topic_id}")
                
                # Создаем новую тему
                new_topic_id = await self.topic_manager._create_user_topic(
                    chat_id=chat_id,
                    user_id=message.from_user.id,
                    username=message.from_user.username,
                    first_name=message.from_user.first_name
                )
                
                if new_topic_id:
                    # Сохраняем новую тему
                    await self.topic_manager._save_user_topic(chat_id, message.from_user.id, new_topic_id)
                    
                    try:
                        await self.bot.forward_message(
                            chat_id=chat_id,
                            from_chat_id=chat_id,
                            message_id=message.message_id,
                            message_thread_id=new_topic_id
                        )
                        logger.info(f"[USERBOT] Forwarded message to new user topic {new_topic_id} in chat {chat_id}")
                    except Exception as retry_error:
                        logger.error(f"[USERBOT] Error forwarding to new user topic: {retry_error}")
                else:
                    logger.error(f"[USERBOT] Failed to create new topic for user {message.from_user.id} in chat {chat_id}")
            else:
                logger.error(f"[USERBOT] Error forwarding to user topic: {e}")

    async def _forward_reply_to_user_topic(self, sent_message, user_id: int, chat_id: int, original_message_id: int):
        """Пересылает ответ UserBot в тему пользователя для сохранения истории переписки"""
        try:
            # Получаем тему пользователя
            user_topic_id = await self.topic_manager._get_active_user_topic(chat_id, user_id)
            
            if user_topic_id:
                # Проверяем, был ли ответ уже отправлен в тему пользователя
                # Если sent_message уже имеет message_thread_id равный user_topic_id, то не пересылаем
                if hasattr(sent_message, 'message_thread_id') and sent_message.message_thread_id == user_topic_id:
                    logger.info(f"[USERBOT][FORWARD] Reply already sent to user topic {user_topic_id}, skipping forwarding")
                    return
                
                # Пересылаем ответ в тему пользователя только если он был отправлен не в тему
                await self.bot.forward_message(
                    chat_id=chat_id,
                    from_chat_id=chat_id,
                    message_id=sent_message.message_id,
                    message_thread_id=user_topic_id
                )
                logger.info(f"[USERBOT][FORWARD] Reply forwarded to user topic {user_topic_id} for user {user_id}")
            else:
                logger.warning(f"[USERBOT][FORWARD] No user topic found for user {user_id} in chat {chat_id}")
                
        except Exception as e:
            error_msg = str(e).lower()
            if "message thread not found" in error_msg:
                logger.warning(f"[USERBOT][FORWARD] User topic not found, will skip forwarding reply")
            else:
                logger.error(f"[USERBOT][FORWARD] Error forwarding reply to user topic: {e}")

    async def _create_task_directly(self, message_data: dict, status: str = "unreacted") -> str:
        """Создает задачу напрямую без агрегатора"""
        try:
            logger.info(f"[USERBOT][DIRECT] Starting direct task creation...")
            
            # Подготавливаем данные задачи
            task_data = {
                "message_id": message_data["message_id"],
                "chat_id": message_data["chat_id"],
                "chat_title": message_data["chat_title"],
                "chat_type": message_data["chat_type"],
                "user_id": message_data["user_id"],
                "first_name": message_data["first_name"],
                "last_name": message_data["last_name"],
                "username": message_data["username"],
                "language_code": message_data["language_code"],
                "is_bot": message_data["is_bot"],
                "text": message_data["text"],
                "status": status,
                "task_number": None,
                "assignee": None,
                "task_link": None,
                "reply": None,
                "created_at": message_data["created_at"],
                "updated_at": message_data["updated_at"],
                "aggregated": False,  # Не агрегированная задача
                "message_count": 1,
                # НОВОЕ ПОЛЕ: источник сообщения для корректной логики ответов
                "message_source": message_data.get("message_source", "main_menu"),
                # Медиафайлы - включаем ВСЕ поля, включая пути к файлам
                "has_photo": message_data.get("has_photo", False),
                "has_video": message_data.get("has_video", False),
                "has_document": message_data.get("has_document", False),
                "photo_file_ids": message_data.get("photo_file_ids", []),
                "photo_file_paths": message_data.get("photo_file_paths", []),  # ВАЖНО: пути к файлам фото
                "video_file_id": message_data.get("video_file_id"),
                "video_file_path": message_data.get("video_file_path"),  # ВАЖНО: путь к файлу видео
                "document_file_id": message_data.get("document_file_id"),
                "document_file_path": message_data.get("document_file_path"),  # ВАЖНО: путь к файлу документа
                "media_group_id": message_data.get("media_group_id"),
                "support_media_message_id": message_data.get("support_media_message_id")
            }
            logger.info(f"[USERBOT][DIRECT] Task data prepared with {len(task_data)} fields")
            logger.info(f"[USERBOT][DIRECT] support_media_message_id in task_data: {task_data.get('support_media_message_id')}")
            
            # Debug: проверяем что пути к файлам включены в task_data
            if task_data.get("has_photo"):
                logger.info(f"📋 [DIRECT] Photo file paths in task_data: {task_data.get('photo_file_paths')}")
            
            # Сохраняем задачу в Redis
            logger.info(f"[USERBOT][DIRECT] Saving task to Redis...")
            task_id = await self.redis.save_task(task_data)
            logger.info(f"[USERBOT][DIRECT] Task saved with ID: {task_id}")
            
            # Небольшая задержка для гарантии сохранения
            await asyncio.sleep(0.1)
            
            # Публикуем событие о новой задаче
            logger.info(f"[USERBOT][DIRECT] Publishing task event...")
            await self._publish_task_event(task_id, task_data)
            logger.info(f"[USERBOT][DIRECT] Task event published")
            
            logger.info(f"[USERBOT][DIRECT] ✅ Task created successfully: {task_id}")
            return task_id
            
        except Exception as e:
            logger.error(f"[USERBOT][DIRECT] ❌ Error creating task directly: {e}", exc_info=True)
            return None
    
    async def _publish_task_event(self, task_id: str, task_data: dict):
        """Публикует событие о новой задаче"""
        try:
            logger.info(f"[PUBSUB][PUBLISH] Starting task event publication for task: {task_id}")
            await self.redis._ensure_connection()
            logger.info(f"[PUBSUB][PUBLISH] Redis connection ensured")
            
            event_data = {
                'task_id': task_id,
                'type': 'new_task',
                'user_id': int(task_data.get('user_id', 0)),
                'username': task_data.get('username', ''),
                'text': task_data.get('text', '')
            }
            logger.info(f"[PUBSUB][PUBLISH] Event data prepared: {event_data}")
            
            event_json = json.dumps(event_data)
            logger.info(f"[PUBSUB][PUBLISH] Event serialized to JSON: {len(event_json)} chars")
            
            result = await self.redis.conn.publish('new_tasks', event_json)
            logger.info(f"[PUBSUB][PUBLISH] ✅ Published to 'new_tasks' channel, subscribers notified: {result}")
            logger.info(f"[USERBOT][STEP 2] Published event to new_tasks: {event_data}")
        except Exception as e:
            logger.error(f"[PUBSUB][PUBLISH] ❌ Error publishing task event: {e}", exc_info=True)

    async def _prepare_message_data(self, message: types.Message) -> dict:
        """Подготавливает данные сообщения для сохранения"""
        # Скачиваем медиафайлы (если есть) и сохраняем пути к ним
        media_data = await self._extract_media_data(message)
        
        # Debug: проверяем что медиа данные содержат пути к файлам
        if media_data.get("has_photo"):
            logger.info(f"🔍 Media data for photo: photo_file_paths={media_data.get('photo_file_paths')}, photo_file_ids={media_data.get('photo_file_ids')}")
        
        # Определяем источник сообщения: главное меню или тема пользователя
        # Если есть message_thread_id - это тема, иначе - главное меню
        if (hasattr(message, 'message_thread_id') and message.message_thread_id):
            message_source = "user_topic"
            logger.info(f"[USERBOT][SOURCE] Message from user topic {message.message_thread_id} in chat {message.chat.id}")
        else:
            message_source = "main_menu"
            logger.info(f"[USERBOT][SOURCE] Message from main menu (chat {message.chat.id})")
        
        task_data = {
            "message_id": message.message_id,
            "chat_id": message.chat.id,
            "chat_title": getattr(message.chat, 'title', 'Private Chat'),
            "chat_type": message.chat.type,
            "user_id": message.from_user.id,
            "first_name": message.from_user.first_name,
            "last_name": message.from_user.last_name,
            "username": message.from_user.username,
            "language_code": message.from_user.language_code,
            "is_bot": message.from_user.is_bot,
            "text": message.text or message.caption or "",
            "status": "unreacted",
            "task_number": None,
            "assignee": None,
            "task_link": None,
            "reply": None,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
            "aggregated": False,
            "message_count": 1,
            # НОВОЕ ПОЛЕ: источник сообщения
            "message_source": message_source,
            # Медиафайлы
            "has_photo": media_data["has_photo"],
            "has_video": media_data["has_video"],
            "has_document": media_data["has_document"],
            "photo_file_ids": media_data["photo_file_ids"],
            "photo_file_paths": media_data.get("photo_file_paths", []),
            "video_file_id": media_data["video_file_id"],
            "video_file_path": media_data.get("video_file_path"),
            "document_file_id": media_data["document_file_id"],
            "document_file_path": media_data.get("document_file_path"),
            "media_group_id": media_data.get("media_group_id")
        }
        
        # Debug: проверяем что task_data содержит пути к файлам
        if task_data.get("has_photo"):
            logger.info(f"📋 Task data for photo: photo_file_paths={task_data.get('photo_file_paths')}, photo_file_ids={task_data.get('photo_file_ids')}")
        
        return task_data

    async def _find_user_active_task(self, user_id: int) -> Optional[str]:
        """Ищет активную задачу пользователя (статусы: unreacted, waiting, in_progress)"""
        try:
            logger.info(f"[USERBOT][GROUPING] Searching for active task for user {user_id}")
            
            # Получаем все задачи пользователя
            user_tasks = await self.redis.get_user_tasks(user_id)
            logger.info(f"[USERBOT][GROUPING] Found {len(user_tasks)} total tasks for user {user_id}")
            
            # Ищем задачи в активных статусах
            active_statuses = ['unreacted', 'waiting', 'in_progress']
            active_tasks = []
            
            for task in user_tasks:
                task_id = task.get('task_id')
                status = task.get('status', 'unreacted')
                logger.info(f"[USERBOT][GROUPING] Task {task_id}: status={status}")
                
                if status in active_statuses:
                    # КРИТИЧЕСКАЯ ПРОВЕРКА: действительно ли задача существует в Redis
                    try:
                        actual_task = await self.redis.get_task(task_id)
                        # ИСПРАВЛЕНИЕ: get_task возвращает {} для несуществующих задач, а не None
                        task_exists = actual_task and len(actual_task) > 0 and 'user_id' in actual_task and 'status' in actual_task
                        
                        if task_exists and actual_task.get('status') in active_statuses:
                            # Двойная проверка статуса - и в списке, и в самой задаче
                            active_tasks.append(task)
                            logger.info(f"[USERBOT][GROUPING] Task {task_id} is active (status: {status}) and exists in Redis")
                        else:
                            if not task_exists:
                                logger.warning(f"[USERBOT][GROUPING] Task {task_id} appears active but doesn't exist in Redis - DELETED TASK, skipping")
                            else:
                                logger.warning(f"[USERBOT][GROUPING] Task {task_id} status mismatch: list={status}, actual={actual_task.get('status')} - skipping")
                    except Exception as task_check_error:
                        logger.error(f"[USERBOT][GROUPING] Error checking task {task_id} existence: {task_check_error}")
            
            if not active_tasks:
                logger.info(f"[USERBOT][GROUPING] No active tasks found for user {user_id}")
                return None
            
            # Если есть несколько активных задач, берем самую новую (по created_at)
            if len(active_tasks) > 1:
                logger.info(f"[USERBOT][GROUPING] Found {len(active_tasks)} active tasks, selecting newest")
                active_tasks.sort(key=lambda x: x.get('created_at', ''), reverse=True)
            
            selected_task = active_tasks[0]
            task_id = selected_task.get('task_id')
            logger.info(f"[USERBOT][GROUPING] Selected active task: {task_id} (status: {selected_task.get('status')})")
            
            # ДОПОЛНИТЕЛЬНАЯ ПРОВЕРКА: убедимся, что задача действительно существует и активна
            final_task_check = await self.redis.get_task(task_id)
            final_task_exists = final_task_check and len(final_task_check) > 0 and 'user_id' in final_task_check and 'status' in final_task_check
            if not final_task_exists or final_task_check.get('status') not in active_statuses:
                logger.warning(f"[USERBOT][GROUPING] Final check failed for task {task_id} - task may have been deleted during processing")
                return None
            
            return task_id
            
        except Exception as e:
            logger.error(f"[USERBOT][GROUPING] Error finding active task for user {user_id}: {e}")
            return None
    
    async def _append_to_existing_task(self, task_id: str, message: types.Message) -> bool:
        """Добавляет сообщение к существующей задаче и отправляет reply в чат поддержки"""
        try:
            logger.info(f"[USERBOT][GROUPING] === ЭТАП 3: Добавление к задаче {task_id} ===")
            
            # Получаем задачу из Redis
            task = await self.redis.get_task(task_id)
            # ИСПРАВЛЕНИЕ: get_task возвращает {} для несуществующих задач, а не None
            if not task or len(task) == 0 or 'user_id' not in task:
                logger.error(f"[USERBOT][GROUPING] Задача {task_id} не найдена в Redis или удалена")
                return False
            
            logger.info(f"[USERBOT][GROUPING] Задача {task_id} найдена, статус: {task.get('status')}")
            
            # ДОПОЛНИТЕЛЬНАЯ ПРОВЕРКА: убедимся, что задача все еще активна
            active_statuses = ['unreacted', 'waiting', 'in_progress']
            if task.get('status') not in active_statuses:
                logger.warning(f"[USERBOT][GROUPING] Задача {task_id} больше не активна (статус: {task.get('status')}), не добавляем сообщение")
                return False
            
            # Подготавливаем данные нового сообщения
            new_message_text = message.text or message.caption or ""
            
            # Извлекаем медиаданные из дополнительного сообщения
            media_data = await self._extract_media_data(message)
            has_media = media_data.get('has_photo') or media_data.get('has_video') or media_data.get('has_document')
            
            # Если нет ни текста, ни медиа - пропускаем
            if not new_message_text and not has_media:
                logger.warning(f"[USERBOT][GROUPING] Пустое сообщение без медиа, пропускаем")
                return False
            
            # Обновляем текст задачи
            current_text = task.get('text', '')
            if new_message_text:
                updated_text = f"{current_text}\n\n--- Дополнительное сообщение ---\n{new_message_text}"
            else:
                updated_text = current_text  # Только медиа без текста
            
            # Обновляем счетчик сообщений
            current_count = int(task.get('message_count', 1))
            new_count = current_count + 1
            
            # Объединяем медиаданные с существующими
            if has_media:
                logger.info(f"[USERBOT][GROUPING] Добавляем медиа к задаче {task_id}")
                
                # Объединяем фото
                if media_data.get('has_photo'):
                    current_photo_ids = task.get('photo_file_ids', [])
                    current_photo_paths = task.get('photo_file_paths', [])
                    
                    new_photo_ids = media_data.get('photo_file_ids', [])
                    new_photo_paths = media_data.get('photo_file_paths', [])
                    
                    task['photo_file_ids'] = current_photo_ids + new_photo_ids
                    task['photo_file_paths'] = current_photo_paths + new_photo_paths
                    task['has_photo'] = True
                    
                    logger.info(f"[USERBOT][GROUPING] Добавлено {len(new_photo_ids)} фото к задаче {task_id}")
                
                # Объединяем видео (заменяем, так как обычно одно видео на сообщение)
                if media_data.get('has_video'):
                    task['video_file_id'] = media_data.get('video_file_id')
                    task['video_file_path'] = media_data.get('video_file_path')
                    task['has_video'] = True
                    logger.info(f"[USERBOT][GROUPING] Добавлено видео к задаче {task_id}")
                
                # Объединяем документы (заменяем)
                if media_data.get('has_document'):
                    task['document_file_id'] = media_data.get('document_file_id')
                    task['document_file_path'] = media_data.get('document_file_path')
                    task['has_document'] = True
                    logger.info(f"[USERBOT][GROUPING] Добавлен документ к задаче {task_id}")
            
            # Обновляем задачу в Redis
            task['text'] = updated_text
            task['message_count'] = new_count
            task['updated_at'] = datetime.now().isoformat()
            
            await self.redis.update_task(task_id, **task)
            logger.info(f"[USERBOT][GROUPING] Задача {task_id} обновлена: {new_count} сообщений")
            
            # Публикуем событие об обновлении задачи
            # Получаем user_topic_id для корректного reply
            user_topic_id = None
            try:
                user_topic_id = await self.topic_manager.get_or_create_user_topic(
                    user_id=message.from_user.id,
                    chat_id=message.chat.id
                )
            except Exception as topic_error:
                logger.warning(f"[USERBOT][GROUPING] Не удалось получить user_topic_id: {topic_error}")
            
            event_data = {
                "type": "task_update",
                "action": "message_appended",
                "task_id": task_id,
                "updated_text": new_message_text,
                "message_count": new_count,
                "has_media": has_media,
                "user_message_id": message.message_id,  # Критично важно для reply
                "user_chat_id": message.chat.id,
                "user_topic_id": user_topic_id
            }
            
            # Добавляем информацию о медиафайлах в событие
            if has_media:
                event_data.update({
                    "has_photo": media_data.get('has_photo', False),
                    "has_video": media_data.get('has_video', False),
                    "has_document": media_data.get('has_document', False),
                    "photo_file_ids": media_data.get('photo_file_ids', []),
                    "photo_file_paths": media_data.get('photo_file_paths', []),
                    "video_file_id": media_data.get('video_file_id'),
                    "video_file_path": media_data.get('video_file_path'),
                    "document_file_id": media_data.get('document_file_id'),
                    "document_file_path": media_data.get('document_file_path')
                })
            
            await self.redis.publish_event("task_updates", event_data)
            logger.info(f"[USERBOT][GROUPING] Опубликовано событие message_appended для задачи {task_id}")
            
            # Отправляем сообщение в пользовательскую тему, если она существует
            # ИСПРАВЛЕНИЕ: проверяем источник текущего сообщения, а не задачи
            try:
                user_topic_id = await self.topic_manager.get_or_create_user_topic(
                    user_id=message.from_user.id,
                    chat_id=message.chat.id
                )
                
                # Проверяем, не пишет ли пользователь уже в своей теме
                current_thread_id = getattr(message, 'message_thread_id', None)
                user_in_own_topic = current_thread_id and current_thread_id == user_topic_id
                
                # Пересылаем сообщение в тему пользователя только если он не в своей теме
                if user_topic_id and not user_in_own_topic:
                    # Пересылаем оригинальное сообщение пользователя в его тему
                    forwarded_message = await self.bot.forward_message(
                        chat_id=message.chat.id,
                        from_chat_id=message.chat.id,
                        message_id=message.message_id,
                        message_thread_id=user_topic_id
                    )
                    
                    logger.info(f"[USERBOT][GROUPING] ✅ Переслано дополнительное сообщение {message.message_id} в тему пользователя {user_topic_id} (forwarded as {forwarded_message.message_id})")
                elif user_in_own_topic:
                    logger.info(f"[USERBOT][GROUPING] Пользователь уже пишет в своей теме {user_topic_id}, пересылка не требуется")
                    
            except Exception as topic_error:
                logger.warning(f"[USERBOT][GROUPING] Не удалось отправить сообщение в тему пользователя: {topic_error}")
            
            # Устанавливаем реакцию на сообщение
            try:
                await self.bot.set_message_reaction(
                    chat_id=message.chat.id,
                    message_id=message.message_id,
                    reaction=[{"type": "emoji", "emoji": "🔄"}]  # Символ "обновление"
                )
                logger.info(f"[USERBOT][GROUPING] Установлена реакция 🔄 на сообщение {message.message_id}")
            except Exception as reaction_error:
                logger.warning(f"[USERBOT][GROUPING] Не удалось установить реакцию: {reaction_error}")
            
            logger.info(f"[USERBOT][GROUPING] ✅ ЭТАП 3 ЗАВЕРШЕН: Сообщение добавлено к задаче {task_id}")
            return True
            
        except Exception as e:
            logger.error(f"[USERBOT][GROUPING] Ошибка при добавлении сообщения к задаче {task_id}: {e}")
            return False

    async def _download_media_file(self, file_id: str, file_extension: str) -> Optional[str]:
        """Скачивает медиа файл и возвращает путь к нему"""
        try:
            # Получаем информацию о файле
            file_info = await self.bot.get_file(file_id)
            
            # Генерируем уникальное имя файла
            unique_filename = f"{uuid.uuid4()}.{file_extension}"
            file_path = os.path.join("temp_media", unique_filename)
            
            # Создаем директорию если не существует
            os.makedirs("temp_media", exist_ok=True)
            
            # Скачиваем файл
            await self.bot.download_file(file_info.file_path, file_path)
            
            logger.info(f"Downloaded media file: {file_path} (size: {os.path.getsize(file_path)} bytes)")
            return file_path
            
        except Exception as e:
            logger.error(f"Error downloading media file {file_id}: {e}")
            return None

    async def _extract_media_data(self, message: types.Message) -> dict:
        """Извлекает данные о медиафайлах из сообщения и скачивает файлы"""
        media_data = {
            "has_photo": False,
            "has_video": False,
            "has_document": False,
            "photo_file_ids": [],
            "photo_file_paths": [],  # Пути к скачанным файлам
            "video_file_id": None,
            "video_file_path": None,  # Путь к скачанному файлу
            "document_file_id": None,
            "document_file_path": None,  # Путь к скачанному файлу
            "caption": None
        }
        
        # Проверяем фото
        if message.photo:
            media_data["has_photo"] = True
            # Сохраняем все размеры фото (Telegram предоставляет несколько размеров)
            media_data["photo_file_ids"] = [p.file_id for p in message.photo]
            # Скачиваем фото (берем самое большое разрешение)
            try:
                largest_photo = message.photo[-1]  # Последнее фото - самое большое
                downloaded_path = await self._download_media_file(largest_photo.file_id, "jpg")
                if downloaded_path:
                    media_data["photo_file_paths"] = [downloaded_path]
                    logger.info(f"Downloaded photo from message {message.message_id}: {downloaded_path}")
                    logger.info(f"✅ Set photo_file_paths in media_data: {media_data['photo_file_paths']}")
                else:
                    logger.warning(f"Failed to download photo from message {message.message_id}")
            except Exception as e:
                logger.error(f"Error processing photo from message {message.message_id}: {e}")
            
            logger.info(f"Found photo in message {message.message_id}, file_ids: {media_data['photo_file_ids']}")
        
        # Проверяем видео
        if message.video:
            media_data["has_video"] = True
            media_data["video_file_id"] = message.video.file_id
            logger.info(f"Found video in message {message.message_id}, file_id: {media_data['video_file_id']}")
            try:
                downloaded_path = await self._download_media_file(message.video.file_id, "mp4")
                if downloaded_path:
                    media_data["video_file_path"] = downloaded_path
                    logger.info(f"Downloaded video from message {message.message_id}: {downloaded_path}")
                else:
                    logger.warning(f"Failed to download video from message {message.message_id}")
            except Exception as e:
                logger.error(f"Error processing video from message {message.message_id}: {e}")
        
        # Проверяем документы (включая видео-заметки, анимации и т.д.)
        if message.document:
            media_data["has_document"] = True
            media_data["document_file_id"] = message.document.file_id
            logger.info(f"Found document in message {message.message_id}, file_id: {media_data['document_file_id']}")
            try:
                # Определяем расширение файла
                file_extension = "bin"  # По умолчанию
                if message.document.file_name:
                    file_extension = message.document.file_name.split(".")[-1] if "." in message.document.file_name else "bin"
                
                downloaded_path = await self._download_media_file(message.document.file_id, file_extension)
                if downloaded_path:
                    media_data["document_file_path"] = downloaded_path
                    logger.info(f"Downloaded document from message {message.message_id}: {downloaded_path}")
                else:
                    logger.warning(f"Failed to download document from message {message.message_id}")
            except Exception as e:
                logger.error(f"Error processing document from message {message.message_id}: {e}")
        
        # Проверяем видео-заметки
        if message.video_note:
            media_data["has_video"] = True
            media_data["video_file_id"] = message.video_note.file_id
            logger.info(f"Found video note in message {message.message_id}, file_id: {media_data['video_file_id']}")
            try:
                downloaded_path = await self._download_media_file(message.video_note.file_id, "mp4")
                if downloaded_path:
                    media_data["video_file_path"] = downloaded_path
                    logger.info(f"Downloaded video note from message {message.message_id}: {downloaded_path}")
                else:
                    logger.warning(f"Failed to download video note from message {message.message_id}")
            except Exception as e:
                logger.error(f"Error processing video note from message {message.message_id}: {e}")
        
        # Проверяем анимации (GIF)
        if message.animation:
            media_data["has_document"] = True
            media_data["document_file_id"] = message.animation.file_id
            logger.info(f"Found animation in message {message.message_id}, file_id: {media_data['document_file_id']}")
            try:
                downloaded_path = await self._download_media_file(message.animation.file_id, "gif")
                if downloaded_path:
                    media_data["document_file_path"] = downloaded_path
                    logger.info(f"Downloaded animation from message {message.message_id}: {downloaded_path}")
                else:
                    logger.warning(f"Failed to download animation from message {message.message_id}")
            except Exception as e:
                logger.error(f"Error processing animation from message {message.message_id}: {e}")
        
        # Сохраняем подпись
        if message.caption:
            media_data["caption"] = message.caption
        
        return media_data

    async def _send_media_reply(self, chat_id: int, topic_id: int, reply_text: str, reply_author: str, update_data: dict):
        """Отправляет ответ с медиафайлами как fallback"""
        try:
            # Формируем текст ответа (без указания автора для единообразия с пересланными сообщениями)
            message_text = f"💬 <b>Ответ поддержки:</b>\n\n{reply_text}" if reply_text else f"💬 <b>Ответ поддержки</b>"
            
            media_sent = False
            
            # Пытаемся отправить фото, если есть
            if update_data.get('has_photo') and update_data.get('photo_file_ids'):
                try:
                    # Безопасное получение photo_file_ids
                    photo_file_ids = update_data.get('photo_file_ids', [])
                    
                    # Проверяем, что это список, а не строка
                    if isinstance(photo_file_ids, str):
                        try:
                            import json
                            photo_file_ids = json.loads(photo_file_ids)
                        except:
                            logger.warning(f"Could not parse photo_file_ids string: {photo_file_ids}")
                            raise ValueError("Invalid photo_file_ids format")
                    
                    if not photo_file_ids or not isinstance(photo_file_ids, list):
                        logger.warning(f"Invalid photo_file_ids: {photo_file_ids}")
                        raise ValueError("Invalid photo_file_ids")
                    
                    # Используем последний (наибольший) размер фото
                    photo_file_id = photo_file_ids[-1]
                    
                    # Проверяем валидность file_id
                    if not photo_file_id or len(photo_file_id) < 10:
                        logger.warning(f"Invalid photo file_id in reply: {photo_file_id}")
                        raise ValueError("Invalid photo file_id")
                    
                    await self.bot.send_photo(
                        chat_id=chat_id,
                        photo=photo_file_id,
                        caption=message_text,
                        message_thread_id=topic_id,
                        parse_mode="HTML"
                    )
                    logger.info(f"Sent photo reply to topic {topic_id} in chat {chat_id}")
                    media_sent = True
                except Exception as photo_error:
                    logger.warning(f"Failed to send photo reply (file_id: {update_data.get('photo_file_ids', [])}): {photo_error}")
                    # Продолжаем с fallback на текстовое сообщение
            
            # Пытаемся отправить видео, если фото не отправилось
            elif not media_sent and update_data.get('has_video') and update_data.get('video_file_id'):
                try:
                    video_file_id = update_data['video_file_id']
                    
                    # Проверяем валидность file_id
                    if not video_file_id or len(video_file_id) < 10:
                        logger.warning(f"Invalid video file_id in reply: {video_file_id}")
                        raise ValueError("Invalid video file_id")
                    
                    await self.bot.send_video(
                        chat_id=chat_id,
                        video=video_file_id,
                        caption=message_text,
                        message_thread_id=topic_id,
                        parse_mode="HTML"
                    )
                    logger.info(f"Sent video reply to topic {topic_id} in chat {chat_id}")
                    media_sent = True
                except Exception as video_error:
                    logger.warning(f"Failed to send video reply (file_id: {update_data.get('video_file_id')}): {video_error}")
                    # Продолжаем с fallback на текстовое сообщение
            
            # Пытаемся отправить документ, если медиа не отправилось
            elif not media_sent and update_data.get('has_document') and update_data.get('document_file_id'):
                try:
                    document_file_id = update_data['document_file_id']
                    
                    # Пересылаем ответ в тему пользователя
                    forwarded_to_topic = False
            
                    # Приоритет: пересылка из темы медиа в чате поддержки (если есть медиа)
                    reply_support_media_message_id = update_data.get('reply_support_media_message_id')
                    if topic_id and reply_support_media_message_id and chat_id:
                        try:
                            logger.info(f"Attempting to forward media reply from support media message {reply_support_media_message_id} to topic {topic_id} in chat {chat_id}")
                            await self.bot.forward_message(
                                chat_id=chat_id,
                                from_chat_id=settings.FORUM_CHAT_ID,
                                message_id=reply_support_media_message_id,
                                message_thread_id=topic_id
                            )
                            logger.info(f"Successfully forwarded media reply from support media to user topic {topic_id} in chat {chat_id}")
                            forwarded_to_topic = True
                        except Exception as e:
                            logger.warning(f"Failed to forward media reply from support media: {e}")
                            # Продолжаем с fallback
            
                    # Fallback: пересылка оригинального сообщения поддержки
                    elif topic_id and update_data.get('reply_message_id') and update_data.get('reply_chat_id') and chat_id and not forwarded_to_topic:
                        try:
                            logger.info(f"Fallback: attempting to forward original message {update_data.get('reply_message_id')} from chat {update_data.get('reply_chat_id')} to topic {topic_id} in chat {chat_id}")
                            await self.bot.forward_message(
                                chat_id=chat_id,
                                from_chat_id=update_data.get('reply_chat_id'),
                                message_id=update_data.get('reply_message_id'),
                                message_thread_id=topic_id
                            )
                            logger.info(f"Successfully forwarded original reply message to user topic {topic_id} in chat {chat_id}")
                            forwarded_to_topic = True
                        except Exception as e:
                            error_msg = str(e).lower()
                            logger.warning(f"Could not forward reply to user topic {topic_id} in chat {chat_id}: {e}")
                            
                            # Если тема не найдена, пробуем восстановить её
                            if "thread not found" in error_msg or "message thread not found" in error_msg:
                                logger.info(f"Topic {topic_id} not found, attempting to recreate for user {update_data.get('user_id')}")
                                try:
                                    # Удаляем старую тему из кэша
                                    await self.topic_manager._delete_user_topic_cache(chat_id, update_data.get('user_id'))
                                    
                                    # Создаём новую тему
                                    new_topic_id = await self.topic_manager.get_or_create_user_topic(
                                        chat_id,
                                        update_data.get('user_id'),
                                        update_data.get('username'),
                                        update_data.get('first_name')
                                    )
                                    
                                    if new_topic_id:
                                        logger.info(f"Created new topic {new_topic_id} for user {update_data.get('user_id')}, retrying forward")
                                        # Повторяем попытку пересылки с новой темой
                                        await self.bot.forward_message(
                                            chat_id=chat_id,
                                            from_chat_id=update_data.get('reply_chat_id'),
                                            message_id=update_data.get('reply_message_id'),
                                            message_thread_id=new_topic_id
                                        )
                                        logger.info(f"Successfully forwarded reply to recreated topic {new_topic_id}")
                                        forwarded_to_topic = True
                                        topic_id = new_topic_id  # Обновляем topic_id для fallback
                                    else:
                                        logger.error(f"Failed to recreate topic for user {update_data.get('user_id')}")
                                        
                                except Exception as retry_e:
                                    logger.error(f"Failed to recreate topic and retry forward: {retry_e}")
                            
                            elif "chat not found" in error_msg:
                                logger.error(f"Chat {chat_id} not found - bot may not be added to this chat or chat doesn't exist")
                            elif "bot is not a member" in error_msg:
                                logger.error(f"Bot is not a member of chat {chat_id}")
                            elif "not enough rights" in error_msg:
                                logger.error(f"Bot doesn't have enough rights in chat {chat_id}")
                            
                            # В любом случае forwarded_to_topic остаётся False для fallback
                    
                    # Проверяем валидность file_id
                    if not document_file_id or len(document_file_id) < 10:
                        logger.warning(f"Invalid document file_id in reply: {document_file_id}")
                        raise ValueError("Invalid document file_id")
                    
                    await self.bot.send_document(
                        chat_id=chat_id,
                        document=document_file_id,
                        caption=message_text,
                        message_thread_id=topic_id,
                        parse_mode="HTML"
                    )
                    logger.info(f"Sent document reply to topic {topic_id} in chat {chat_id}")
                    media_sent = True
                except Exception as doc_error:
                    logger.warning(f"Failed to send document reply (file_id: {update_data.get('document_file_id')}): {doc_error}")
                    # Продолжаем с fallback на текстовое сообщение
            
            # Если медиа не отправилось, отправляем fallback-сообщение
            if not media_sent:
                # Не добавляем информацию о медиа - пользователь не хочет видеть такие уведомления
                # Оставляем только чистый текст ответа
                
                # Отправляем fallback-сообщение
                await self.bot.send_message(
                    chat_id=chat_id,
                    text=message_text,
                    message_thread_id=topic_id,
                    parse_mode="HTML"
                )
                logger.info(f"Sent text reply (media fallback) to topic {topic_id} in chat {chat_id}")
            
            return True
                
        except Exception as e:
            logger.error(f"Failed to send media reply to topic {topic_id} in chat {chat_id}: {e}")
            return False

    async def _send_media_reply_direct(self, chat_id: int, original_message_id: int, reply_text: str, reply_author: str, update_data: dict):
        """Отправляет медиа-ответ напрямую пользователю используя файловую систему"""
        try:
            from aiogram.types import FSInputFile
            
            # Формируем текст ответа
            message_text = f"💬 <b>Ответ поддержки:</b>\n\n{reply_text}" if reply_text else f"💬 <b>Ответ поддержки</b>"
            
            media_sent = False
            
            # Пытаемся отправить фото из файла, если есть
            if update_data.get('has_photo') and update_data.get('photo_file_paths'):
                try:
                    photo_file_paths = update_data.get('photo_file_paths', [])
                    
                    if photo_file_paths and len(photo_file_paths) > 0:
                        photo_path = photo_file_paths[0]  # Берём первое фото
                        
                        # Проверяем существование файла
                        if not os.path.exists(photo_path):
                            logger.warning(f"Photo file not found: {photo_path}")
                            raise FileNotFoundError(f"Photo file not found: {photo_path}")
                        
                        photo_file = FSInputFile(photo_path)
                        
                        await self.bot.send_photo(
                            chat_id=chat_id,
                            photo=photo_file,
                            caption=message_text,
                            reply_to_message_id=original_message_id if original_message_id else None,
                            parse_mode="HTML"
                        )
                        media_sent = True
                        logger.info(f"✅ Sent photo reply from file: {photo_path}")
                        
                        # НЕ удаляем временный файл - он может понадобиться для дальнейших операций
                        logger.info(f"📁 Сохраняем временный файл для возможного использования: {photo_path}")
                            
                except Exception as photo_error:
                    logger.warning(f"Failed to send direct photo reply from file: {photo_error}")
                    # Продолжаем с fallback на текстовое сообщение
            
            # Пытаемся отправить видео из файла, если фото не отправилось
            elif not media_sent and update_data.get('has_video') and update_data.get('video_file_path'):
                try:
                    video_path = update_data.get('video_file_path')
                    
                    if video_path and os.path.exists(video_path):
                        video_file = FSInputFile(video_path)
                        
                        await self.bot.send_video(
                            chat_id=chat_id,
                            video=video_file,
                            caption=message_text,
                            reply_to_message_id=original_message_id if original_message_id else None,
                            parse_mode="HTML"
                        )
                        media_sent = True
                        logger.info(f"✅ Sent video reply from file: {video_path}")
                        
                        # НЕ удаляем временный файл - он может понадобиться для дальнейших операций
                        logger.info(f"📁 Сохраняем временный файл для возможного использования: {video_path}")
                    else:
                        logger.warning(f"Video file not found: {video_path}")
                        raise FileNotFoundError(f"Video file not found: {video_path}")
                            
                except Exception as video_error:
                    logger.warning(f"Failed to send direct video reply from file: {video_error}")
                    # Продолжаем с fallback на текстовое сообщение
            
            # Пытаемся отправить документ из файла, если медиа не отправилось
            elif not media_sent and update_data.get('has_document') and update_data.get('document_file_path'):
                try:
                    document_path = update_data.get('document_file_path')
                    
                    if document_path and os.path.exists(document_path):
                        document_file = FSInputFile(document_path)
                        
                        await self.bot.send_document(
                            chat_id=chat_id,
                            document=document_file,
                            caption=message_text,
                            reply_to_message_id=original_message_id if original_message_id else None,
                            parse_mode="HTML"
                        )
                        media_sent = True
                        logger.info(f"✅ Sent document reply from file: {document_path}")
                        
                        # НЕ удаляем временный файл - он может понадобиться для дальнейших операций
                        logger.info(f"📁 Сохраняем временный файл для возможного использования: {document_path}")
                    else:
                        logger.warning(f"Document file not found: {document_path}")
                        raise FileNotFoundError(f"Document file not found: {document_path}")
                            
                except Exception as doc_error:
                    logger.warning(f"Failed to send direct document reply from file: {doc_error}")
                    # Продолжаем с fallback на текстовое сообщение
            
            # Если медиа не отправилось, отправляем fallback-сообщение
            if not media_sent:
                # Не добавляем информацию о медиа - пользователь не хочет видеть такие уведомления
                # Оставляем только чистый текст ответа
                
                # Отправляем fallback-сообщение
                await self.bot.send_message(
                    chat_id=chat_id,
                    text=message_text,
                    reply_to_message_id=original_message_id if original_message_id else None,
                    parse_mode="HTML"
                )
                logger.info(f"Sent text direct reply (media fallback) to chat {chat_id}")
            
            return media_sent
                
        except Exception as e:
            logger.error(f"Failed to send direct media reply to chat {chat_id}: {e}")
            return False

    async def _send_text_reply_direct(self, chat_id: int, original_message_id: int, reply_text: str, reply_author: str):
        """Отправляет текстовый ответ напрямую пользователю"""
        try:
            message_text = f"💬 <b>Ответ поддержки:</b>\n\n{reply_text}"
            
            if original_message_id:
                # Отправляем ответ как reply к оригинальному сообщению
                await self.bot.send_message(
                    chat_id=chat_id,
                    reply_to_message_id=int(original_message_id),
                    text=message_text,
                    parse_mode="HTML"
                )
            else:
                # Если нет ID оригинального сообщения, отправляем обычное сообщение
                await self.bot.send_message(
                    chat_id=chat_id,
                    text=message_text,
                    parse_mode="HTML"
                )
            return True
                
        except Exception as e:
            logger.error(f"Failed to send direct text reply to chat {chat_id}: {e}")
            return False

    async def _send_media_reply_direct_with_id(self, chat_id: int, original_message_id: int, reply_text: str, reply_author: str, update_data: dict) -> Optional[int]:
        """Отправляет медиа-ответ напрямую пользователю и возвращает ID отправленного сообщения"""
        try:
            from aiogram.types import FSInputFile
            
            # Формируем текст ответа
            message_text = f"💬 <b>Ответ поддержки:</b>\n\n{reply_text}" if reply_text else f"💬 <b>Ответ поддержки</b>"
            
            sent_message = None
            
            # Пытаемся отправить фото из файла, если есть
            if update_data.get('has_photo') and update_data.get('photo_file_paths'):
                try:
                    photo_file_paths = update_data.get('photo_file_paths', [])
                    
                    if photo_file_paths and len(photo_file_paths) > 0:
                        photo_path = photo_file_paths[0]  # Берём первое фото
                        
                        # Проверяем существование файла
                        if not os.path.exists(photo_path):
                            logger.warning(f"Photo file not found: {photo_path}")
                            raise FileNotFoundError(f"Photo file not found: {photo_path}")
                        
                        photo_file = FSInputFile(photo_path)
                        
                        sent_message = await self.bot.send_photo(
                            chat_id=chat_id,
                            photo=photo_file,
                            caption=message_text,
                            reply_to_message_id=original_message_id if original_message_id else None,
                            parse_mode="HTML"
                        )
                        logger.info(f"✅ Sent photo reply from file: {photo_path}")
                        logger.info(f"📁 Сохраняем временный файл для возможного использования: {photo_path}")
                        return sent_message.message_id
                            
                except Exception as photo_error:
                    logger.warning(f"Failed to send direct photo reply from file: {photo_error}")
            
            # Пытаемся отправить видео из файла, если фото не отправилось
            if not sent_message and update_data.get('has_video') and update_data.get('video_file_path'):
                try:
                    video_path = update_data.get('video_file_path')
                    
                    if video_path and os.path.exists(video_path):
                        video_file = FSInputFile(video_path)
                        
                        sent_message = await self.bot.send_video(
                            chat_id=chat_id,
                            video=video_file,
                            caption=message_text,
                            reply_to_message_id=original_message_id if original_message_id else None,
                            parse_mode="HTML"
                        )
                        logger.info(f"✅ Sent video reply from file: {video_path}")
                        logger.info(f"📁 Сохраняем временный файл для возможного использования: {video_path}")
                        return sent_message.message_id
                    else:
                        logger.warning(f"Video file not found: {video_path}")
                        raise FileNotFoundError(f"Video file not found: {video_path}")
                            
                except Exception as video_error:
                    logger.warning(f"Failed to send direct video reply from file: {video_error}")
            
            # Пытаемся отправить документ из файла, если медиа не отправилось
            if not sent_message and update_data.get('has_document') and update_data.get('document_file_path'):
                try:
                    document_path = update_data.get('document_file_path')
                    
                    if document_path and os.path.exists(document_path):
                        document_file = FSInputFile(document_path)
                        
                        sent_message = await self.bot.send_document(
                            chat_id=chat_id,
                            document=document_file,
                            caption=message_text,
                            reply_to_message_id=original_message_id if original_message_id else None,
                            parse_mode="HTML"
                        )
                        logger.info(f"✅ Sent document reply from file: {document_path}")
                        logger.info(f"📁 Сохраняем временный файл для возможного использования: {document_path}")
                        return sent_message.message_id
                    else:
                        logger.warning(f"Document file not found: {document_path}")
                        raise FileNotFoundError(f"Document file not found: {document_path}")
                            
                except Exception as document_error:
                    logger.warning(f"Failed to send direct document reply from file: {document_error}")
            
            # Если медиа не удалось отправить, возвращаем None
            return None
                
        except Exception as e:
            logger.error(f"Failed to send direct media reply to chat {chat_id}: {e}")
            return None

    async def _send_text_reply_direct_with_id(self, chat_id: int, original_message_id: int, reply_text: str, reply_author: str) -> Optional[int]:
        """Отправляет текстовый ответ напрямую пользователю и возвращает ID отправленного сообщения"""
        try:
            message_text = f"💬 <b>Ответ поддержки:</b>\n\n{reply_text}"
            
            if original_message_id:
                # Отправляем ответ как reply к оригинальному сообщению
                sent_message = await self.bot.send_message(
                    chat_id=chat_id,
                    reply_to_message_id=int(original_message_id),
                    text=message_text,
                    parse_mode="HTML"
                )
            else:
                # Если нет ID оригинального сообщения, отправляем обычное сообщение
                sent_message = await self.bot.send_message(
                    chat_id=chat_id,
                    text=message_text,
                    parse_mode="HTML"
                )
            return sent_message.message_id
                
        except Exception as e:
            logger.error(f"Failed to send direct text reply to chat {chat_id}: {e}")
            return None

    async def _send_media_to_support_topic(self, message: types.Message) -> Optional[int]:
        """Отправляет медиа в тему поддержки напрямую через send_photo/video/document"""
        try:
            # Получаем или создаем тему для медиа
            media_topic_id = await self._get_or_create_media_topic()
            if not media_topic_id:
                logger.error("Failed to get or create media topic")
                return None
            
            # Отправляем медиа напрямую, как в demo.txt
            sent_message = None
            
            if message.photo:
                # Берем самое большое фото
                photo = message.photo[-1]
                sent_message = await self.bot.send_photo(
                    chat_id=settings.FORUM_CHAT_ID,
                    message_thread_id=media_topic_id,
                    photo=photo.file_id,
                    caption=message.caption or f"Медиа от @{message.from_user.username or message.from_user.first_name}"
                )
                logger.info(f"Sent photo to media topic {media_topic_id} as {sent_message.message_id}")
                
            elif message.video:
                sent_message = await self.bot.send_video(
                    chat_id=settings.FORUM_CHAT_ID,
                    message_thread_id=media_topic_id,
                    video=message.video.file_id,
                    caption=message.caption or f"Медиа от @{message.from_user.username or message.from_user.first_name}"
                )
                logger.info(f"Sent video to media topic {media_topic_id} as {sent_message.message_id}")
                
            elif message.document:
                sent_message = await self.bot.send_document(
                    chat_id=settings.FORUM_CHAT_ID,
                    message_thread_id=media_topic_id,
                    document=message.document.file_id,
                    caption=message.caption or f"Медиа от @{message.from_user.username or message.from_user.first_name}"
                )
                logger.info(f"Sent document to media topic {media_topic_id} as {sent_message.message_id}")
                
            elif message.video_note:
                sent_message = await self.bot.send_video_note(
                    chat_id=settings.FORUM_CHAT_ID,
                    message_thread_id=media_topic_id,
                    video_note=message.video_note.file_id
                )
                logger.info(f"Sent video note to media topic {media_topic_id} as {sent_message.message_id}")
                
            elif message.animation:
                sent_message = await self.bot.send_animation(
                    chat_id=settings.FORUM_CHAT_ID,
                    message_thread_id=media_topic_id,
                    animation=message.animation.file_id,
                    caption=message.caption or f"Медиа от @{message.from_user.username or message.from_user.first_name}"
                )
                logger.info(f"Sent animation to media topic {media_topic_id} as {sent_message.message_id}")
            
            if sent_message:
                return sent_message.message_id
            else:
                logger.warning("No media found in message to send")
                return None
            
        except Exception as e:
            logger.error(f"Error sending media to support topic: {e}")
            return None
    
    async def _get_or_create_media_topic(self) -> Optional[int]:
        """Получает или создает тему для медиа в чате поддержки"""
        try:
            # Используем единый Redis клиент для совместимости с MoverBot
            from core.redis_client import redis_client
            await redis_client._ensure_connection()
            
            # Проверяем кэш
            media_topic_key = f"media_topic:{settings.FORUM_CHAT_ID}"
            cached_topic = await redis_client.get(media_topic_key)
            if cached_topic:
                logger.info(f"Found cached media topic: {cached_topic.decode() if isinstance(cached_topic, bytes) else cached_topic}")
                topic_id = cached_topic.decode() if isinstance(cached_topic, bytes) else cached_topic
                return int(topic_id)
            
            # Создаем новую тему для медиа
            topic_name = "📎 Медиафайлы задач"
            logger.info(f"Creating new media topic: {topic_name}")
            topic = await self.bot.create_forum_topic(
                chat_id=settings.FORUM_CHAT_ID,
                name=topic_name
            )
            
            # Сохраняем в кэш через единый Redis клиент
            await redis_client.set(media_topic_key, str(topic.message_thread_id))
            logger.info(f"Created and cached media topic '{topic_name}' with ID {topic.message_thread_id}")
            
            return topic.message_thread_id
        except Exception as e:
            logger.error(f"Failed to create media topic: {e}")
            return None

    async def _create_support_reply_with_media(self, chat_id: int, reply_text: str, reply_author: str, update_data: dict) -> Optional[int]:
        """Создает медиа-ответ в чате поддержки и возвращает message_id"""
        try:
            from aiogram.types import FSInputFile
            
            # Формируем текст ответа БЕЗ ника поддержки
            message_text = f"💬 <b>Ответ поддержки:</b>\n\n{reply_text}" if reply_text else f"💬 <b>Ответ поддержки</b>"
            
            sent_message = None
            
            # Пытаемся отправить фото из файла, если есть
            if update_data.get('has_photo') and update_data.get('photo_file_paths'):
                try:
                    photo_file_paths = update_data.get('photo_file_paths', [])
                    
                    if photo_file_paths and len(photo_file_paths) > 0:
                        photo_path = photo_file_paths[0]  # Берём первое фото
                        
                        # Проверяем существование файла
                        if not os.path.exists(photo_path):
                            logger.warning(f"Photo file not found: {photo_path}")
                            raise FileNotFoundError(f"Photo file not found: {photo_path}")
                        
                        photo_file = FSInputFile(photo_path)
                        
                        sent_message = await self.bot.send_photo(
                            chat_id=chat_id,
                            photo=photo_file,
                            caption=message_text,
                            parse_mode="HTML"
                        )
                        logger.info(f"✅ Created photo support reply: {sent_message.message_id}")
                        return sent_message.message_id
                            
                except Exception as photo_error:
                    logger.warning(f"Failed to create photo support reply: {photo_error}")
                    # Продолжаем с другими типами медиа
            
            # Пытаемся отправить видео из файла
            if update_data.get('has_video') and update_data.get('video_file_path'):
                try:
                    video_path = update_data.get('video_file_path')
                    
                    if video_path and os.path.exists(video_path):
                        video_file = FSInputFile(video_path)
                        
                        sent_message = await self.bot.send_video(
                            chat_id=chat_id,
                            video=video_file,
                            caption=message_text,
                            parse_mode="HTML"
                        )
                        logger.info(f"✅ Created video support reply: {sent_message.message_id}")
                        return sent_message.message_id
                        
                except Exception as video_error:
                    logger.warning(f"Failed to create video support reply: {video_error}")
                    # Продолжаем с документами
            
            # Пытаемся отправить документ из файла
            if update_data.get('has_document') and update_data.get('document_file_path'):
                try:
                    document_path = update_data.get('document_file_path')
                    
                    if document_path and os.path.exists(document_path):
                        document_file = FSInputFile(document_path)
                        
                        sent_message = await self.bot.send_document(
                            chat_id=chat_id,
                            document=document_file,
                            caption=message_text,
                            parse_mode="HTML"
                        )
                        logger.info(f"✅ Created document support reply: {sent_message.message_id}")
                        return sent_message.message_id
                        
                except Exception as document_error:
                    logger.warning(f"Failed to create document support reply: {document_error}")
            
            # Если медиа не удалось отправить, создаем текстовое сообщение
            logger.warning("No media could be sent, creating text support reply instead")
            return await self._create_support_reply_text(chat_id, reply_text, reply_author)
            
        except Exception as e:
            logger.error(f"Failed to create media support reply: {e}")
            return None

    async def _create_support_reply_text(self, chat_id: int, reply_text: str, reply_author: str) -> Optional[int]:
        """Создает текстовый ответ в чате поддержки и возвращает message_id"""
        try:
            # Формируем текст ответа БЕЗ ника поддержки
            message_text = f"💬 <b>Ответ поддержки:</b>\n\n{reply_text}" if reply_text else f"💬 <b>Ответ поддержки</b>"
            
            sent_message = await self.bot.send_message(
                chat_id=chat_id,
                text=message_text,
                parse_mode="HTML"
            )
            
            logger.info(f"✅ Created text support reply: {sent_message.message_id}")
            return sent_message.message_id
            
        except Exception as e:
            logger.error(f"Failed to create text support reply: {e}")
            return None

    async def _set_error_reaction(self, message: types.Message):
        """Устанавливает реакцию ошибки"""
        try:
            await self.bot.set_message_reaction(
                chat_id=message.chat.id,
                message_id=message.message_id,
                reaction=[{"type": "emoji", "emoji": "❌"}]
            )
            logger.debug(f"Set reaction '❌' for message {message.message_id} in chat {message.chat.id}")
        except Exception as e:
            logger.debug(f"Could not set error reaction: {e}")

    async def _append_to_task(self, task_id: str, message: types.Message):
        """Добавляет сообщение к существующей задаче"""
        try:
            # Получаем задачу
            task = await self.redis.get_task(task_id)
            if not task:
                return
            
            # Добавляем текст сообщения к задаче
            task['text'] += "\n" + message.text
            
            # Обновляем задачу в Redis
            await self.redis.update_task(task_id, **task)
            
            # Обновляем сообщение в чате поддержки
            await self.task_bot.update_task_message(task_id, task['text'])
            
            logger.info(f"Appended message to task {task_id}")
            
        except Exception as e:
            logger.error(f"Error appending message to task: {e}")

    async def _create_new_task(self, message: types.Message):
        """
        Создаёт новую задачу
        """
        try:
            # Подготавливаем данные сообщения
            message_data = await self._prepare_message_data(message)

            # ЭТАП 1: Сообщение пользователя записано в базу (Redis)
            task_id = await self.redis.save_task(message_data)
            logger.info(f"[USERBOT][STEP 1] Сообщение пользователя сохранено в Redis как задача {task_id}")

            # ЭТАП 2: UserBot отправил TaskBot сигнал по Pub/Sub
            await self.redis.publish_event("new_tasks", {
                "type": "new_task",
                "task_id": task_id
            })
            logger.info(f"[USERBOT][STEP 2] Отправлен сигнал по Pub/Sub о новой задаче: {task_id}")

            # ЭТАП 3: (TaskBot должен получить сигнал, логируется на стороне TaskBot)

            # Отправляем задачу в саппорт чат через TaskBot (если вызывается напрямую)
            if hasattr(self, 'task_bot') and self.task_bot:
                task_message = await self.task_bot.send_task_to_support(message_data)
                # Сохраняем ID сообщения в чате поддержки
                message_data['support_message_id'] = task_message.message_id
                await self.redis.set_task(task_id, message_data)
                logger.info(f"[USERBOT][STEP 5] Сообщение создано в саппорт чате через TaskBot (ручной вызов)")
            
            # Сохраняем активную задачу
            await self.redis.set(f"active_task:{message_data['user_id']}:{message_data['chat_id']}", task_id, ex=settings.MESSAGE_AGGREGATION_TIMEOUT)
            
        except Exception as e:
            logger.error(f"Error creating new task: {e}")

    def _start_background_tasks(self):
        """Запускает фоновые задачи"""
        # Запускаем PubSub слушатель
        asyncio.create_task(self._start_pubsub_listener())
        
        # Запускаем периодическую очистку
        asyncio.create_task(self._periodic_cleanup())

    async def _start_pubsub_listener(self):
        """Запускает PubSub слушатель с новым менеджером"""
        try:
            logger.info("Starting PubSub listener...")
            await self.pubsub_manager.start_background_listener(
                channels=["task_updates"],
                handler_func=self._pubsub_message_handler
            )
        except Exception as e:
            logger.error(f"PubSub listener error: {e}")

    async def _pubsub_message_handler(self, channel: str, message: dict):
        """Обработчик PubSub событий"""
        try:
            logger.info(f"[USERBOT][PUBSUB] Received event on channel {channel}: {message}")
            message_type = message.get('type')
            task_id = message.get('task_id')
            logger.info(f"[USERBOT][PUBSUB] Processing event type: {message_type}, task_id: {task_id}")
            
            if message_type == 'status_change':
                await self._handle_status_change(task_id, message)
            elif message_type == 'new_reply':
                await self._handle_new_reply_pubsub(message)
            elif message_type == 'task_deleted':
                # Обрабатываем удаление задачи - сбрасываем кэшированную информацию
                logger.info(f"[USERBOT][TASK_DELETED] Task {task_id} was deleted, clearing any cached references")
                # Удаляем задачу из processed_tasks если она там есть
                for user_id, cached_task_id in list(self.message_aggregator.processed_tasks.items()):
                    if cached_task_id == task_id:
                        del self.message_aggregator.processed_tasks[user_id]
                        logger.info(f"[USERBOT][TASK_DELETED] Cleared cached task {task_id} for user {user_id}")
            elif message_type == 'task_update':
                # Обрабатываем обновление задачи
                await self._handle_task_update(channel, message)
            elif message_type == 'additional_message_reply':
                # Обрабатываем ответ на дополнительное сообщение
                await self._handle_additional_reply(message)
        except Exception as e:
            logger.error(f"Error handling task update: {e}", exc_info=True)

    async def _handle_status_change(self, task_id: str, update_data: dict):
        """Обрабатывает изменение статуса задачи"""
        try:
            new_status = update_data.get('new_status')
            logger.info(f"[USERBOT][REACTION] Processing status change for task {task_id}: {new_status}")
            
            # Получаем задачу из Redis
            task = await self.redis.get_task(task_id)
            logger.info(f"Received status_update event for task {task_id}")
            logger.info(f"Retrieved task {task_id} for status update")
            
            if not task:
                logger.warning(f"[USERBOT][REACTION] Task {task_id} not found in Redis")
                return
            
            user_id = int(task.get('user_id', 0))
            chat_id = int(task.get('chat_id', 0))
            message_id = int(task.get('message_id', 0))
            
            # Устанавливаем соответствующую реакцию
            reaction_map = {
                'waiting': '⚡',      # Стала задачей
                'in_progress': '⚡',  # Взята в работу (изменено с 🔥 на ⚡)
                'completed': '👌'     # Завершена (изменено с ✅ на 👌)
            }
            
            if new_status in reaction_map:
                try:
                    await self.bot.set_message_reaction(
                        chat_id=chat_id,
                        message_id=message_id,
                        reaction=[{"type": "emoji", "emoji": reaction_map[new_status]}]
                    )
                    logger.debug(f"Set reaction '{reaction_map[new_status]}' for task {task_id}")
                    logger.info(f"Set reaction {reaction_map[new_status]} for task {task_id}")
                except Exception as e:
                    logger.debug(f"Could not set status reaction: {e}")
                    
        except Exception as e:
            logger.error(f"Error handling status change: {e}")

    async def _handle_new_reply(self, task_id: str, update_data: dict):
        """Обрабатывает новый ответ на задачу"""
        try:
            # Получаем задачу
            task = await self.redis.get_task(task_id)
            if not task:
                return
            
            user_id = int(task.get('user_id', 0))
            chat_id = int(task.get('chat_id', 0))
            # Получаем данные ответа из события или из задачи
            reply_text = update_data.get('reply_text') or task.get('reply', '')
            reply_author = update_data.get('reply_author') or task.get('reply_author', '')
            reply_message_id = update_data.get('reply_message_id')
            reply_chat_id = update_data.get('reply_chat_id')
            
            # Проверяем наличие медиафайлов в ответе
            has_media = (
                update_data.get('has_photo', False) or 
                update_data.get('has_video', False) or 
                update_data.get('has_document', False)
            )
            
            if (reply_text or has_media) and user_id and chat_id:
                logger.info(f"Processing reply for task {task_id}, user {user_id}, has_media: {has_media}, trying to forward to user topic")
                
                # Проверяем, является ли оригинальный чат форумом
                topic_id = None
                target_chat_id = chat_id  # Используем оригинальный чат
                
                try:
                    # Проверяем, является ли чат форумом
                    chat_info = await self.bot.get_chat(target_chat_id)
                    if chat_info.is_forum:
                        # Если оригинальный чат - форум, создаём тему там
                        topic_id = await self.topic_manager.get_or_create_user_topic(
                            target_chat_id,
                            user_id,
                            task.get('username'),
                            task.get('first_name')
                        )
                        logger.info(f"Got user topic {topic_id} for user {user_id} in forum chat {target_chat_id}")
                    else:
                        # Если оригинальный чат не форум, пробуем форумный чат из настроек
                        logger.info(f"Original chat {target_chat_id} is not a forum, trying forum chat {settings.FORUM_CHAT_ID}")
                        target_chat_id = settings.FORUM_CHAT_ID
                        topic_id = await self.topic_manager.get_or_create_user_topic(
                            target_chat_id,
                            user_id,
                            task.get('username'),
                            task.get('first_name')
                        )
                        logger.info(f"Got user topic {topic_id} for user {user_id} in forum chat {target_chat_id}")
                except Exception as e:
                    logger.error(f"Failed to get/create user topic for user {user_id}: {e}")
                    topic_id = None
                    target_chat_id = None
                
                # НОВАЯ ЛОГИКА: Используем поле message_source для определения поведения
                message_source = task.get('message_source', 'main_menu')  # по умолчанию главное меню
                logger.info(f"[REPLY][SOURCE] Task {task_id} originated from: {message_source}")
                
                # Если сообщение из темы пользователя - только прямой ответ, никаких пересылок
                if message_source == "user_topic":
                    logger.info(f"Message from user topic - sending direct reply only, no forwarding")
                    should_forward_to_topic = False
                else:
                    # Если из главного меню - прямой ответ + пересылка в тему (если есть)
                    logger.info(f"Message from main menu - sending direct reply + forwarding to topic if available")
                    should_forward_to_topic = True
                
                # НОВАЯ ЛОГИКА: Создаем сообщение-ответ в чате поддержки, затем пересылаем его
                forwarded_to_topic = False
                support_reply_message_id = None
                
                # Шаг 1: Создаем сообщение-ответ в чате поддержки
                try:
                    logger.info(f"Creating support reply message in support chat {settings.FORUM_CHAT_ID}")
                    
                    if has_media:
                        # Создаем медиа-ответ в чате поддержки
                        support_reply_message_id = await self._create_support_reply_with_media(
                            settings.FORUM_CHAT_ID, reply_text, reply_author, update_data
                        )
                    else:
                        # Создаем текстовый ответ в чате поддержки
                        support_reply_message_id = await self._create_support_reply_text(
                            settings.FORUM_CHAT_ID, reply_text, reply_author
                        )
                    
                    if support_reply_message_id:
                        logger.info(f"Created support reply message {support_reply_message_id} in support chat")
                    else:
                        logger.error(f"Failed to create support reply message in support chat")
                        
                except Exception as e:
                    logger.error(f"Error creating support reply message: {e}")
                    support_reply_message_id = None
                
                # Шаг 2: Пересылаем созданное сообщение в тему пользователя (только если должны пересылать)
                if topic_id and support_reply_message_id and target_chat_id and should_forward_to_topic:
                    try:
                        logger.info(f"Forwarding support reply message {support_reply_message_id} to user topic {topic_id} in chat {target_chat_id}")
                        await self.bot.forward_message(
                            chat_id=target_chat_id,
                            from_chat_id=settings.FORUM_CHAT_ID,
                            message_id=support_reply_message_id,
                            message_thread_id=topic_id
                        )
                        logger.info(f"Successfully forwarded support reply to user topic {topic_id}")
                        forwarded_to_topic = True
                    except Exception as e:
                        error_msg = str(e).lower()
                        logger.warning(f"Could not forward support reply to user topic {topic_id} in chat {target_chat_id}: {e}")
                        
                        # Если тема не найдена, пробуем восстановить её
                        if "thread not found" in error_msg or "message thread not found" in error_msg:
                            logger.info(f"Topic {topic_id} not found, attempting to recreate for user {user_id}")
                            try:
                                # Удаляем старую тему из кэша
                                await self.topic_manager._delete_user_topic_cache(target_chat_id, user_id)
                                
                                # Создаём новую тему
                                new_topic_id = await self.topic_manager.get_or_create_user_topic(
                                    target_chat_id,
                                    user_id,
                                    task.get('username'),
                                    task.get('first_name')
                                )
                                
                                if new_topic_id:
                                    logger.info(f"Created new topic {new_topic_id} for user {user_id}, retrying forward")
                                    # Повторяем попытку пересылки с новой темой
                                    await self.bot.forward_message(
                                        chat_id=target_chat_id,
                                        from_chat_id=settings.FORUM_CHAT_ID,
                                        message_id=support_reply_message_id,
                                        message_thread_id=new_topic_id
                                    )
                                    logger.info(f"Successfully forwarded support reply to recreated topic {new_topic_id}")
                                    forwarded_to_topic = True
                                    topic_id = new_topic_id  # Обновляем topic_id для fallback
                                else:
                                    logger.error(f"Failed to recreate topic for user {user_id}")
                                    
                            except Exception as retry_e:
                                logger.error(f"Failed to recreate topic and retry forward: {retry_e}")
                        
                        elif "chat not found" in error_msg:
                            logger.error(f"Chat {target_chat_id} not found - bot may not be added to this chat or chat doesn't exist")
                        elif "bot is not a member" in error_msg:
                            logger.error(f"Bot is not a member of chat {target_chat_id}")
                        elif "not enough rights" in error_msg:
                            logger.error(f"Bot doesn't have enough rights in chat {target_chat_id}")
                        
                        # В любом случае forwarded_to_topic остаётся False для fallback
                elif not topic_id:
                    logger.warning(f"No user topic available for user {user_id}, skipping topic forwarding")
                elif not support_reply_message_id:
                    logger.warning(f"No support reply message created, skipping forwarding")
                
                # ИСПРАВЛЕННАЯ ЛОГИКА: Отправляем ответ пользователю напрямую ТОЛЬКО ОДИН РАЗ
                # Независимо от того, удалось ли создать/переслать сообщение в support chat
                direct_reply_message_id = None
                try:
                    original_message_id = task.get('message_id')
                    
                    # Проверяем наличие медиафайлов для прямого ответа
                    if has_media:
                        # Отправляем медиа напрямую пользователю
                        direct_reply_message_id = await self._send_media_reply_direct_with_id(chat_id, original_message_id, reply_text, reply_author, update_data)
                        if direct_reply_message_id:
                            logger.info(f"Sent direct media reply to user {user_id} in chat {chat_id}")
                        else:
                            # Fallback на текстовое сообщение
                            direct_reply_message_id = await self._send_text_reply_direct_with_id(chat_id, original_message_id, reply_text, reply_author)
                    else:
                        # Отправляем обычный текстовый ответ
                        direct_reply_message_id = await self._send_text_reply_direct_with_id(chat_id, original_message_id, reply_text, reply_author)
                    
                    logger.info(f"Sent direct reply to user {user_id} in chat {chat_id}")
                    
                    # НОВАЯ ЛОГИКА: Если сообщение из главного меню и есть тема пользователя,
                    # пересылаем оригинальный ответ в тему (не создаем новое сообщение)
                    if (message_source == "main_menu" and topic_id and target_chat_id and 
                        direct_reply_message_id and should_forward_to_topic):
                        try:
                            logger.info(f"Forwarding direct reply message {direct_reply_message_id} to user topic {topic_id} in chat {target_chat_id}")
                            await self.bot.forward_message(
                                chat_id=target_chat_id,
                                from_chat_id=chat_id,
                                message_id=direct_reply_message_id,
                                message_thread_id=topic_id
                            )
                            logger.info(f"Successfully forwarded direct reply to user topic {topic_id}")
                        except Exception as forward_e:
                            logger.warning(f"Could not forward direct reply to user topic {topic_id}: {forward_e}")
                            # Fallback: создаем текстовое сообщение в теме только если пересылка не удалась
                            try:
                                await self.bot.send_message(
                                    chat_id=target_chat_id,
                                    message_thread_id=topic_id,
                                    text=f"💬 <b>Ответ поддержки:</b>\n\n{reply_text}",
                                    parse_mode="HTML"
                                )
                                logger.info(f"Sent fallback text reply to topic {topic_id} in chat {target_chat_id}")
                            except Exception as fallback_e:
                                logger.error(f"Failed to send fallback reply to user topic: {fallback_e}")
                        
                except Exception as e:
                    logger.error(f"Could not send direct reply to user: {e}")
                    
                    # ТОЛЬКО ЕСЛИ НЕ УДАЛОСЬ ОТПРАВИТЬ ПРЯМОЙ ОТВЕТ - пробуем отправить в тему как последний fallback
                    if topic_id and target_chat_id:
                        try:
                            logger.info(f"Direct reply failed, attempting fallback to user topic {topic_id}")
                            # Используем новый метод для отправки медиа в тему
                            success = await self._send_media_reply(target_chat_id, topic_id, reply_text, reply_author, update_data)
                            if success:
                                logger.info(f"Sent media fallback message to user topic {topic_id} in chat {target_chat_id}")
                            else:
                                # Fallback на обычное текстовое сообщение
                                await self.bot.send_message(
                                    chat_id=target_chat_id,
                                    message_thread_id=topic_id,
                                    text=f"💬 <b>Ответ поддержки:</b>\n\n{reply_text}",
                                    parse_mode="HTML"
                                )
                                logger.info(f"Sent text fallback message to user topic {topic_id} in chat {target_chat_id}")
                        except Exception as fallback_e:
                            logger.error(f"Failed to send fallback message to user topic: {fallback_e}")
                    
                logger.info(f"Sent reply for task {task_id} to user {user_id}")
                
        except Exception as e:
            logger.error(f"Error handling new reply: {e}")
    async def _periodic_cleanup(self):
        """Периодическая очистка неактивных данных"""
        while True:
            try:
                await asyncio.sleep(3600)  # Каждый час
                
                # Получаем все активные чаты с темами пользователей
                pattern = "user_topic:*"
                keys = []
                
                # Используем scan для получения ключей
                cursor = 0
                while True:
                    cursor, batch = await self.redis.conn.scan(cursor, match=pattern, count=100)
                    keys.extend(batch)
                    if cursor == 0:
                        break
                
                # Извлекаем уникальные chat_id из ключей
                chat_ids = set()
                for key in keys:
                    if isinstance(key, bytes):
                        key = key.decode('utf-8')
                    # Формат ключа: user_topic:chat_id:user_id
                    parts = key.split(':')
                    if len(parts) >= 3:
                        try:
                            chat_id = int(parts[1])
                            chat_ids.add(chat_id)
                        except ValueError:
                            continue
                
                # Очищаем неактивные темы для каждого чата
                for chat_id in chat_ids:
                    try:
                        await self.topic_manager.cleanup_inactive_topics(chat_id)
                        logger.info(f"Cleaned up topics for chat {chat_id}")
                    except Exception as e:
                        logger.error(f"Error cleaning up topics for chat {chat_id}: {e}")
                
                logger.info(f"Completed periodic cleanup for {len(chat_ids)} chats")
                
            except Exception as e:
                logger.error(f"Error in periodic cleanup: {e}")



    async def _handle_additional_message_reply(self, update_data: dict):
        """Обрабатывает ответы на дополнительные сообщения"""
        try:
            logger.info(f"Processing additional message reply: {update_data}")
            
            # Извлекаем данные из события
            task_id = update_data.get('task_id')
            reply_text = update_data.get('reply_text', '')
            reply_author = update_data.get('reply_author', '')
            user_message_id = update_data.get('user_message_id')
            user_chat_id = update_data.get('user_chat_id')
            user_topic_id = update_data.get('user_topic_id')
            
            if not all([task_id, user_message_id, user_chat_id]):
                logger.error(f"Missing required data for additional message reply: task_id={task_id}, user_message_id={user_message_id}, user_chat_id={user_chat_id}")
                return
            
            # Получаем задачу из Redis для определения источника сообщения
            task = await self.redis.get_task(task_id)
            if not task:
                logger.error(f"Task {task_id} not found for additional message reply processing")
                return
            
            message_source = task.get('message_source', 'main_menu')
            
            # Определяем, нужно ли пересылать ответ в тему пользователя
            # Используем user_topic_id из события для определения источника текущего сообщения
            current_message_from_user_topic = user_topic_id is not None and update_data.get('user_chat_id') == user_topic_id
            should_forward_to_topic = False
            if current_message_from_user_topic:
                logger.info(f"Reply to additional message from user topic - sending direct reply only, no forwarding")
                should_forward_to_topic = False
            else:
                logger.info(f"Reply to additional message from main menu - sending direct reply + forwarding to topic if available")
                should_forward_to_topic = True
            
            # Отправляем ответ как reply к дополнительному сообщению пользователя
            direct_reply_message_id = None
            try:
                # Проверяем наличие медиафайлов
                has_media = any([
                    update_data.get('reply_has_photo', False),
                    update_data.get('reply_has_video', False),
                    update_data.get('reply_has_document', False)
                ])
                
                if has_media:
                    # Отправляем медиа-ответ
                    direct_reply_message_id = await self._send_media_reply_to_additional_message(
                        user_chat_id, user_message_id, user_topic_id, 
                        reply_text, reply_author, update_data
                    )
                else:
                    # Отправляем текстовый ответ как reply к дополнительному сообщению
                    message_text = f"💬 <b>Ответ поддержки:</b>\n\n{reply_text}" if reply_text else "💬 <b>Ответ поддержки</b>"
                    
                    sent_message = await self.bot.send_message(
                        chat_id=user_chat_id,
                        text=message_text,
                        reply_to_message_id=user_message_id,
                        message_thread_id=user_topic_id if user_topic_id else None,
                        parse_mode="HTML"
                    )
                    direct_reply_message_id = sent_message.message_id
                
                logger.info(f"Successfully sent additional message reply to user {user_chat_id}, message {user_message_id}")
                
            except Exception as send_error:
                logger.error(f"Failed to send additional message reply: {send_error}")
                return
            
            # Пересылаем ответ в тему пользователя, если нужно
            if (should_forward_to_topic and user_topic_id and user_chat_id and 
                direct_reply_message_id and message_source == "main_menu"):
                try:
                    logger.info(f"Forwarding direct reply message {direct_reply_message_id} to user topic {user_topic_id} in chat {user_chat_id}")
                    await self.bot.forward_message(
                        chat_id=user_chat_id,
                        from_chat_id=user_chat_id,
                        message_id=direct_reply_message_id,
                        message_thread_id=user_topic_id
                    )
                    logger.info(f"Successfully forwarded direct reply to user topic {user_topic_id}")
                except Exception as forward_e:
                    logger.warning(f"Could not forward direct reply to user topic {user_topic_id}: {forward_e}")
                    # Fallback: создаем текстовое сообщение в теме только если пересылка не удалась
                    try:
                        message_text = f"💬 <b>Ответ поддержки:</b>\n\n{reply_text}" if reply_text else "💬 <b>Ответ поддержки</b>"
                        await self.bot.send_message(
                            chat_id=user_chat_id,
                            text=message_text,
                            message_thread_id=user_topic_id,
                            parse_mode="HTML"
                        )
                        logger.info(f"Sent fallback text message to user topic {user_topic_id}")
                    except Exception as fallback_e:
                        logger.error(f"Failed to send fallback message to user topic {user_topic_id}: {fallback_e}")
            
        except Exception as e:
            logger.error(f"Error handling additional message reply: {e}", exc_info=True)

    async def _send_media_reply_to_additional_message(self, chat_id: int, message_id: int, topic_id: int, reply_text: str, reply_author: str, update_data: dict):
        """Отправляет медиа-ответ как reply к дополнительному сообщению"""
        try:
            message_text = f"💬 <b>Ответ поддержки:</b>\n\n{reply_text}" if reply_text else "💬 <b>Ответ поддержки</b>"
            media_sent = False
            sent_message_id = None
            
            # Пытаемся отправить фото
            if update_data.get('reply_has_photo') and update_data.get('reply_photo_file_ids'):
                try:
                    photo_file_ids = update_data.get('reply_photo_file_ids', [])
                    if isinstance(photo_file_ids, str):
                        import json
                        photo_file_ids = json.loads(photo_file_ids)
                    
                    if photo_file_ids and isinstance(photo_file_ids, list):
                        photo_file_id = photo_file_ids[-1]  # Берем наибольшее разрешение
                        
                        sent_message = await self.bot.send_photo(
                            chat_id=chat_id,
                            photo=photo_file_id,
                            caption=message_text,
                            reply_to_message_id=message_id,
                            message_thread_id=topic_id if topic_id else None,
                            parse_mode="HTML"
                        )
                        sent_message_id = sent_message.message_id
                        media_sent = True
                        logger.info(f"Sent photo reply to additional message {message_id}")
                        
                except Exception as photo_error:
                    logger.warning(f"Failed to send photo reply to additional message: {photo_error}")
            
            # Пытаемся отправить видео, если фото не отправилось
            elif not media_sent and update_data.get('reply_has_video') and update_data.get('reply_video_file_id'):
                try:
                    video_file_id = update_data['reply_video_file_id']
                    
                    sent_message = await self.bot.send_video(
                        chat_id=chat_id,
                        video=video_file_id,
                        caption=message_text,
                        reply_to_message_id=message_id,
                        message_thread_id=topic_id if topic_id else None,
                        parse_mode="HTML"
                    )
                    sent_message_id = sent_message.message_id
                    media_sent = True
                    logger.info(f"Sent video reply to additional message {message_id}")
                    
                except Exception as video_error:
                    logger.warning(f"Failed to send video reply to additional message: {video_error}")
            
            # Пытаемся отправить документ, если медиа не отправилось
            elif not media_sent and update_data.get('reply_has_document') and update_data.get('reply_document_file_id'):
                try:
                    document_file_id = update_data['reply_document_file_id']
                    
                    sent_message = await self.bot.send_document(
                        chat_id=chat_id,
                        document=document_file_id,
                        caption=message_text,
                        reply_to_message_id=message_id,
                        message_thread_id=topic_id if topic_id else None,
                        parse_mode="HTML"
                    )
                    sent_message_id = sent_message.message_id
                    media_sent = True
                    logger.info(f"Sent document reply to additional message {message_id}")
                    
                except Exception as doc_error:
                    logger.warning(f"Failed to send document reply to additional message: {doc_error}")
            
            # Если медиа не отправилось, отправляем текстовое сообщение
            if not media_sent:
                sent_message = await self.bot.send_message(
                    chat_id=chat_id,
                    text=message_text,
                    reply_to_message_id=message_id,
                    message_thread_id=topic_id if topic_id else None,
                    parse_mode="HTML"
                )
                sent_message_id = sent_message.message_id
                logger.info(f"Sent text reply to additional message {message_id} (media fallback)")
            
            return sent_message_id
                
        except Exception as e:
            logger.error(f"Error sending media reply to additional message: {e}")
            return None

    async def _handle_task_reply(self, update_data: dict):
        """Обрабатывает обычные ответы на задачи (события new_reply)"""
        try:
            logger.info(f"Processing task reply: {update_data}")
            
            # Извлекаем данные из события
            task_id = update_data.get('task_id')
            reply_text = update_data.get('reply_text', '')
            reply_author = update_data.get('reply_author', '')
            reply_message_id = update_data.get('reply_message_id')
            reply_chat_id = update_data.get('reply_chat_id')
            
            if not task_id:
                logger.error(f"Missing task_id in task reply event: {update_data}")
                return
            
            # Получаем задачу из Redis
            task = await self.redis.get_task(task_id)
            if not task:
                logger.error(f"Task {task_id} not found for reply processing")
                return
            
            # Извлекаем данные пользователя и чата
            user_id = task.get('user_id')
            original_chat_id = int(task.get('chat_id', 0))  # Оригинальный чат пользователя
            original_message_id = int(task.get('message_id', 0))
            message_source = task.get('message_source', 'main_menu')
            
            # Используем chat_id из события для отправки reply
            # reply_chat_id - это чат, где находится сообщение, на которое мы отвечаем
            target_chat_id = reply_chat_id if reply_chat_id else original_chat_id
            
            if not all([user_id, target_chat_id, original_message_id]):
                logger.error(f"Missing required task data: user_id={user_id}, target_chat_id={target_chat_id}, message_id={original_message_id}")
                return
            
            # Получаем user_topic_id для пользователя
            user_topic_id = None
            try:
                user_topic_id = await self.topic_manager.get_or_create_user_topic(
                    user_id=int(user_id),
                    chat_id=original_chat_id
                )
            except Exception as topic_error:
                logger.warning(f"Could not get user topic for user {user_id}: {topic_error}")
            
            # Определяем, нужно ли пересылать ответ в тему пользователя
            # Используем user_topic_id и reply_chat_id из события для определения источника текущего сообщения
            current_message_from_user_topic = user_topic_id is not None and update_data.get('reply_chat_id') == user_topic_id
            should_forward_to_topic = False
            if current_message_from_user_topic:
                logger.info(f"Reply to message from user topic - sending direct reply only, no forwarding")
                should_forward_to_topic = False
            else:
                logger.info(f"Reply to message from main menu - sending direct reply + forwarding to topic if available")
                should_forward_to_topic = True
            
            # Отправляем ответ как reply к основному сообщению задачи
            direct_reply_message_id = None
            try:
                # Проверяем наличие медиафайлов
                has_media = any([
                    update_data.get('has_photo', False),
                    update_data.get('has_video', False),
                    update_data.get('has_document', False)
                ])
                
                if has_media:
                    # Отправляем медиа-ответ
                    direct_reply_message_id = await self._send_media_reply_to_task(
                        target_chat_id, original_message_id, user_topic_id, 
                        reply_text, reply_author, update_data
                    )
                else:
                    # Отправляем текстовый ответ как reply к основной задаче
                    message_text = f"💬 <b>Ответ поддержки:</b>\n\n{reply_text}" if reply_text else "💬 <b>Ответ поддержки</b>"
                    
                    sent_message = await self.bot.send_message(
                        chat_id=target_chat_id,
                        text=message_text,
                        reply_to_message_id=original_message_id,
                        message_thread_id=user_topic_id if user_topic_id else None,
                        parse_mode="HTML"
                    )
                    direct_reply_message_id = sent_message.message_id
                
                logger.info(f"Successfully sent task reply to user {user_id}, chat {target_chat_id}, message {original_message_id}")
                
            except Exception as send_error:
                logger.error(f"Failed to send task reply: {send_error}")
                return
            
            # Пересылаем ответ в тему пользователя, если нужно
            if (should_forward_to_topic and user_topic_id and target_chat_id and 
                direct_reply_message_id and message_source == "main_menu"):
                try:
                    logger.info(f"Forwarding direct reply message {direct_reply_message_id} to user topic {user_topic_id} in chat {original_chat_id}")
                    await self.bot.forward_message(
                        chat_id=original_chat_id,
                        from_chat_id=target_chat_id,
                        message_id=direct_reply_message_id,
                        message_thread_id=user_topic_id
                    )
                    logger.info(f"Successfully forwarded direct reply to user topic {user_topic_id}")
                except Exception as forward_e:
                    logger.warning(f"Could not forward direct reply to user topic {user_topic_id}: {forward_e}")
                    # Fallback: создаем текстовое сообщение в теме только если пересылка не удалась
                    try:
                        message_text = f"💬 <b>Ответ поддержки:</b>\n\n{reply_text}" if reply_text else "💬 <b>Ответ поддержки</b>"
                        await self.bot.send_message(
                            chat_id=original_chat_id,
                            text=message_text,
                            message_thread_id=user_topic_id,
                            parse_mode="HTML"
                        )
                        logger.info(f"Sent fallback text message to user topic {user_topic_id}")
                    except Exception as fallback_e:
                        logger.error(f"Failed to send fallback message to user topic {user_topic_id}: {fallback_e}")
            
        except Exception as e:
            logger.error(f"Error handling task reply: {e}", exc_info=True)

        logger.info(f"Processing task reply: {update_data}")
        
        # Извлекаем данные из события
        task_id = update_data.get('task_id')
        reply_text = update_data.get('reply_text', '')
        reply_author = update_data.get('reply_author', '')
        reply_message_id = update_data.get('reply_message_id')
        reply_chat_id = update_data.get('reply_chat_id')
        
        if not task_id:
            logger.error(f"Missing task_id in task reply event: {update_data}")
            return
        
        # Получаем задачу из Redis
        task = await self.redis.get_task(task_id)
        if not task:
            logger.error(f"Task {task_id} not found for reply processing")
            return
        
        # Извлекаем данные пользователя и чата
        user_id = task.get('user_id')
        original_chat_id = int(task.get('chat_id', 0))  # Оригинальный чат пользователя
        original_message_id = int(task.get('message_id', 0))
        message_source = task.get('message_source', 'main_menu')
        
        # Используем chat_id из события для отправки reply
        # reply_chat_id - это чат, где находится сообщение, на которое мы отвечаем
        target_chat_id = reply_chat_id if reply_chat_id else original_chat_id
        
        if not all([user_id, target_chat_id, original_message_id]):
            logger.error(f"Missing required task data: user_id={user_id}, target_chat_id={target_chat_id}, message_id={original_message_id}")
            return
        
        # Получаем user_topic_id для пользователя
        user_topic_id = None
        try:
            user_topic_id = await self.topic_manager.get_or_create_user_topic(
                user_id=int(user_id),
                chat_id=original_chat_id
            )
            
            if not task_id:
                logger.error(f"Missing task_id in task reply event: {update_data}")
                return
            
            # Получаем задачу из Redis
            task = await self.redis.get_task(task_id)
            if not task:
                logger.error(f"Task {task_id} not found for reply processing")
                return
            
            # Извлекаем данные пользователя и чата
            user_id = task.get('user_id')
            original_chat_id = int(task.get('chat_id', 0))  # Оригинальный чат пользователя
            original_message_id = int(task.get('message_id', 0))
            message_source = task.get('message_source', 'main_menu')
            
            # Используем chat_id из события для отправки reply
            # reply_chat_id - это чат, где находится сообщение, на которое мы отвечаем
            target_chat_id = reply_chat_id if reply_chat_id else original_chat_id
            
            if not all([user_id, target_chat_id, original_message_id]):
                logger.error(f"Missing required task data: user_id={user_id}, target_chat_id={target_chat_id}, message_id={original_message_id}")
                return
            
            # Получаем user_topic_id для пользователя
            user_topic_id = None
            try:
                user_topic_id = await self.topic_manager.get_or_create_user_topic(
                    user_id=int(user_id),
                    chat_id=original_chat_id
                )
            except Exception as topic_error:
                logger.warning(f"Could not get user topic for user {user_id}: {topic_error}")
            
            # Определяем, нужно ли пересылать ответ в тему пользователя
            # Используем user_topic_id и reply_chat_id из события для определения источника текущего сообщения
            current_message_from_user_topic = user_topic_id is not None and update_data.get('reply_chat_id') == user_topic_id
            should_forward_to_topic = False
            if current_message_from_user_topic:
                logger.info(f"Reply to message from user topic - sending direct reply only, no forwarding")
                should_forward_to_topic = False
            else:
                logger.info(f"Reply to message from main menu - sending direct reply + forwarding to topic if available")
                should_forward_to_topic = True
            
            # Отправляем ответ как reply к основному сообщению задачи
            direct_reply_message_id = None
            try:
                # Проверяем наличие медиафайлов
                has_media = any([
                    update_data.get('has_photo', False),
                    update_data.get('has_video', False),
                    update_data.get('has_document', False)
                ])
                
                if has_media:
                    # Отправляем медиа-ответ
                    direct_reply_message_id = await self._send_media_reply_to_task(
                        target_chat_id, original_message_id, user_topic_id, 
                        reply_text, reply_author, update_data
                    )
                else:
                    # Отправляем текстовый ответ как reply к основной задаче
                    message_text = f"💬 <b>Ответ поддержки:</b>\n\n{reply_text}" if reply_text else "💬 <b>Ответ поддержки</b>"
                    
                    sent_message = await self.bot.send_message(
                        chat_id=target_chat_id,
                        text=message_text,
                        reply_to_message_id=original_message_id,
                        message_thread_id=user_topic_id if user_topic_id else None,
                        parse_mode="HTML"
                    )
                    direct_reply_message_id = sent_message.message_id
                
                logger.info(f"Successfully sent task reply to user {user_id}, chat {target_chat_id}, message {original_message_id}")
                
            except Exception as send_error:
                logger.error(f"Failed to send task reply: {send_error}")
                return
            
            # Пересылаем ответ в тему пользователя, если нужно
            if (should_forward_to_topic and user_topic_id and target_chat_id and 
                direct_reply_message_id and message_source == "main_menu"):
                try:
                    logger.info(f"Forwarding direct reply message {direct_reply_message_id} to user topic {user_topic_id} in chat {original_chat_id}")
                    await self.bot.forward_message(
                        chat_id=original_chat_id,
                        from_chat_id=target_chat_id,
                        message_id=direct_reply_message_id,
                        message_thread_id=user_topic_id
                    )
                    logger.info(f"Successfully forwarded direct reply to user topic {user_topic_id}")
                except Exception as forward_e:
                    logger.warning(f"Could not forward direct reply to user topic {user_topic_id}: {forward_e}")
                    # Fallback: создаем текстовое сообщение в теме только если пересылка не удалась
                    try:
                        message_text = f"💬 <b>Ответ поддержки:</b>\n\n{reply_text}" if reply_text else "💬 <b>Ответ поддержки</b>"
                        await self.bot.send_message(
                            chat_id=original_chat_id,
                            text=message_text,
                            message_thread_id=user_topic_id,
                            parse_mode="HTML"
                        )
                        logger.info(f"Sent fallback text message to user topic {user_topic_id}")
                    except Exception as fallback_e:
                        logger.error(f"Failed to send fallback message to user topic {user_topic_id}: {fallback_e}")
                except Exception as e:
                    logger.error(f"Error handling task reply: {e}", exc_info=True)

        except Exception as e:
            logger.error(f"Error handling task reply: {e}", exc_info=True)

    async def _send_media_reply_to_task(self, chat_id: int, message_id: int, topic_id: int, reply_text: str, reply_author: str, update_data: dict):
        """Отправляет медиа-ответ как reply к основной задаче"""
        try:
            message_text = f"💬 <b>Ответ поддержки:</b>\n\n{reply_text}" if reply_text else "💬 <b>Ответ поддержки</b>"
            media_sent = False
            sent_message_id = None
            
            # Пытаемся отправить фото
            if update_data.get('has_photo') and update_data.get('photo_file_ids'):
                try:
                    photo_file_ids = update_data.get('photo_file_ids', [])
                    if isinstance(photo_file_ids, str):
                        import json
                        photo_file_ids = json.loads(photo_file_ids)
                    
                    if photo_file_ids and isinstance(photo_file_ids, list):
                        photo_file_id = photo_file_ids[-1]  # Берем наибольшее разрешение
                        
                        sent_message = await self.bot.send_photo(
                            chat_id=chat_id,
                            photo=photo_file_id,
                            caption=message_text,
                            reply_to_message_id=message_id,
                            message_thread_id=topic_id if topic_id else None,
                            parse_mode="HTML"
                        )
                        sent_message_id = sent_message.message_id
                        media_sent = True
                        logger.info(f"Sent photo reply to task message {message_id}")
                        
                except Exception as photo_error:
                    logger.warning(f"Failed to send photo reply to task: {photo_error}")
            
            # Пытаемся отправить видео, если фото не отправилось
            elif not media_sent and update_data.get('has_video') and update_data.get('video_file_id'):
                try:
                    video_file_id = update_data['video_file_id']
                    
                    sent_message = await self.bot.send_video(
                        chat_id=chat_id,
                        video=video_file_id,
                        caption=message_text,
                        reply_to_message_id=message_id,
                        message_thread_id=topic_id if topic_id else None,
                        parse_mode="HTML"
                    )
                    sent_message_id = sent_message.message_id
                    media_sent = True
                    logger.info(f"Sent video reply to task message {message_id}")
                    
                except Exception as video_error:
                    logger.warning(f"Failed to send video reply to task: {video_error}")
            
            # Пытаемся отправить документ, если медиа не отправилось
            elif not media_sent and update_data.get('has_document') and update_data.get('document_file_id'):
                try:
                    document_file_id = update_data['document_file_id']
                    
                    sent_message = await self.bot.send_document(
                        chat_id=chat_id,
                        document=document_file_id,
                        caption=message_text,
                        reply_to_message_id=message_id,
                        message_thread_id=topic_id if topic_id else None,
                        parse_mode="HTML"
                    )
                    sent_message_id = sent_message.message_id
                    media_sent = True
                    logger.info(f"Sent document reply to task message {message_id}")
                    
                except Exception as doc_error:
                    logger.warning(f"Failed to send document reply to task: {doc_error}")
            
            # Если медиа не отправилось, отправляем текстовое сообщение
            if not media_sent:
                sent_message = await self.bot.send_message(
                    chat_id=chat_id,
                    text=message_text,
                    reply_to_message_id=message_id,
                    message_thread_id=topic_id if topic_id else None,
                    parse_mode="HTML"
                )
                sent_message_id = sent_message.message_id
                logger.info(f"Sent text reply to task message {message_id} (media fallback)")
            
            return sent_message_id
                
        except Exception as e:
            logger.error(f"Error sending media reply to task: {e}")
            return None



    async def start_polling(self):
        """Запуск бота в режиме polling с полной инициализацией"""
        try:
            logger.info("[USERBOT][STEP 0] Starting UserBot in polling mode...")
            
            # Подключаемся к Redis
            logger.info("[USERBOT][STEP 0.0] Connecting to Redis...")
            await self.redis.connect()
            logger.info("[USERBOT][STEP 0.0] Redis connection established")
            
            # Подписываемся на каналы
            logger.info("[USERBOT][STEP 0.1] Subscribing to PubSub channels...")
            await self.pubsub_manager.subscribe("health_check", self._pubsub_message_handler)
            await self.pubsub_manager.subscribe("task_updates", self._pubsub_message_handler)
            logger.info("[USERBOT][STEP 0.2] Subscriptions completed")
            
            # Запускаем слушателя PubSub
            logger.info("[USERBOT][STEP 0.3] Starting PubSub listener...")
            await self.pubsub_manager.start()
            logger.info("[USERBOT][STEP 0.4] PubSub listener started")
            
            # Запускаем polling
            logger.info("[USERBOT][STEP 0.5] Starting polling...")
            await self.dp.start_polling(self.bot)
            
        except Exception as e:
            logger.error(f"[USERBOT][ERROR] Failed to start UserBot: {e}", exc_info=True)
        finally:
            await self.bot.session.close()

    async def start(self):
        """Запускает бота с полной инициализацией PubSub"""
        try:
            logger.info("[USERBOT][STEP 0] Starting UserBot in polling mode...")
            
            # Подключаемся к Redis
            logger.info("[USERBOT][STEP 0.0] Connecting to Redis...")
            await self.redis.connect()
            logger.info("[USERBOT][STEP 0.0] Redis connection established")
            
            # Подписываемся на каналы
            logger.info("[USERBOT][STEP 0.1] Subscribing to PubSub channels...")
            await self.pubsub_manager.subscribe("health_check", self._pubsub_message_handler)
            await self.pubsub_manager.subscribe("task_updates", self._pubsub_message_handler)
            logger.info("[USERBOT][STEP 0.2] Subscriptions completed")
            
            # Запускаем слушателя PubSub
            logger.info("[USERBOT][STEP 0.3] Starting PubSub listener...")
            await self.pubsub_manager.start()
            logger.info("[USERBOT][STEP 0.4] PubSub listener started")
            
            # Запускаем фоновые задачи
            self._start_background_tasks()
            
            # Небольшая задержка для синхронизации с TaskBot
            logger.info("[USERBOT][STEP 0.5] Waiting for TaskBot to subscribe to channels...")
            await asyncio.sleep(2)  # 2 секунды должно быть достаточно
            
            # Запускаем polling
            logger.info("[USERBOT][STEP 0.6] Starting polling...")
            await self.dp.start_polling(self.bot)
            
        except Exception as e:
            logger.error(f"[USERBOT][ERROR] Failed to start UserBot: {e}", exc_info=True)
        finally:
            await self.bot.session.close()

    async def _handle_status_change(self, task_id: str, update_data: dict):
        """Обрабатывает изменение статуса задачи"""
        try:
            new_status = update_data.get('new_status')
            logger.info(f"[USERBOT][REACTION] Processing status change for task {task_id}: {new_status}")
            
            # Получаем задачу из Redis
            task = await self.redis.get_task(task_id)
            if not task:
                logger.warning(f"[USERBOT][REACTION] Task {task_id} not found in Redis")
                return
            
            user_id = int(task.get('user_id', 0))
            chat_id = int(task.get('chat_id', 0))
            message_id = int(task.get('message_id', 0))
            
            # Обновляем статус задачи
            if new_status:
                # Обновляем исполнителя если указан
                assignee = update_data.get("assignee", task.get("assignee"))
                
                # Сохраняем обновленную задачу
                await self.redis.update_task(task_id, status=new_status, assignee=assignee)
                logger.info(f"[USERBOT] Task {task_id} status updated to {new_status}")
            
            # Устанавливаем соответствующую реакцию
            reaction_map = {
                'waiting': '⚡',      # Стала задачей
                'in_progress': '⚡',  # Взята в работу (изменено с 🔥 на ⚡)
                'completed': '👌'     # Завершена (изменено с ✅ на 👌)
            }
            
            if new_status in reaction_map:
                try:
                    await self.bot.set_message_reaction(
                        chat_id=chat_id,
                        message_id=message_id,
                        reaction=[{"type": "emoji", "emoji": reaction_map[new_status]}]
                    )
                    logger.info(f"[USERBOT][REACTION] Set reaction {reaction_map[new_status]} for task {task_id}")
                except Exception as e:
                    logger.warning(f"[USERBOT][REACTION] Could not set status reaction: {e}")
                    
        except Exception as e:
            logger.error(f"Error handling status change: {e}", exc_info=True)
    
    async def _handle_new_reply_pubsub(self, message: dict):
        """Обрабатывает новый ответ на задачу из PubSub"""
        try:
            logger.info(f"[USERBOT] Handling new reply from PubSub: {message}")
            
            task_id = message["task_id"]
            reply_text = message["reply_text"]
            reply_author = message["reply_author"]
            
            # Получаем текущую задачу
            task = await self.redis.get_task(task_id)
            if not task:
                logger.warning(f"[USERBOT] Task {task_id} not found for new reply")
                return
            
            # Обновляем задачу с ответом
            await self.redis.update_task(
                task_id, 
                reply=reply_text,
                reply_author=reply_author,
                reply_at=message.get("reply_at", datetime.now().isoformat())
            )
            logger.info(f"[USERBOT] Reply saved to task {task_id}")
            
            # Отправляем ответ пользователю
            user_id = int(task["user_id"])
            chat_id = int(task["chat_id"])
            message_id = int(task["message_id"])
            
            # Проверяем наличие медиафайлов в ответе
            has_photo = message.get('has_photo', False)
            has_video = message.get('has_video', False)
            has_document = message.get('has_document', False)
            reply_support_media_message_id = message.get('reply_support_media_message_id')
            
            logger.info(f"[USERBOT][MEDIA] Reply media info: photo={has_photo}, video={has_video}, doc={has_document}, media_msg_id={reply_support_media_message_id}")
            
            try:
                # Если есть медиафайлы, отправляем их из локальных файлов
                if has_photo or has_video or has_document:
                    photo_file_paths = message.get('photo_file_paths', [])
                    video_file_path = message.get('video_file_path')
                    document_file_path = message.get('document_file_path')
                    
                    logger.info(f"[USERBOT][MEDIA] Reply media files: photo={photo_file_paths}, video={video_file_path}, doc={document_file_path}")
                    
                    # Отправляем фото
                    if has_photo and photo_file_paths:
                        try:
                            photo_path = photo_file_paths[0]  # Берём первое фото
                            if os.path.exists(photo_path):
                                photo_file = FSInputFile(photo_path)
                                sent_message = await self.bot.send_photo(
                                    chat_id=chat_id,
                                    photo=photo_file,
                                    caption=f"✅ <b>Ответ:</b>\n\n{reply_text}" if reply_text else "✅ <b>Ответ</b>",
                                    reply_to_message_id=message_id,
                                    parse_mode="HTML"
                                )
                                logger.info(f"[USERBOT][MEDIA] Photo reply sent to user: {sent_message.message_id}")
                                
                                # Пересылаем ответ в тему пользователя
                                await self._forward_reply_to_user_topic(sent_message, user_id, chat_id, message_id)
                            else:
                                logger.warning(f"[USERBOT][MEDIA] Photo file not found: {photo_path}")
                                raise FileNotFoundError(f"Photo file not found: {photo_path}")
                        except Exception as photo_error:
                            logger.error(f"[USERBOT][MEDIA] Failed to send photo reply: {photo_error}")
                            # Fallback к текстовому сообщению
                            if reply_text:
                                sent_message = await self.bot.send_message(
                                    chat_id=chat_id,
                                    text=f"✅ <b>Ответ:</b>\n\n{reply_text}",
                                    reply_to_message_id=message_id,
                                    parse_mode="HTML"
                                )
                                logger.info(f"[USERBOT] Text-only reply sent after photo failure: {sent_message.message_id}")
                    
                    # Отправляем видео
                    elif has_video and video_file_path:
                        try:
                            if os.path.exists(video_file_path):
                                video_file = FSInputFile(video_file_path)
                                sent_message = await self.bot.send_video(
                                    chat_id=chat_id,
                                    video=video_file,
                                    caption=f"✅ <b>Ответ:</b>\n\n{reply_text}" if reply_text else "✅ <b>Ответ</b>",
                                    reply_to_message_id=message_id,
                                    parse_mode="HTML"
                                )
                                logger.info(f"[USERBOT][MEDIA] Video reply sent to user: {sent_message.message_id}")
                                
                                # Пересылаем ответ в тему пользователя
                                await self._forward_reply_to_user_topic(sent_message, user_id, chat_id, message_id)
                            else:
                                logger.warning(f"[USERBOT][MEDIA] Video file not found: {video_file_path}")
                                raise FileNotFoundError(f"Video file not found: {video_file_path}")
                        except Exception as video_error:
                            logger.error(f"[USERBOT][MEDIA] Failed to send video reply: {video_error}")
                            # Fallback к текстовому сообщению
                            if reply_text:
                                sent_message = await self.bot.send_message(
                                    chat_id=chat_id,
                                    text=f"✅ <b>Ответ:</b>\n\n{reply_text}",
                                    reply_to_message_id=message_id,
                                    parse_mode="HTML"
                                )
                                logger.info(f"[USERBOT] Text-only reply sent after video failure: {sent_message.message_id}")
                                
                                # Пересылаем ответ в тему пользователя
                                await self._forward_reply_to_user_topic(sent_message, user_id, chat_id, message_id)
                    
                    # Отправляем документ
                    elif has_document and document_file_path:
                        try:
                            if os.path.exists(document_file_path):
                                document_file = FSInputFile(document_file_path)
                                sent_message = await self.bot.send_document(
                                    chat_id=chat_id,
                                    document=document_file,
                                    caption=f"✅ <b>Ответ:</b>\n\n{reply_text}" if reply_text else "✅ <b>Ответ</b>",
                                    reply_to_message_id=message_id,
                                    parse_mode="HTML"
                                )
                                logger.info(f"[USERBOT][MEDIA] Document reply sent to user: {sent_message.message_id}")
                                
                                # Пересылаем ответ в тему пользователя
                                await self._forward_reply_to_user_topic(sent_message, user_id, chat_id, message_id)
                            else:
                                logger.warning(f"[USERBOT][MEDIA] Document file not found: {document_file_path}")
                                raise FileNotFoundError(f"Document file not found: {document_file_path}")
                        except Exception as doc_error:
                            logger.error(f"[USERBOT][MEDIA] Failed to send document reply: {doc_error}")
                            # Fallback к текстовому сообщению
                            if reply_text:
                                sent_message = await self.bot.send_message(
                                    chat_id=chat_id,
                                    text=f"✅ <b>Ответ:</b>\n\n{reply_text}",
                                    reply_to_message_id=message_id,
                                    parse_mode="HTML"
                                )
                                logger.info(f"[USERBOT] Text-only reply sent after document failure: {sent_message.message_id}")
                                
                                # Пересылаем ответ в тему пользователя
                                await self._forward_reply_to_user_topic(sent_message, user_id, chat_id, message_id)
                    
                    # Если нет подходящих файлов, отправляем только текст
                    else:
                        if reply_text:
                            sent_message = await self.bot.send_message(
                                chat_id=chat_id,
                                text=f"✅ <b>Ответ:</b>\n\n{reply_text}",
                                reply_to_message_id=message_id,
                                parse_mode="HTML"
                            )
                            logger.info(f"[USERBOT] Text-only reply sent (no valid media files): {sent_message.message_id}")
                            
                            # Пересылаем ответ в тему пользователя
                            await self._forward_reply_to_user_topic(sent_message, user_id, chat_id, message_id)
                else:
                    # Отправляем обычный текстовый ответ
                    if reply_text:
                        sent_message = await self.bot.send_message(
                            chat_id=chat_id,
                            text=f"✅ <b>Ответ:</b>\n\n{reply_text}",
                            reply_to_message_id=message_id,
                            parse_mode="HTML"
                        )
                        logger.info(f"[USERBOT] Text reply sent to user: {sent_message.message_id}")
                        
                        # Пересылаем ответ в тему пользователя
                        await self._forward_reply_to_user_topic(sent_message, user_id, chat_id, message_id)
                        
            except Exception as reply_error:
                logger.error(f"[USERBOT] Failed to send reply as reply, sending as regular message: {reply_error}")
                # Если не удалось отправить как reply, отправляем как обычное сообщение
                if reply_text:
                    sent_message = await self.bot.send_message(
                        chat_id=chat_id,
                        text=f"✅ <b>Ответ:</b>\n\n{reply_text}",
                        parse_mode="HTML"
                    )
                    logger.info(f"[USERBOT] Reply sent as regular message: {sent_message.message_id}")
                    
                    # Пересылаем ответ в тему пользователя
                    await self._forward_reply_to_user_topic(sent_message, user_id, chat_id, message_id)
            
        except Exception as e:
            logger.error(f"Error handling new reply for task {task_id}: {e}", exc_info=True)
    
    async def _handle_task_update(self, channel: str, message: dict):
        """Обрабатывает обновления задач из PubSub"""
        try:
            logger.info(f"[USERBOT] Received task update: {message}")
            event_type = message.get("type")
            
            if event_type == "task_update":
                # Обновляем текст задачи
                task_id = message["task_id"]
                new_text = message.get("updated_text", "")
                
                # Получаем текущую задачу
                task = await self.redis.get_task(task_id)
                if task:
                    # Обновляем текст задачи
                    task["text"] = new_text
                    await self.redis.set_task(task_id, task)
                    
                    logger.info(f"[USERBOT] Task {task_id} text updated")
            
        except Exception as e:
            logger.error(f"Error handling task update: {e}", exc_info=True)
    
    async def _handle_additional_reply(self, message: dict):
        """Обрабатывает ответ на дополнительное сообщение"""
        try:
            logger.info(f"[USERBOT] Handling additional reply: {message}")
            
            task_id = message["task_id"]
            reply_text = message["reply_text"]
            reply_author = message["reply_author"]
            user_message_id = message["user_message_id"]
            user_chat_id = message["user_chat_id"]
            
            # Получаем user_id из задачи для пересылки в тему
            task = await self.redis.get_task(task_id)
            user_id = int(task["user_id"]) if task else None
            
            # Проверяем наличие медиафайлов в ответе
            has_photo = message.get('has_photo', False)
            has_video = message.get('has_video', False)
            has_document = message.get('has_document', False)
            reply_support_media_message_id = message.get('reply_support_media_message_id')
            
            logger.info(f"[USERBOT][ADDITIONAL][MEDIA] Reply media info: photo={has_photo}, video={has_video}, doc={has_document}, media_msg_id={reply_support_media_message_id}")
            
            # Отправляем ответ пользователю
            if user_message_id and user_chat_id:
                try:
                    # Если есть медиафайлы, отправляем их из локальных файлов
                    if has_photo or has_video or has_document:
                        photo_file_paths = message.get('photo_file_paths', [])
                        video_file_path = message.get('video_file_path')
                        document_file_path = message.get('document_file_path')
                        
                        logger.info(f"[USERBOT][ADDITIONAL][MEDIA] Reply media files: photo={photo_file_paths}, video={video_file_path}, doc={document_file_path}")
                        
                        # Отправляем фото
                        if has_photo and photo_file_paths:
                            try:
                                photo_path = photo_file_paths[0]  # Берём первое фото
                                if os.path.exists(photo_path):
                                    photo_file = FSInputFile(photo_path)
                                    sent_message = await self.bot.send_photo(
                                        chat_id=user_chat_id,
                                        photo=photo_file,
                                        caption=f"✅ <b>Ответ:</b>\n\n{reply_text}" if reply_text else "✅ <b>Ответ</b>",
                                        reply_to_message_id=user_message_id,
                                        parse_mode="HTML"
                                    )
                                    logger.info(f"[USERBOT][ADDITIONAL][MEDIA] Photo reply sent to user: {sent_message.message_id}")
                                    
                                    # Пересылаем ответ в тему пользователя
                                    if user_id:
                                        await self._forward_reply_to_user_topic(sent_message, user_id, user_chat_id, user_message_id)
                                else:
                                    logger.warning(f"[USERBOT][ADDITIONAL][MEDIA] Photo file not found: {photo_path}")
                                    raise FileNotFoundError(f"Photo file not found: {photo_path}")
                            except Exception as photo_error:
                                logger.error(f"[USERBOT][ADDITIONAL][MEDIA] Failed to send photo reply: {photo_error}")
                                # Fallback к текстовому сообщению
                                if reply_text:
                                    sent_message = await self.bot.send_message(
                                        chat_id=user_chat_id,
                                        text=f"✅ <b>Ответ:</b>\n\n{reply_text}",
                                        reply_to_message_id=user_message_id,
                                        parse_mode="HTML"
                                    )
                                    logger.info(f"[USERBOT][ADDITIONAL] Text-only reply sent after photo failure: {sent_message.message_id}")
                                    
                                    # Пересылаем ответ в тему пользователя
                                    if user_id:
                                        await self._forward_reply_to_user_topic(sent_message, user_id, user_chat_id, user_message_id)
                        
                        # Отправляем видео
                        elif has_video and video_file_path:
                            try:
                                if os.path.exists(video_file_path):
                                    video_file = FSInputFile(video_file_path)
                                    sent_message = await self.bot.send_video(
                                        chat_id=user_chat_id,
                                        video=video_file,
                                        caption=f"✅ <b>Ответ:</b>\n\n{reply_text}" if reply_text else "✅ <b>Ответ</b>",
                                        reply_to_message_id=user_message_id,
                                        parse_mode="HTML"
                                    )
                                    logger.info(f"[USERBOT][ADDITIONAL][MEDIA] Video reply sent to user: {sent_message.message_id}")
                                    
                                    # Пересылаем ответ в тему пользователя
                                    if user_id:
                                        await self._forward_reply_to_user_topic(sent_message, user_id, user_chat_id, user_message_id)
                                else:
                                    logger.warning(f"[USERBOT][ADDITIONAL][MEDIA] Video file not found: {video_file_path}")
                                    raise FileNotFoundError(f"Video file not found: {video_file_path}")
                            except Exception as video_error:
                                logger.error(f"[USERBOT][ADDITIONAL][MEDIA] Failed to send video reply: {video_error}")
                                # Fallback к текстовому сообщению
                                if reply_text:
                                    sent_message = await self.bot.send_message(
                                        chat_id=user_chat_id,
                                        text=f"✅ <b>Ответ:</b>\n\n{reply_text}",
                                        reply_to_message_id=user_message_id,
                                        parse_mode="HTML"
                                    )
                                    logger.info(f"[USERBOT][ADDITIONAL] Text-only reply sent after video failure: {sent_message.message_id}")
                                    
                                    # Пересылаем ответ в тему пользователя
                                    if user_id:
                                        await self._forward_reply_to_user_topic(sent_message, user_id, user_chat_id, user_message_id)
                        
                        # Отправляем документ
                        elif has_document and document_file_path:
                            try:
                                if os.path.exists(document_file_path):
                                    document_file = FSInputFile(document_file_path)
                                    sent_message = await self.bot.send_document(
                                        chat_id=user_chat_id,
                                        document=document_file,
                                        caption=f"✅ <b>Ответ:</b>\n\n{reply_text}" if reply_text else "✅ <b>Ответ</b>",
                                        reply_to_message_id=user_message_id,
                                        parse_mode="HTML"
                                    )
                                    logger.info(f"[USERBOT][ADDITIONAL][MEDIA] Document reply sent to user: {sent_message.message_id}")
                                    
                                    # Пересылаем ответ в тему пользователя
                                    if user_id:
                                        await self._forward_reply_to_user_topic(sent_message, user_id, user_chat_id, user_message_id)
                                else:
                                    logger.warning(f"[USERBOT][ADDITIONAL][MEDIA] Document file not found: {document_file_path}")
                                    raise FileNotFoundError(f"Document file not found: {document_file_path}")
                            except Exception as doc_error:
                                logger.error(f"[USERBOT][ADDITIONAL][MEDIA] Failed to send document reply: {doc_error}")
                                # Fallback к текстовому сообщению
                                if reply_text:
                                    sent_message = await self.bot.send_message(
                                        chat_id=user_chat_id,
                                        text=f"✅ <b>Ответ:</b>\n\n{reply_text}",
                                        reply_to_message_id=user_message_id,
                                        parse_mode="HTML"
                                    )
                                    logger.info(f"[USERBOT][ADDITIONAL] Text-only reply sent after document failure: {sent_message.message_id}")
                                    
                                    # Пересылаем ответ в тему пользователя
                                    if user_id:
                                        await self._forward_reply_to_user_topic(sent_message, user_id, user_chat_id, user_message_id)
                        
                        # Если нет подходящих файлов, отправляем только текст
                        else:
                            if reply_text:
                                sent_message = await self.bot.send_message(
                                    chat_id=user_chat_id,
                                    text=f"✅ <b>Ответ:</b>\n\n{reply_text}",
                                    reply_to_message_id=user_message_id,
                                    parse_mode="HTML"
                                )
                                logger.info(f"[USERBOT][ADDITIONAL] Text-only reply sent (no valid media files): {sent_message.message_id}")
                                
                                # Пересылаем ответ в тему пользователя
                                if user_id:
                                    await self._forward_reply_to_user_topic(sent_message, user_id, user_chat_id, user_message_id)
                    else:
                        # Отправляем обычный текстовый ответ
                        if reply_text:
                            sent_message = await self.bot.send_message(
                                chat_id=user_chat_id,
                                text=f"✅ <b>Ответ:</b>\n\n{reply_text}",
                                reply_to_message_id=user_message_id,
                                parse_mode="HTML"
                            )
                            logger.info(f"[USERBOT][ADDITIONAL] Text reply sent to user: {sent_message.message_id}")
                            
                            # Пересылаем ответ в тему пользователя
                            if user_id:
                                await self._forward_reply_to_user_topic(sent_message, user_id, user_chat_id, user_message_id)
                            
                except Exception as reply_error:
                    logger.error(f"[USERBOT][ADDITIONAL] Failed to send reply as reply, sending as regular message: {reply_error}")
                    # Если не удалось отправить как reply, отправляем как обычное сообщение
                    if reply_text:
                        sent_message = await self.bot.send_message(
                            chat_id=user_chat_id,
                            text=f"✅ <b>Ответ:</b>\n\n{reply_text}",
                            parse_mode="HTML"
                        )
                        logger.info(f"[USERBOT][ADDITIONAL] Reply sent as regular message: {sent_message.message_id}")
                        
                        # Пересылаем ответ в тему пользователя
                        if user_id:
                            await self._forward_reply_to_user_topic(sent_message, user_id, user_chat_id, user_message_id)
            
        except Exception as e:
            logger.error(f"Error handling additional reply: {e}", exc_info=True)

# Создание экземпляра бота
user_bot_instance = UserBot()

# Для интеграции с другими модулями
async def get_user_bot() -> UserBot:
    return user_bot_instance

if __name__ == "__main__":
    asyncio.run(user_bot_instance.start())

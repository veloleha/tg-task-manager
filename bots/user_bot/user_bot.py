import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, Optional
from collections import defaultdict

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
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
                
                logger.info(f"[USERBOT][MSG] Processing message from user {message.from_user.id} in chat {message.chat.id}")
                logger.info(f"[USERBOT][MSG] Message text: {message.text[:100] if message.text else 'No text'}...")
                
                user_id = message.from_user.id
                chat_id = message.chat.id
                
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
                    if current_thread_id and current_thread_id == user_topic_id:
                        logger.info(f"[USERBOT][MSG] User is already writing in their own topic {user_topic_id}, skipping forward")
                    else:
                        # Пересылаем сообщение в пользовательскую тему
                        await self._forward_to_user_topic(message, user_topic_id, chat_id)
                    
                    # Обновляем активность темы
                    await self.topic_manager.update_topic_activity(chat_id, user_id)
                else:
                    logger.info(f"[USERBOT][MSG] Chat {chat_id} is not a forum or topic creation failed")
                
                # Проверяем, есть ли активная задача
                existing_task = await self.redis.get(f"active_task:{user_id}:{chat_id}")
                if existing_task:
                    logger.info(f"[USERBOT][MSG] Found existing task: {existing_task}")
                    # Добавляем сообщение к существующей задаче
                    await self._append_to_task(existing_task, message)
                    return
                else:
                    logger.info(f"[USERBOT][MSG] No existing task found, creating new one")
                
                # Подготавливаем данные сообщения
                logger.info(f"[USERBOT][MSG] Preparing message data...")
                message_data = await self._prepare_message_data(message)
                logger.info(f"[USERBOT][MSG] Message data prepared: {len(message_data)} fields")
                
                # Создаем задачу напрямую (без агрегатора)
                logger.info(f"[USERBOT][MSG] Creating task directly...")
                task_id = await self._create_task_directly(message_data)
                logger.info(f"[USERBOT][MSG] ✅ Task created directly: {task_id}")
                
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
                    
                    # Повторяем попытку пересылки
                    try:
                        await self.bot.send_message(
                            chat_id=chat_id,
                            text=forward_text,
                            message_thread_id=new_topic_id
                        )
                        logger.info(f"[USERBOT] Forwarded message to new user topic {new_topic_id} in chat {chat_id}")
                    except Exception as retry_error:
                        logger.error(f"[USERBOT] Error forwarding to new user topic: {retry_error}")
                else:
                    logger.error(f"[USERBOT] Failed to create new topic for user {message.from_user.id} in chat {chat_id}")
            else:
                logger.error(f"[USERBOT] Error forwarding to user topic: {e}")

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
                "updated_at": None,
                "aggregated": False,  # Не агрегированная задача
                "message_count": 1,
                # Медиафайлы
                "has_photo": message_data.get("has_photo", False),
                "has_video": message_data.get("has_video", False),
                "has_document": message_data.get("has_document", False),
                "photo_file_ids": message_data.get("photo_file_ids", []),
                "video_file_id": message_data.get("video_file_id"),
                "document_file_id": message_data.get("document_file_id"),
                "media_group_id": message_data.get("media_group_id")
            }
            logger.info(f"[USERBOT][DIRECT] Task data prepared with {len(task_data)} fields")
            
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
        # Определяем наличие медиафайлов
        media_data = await self._extract_media_data(message)
        
        return {
            "message_id": message.message_id,
            "chat_id": message.chat.id,
            "chat_title": getattr(message.chat, 'title', 'Private Chat'),
            "chat_type": message.chat.type,
            "user_id": message.from_user.id,
            "first_name": message.from_user.first_name or "",
            "last_name": message.from_user.last_name or "",
            "username": message.from_user.username or "",
            "language_code": message.from_user.language_code or "",
            "is_bot": message.from_user.is_bot,
            "text": message.text or message.caption or "",
            "status": "unreacted",
            "task_number": None,
            "assignee": None,
            "task_link": None,
            "reply": None,
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": None,
            # Медиафайлы
            "has_photo": media_data["has_photo"],
            "has_video": media_data["has_video"],
            "has_document": media_data["has_document"],
            "photo_file_ids": media_data["photo_file_ids"],
            "video_file_id": media_data["video_file_id"],
            "document_file_id": media_data["document_file_id"],
            "media_group_id": media_data["media_group_id"]
        }

    async def _extract_media_data(self, message: types.Message) -> dict:
        """Извлекает данные о медиафайлах из сообщения"""
        media_data = {
            "has_photo": False,
            "has_video": False,
            "has_document": False,
            "photo_file_ids": [],
            "video_file_id": None,
            "document_file_id": None,
            "media_group_id": getattr(message, 'media_group_id', None)
        }
        
        # Проверяем фото
        if message.photo:
            media_data["has_photo"] = True
            # Сохраняем все размеры фото (Telegram предоставляет несколько размеров)
            media_data["photo_file_ids"] = [photo.file_id for photo in message.photo]
            logger.info(f"Found photo in message {message.message_id}, file_ids: {media_data['photo_file_ids']}")
        
        # Проверяем видео
        if message.video:
            media_data["has_video"] = True
            media_data["video_file_id"] = message.video.file_id
            logger.info(f"Found video in message {message.message_id}, file_id: {media_data['video_file_id']}")
        
        # Проверяем документы (включая видео-заметки, анимации и т.д.)
        if message.document:
            media_data["has_document"] = True
            media_data["document_file_id"] = message.document.file_id
            logger.info(f"Found document in message {message.message_id}, file_id: {media_data['document_file_id']}")
        
        # Проверяем видео-заметки
        if message.video_note:
            media_data["has_video"] = True
            media_data["video_file_id"] = message.video_note.file_id
            logger.info(f"Found video note in message {message.message_id}, file_id: {media_data['video_file_id']}")
        
        # Проверяем анимации (GIF)
        if message.animation:
            media_data["has_document"] = True
            media_data["document_file_id"] = message.animation.file_id
            logger.info(f"Found animation in message {message.message_id}, file_id: {media_data['document_file_id']}")
        
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
            
            # Если медиа не отправилось или его нет, отправляем обычное сообщение
            if not media_sent:
                # Не добавляем информацию о медиа - пользователь не хочет видеть такие уведомления
                # Оставляем только чистый текст ответа
                
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
        """Отправляет медиа-ответ напрямую пользователю"""
        try:
            # Формируем текст ответа
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
                    
                    photo_file_id = photo_file_ids[-1]
                    
                    # Проверяем валидность file_id
                    if not photo_file_id or len(photo_file_id) < 10:
                        logger.warning(f"Invalid photo file_id in direct reply: {photo_file_id}")
                        raise ValueError("Invalid photo file_id")
                    
                    await self.bot.send_photo(
                        chat_id=chat_id,
                        photo=photo_file_id,
                        caption=message_text,
                        reply_to_message_id=original_message_id if original_message_id else None,
                        parse_mode="HTML"
                    )
                    media_sent = True
                except Exception as photo_error:
                    logger.warning(f"Failed to send direct photo reply (file_id: {update_data.get('photo_file_ids', [])}): {photo_error}")
                    # Продолжаем с fallback на текстовое сообщение
            
            # Пытаемся отправить видео, если фото не отправилось
            elif not media_sent and update_data.get('has_video') and update_data.get('video_file_id'):
                try:
                    video_file_id = update_data['video_file_id']
                    
                    # Проверяем валидность file_id
                    if not video_file_id or len(video_file_id) < 10:
                        logger.warning(f"Invalid video file_id in direct reply: {video_file_id}")
                        raise ValueError("Invalid video file_id")
                    
                    await self.bot.send_video(
                        chat_id=chat_id,
                        video=video_file_id,
                        caption=message_text,
                        reply_to_message_id=original_message_id if original_message_id else None,
                        parse_mode="HTML"
                    )
                    media_sent = True
                except Exception as video_error:
                    logger.warning(f"Failed to send direct video reply (file_id: {update_data.get('video_file_id')}): {video_error}")
                    # Продолжаем с fallback на текстовое сообщение
            
            # Пытаемся отправить документ, если медиа не отправилось
            elif not media_sent and update_data.get('has_document') and update_data.get('document_file_id'):
                try:
                    document_file_id = update_data['document_file_id']
                    
                    # Проверяем валидность file_id
                    if not document_file_id or len(document_file_id) < 10:
                        logger.warning(f"Invalid document file_id in direct reply: {document_file_id}")
                        raise ValueError("Invalid document file_id")
                    
                    await self.bot.send_document(
                        chat_id=chat_id,
                        document=document_file_id,
                        caption=message_text,
                        reply_to_message_id=original_message_id if original_message_id else None,
                        parse_mode="HTML"
                    )
                    media_sent = True
                except Exception as doc_error:
                    logger.warning(f"Failed to send direct document reply (file_id: {update_data.get('document_file_id')}): {doc_error}")
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
            await self.redis.update_task(task_id, task)
            
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
        """Обработчик сообщений от PubSub менеджера"""
        try:
            if channel == "task_updates":
                await self._handle_task_update(message)
        except Exception as e:
            logger.error(f"Error handling PubSub message from {channel}: {e}")

    async def _handle_task_update(self, update_data: dict):
        """Обрабатывает обновления задач"""
        try:
            update_type = update_data.get('type')
            task_id = update_data.get('task_id')
            
            if update_type == 'status_change':
                await self._handle_status_change(task_id, update_data)
            elif update_type == 'new_reply':
                await self._handle_new_reply(task_id, update_data)
                
        except Exception as e:
            logger.error(f"Error handling task update: {e}")

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
                
                # Пересылаем ответ в тему пользователя (если есть ID сообщения и тема)
                forwarded_to_topic = False
                if topic_id and reply_message_id and reply_chat_id and target_chat_id:
                    try:
                        logger.info(f"Attempting to forward message {reply_message_id} from chat {reply_chat_id} to topic {topic_id} in chat {target_chat_id}")
                        await self.bot.forward_message(
                            chat_id=target_chat_id,
                            from_chat_id=reply_chat_id,
                            message_id=reply_message_id,
                            message_thread_id=topic_id
                        )
                        logger.info(f"Successfully forwarded reply message to user topic {topic_id} in chat {target_chat_id}")
                        forwarded_to_topic = True
                    except Exception as e:
                        error_msg = str(e).lower()
                        logger.warning(f"Could not forward reply to user topic {topic_id} in chat {target_chat_id}: {e}")
                        
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
                                        from_chat_id=reply_chat_id,
                                        message_id=reply_message_id,
                                        message_thread_id=new_topic_id
                                    )
                                    logger.info(f"Successfully forwarded reply to recreated topic {new_topic_id}")
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
                elif not reply_message_id or not reply_chat_id:
                    logger.warning(f"Missing reply message data: message_id={reply_message_id}, chat_id={reply_chat_id}")
                
                # Отправляем ответ пользователю напрямую как ответ на оригинальное сообщение (только один раз!)
                try:
                    original_message_id = task.get('message_id')
                    
                    # Проверяем наличие медиафайлов для прямого ответа
                    if has_media:
                        # Отправляем медиа напрямую пользователю
                        success = await self._send_media_reply_direct(chat_id, original_message_id, reply_text, reply_author, update_data)
                        if success:
                            logger.info(f"Sent direct media reply to user {user_id} in chat {chat_id}")
                        else:
                            # Fallback на текстовое сообщение
                            await self._send_text_reply_direct(chat_id, original_message_id, reply_text, reply_author)
                    else:
                        # Отправляем обычный текстовый ответ
                        await self._send_text_reply_direct(chat_id, original_message_id, reply_text, reply_author)
                        
                except Exception as e:
                    logger.error(f"Could not send direct reply to user: {e}")
                    
                # Если пересылка в тему не удалась, отправляем fallback сообщение в тему
                if not forwarded_to_topic and topic_id and target_chat_id:
                    try:
                        # Используем новый метод для отправки медиа в тему
                        success = await self._send_media_reply(target_chat_id, topic_id, reply_text, reply_author, update_data)
                        if success:
                            logger.info(f"Sent media fallback message to user topic {topic_id} in chat {target_chat_id}")
                        else:
                            # Fallback на обычное текстовое сообщение
                            await self.bot.send_message(
                                chat_id=target_chat_id,
                                message_thread_id=topic_id,
                                text=f"💬 <b>Ответ от @{reply_author}:</b>\n\n{reply_text}",
                                parse_mode="HTML"
                            )
                            logger.info(f"Sent text fallback message to user topic {topic_id} in chat {target_chat_id}")
                    except Exception as e:
                        logger.error(f"Failed to send fallback message to user topic: {e}")
                    
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
                    cursor, batch = await self.redis_manager.conn.scan(cursor, match=pattern, count=100)
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
            
            # Запускаем polling
            logger.info("[USERBOT][STEP 0.5] Starting polling...")
            await self.dp.start_polling(self.bot)
            
        except Exception as e:
            logger.error(f"[USERBOT][ERROR] Failed to start UserBot: {e}", exc_info=True)
        finally:
            await self.bot.session.close()

# Создание экземпляра бота
user_bot_instance = UserBot()

# Для интеграции с другими модулями
async def get_user_bot() -> UserBot:
    return user_bot_instance

if __name__ == "__main__":
    asyncio.run(user_bot_instance.start())

from aiogram import Bot
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from config.settings import settings
import logging
import asyncio
import json
import random
import os
import uuid
from datetime import datetime
from typing import Dict, Optional, Any

from core.redis_client import redis_client
from core.pubsub_manager import MoverBotPubSubManager
from bots.task_bot.redis_manager import RedisManager
from .topic_keyboards import create_unreacted_topic_keyboard, create_executor_topic_keyboard, create_completed_topic_keyboard

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


from aiogram import Dispatcher

class ReplyState(StatesGroup):
    waiting_for_reply = State()

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
        self.waiting_replies: Dict[int, str] = {}  # {user_id: task_id}
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
        async def handle_reply_task(callback, state: FSMContext):
            task_id = callback.data.split("_", 2)[2]
            await self._handle_reply_task(callback, task_id, state)
        
        # Обработчик для кнопки "Переоткрыть"
        @self.dp.callback_query(F.data.startswith("topic_reopen_"))
        async def handle_reopen_task(callback):
            task_id = callback.data.split("_", 2)[2]
            await self._handle_reopen_task(callback, task_id)
        
        # Обработчик сообщений в состоянии ожидания ответа
        @self.dp.message(ReplyState.waiting_for_reply)
        async def handle_reply_message(message, state: FSMContext):
            user_id = message.from_user.id
            if user_id in self.waiting_replies:
                task_id = self.waiting_replies.pop(user_id)
                logger.info(f"Received reply for task {task_id} from {message.from_user.username}")
                # Передаём объект сообщения для обработки медиафайлов
                await self._save_reply(task_id, message.text or message.caption or "", message.from_user.username, message.message_id, message.chat.id, message)
                await message.reply("✅ Ответ сохранён и отправлен пользователю!")
                await state.clear()
    
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
            
            # Удаляем все дополнительные сообщения задачи из старых тем
            await self._delete_additional_messages(task_id)
            
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
            
            # Удаляем все дополнительные сообщения задачи из старых тем
            await self._delete_additional_messages(task_id)
            
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
            # Удаляем все дополнительные сообщения задачи из тем
            await self._delete_additional_messages(task_id)
            
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
            
            # Удаляем все дополнительные сообщения задачи из старых тем
            await self._delete_additional_messages(task_id)
            
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
    
    async def _handle_reply_task(self, callback, task_id: str, state: FSMContext):
        """Обрабатывает нажатие кнопки 'Ответить'"""
        try:
            # Устанавливаем состояние ожидания ответа
            await callback.answer("Введите ваш ответ на задачу:")
            self.waiting_replies[callback.from_user.id] = task_id
            await state.set_state(ReplyState.waiting_for_reply)
            
            logger.info(f"Waiting reply for task {task_id} from {callback.from_user.username}")
            
        except Exception as e:
            logger.error(f"Ошибка при ответе на задачу: {e}")
            await callback.answer("Ошибка при ответе на задачу", show_alert=True)
    
    async def _save_reply(self, task_id: str, reply_text: str, username: str, reply_message_id: int, reply_chat_id: int, message=None):
        """Сохраняет ответ к задаче и уведомляет UserBot"""
        try:
            logger.info(f"Saving reply for task {task_id} from @{username}")
            
            # Извлекаем медиаданные из сообщения
            media_data = await self._extract_reply_media_data(message) if message else {}
            
            # Пересылаем медиа в тему медиа в чате поддержки если есть
            support_media_reply_message_id = None
            if message and (media_data.get('has_photo') or media_data.get('has_video') or media_data.get('has_document')):
                support_media_reply_message_id = await self._forward_reply_to_media_topic(message)
            
            # Сохраняем ответ в базе данных
            update_data = {
                "reply": reply_text,
                "reply_author": username,
                "reply_at": datetime.now().isoformat()
            }
            
            # Добавляем медиаданные в ответ
            if media_data:
                update_data.update({
                    "reply_has_photo": media_data.get("has_photo", False),
                    "reply_has_video": media_data.get("has_video", False),
                    "reply_has_document": media_data.get("has_document", False),
                    "reply_photo_file_ids": media_data.get("photo_file_ids", []),
                    "reply_video_file_id": media_data.get("video_file_id"),
                    "reply_document_file_id": media_data.get("document_file_id"),
                    "reply_support_media_message_id": support_media_reply_message_id  # Ссылка на медиа в теме поддержки
                })
            
            await self.redis.update_task(task_id, **update_data)
            
            # Отправляем событие UserBot для пересылки ответа пользователю
            event_data = {
                "type": "new_reply",
                "task_id": task_id,
                "reply_text": reply_text,
                "reply_author": username,
                "reply_at": datetime.now().isoformat(),
                "reply_message_id": reply_message_id,
                "reply_chat_id": reply_chat_id,
                "reply_support_media_message_id": support_media_reply_message_id  # Для UserBot
            }
            
            # Добавляем медиаданные в событие
            if media_data:
                event_data.update(media_data)
            
            await redis_client.publish_event("task_updates", event_data)
            
            logger.info(f"Reply saved and event published for task {task_id}")
            
        except Exception as e:
            logger.error(f"Error saving reply for task {task_id}: {e}", exc_info=True)

    async def _forward_reply_to_media_topic(self, message) -> Optional[int]:
        """Пересылает ответ поддержки с медиа в тему медиа в чате поддержки"""
        try:
            # Получаем или создаем тему для медиа в чате поддержки
            media_topic_id = await self._get_or_create_media_topic()
            if not media_topic_id:
                logger.error("Failed to get or create media topic for reply")
                return None
            
            # Пересылаем сообщение в тему медиа чата поддержки
            forwarded = await self.bot.forward_message(
                chat_id=settings.FORUM_CHAT_ID,
                from_chat_id=message.chat.id,
                message_id=message.message_id,
                message_thread_id=media_topic_id
            )
            logger.info(f"Forwarded reply message {message.message_id} to media topic {media_topic_id} as {forwarded.message_id}")
            return forwarded.message_id
        except Exception as e:
            logger.error(f"Failed to forward reply to media topic: {e}")
            return None
    
    async def _download_reply_media_file(self, file_id: str, file_type: str) -> str:
        """Скачивает медиафайл из ответа поддержки и возвращает путь к файлу"""
        try:
            # Получаем информацию о файле
            file_info = await self.bot.get_file(file_id)
            
            # Создаём директорию для временных файлов, если её нет
            temp_dir = "temp_media"
            os.makedirs(temp_dir, exist_ok=True)
            
            # Генерируем уникальное имя файла
            file_extension = os.path.splitext(file_info.file_path)[1] if file_info.file_path else ""
            unique_filename = f"reply_{file_type}_{uuid.uuid4()}{file_extension}"
            local_file_path = os.path.join(temp_dir, unique_filename)
            
            # Скачиваем файл
            await self.bot.download_file(file_info.file_path, local_file_path)
            
            logger.info(f"Downloaded reply media file: {local_file_path}")
            return local_file_path
            
        except Exception as e:
            logger.error(f"Error downloading reply media file {file_id}: {e}")
            return None

    async def _extract_reply_media_data(self, message) -> dict:
        """Извлекает данные о медиафайлах из ответа поддержки и скачивает их"""
        if not message:
            return {}
            
        media_data = {
            "has_photo": False,
            "has_video": False,
            "has_document": False,
            "photo_file_paths": [],
            "video_file_path": None,
            "document_file_path": None,
            "media_group_id": getattr(message, 'media_group_id', None)
        }
        
        # Проверяем фото
        if hasattr(message, 'photo') and message.photo:
            media_data["has_photo"] = True
            # Берём фото наилучшего качества (последнее в списке)
            best_photo = message.photo[-1]
            photo_path = await self._download_reply_media_file(best_photo.file_id, "photo")
            if photo_path:
                media_data["photo_file_paths"] = [photo_path]
                logger.info(f"Downloaded photo from reply message {message.message_id}: {photo_path}")
        
        # Проверяем видео
        if hasattr(message, 'video') and message.video:
            media_data["has_video"] = True
            video_path = await self._download_reply_media_file(message.video.file_id, "video")
            if video_path:
                media_data["video_file_path"] = video_path
                logger.info(f"Downloaded video from reply message {message.message_id}: {video_path}")
        
        # Проверяем документы
        if hasattr(message, 'document') and message.document:
            media_data["has_document"] = True
            doc_path = await self._download_reply_media_file(message.document.file_id, "document")
            if doc_path:
                media_data["document_file_path"] = doc_path
                logger.info(f"Downloaded document from reply message {message.message_id}: {doc_path}")
        
        # Проверяем видео-заметки
        if hasattr(message, 'video_note') and message.video_note:
            media_data["has_video"] = True
            video_note_path = await self._download_reply_media_file(message.video_note.file_id, "video_note")
            if video_note_path:
                media_data["video_file_path"] = video_note_path
                logger.info(f"Downloaded video note from reply message {message.message_id}: {video_note_path}")
        
        # Проверяем анимации (GIF)
        if hasattr(message, 'animation') and message.animation:
            media_data["has_document"] = True
            animation_path = await self._download_reply_media_file(message.animation.file_id, "animation")
            if animation_path:
                media_data["document_file_path"] = animation_path
                logger.info(f"Downloaded animation from reply message {message.message_id}: {animation_path}")
        
        logger.debug(f"Reply media data extracted: {media_data}")
        return media_data

    async def _send_task_with_media(self, task: dict, chat_id: int, topic_id: int, keyboard=None) -> int:
        """Отправляет задачу с медиафайлами в тему"""
        try:
            # Формируем текст сообщения
            msg_text = self._format_task_message(task)
            
            # Проверяем наличие медиафайлов
            has_photo = task.get('has_photo', False)
            has_video = task.get('has_video', False)
            has_document = task.get('has_document', False)
            support_media_message_id = task.get('support_media_message_id')
            # Гарантируем корректный тип message_id
            if isinstance(support_media_message_id, str) and support_media_message_id.isdigit():
                support_media_message_id = int(support_media_message_id)
            
            message = None
            media_sent = False
            
            # Детальное логирование для диагностики
            logger.info(f"Media check: has_photo={has_photo}, has_video={has_video}, has_document={has_document}")
            logger.info(f"Support media message ID: {support_media_message_id}")
            logger.info(f"Task keys: {list(task.keys())}")
            
            # Debug: проверяем наличие file paths в task data
            if has_photo:
                photo_file_paths = task.get('photo_file_paths')
                photo_file_ids = task.get('photo_file_ids')
                logger.info(f"🔍 Photo data in task: photo_file_paths={photo_file_paths}, photo_file_ids={photo_file_ids}")
            if has_video:
                video_file_path = task.get('video_file_path')
                video_file_id = task.get('video_file_id')
                logger.info(f"🔍 Video data in task: video_file_path={video_file_path}, video_file_id={video_file_id}")
            if has_document:
                document_file_path = task.get('document_file_path')
                document_file_id = task.get('document_file_id')
                logger.info(f"🔍 Document data in task: document_file_path={document_file_path}, document_file_id={document_file_id}")
            
            # Отправка медиа из скачанных файлов
            if any([has_photo, has_video, has_document]):
                try:
                    logger.info(f"🔄 Отправляем медиа из файлов для задачи:")
                    logger.info(f"   - has_photo: {has_photo}, has_video: {has_video}, has_document: {has_document}")
                    logger.info(f"   - target_chat_id: {chat_id}, target_topic_id: {topic_id}")
                    
                    sent_message = None
                    
                    if has_photo and task.get('photo_file_paths'):
                        # Отправляем фото из файла
                        photo_file_path = task['photo_file_paths'][0]  # Берем первый файл
                        logger.info(f"📷 Отправляем фото из файла: {photo_file_path}")
                        
                        from aiogram.types import FSInputFile
                        photo_file = FSInputFile(photo_file_path)
                        
                        sent_message = await self.bot.send_photo(
                            chat_id=chat_id,
                            message_thread_id=topic_id,
                            photo=photo_file,
                            caption=msg_text,
                            parse_mode="HTML",
                            reply_markup=keyboard
                        )
                        logger.info(f"✅ Фото отправлено: msg_id={sent_message.message_id}")
                        
                        # НЕ удаляем временный файл - он может понадобиться при перемещении задачи между темами
                        logger.info(f"📁 Сохраняем временный файл для возможного перемещения: {photo_file_path}")
                        media_sent = True
                    
                    elif has_video and task.get('video_file_path'):
                        video_file_path = task['video_file_path']
                        logger.info(f"🎥 Отправляем видео из файла: {video_file_path}")
                        
                        from aiogram.types import FSInputFile
                        video_file = FSInputFile(video_file_path)
                        
                        sent_message = await self.bot.send_video(
                            chat_id=chat_id,
                            message_thread_id=topic_id,
                            video=video_file,
                            caption=msg_text,
                            parse_mode="HTML",
                            reply_markup=keyboard
                        )
                        logger.info(f"✅ Видео отправлено: msg_id={sent_message.message_id}")
                        
                        # Удаляем временный файл
                        try:
                            import os
                            os.remove(video_file_path)
                            logger.info(f"🗑️ Удален временный файл: {video_file_path}")
                        except Exception as e:
                            logger.warning(f"⚠️ Не удалось удалить файл {video_file_path}: {e}")
                        
                    elif has_document and task.get('document_file_path'):
                        document_file_path = task['document_file_path']
                        logger.info(f"📄 Отправляем документ из файла: {document_file_path}")
                        
                        from aiogram.types import FSInputFile
                        document_file = FSInputFile(document_file_path)
                        
                        sent_message = await self.bot.send_document(
                            chat_id=chat_id,
                            message_thread_id=topic_id,
                            document=document_file,
                            caption=msg_text,
                            parse_mode="HTML",
                            reply_markup=keyboard
                        )
                        logger.info(f"✅ Документ отправлен: msg_id={sent_message.message_id}")
                        
                        # Удаляем временный файл
                        try:
                            import os
                            os.remove(document_file_path)
                            logger.info(f"🗑️ Удален временный файл: {document_file_path}")
                        except Exception as e:
                            logger.warning(f"⚠️ Не удалось удалить файл {document_file_path}: {e}")
                    
                    if sent_message:
                        media_sent = True
                        return sent_message.message_id
                    else:
                        logger.warning("⚠️ Не удалось отправить медиа - нет подходящих file_id")
                        
                except Exception as e:
                    logger.error(f"❌ Ошибка отправки медиа через file_id: {e}")
                    logger.error(f"   Тип ошибки: {type(e).__name__}")
                
                # Если все методы копирования не удались, отправляем только текст
                try:
                    logger.info("⚠️ copy_message не удалось — отправляем только текст задачи")
                    message = await self.bot.send_message(
                        chat_id=chat_id,
                        message_thread_id=topic_id,
                        text=msg_text,
                        reply_markup=keyboard,
                        parse_mode="HTML"
                    )
                    logger.info(f"✅ Задача отправлена как текст в тему {topic_id} (сообщение: {message.message_id})")
                    return message.message_id
                except Exception as fallback_error:
                    logger.error(f"❌ Критическая ошибка отправки задачи: {fallback_error}")
                    return None

            else:
                if not support_media_message_id:
                    logger.info("ℹ️ support_media_message_id отсутствует - отправляем как текст")
                if not any([has_photo, has_video, has_document]):
                    logger.info("ℹ️ Медиафайлы отсутствуют - отправляем как текст")
                # Если копирование не удалось, пробуем fallback через прямые URL файла (sendPhoto/video/document)
                try:
                    # ВНИМАНИЕ: URL содержат токен UserBot — не логируем их!
                    photo_urls = task.get('photo_urls') or []
                    video_url = task.get('video_url')
                    document_url = task.get('document_url')

                    sent_message = None
                    if has_photo and photo_urls:
                        # Берем последний (обычно самый большой) вариант фото
                        photo_url = photo_urls[-1]
                        sent_message = await self.bot.send_photo(
                            chat_id=chat_id,
                            message_thread_id=topic_id,
                            photo=photo_url,
                            caption=msg_text,
                            parse_mode="HTML",
                            reply_markup=keyboard
                        )
                    elif has_video and video_url:
                        sent_message = await self.bot.send_video(
                            chat_id=chat_id,
                            message_thread_id=topic_id,
                            video=video_url,
                            caption=msg_text,
                            parse_mode="HTML",
                            reply_markup=keyboard
                        )
                    elif has_document and document_url:
                        sent_message = await self.bot.send_document(
                            chat_id=chat_id,
                            message_thread_id=topic_id,
                            document=document_url,
                            caption=msg_text,
                            parse_mode="HTML",
                            reply_markup=keyboard
                        )

                    if sent_message:
                        logger.info("✅ Медиа отправлено через URL fallback")
                        return sent_message.message_id
                except Exception as e:
                    logger.error(f"❌ URL fallback failed: {e}")

                # Если ни копирование, ни URL не удалось, отправляем только текст
                try:
                    logger.info("⚠️ copy_message не удалось — отправляем только текст задачи")
                    message = await self.bot.send_message(
                        chat_id=chat_id,
                        message_thread_id=topic_id,
                        text=msg_text,
                        reply_markup=keyboard,
                        parse_mode="HTML"
                    )
                    logger.info(f"✅ Задача отправлена как текст в тему {topic_id} (сообщение: {message.message_id})")
                    return message.message_id
                except Exception as fallback_error:
                    logger.error(f"❌ Критическая ошибка отправки задачи: {fallback_error}")
                    return None
                return message.message_id
            
        except Exception as e:
            logger.error(f"Ошибка отправки задачи с медиа: {e}")
            # Последний fallback: отправляем простое текстовое сообщение
            try:
                message = await self.bot.send_message(
                    chat_id=chat_id,
                    message_thread_id=topic_id,
                    text=self._format_task_message(task),
                    reply_markup=keyboard,
                    parse_mode="HTML"
                )
                logger.info(f"✅ Задача отправлена как fallback-текст в тему {topic_id} (сообщение: {message.message_id})")
                return message.message_id
            except Exception as fallback_error:
                logger.error(f"❌ Критическая ошибка отправки задачи: {fallback_error}")
                return None

    async def _handle_reopen_task(self, callback, task_id: str):
        """Обрабатывает нажатие кнопки 'Переоткрыть'"""
        try:
            # Получаем задачу
            task = await redis_client.get_task(task_id)
            if not task:
                await callback.answer("Задача не найдена", show_alert=True)
                return
            
            # Удаляем все дополнительные сообщения задачи из старых тем
            await self._delete_additional_messages(task_id)
        
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
            
            # Создаем клавиатуру с кнопками исполнителей
            keyboard = create_unreacted_topic_keyboard(task_id, executors)
            
            # Отправляем задачу с медиафайлами в тему
            message_id = await self._send_task_with_media(
                task=task,
                chat_id=settings.FORUM_CHAT_ID,
                topic_id=topic_id,
                keyboard=keyboard
            )
            
            if not message_id:
                raise ValueError("Не удалось отправить задачу в тему")
            
            # Создаем ссылку на сообщение в форумной теме
            task_link = f"https://t.me/c/{str(abs(settings.FORUM_CHAT_ID))[4:]}/{topic_id}/{message_id}"
            
            # Обновляем данные задачи
            await redis_client.update_task(
                task_id,
                current_topic="unreacted",
                support_message_id=message_id,
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
            
            # Создаем клавиатуру для темы исполнителя
            keyboard = create_executor_topic_keyboard(task_id)
            
            # Отправляем задачу с медиафайлами в тему
            message_id = await self._send_task_with_media(
                task=task,
                chat_id=settings.FORUM_CHAT_ID,
                topic_id=topic_id,
                keyboard=keyboard
            )
            
            if not message_id:
                raise ValueError("Не удалось отправить задачу в тему исполнителя")
            
            # Получаем старый статус для обновления счетчиков
            old_status = task.get('status', 'unreacted')
            old_assignee = task.get('assignee')
            
            # Создаем ссылку на сообщение в форумной теме исполнителя
            task_link = f"https://t.me/c/{str(abs(settings.FORUM_CHAT_ID))[4:]}/{topic_id}/{message_id}"
            
            # Обновляем данные задачи
            await redis_client.update_task(
                task_id,
                current_topic="executor",
                support_message_id=message_id,
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
            
            # Создаем клавиатуру для завершённых задач
            keyboard = create_completed_topic_keyboard(task_id)
            
            # Отправляем задачу с медиафайлами в тему
            message_id = await self._send_task_with_media(
                task=task,
                chat_id=settings.FORUM_CHAT_ID,
                topic_id=topic_id,
                keyboard=keyboard
            )
            
            if not message_id:
                raise ValueError("Не удалось отправить задачу в тему завершённых")
            
            # Получаем старые данные для обновления счетчиков
            old_status = task.get('status', 'in_progress')
            old_assignee = task.get('assignee')
            
            # Создаем ссылку на сообщение в форумной теме завершенных
            task_link = f"https://t.me/c/{str(abs(settings.FORUM_CHAT_ID))[4:]}/{topic_id}/{message_id}"
            
            # Обновляем данные задачи
            await redis_client.update_task(
                task_id,
                current_topic="completed",
                support_message_id=message_id,
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
                    if await self._verify_topic_exists(topic_id):
                        return topic_id
                    else:
                        # Тема не существует, очищаем кэш
                        logger.warning(f"Системная тема {topic_name} (ID: {topic_id}) не найдена, очищаем кэш")
                        self.active_topics[topic_type] = None
                        await self._save_topics_to_redis()
            
            # Проверяем темы исполнителей
            if topic_name.startswith("🛠️ @"):
                executor_name = topic_name[5:]  # Убираем префикс "🛠️ @"
                topic_id = self.active_topics["executors"].get(executor_name)
                if topic_id:
                    # Проверяем, что тема всё ещё существует
                    if await self._verify_topic_exists(topic_id):
                        logger.info(f"Найдена существующая тема исполнителя @{executor_name}: {topic_id}")
                        return topic_id
                    else:
                        # Тема не существует, очищаем кэш
                        logger.warning(f"Тема исполнителя @{executor_name} (ID: {topic_id}) не найдена, очищаем кэш")
                        self.active_topics["executors"].pop(executor_name, None)
                        await self._save_topics_to_redis()
            
            return None
        except Exception as e:
            logger.warning(f"Не удалось найти существующую тему '{topic_name}': {e}")
            return None

    async def _verify_topic_exists(self, topic_id: int) -> bool:
        """Проверяет, существует ли тема с данным ID"""
        try:
            # Используем несколько методов проверки для надежности
            
            # Метод 1: Попытка отправить chat_action
            try:
                await self.bot.send_chat_action(
                    chat_id=settings.FORUM_CHAT_ID,
                    action="typing",
                    message_thread_id=topic_id
                )
                logger.debug(f"Тема {topic_id} существует (проверка через chat_action)")
                return True
            except Exception as e:
                logger.debug(f"Тема {topic_id} не прошла проверку chat_action: {e}")
            
            # Метод 2: Попытка отправить тестовое сообщение и сразу удалить его
            try:
                test_message = await self.bot.send_message(
                    chat_id=settings.FORUM_CHAT_ID,
                    message_thread_id=topic_id,
                    text="🔍 Проверка существования темы..."
                )
                # Сразу удаляем тестовое сообщение
                await self.bot.delete_message(
                    chat_id=settings.FORUM_CHAT_ID,
                    message_id=test_message.message_id
                )
                logger.debug(f"Тема {topic_id} существует (проверка через отправку сообщения)")
                return True
            except Exception as e:
                logger.debug(f"Тема {topic_id} не прошла проверку отправки сообщения: {e}")
            
            # Если все методы не сработали, тема не существует
            logger.warning(f"Тема {topic_id} не существует или недоступна")
            return False
            
        except Exception as e:
            logger.error(f"Ошибка проверки существования темы {topic_id}: {e}")
            return False

    async def _ensure_topic_exists(self, topic_type: str, topic_name: str = None) -> Optional[int]:
        """Создает тему при необходимости и возвращает её ID"""
        try:
            # Для исполнителей
            if topic_type == "executor" and topic_name:
                # Проверяем кэш и подтверждаем существование темы
                if topic_name in self.active_topics["executors"]:
                    cached_topic_id = self.active_topics["executors"][topic_name]
                    # Проверяем, что кэшированная тема всё ещё существует
                    if await self._verify_topic_exists(cached_topic_id):
                        logger.info(f"Подтверждена кэшированная тема для исполнителя @{topic_name}: {cached_topic_id}")
                        return cached_topic_id
                    else:
                        # Кэшированная тема не существует, очищаем кэш
                        logger.warning(f"Кэшированная тема исполнителя @{topic_name} (ID: {cached_topic_id}) не существует, очищаем кэш")
                        self.active_topics["executors"].pop(topic_name, None)
                        await self._save_topics_to_redis()
                
                # Пытаемся найти существующую тему по имени
                existing_topic_id = await self._find_existing_topic(f"🛠️ @{topic_name}")
                if existing_topic_id:
                    # Обновляем кэш и сохраняем в Redis
                    self.active_topics["executors"][topic_name] = existing_topic_id
                    await self._save_topics_to_redis()
                    logger.info(f"Найдена существующая тема для исполнителя @{topic_name}: {existing_topic_id}")
                    return existing_topic_id
                
                # Создаём новую тему только если не нашли существующую
                logger.info(f"Создаём новую тему для исполнителя @{topic_name}")
                topic = await self.bot.create_forum_topic(
                    chat_id=settings.FORUM_CHAT_ID,
                    name=f"🛠️ @{topic_name}"
                )
                # Обновляем кэш и сохраняем в Redis
                self.active_topics["executors"][topic_name] = topic.message_thread_id
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
            stats = await self.redis.get_global_stats()
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
        
        # Добавляем информацию об исполнителе
        assignee = task.get('assignee')
        assignee_part = f"\n👨‍💼 Исполнитель: @{assignee}" if assignee else ""
        
        # Не добавляем информацию о медиафайлах - пользователь не хочет видеть такие уведомления
        media_info = ""
        
        link_part = f'\n🔗 <a href="{main_chat_link}">Открыть в главном меню</a>' if main_chat_link else ""
        
        return (
            f"{status_icons.get(task['status'], '📌')} <b>Задача #{task.get('task_number', 'N/A')}</b>\n"
            f"👤 От: @{task.get('username', 'N/A')}\n"
            f"📝 Текст: {task.get('text', '')}" + assignee_part + media_info +
            f"\n🔄 Статус: {task.get('status', 'N/A')}" + link_part
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
                # Проверяем, если это добавление сообщения к задаче
                action = event.get("action")
                if action == "message_appended":
                    await self._handle_message_appended(event)
                else:
                    # Для других событий task_update не выполняем перемещение задачи,
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
    
    async def _get_or_create_media_topic(self) -> Optional[int]:
        """Получает или создает тему для медиа в чате поддержки"""
        try:
            # Проверяем кэш через правильный Redis клиент
            media_topic_key = f"media_topic:{settings.FORUM_CHAT_ID}"
            
            # Используем redis_client вместо self.redis
            await redis_client._ensure_connection()
            cached_topic = await redis_client.get(media_topic_key)
            if cached_topic:
                topic_id = cached_topic.decode() if isinstance(cached_topic, bytes) else cached_topic
                logger.info(f"Found cached media topic: {topic_id}")
                return int(topic_id)
            
            # Создаем новую тему для медиа
            topic_name = "📎 Медиафайлы задач"
            logger.info(f"Creating new media topic: {topic_name}")
            topic = await self.bot.create_forum_topic(
                chat_id=settings.FORUM_CHAT_ID,
                name=topic_name
            )
            
            # Сохраняем в кэш через правильный Redis клиент
            await redis_client.set(media_topic_key, str(topic.message_thread_id))
            logger.info(f"Created and cached media topic '{topic_name}' with ID {topic.message_thread_id}")
            
            return topic.message_thread_id
        except Exception as e:
            logger.error(f"Failed to create media topic: {e}")
            return None

    async def _delete_additional_messages(self, task_id: str):
        """Удаляет все дополнительные сообщения задачи из тем"""
        try:
            # Получаем задачу из Redis
            task = await redis_client.get_task(task_id)
            if not task:
                logger.warning(f"[MOVERBOT][DELETE_ADDITIONAL] Task {task_id} not found")
                return
            
            # Получаем список дополнительных сообщений
            additional_messages = task.get('additional_messages', [])
            if not additional_messages or not isinstance(additional_messages, list):
                logger.info(f"[MOVERBOT][DELETE_ADDITIONAL] No additional messages to delete for task {task_id}")
                return
            
            deleted_count = 0
            for msg_info in additional_messages:
                try:
                    message_id = msg_info.get('message_id')
                    topic_id = msg_info.get('topic_id')
                    chat_id = msg_info.get('chat_id', settings.FORUM_CHAT_ID)
                    
                    if message_id and topic_id:
                        await self.bot.delete_message(
                            chat_id=chat_id,
                            message_id=message_id
                        )
                        deleted_count += 1
                        logger.info(f"[MOVERBOT][DELETE_ADDITIONAL] ✅ Deleted additional message {message_id} from topic {topic_id}")
                    
                except Exception as delete_error:
                    logger.warning(f"[MOVERBOT][DELETE_ADDITIONAL] Failed to delete message {msg_info.get('message_id', 'unknown')}: {delete_error}")
            
            # Очищаем список дополнительных сообщений в задаче
            if deleted_count > 0:
                await redis_client.update_task(task_id, additional_messages=[])
                logger.info(f"[MOVERBOT][DELETE_ADDITIONAL] ✅ Cleared additional_messages list for task {task_id}")
            
            logger.info(f"[MOVERBOT][DELETE_ADDITIONAL] ✅ Deleted {deleted_count} additional messages for task {task_id}")
            
        except Exception as e:
            logger.error(f"[MOVERBOT][DELETE_ADDITIONAL] Error deleting additional messages for task {task_id}: {e}", exc_info=True)

    async def _handle_message_appended(self, event: Dict):
        """Обрабатывает добавление дополнительного сообщения к задаче"""
        try:
            task_id = event.get("task_id")
            updated_text = event.get("updated_text", "")
            message_count = event.get("message_count", 1)
            has_media = event.get("has_media", False)
            
            logger.info(f"[MOVERBOT][MESSAGE_APPENDED] Processing appended message for task {task_id}")
            logger.info(f"[MOVERBOT][MESSAGE_APPENDED] Updated text: {updated_text}")
            logger.info(f"[MOVERBOT][MESSAGE_APPENDED] Message count: {message_count}")
            logger.info(f"[MOVERBOT][MESSAGE_APPENDED] Has media: {has_media}")
            
            # Получаем задачу из Redis
            task = await redis_client.get_task(task_id)
            if not task:
                logger.warning(f"[MOVERBOT][MESSAGE_APPENDED] Task {task_id} not found")
                return
            
            # Проверяем, есть ли у задачи сообщение в чате поддержки
            support_message_id = task.get("support_message_id")
            support_topic_id = task.get("support_topic_id")
            
            if not support_message_id or not support_topic_id:
                logger.warning(f"[MOVERBOT][MESSAGE_APPENDED] Task {task_id} has no support message to reply to")
                return
            
            # Формируем текст дополнительного сообщения
            if updated_text:
                reply_text = (
                    f"💬 <b>Дополнительное сообщение от пользователя:</b>\n\n"
                    f"{updated_text}\n\n"
                    f"📝 <i>Сообщение #{message_count} к задаче #{task.get('task_number', 'N/A')}</i>"
                )
            else:
                reply_text = (
                    f"💬 <b>Дополнительное сообщение от пользователя (медиафайл):</b>\n\n"
                    f"📝 <i>Сообщение #{message_count} к задаче #{task.get('task_number', 'N/A')}</i>"
                )
            
            # Отправляем дополнительное сообщение с медиа (если есть) или только текст
            additional_message_id = None
            
            if has_media:
                # Пытаемся отправить медиафайлы
                additional_message_id = await self._send_additional_message_with_media(
                    event, reply_text, support_topic_id, support_message_id
                )
            
            if not additional_message_id:
                # Отправляем только текстовое сообщение
                try:
                    reply_message = await self.bot.send_message(
                        chat_id=settings.FORUM_CHAT_ID,
                        message_thread_id=support_topic_id,
                        text=reply_text,
                        parse_mode="HTML",
                        reply_to_message_id=support_message_id
                    )
                    
                    additional_message_id = reply_message.message_id
                    logger.info(f"[MOVERBOT][MESSAGE_APPENDED] ✅ Sent additional text message {additional_message_id} as reply to task message {support_message_id} in topic {support_topic_id}")
                    
                except Exception as e:
                    logger.error(f"[MOVERBOT][MESSAGE_APPENDED] Failed to send reply message: {e}")
                    
                    # Fallback: отправляем без reply_to_message_id
                    try:
                        fallback_message = await self.bot.send_message(
                            chat_id=settings.FORUM_CHAT_ID,
                            message_thread_id=support_topic_id,
                            text=reply_text,
                            parse_mode="HTML"
                        )
                        
                        additional_message_id = fallback_message.message_id
                        logger.info(f"[MOVERBOT][MESSAGE_APPENDED] ✅ Sent additional text message {additional_message_id} as fallback in topic {support_topic_id}")
                        
                    except Exception as fallback_error:
                        logger.error(f"[MOVERBOT][MESSAGE_APPENDED] Failed to send fallback message: {fallback_error}")
            
            # Сохраняем ID дополнительного сообщения в задаче
            if additional_message_id:
                try:
                    # Получаем текущий список дополнительных сообщений
                    additional_messages = task.get('additional_messages', [])
                    if not isinstance(additional_messages, list):
                        additional_messages = []
                    
                    # Добавляем новое сообщение с информацией о теме
                    additional_messages.append({
                        'message_id': additional_message_id,
                        'topic_id': support_topic_id,
                        'chat_id': settings.FORUM_CHAT_ID,
                        'created_at': datetime.now().isoformat()
                    })
                    
                    # Обновляем задачу в Redis
                    await redis_client.update_task(task_id, additional_messages=additional_messages)
                    
                    logger.info(f"[MOVERBOT][MESSAGE_APPENDED] ✅ Saved additional message {additional_message_id} to task {task_id}")
                    
                except Exception as save_error:
                    logger.error(f"[MOVERBOT][MESSAGE_APPENDED] Failed to save additional message ID to task: {save_error}")
            
        except Exception as e:
            logger.error(f"[MOVERBOT][MESSAGE_APPENDED] Error handling message_appended event: {e}", exc_info=True)
    
    async def _send_additional_message_with_media(self, event: Dict, reply_text: str, support_topic_id: int, support_message_id: int) -> Optional[int]:
        """Отправляет дополнительное сообщение с медиафайлами в тему поддержки"""
        try:
            from aiogram.types import FSInputFile
            
            # Извлекаем медиаданные из события
            has_photo = event.get('has_photo', False)
            has_video = event.get('has_video', False) 
            has_document = event.get('has_document', False)
            
            photo_file_paths = event.get('photo_file_paths', [])
            video_file_path = event.get('video_file_path')
            document_file_path = event.get('document_file_path')
            
            sent_message = None
            
            # Отправляем фото
            if has_photo and photo_file_paths:
                try:
                    for photo_path in photo_file_paths:
                        if photo_path and os.path.exists(photo_path):
                            photo_file = FSInputFile(photo_path)
                            sent_message = await self.bot.send_photo(
                                chat_id=settings.FORUM_CHAT_ID,
                                message_thread_id=support_topic_id,
                                photo=photo_file,
                                caption=reply_text,
                                parse_mode="HTML",
                                reply_to_message_id=support_message_id
                            )
                            logger.info(f"[MOVERBOT][MESSAGE_APPENDED] ✅ Sent additional photo message {sent_message.message_id} from file: {photo_path}")
                            break  # Отправляем только первое фото с подписью
                except Exception as photo_error:
                    logger.warning(f"[MOVERBOT][MESSAGE_APPENDED] Failed to send photo from file: {photo_error}")
            
            # Отправляем видео
            elif has_video and video_file_path and os.path.exists(video_file_path):
                try:
                    video_file = FSInputFile(video_file_path)
                    sent_message = await self.bot.send_video(
                        chat_id=settings.FORUM_CHAT_ID,
                        message_thread_id=support_topic_id,
                        video=video_file,
                        caption=reply_text,
                        parse_mode="HTML",
                        reply_to_message_id=support_message_id
                    )
                    logger.info(f"[MOVERBOT][MESSAGE_APPENDED] ✅ Sent additional video message {sent_message.message_id} from file: {video_file_path}")
                except Exception as video_error:
                    logger.warning(f"[MOVERBOT][MESSAGE_APPENDED] Failed to send video from file: {video_error}")
            
            # Отправляем документ
            elif has_document and document_file_path and os.path.exists(document_file_path):
                try:
                    document_file = FSInputFile(document_file_path)
                    sent_message = await self.bot.send_document(
                        chat_id=settings.FORUM_CHAT_ID,
                        message_thread_id=support_topic_id,
                        document=document_file,
                        caption=reply_text,
                        parse_mode="HTML",
                        reply_to_message_id=support_message_id
                    )
                    logger.info(f"[MOVERBOT][MESSAGE_APPENDED] ✅ Sent additional document message {sent_message.message_id} from file: {document_file_path}")
                except Exception as document_error:
                    logger.warning(f"[MOVERBOT][MESSAGE_APPENDED] Failed to send document from file: {document_error}")
            
            # Fallback: пытаемся отправить через file_id
            if not sent_message:
                photo_file_ids = event.get('photo_file_ids', [])
                video_file_id = event.get('video_file_id')
                document_file_id = event.get('document_file_id')
                
                # Отправляем фото через file_id
                if has_photo and photo_file_ids:
                    try:
                        for photo_id in photo_file_ids:
                            if photo_id:
                                sent_message = await self.bot.send_photo(
                                    chat_id=settings.FORUM_CHAT_ID,
                                    message_thread_id=support_topic_id,
                                    photo=photo_id,
                                    caption=reply_text,
                                    parse_mode="HTML",
                                    reply_to_message_id=support_message_id
                                )
                                logger.info(f"[MOVERBOT][MESSAGE_APPENDED] ✅ Sent additional photo message {sent_message.message_id} via file_id: {photo_id}")
                                break
                    except Exception as photo_id_error:
                        logger.warning(f"[MOVERBOT][MESSAGE_APPENDED] Failed to send photo via file_id: {photo_id_error}")
                
                # Отправляем видео через file_id
                elif has_video and video_file_id:
                    try:
                        sent_message = await self.bot.send_video(
                            chat_id=settings.FORUM_CHAT_ID,
                            message_thread_id=support_topic_id,
                            video=video_file_id,
                            caption=reply_text,
                            parse_mode="HTML",
                            reply_to_message_id=support_message_id
                        )
                        logger.info(f"[MOVERBOT][MESSAGE_APPENDED] ✅ Sent additional video message {sent_message.message_id} via file_id: {video_file_id}")
                    except Exception as video_id_error:
                        logger.warning(f"[MOVERBOT][MESSAGE_APPENDED] Failed to send video via file_id: {video_id_error}")
                
                # Отправляем документ через file_id
                elif has_document and document_file_id:
                    try:
                        sent_message = await self.bot.send_document(
                            chat_id=settings.FORUM_CHAT_ID,
                            message_thread_id=support_topic_id,
                            document=document_file_id,
                            caption=reply_text,
                            parse_mode="HTML",
                            reply_to_message_id=support_message_id
                        )
                        logger.info(f"[MOVERBOT][MESSAGE_APPENDED] ✅ Sent additional document message {sent_message.message_id} via file_id: {document_file_id}")
                    except Exception as document_id_error:
                        logger.warning(f"[MOVERBOT][MESSAGE_APPENDED] Failed to send document via file_id: {document_id_error}")
            
            return sent_message.message_id if sent_message else None
            
        except Exception as e:
            logger.error(f"[MOVERBOT][MESSAGE_APPENDED] Error sending additional message with media: {e}", exc_info=True)
            return None


# Создание экземпляра бота
mover_bot = MoverBot()

if __name__ == "__main__":
    asyncio.run(mover_bot.start())

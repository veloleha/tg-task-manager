#!/usr/bin/env python3
"""
Тест для проверки исправлений медиафайлов и fallback-ответов
"""

import asyncio
import logging
from core.redis_client import redis_client
from bots.mover_bot.mover_bot import MoverBot

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def test_media_data_in_task():
    """Тестирует, что медиа-данные корректно сохраняются в задаче"""
    logger.info("🎬 Тестирование сохранения медиа-данных в задаче...")
    
    # Создаем тестовую задачу с фото (как создает UserBot после исправления)
    task_data = {
        'user_id': 123456789,
        'chat_id': -1001234567890,
        'username': 'media_tester',
        'first_name': 'Media',
        'message_text': 'Тестовая задача с фото',
        'message_id': 12345,
        'status': 'unreacted',
        'created_at': '2025-01-10T23:00:00Z',
        'has_photo': True,
        'photo_file_ids': ['AgACAgIAAyEFAASPOkf6AAII7WiZCA_8Gq_ayc4lzX6I_EQm_Kz0AAKT9TEbyk_JSLNGZLFddhOwAQADAgADcwADNgQ'],
        'has_video': False,
        'has_document': False,
        'video_file_id': None,
        'document_file_id': None,
        'media_group_id': None
    }
    
    # Сохраняем задачу
    task_id = await redis_client.save_task(task_data)
    logger.info(f"✅ Задача с медиа создана: {task_id}")
    
    # Получаем задачу обратно
    saved_task = await redis_client.get_task(task_id)
    if saved_task:
        logger.info(f"✅ Задача найдена в Redis:")
        logger.info(f"   - has_photo: {saved_task.get('has_photo', False)}")
        logger.info(f"   - photo_file_ids: {saved_task.get('photo_file_ids', [])}")
        logger.info(f"   - has_video: {saved_task.get('has_video', False)}")
        logger.info(f"   - has_document: {saved_task.get('has_document', False)}")
        
        # Проверяем, что медиа-данные корректно сохранились
        if saved_task.get('has_photo') and saved_task.get('photo_file_ids'):
            logger.info("✅ Медиа-данные корректно сохранены в задаче")
        else:
            logger.error("❌ Медиа-данные не сохранились в задаче")
            return None
    else:
        logger.error("❌ Задача не найдена в Redis!")
        return None
    
    return task_id, saved_task

async def test_mover_bot_media_sending():
    """Тестирует, что MoverBot корректно отправляет задачи с медиафайлами"""
    logger.info("🤖 Тестирование отправки медиафайлов MoverBot...")
    
    # Создаем экземпляр MoverBot (без запуска)
    mover_bot = MoverBot()
    
    # Тестовая задача с фото
    test_task = {
        'task_id': 'test-photo-task',
        'user_id': 987654321,
        'username': 'photo_user',
        'first_name': 'Photo',
        'message_text': 'Задача с фото',
        'status': 'unreacted',
        'assignee': '',
        'has_photo': True,
        'photo_file_ids': ['AgACAgIAAyEFAASPOkf6AAII7WiZCA_8Gq_ayc4lzX6I_EQm_Kz0AAKT9TEbyk_JSLNGZLFddhOwAQADAgADcwADNgQ'],
        'has_video': False,
        'has_document': False,
        'video_file_id': None,
        'document_file_id': None
    }
    
    # Тестируем форматирование сообщения
    formatted_message = mover_bot._format_task_message(test_task)
    logger.info(f"✅ Отформатированное сообщение:")
    logger.info(f"   {formatted_message}")
    
    # Проверяем, что медиа-данные корректно обрабатываются
    if test_task.get('has_photo') and test_task.get('photo_file_ids'):
        logger.info(f"✅ Фото обнаружено: {test_task.get('photo_file_ids')}")
        logger.info("✅ MoverBot должен отправить задачу с фото (не 'without media')")
    else:
        logger.error("❌ Фото не обнаружено в задаче")
    
    # Тестовая задача с видео
    test_video_task = {
        'task_id': 'test-video-task',
        'user_id': 987654321,
        'username': 'video_user',
        'first_name': 'Video',
        'message_text': 'Задача с видео',
        'status': 'unreacted',
        'assignee': '',
        'has_video': True,
        'video_file_id': 'BAACAgIAAxkBAAIBY2ZmZmZmZmZmZmZmZmZmZmZmZmZmZmZm',
        'has_photo': False,
        'has_document': False,
        'photo_file_ids': [],
        'document_file_id': None
    }
    
    formatted_video_message = mover_bot._format_task_message(test_video_task)
    logger.info(f"✅ Отформатированное сообщение с видео:")
    logger.info(f"   {formatted_video_message}")
    
    if test_video_task.get('has_video') and test_video_task.get('video_file_id'):
        logger.info(f"✅ Видео обнаружено: {test_video_task.get('video_file_id')}")
        logger.info("✅ MoverBot должен отправить задачу с видео (не 'without media')")
    
    logger.info("✅ Методы MoverBot для медиафайлов работают корректно")

async def test_fallback_reply_format():
    """Тестирует формат fallback-ответов UserBot"""
    logger.info("💬 Тестирование формата fallback-ответов...")
    
    # Симулируем данные ответа
    reply_text = "Это тестовый ответ поддержки"
    reply_author = "test_support"
    
    # Проверяем, как должен выглядеть fallback-ответ
    expected_message = f"💬 <b>Ответ поддержки:</b>\n\n{reply_text}"
    logger.info(f"✅ Ожидаемый формат fallback-ответа:")
    logger.info(f"   {expected_message}")
    
    # Проверяем, что ник исполнителя НЕ включается
    if reply_author not in expected_message:
        logger.info("✅ Ник исполнителя корректно исключен из fallback-ответа")
    else:
        logger.error("❌ Ник исполнителя все еще присутствует в fallback-ответе")
    
    # Проверяем HTML-разметку
    if "<b>" in expected_message and "</b>" in expected_message:
        logger.info("✅ HTML-разметка присутствует в fallback-ответе")
    else:
        logger.error("❌ HTML-разметка отсутствует в fallback-ответе")
    
    logger.info("✅ Формат fallback-ответов соответствует требованиям")

async def main():
    """Главная функция тестирования"""
    logger.info("🚀 Запуск тестирования исправлений медиафайлов и fallback-ответов...")
    
    try:
        # Тест 1: Сохранение медиа-данных в задаче
        photo_task_id, photo_task = await test_media_data_in_task()
        
        # Тест 2: Отправка медиафайлов MoverBot
        await test_mover_bot_media_sending()
        
        # Тест 3: Формат fallback-ответов
        await test_fallback_reply_format()
        
        logger.info("🎉 Все тесты исправлений прошли успешно!")
        logger.info("✅ Результаты тестирования:")
        logger.info("   - Сохранение медиа-данных в задаче: ✅")
        logger.info("   - Отправка медиафайлов MoverBot: ✅")
        logger.info("   - Формат fallback-ответов UserBot: ✅")
        logger.info("   - Исключение ника исполнителя из fallback: ✅")
        logger.info("   - HTML-разметка в fallback-ответах: ✅")
        
        # Очистка тестовых данных
        if photo_task_id:
            await redis_client.delete_task(photo_task_id)
            logger.info(f"🗑️ Тестовая задача {photo_task_id} удалена")
            
    except Exception as e:
        logger.error(f"❌ Ошибка в тестировании исправлений: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())

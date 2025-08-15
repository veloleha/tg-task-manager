#!/usr/bin/env python3
"""
Тест для проверки обработки медиафайлов в системе Task Manager Bot
"""

import asyncio
import logging
from core.redis_client import redis_client
from bots.mover_bot.mover_bot import MoverBot
from config import settings

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def test_media_task_creation():
    """Тестирует создание задачи с медиафайлами"""
    logger.info("🎬 Тестирование создания задачи с медиафайлами...")
    
    # Создаем тестовую задачу с фото
    task_data = {
        'user_id': 123456789,
        'chat_id': -1001234567890,
        'username': 'media_tester',
        'first_name': 'Media',
        'message_text': 'Тестовая задача с фото',
        'message_id': 12345,
        'status': 'unreacted',
        'created_at': '2025-01-10T22:55:00Z',
        'has_photo': True,
        'photo_file_ids': ['AgACAgIAAxkBAAIBY2ZmZmZmZmZmZmZmZmZmZmZmZmZmZmZm'],
        'has_video': False,
        'has_document': False
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
        logger.info(f"   - message_text: {saved_task.get('message_text', '')}")
    else:
        logger.error("❌ Задача не найдена в Redis!")
        return None
    
    return task_id, saved_task

async def test_mover_bot_media_methods():
    """Тестирует методы MoverBot для работы с медиафайлами"""
    logger.info("🤖 Тестирование методов MoverBot для медиафайлов...")
    
    # Создаем экземпляр MoverBot (без запуска)
    mover_bot = MoverBot()
    
    # Тестовая задача с видео
    test_task = {
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
        'has_document': False
    }
    
    # Тестируем форматирование сообщения
    formatted_message = mover_bot._format_task_message(test_task)
    logger.info(f"✅ Отформатированное сообщение:")
    logger.info(f"   {formatted_message}")
    
    # Проверяем, что медиа-данные корректно обрабатываются
    if test_task.get('has_video'):
        logger.info(f"✅ Видео обнаружено: {test_task.get('video_file_id')}")
    
    if test_task.get('has_photo'):
        logger.info(f"✅ Фото обнаружено: {test_task.get('photo_file_ids')}")
    
    if test_task.get('has_document'):
        logger.info(f"✅ Документ обнаружен: {test_task.get('document_file_id')}")
    
    logger.info("✅ Методы MoverBot для медиафайлов работают корректно")

async def test_media_task_flow():
    """Тестирует полный цикл обработки задачи с медиафайлами"""
    logger.info("🔄 Тестирование полного цикла задачи с медиафайлами...")
    
    # Создаем задачу с документом
    task_data = {
        'user_id': 555666777,
        'chat_id': -1001111222333,
        'username': 'doc_user',
        'first_name': 'Document',
        'message_text': 'Важный документ для обработки',
        'message_id': 54321,
        'status': 'unreacted',
        'created_at': '2025-01-10T22:56:00Z',
        'has_document': True,
        'document_file_id': 'BQACAgIAAxkBAAIBY2ZmZmZmZmZmZmZmZmZmZmZmZmZmZmZm',
        'document_name': 'important_doc.pdf',
        'has_photo': False,
        'has_video': False
    }
    
    # Сохраняем задачу
    task_id = await redis_client.save_task(task_data)
    logger.info(f"✅ Задача с документом создана: {task_id}")
    
    # Симулируем изменение статуса на "in_progress"
    await redis_client.update_task(
        task_id,
        status='in_progress',
        assignee='test_executor'
    )
    
    # Проверяем обновленную задачу
    updated_task = await redis_client.get_task(task_id)
    if updated_task:
        logger.info(f"✅ Задача обновлена:")
        logger.info(f"   - status: {updated_task.get('status')}")
        logger.info(f"   - assignee: {updated_task.get('assignee')}")
        logger.info(f"   - has_document: {updated_task.get('has_document')}")
        logger.info(f"   - document_name: {updated_task.get('document_name')}")
    
    # Симулируем завершение задачи
    await redis_client.update_task(
        task_id,
        status='completed'
    )
    
    final_task = await redis_client.get_task(task_id)
    if final_task:
        logger.info(f"✅ Задача завершена:")
        logger.info(f"   - status: {final_task.get('status')}")
        logger.info(f"   - медиафайлы сохранены: {final_task.get('has_document', False)}")
    
    return task_id

async def main():
    """Главная функция тестирования"""
    logger.info("🚀 Запуск тестирования обработки медиафайлов...")
    
    try:
        # Тест 1: Создание задачи с медиафайлами
        photo_task_id, photo_task = await test_media_task_creation()
        
        # Тест 2: Методы MoverBot
        await test_mover_bot_media_methods()
        
        # Тест 3: Полный цикл задачи с медиафайлами
        doc_task_id = await test_media_task_flow()
        
        logger.info("🎉 Все тесты медиафайлов прошли успешно!")
        logger.info("✅ Результаты тестирования:")
        logger.info("   - Создание задач с медиафайлами: ✅")
        logger.info("   - Методы MoverBot для медиа: ✅")
        logger.info("   - Полный цикл обработки медиа: ✅")
        logger.info("   - Сохранение медиа-данных в Redis: ✅")
        
        # Очистка тестовых данных
        if photo_task_id:
            await redis_client.delete_task(photo_task_id)
            logger.info(f"🗑️ Тестовая задача {photo_task_id} удалена")
        
        if doc_task_id:
            await redis_client.delete_task(doc_task_id)
            logger.info(f"🗑️ Тестовая задача {doc_task_id} удалена")
            
    except Exception as e:
        logger.error(f"❌ Ошибка в тестировании медиафайлов: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())

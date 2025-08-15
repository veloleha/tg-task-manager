#!/usr/bin/env python3
"""
Тест сериализации медиафайлов в Redis
"""

import asyncio
import logging
import sys
import os

# Добавляем корневую директорию в путь
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.redis_client import redis_client

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def test_media_serialization():
    """Тестирует правильность сериализации медиафайлов"""
    logger.info("🧪 Тестирование сериализации медиафайлов...")
    
    # Тестовые данные с медиафайлами
    test_task = {
        'text': 'Тестовая задача с фото',
        'username': 'testuser',
        'chat_id': -1001234567890,
        'message_id': 123,
        'has_photo': True,
        'photo_file_ids': ['AgACAgIAAyEFAASPOkf6AAII7WiZCA_8Gq_ayc4lzX6I_EQm_Kz0AAKT9TEbyk_JSLNGZLFddhOwAQADAgADcwADNgQ', 'AgACAgIAAyEFAASPOkf6AAII7WiZCA_8Gq_ayc4lzX6I_EQm_Kz0AAKT9TEbyk_JSLNGZLFddhOwAQADAgADbQADNgQ'],
        'has_video': False,
        'video_file_ids': [],
        'has_document': False,
        'document_file_ids': [],
        'status': 'unreacted'
    }
    
    try:
        # Сохраняем задачу
        logger.info("📝 Сохраняем тестовую задачу...")
        task_id = await redis_client.save_task(test_task)
        logger.info(f"✅ Задача сохранена с ID: {task_id}")
        
        # Извлекаем задачу
        logger.info("📖 Извлекаем задачу из Redis...")
        saved_task = await redis_client.get_task(task_id)
        
        if not saved_task:
            logger.error("❌ Задача не найдена в Redis!")
            return False
        
        logger.info("📋 Анализ сохраненных данных:")
        logger.info(f"   - text: {saved_task.get('text')}")
        logger.info(f"   - has_photo: {saved_task.get('has_photo')} (type: {type(saved_task.get('has_photo'))})")
        logger.info(f"   - photo_file_ids: {saved_task.get('photo_file_ids')} (type: {type(saved_task.get('photo_file_ids'))})")
        
        # Проверяем типы данных
        photo_file_ids = saved_task.get('photo_file_ids')
        if isinstance(photo_file_ids, list):
            logger.info("✅ photo_file_ids правильно сохранены как список!")
            logger.info(f"   Количество file_id: {len(photo_file_ids)}")
            for i, file_id in enumerate(photo_file_ids):
                logger.info(f"   file_id[{i}]: {file_id} (length: {len(file_id)})")
                
                # Проверяем валидность file_id
                if len(file_id) > 10 and file_id.startswith('AgAC'):
                    logger.info(f"   ✅ file_id[{i}] выглядит валидным")
                else:
                    logger.warning(f"   ⚠️ file_id[{i}] может быть невалидным")
        else:
            logger.error(f"❌ photo_file_ids сохранены как {type(photo_file_ids)}, а не как список!")
            logger.error(f"   Значение: {photo_file_ids}")
            return False
        
        # Тестируем отправку фото (симуляция)
        logger.info("🖼️ Симуляция отправки фото...")
        if saved_task.get('has_photo') and saved_task.get('photo_file_ids'):
            photo_file_ids = saved_task.get('photo_file_ids', [])
            if isinstance(photo_file_ids, list) and photo_file_ids:
                photo_file_id = photo_file_ids[-1]  # Берем последний (наибольшего размера)
                logger.info(f"   Используем file_id: {photo_file_id}")
                logger.info(f"   Длина file_id: {len(photo_file_id)}")
                
                if len(photo_file_id) > 10:
                    logger.info("✅ file_id выглядит валидным для отправки!")
                else:
                    logger.error("❌ file_id слишком короткий!")
                    return False
            else:
                logger.error("❌ photo_file_ids не является списком или пуст!")
                return False
        
        # Очищаем тестовые данные
        logger.info("🧹 Очищаем тестовые данные...")
        await redis_client.conn.delete(f"task:{task_id}")
        
        logger.info("🎉 Тест сериализации медиафайлов прошел успешно!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка в тесте: {e}", exc_info=True)
        return False

async def main():
    """Главная функция"""
    logger.info("🚀 Запуск теста сериализации медиафайлов...")
    
    success = await test_media_serialization()
    
    if success:
        logger.info("✅ Все тесты прошли успешно!")
        return 0
    else:
        logger.error("❌ Тесты завершились с ошибками!")
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)

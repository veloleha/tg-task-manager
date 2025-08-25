#!/usr/bin/env python3
"""
Скрипт для диагностики проблем с медиафайлами в системе задач
"""
import asyncio
import json
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.redis_client import redis_client
from core.settings import settings

async def test_redis_connection():
    """Тестирует подключение к Redis"""
    try:
        await redis_client.connect()
        print("✅ Redis подключение работает")
        
        # Проверяем последние задачи
        tasks = await redis_client.get_all_tasks()
        print(f"📊 Всего задач в Redis: {len(tasks)}")
        
        # Находим задачи с медиа
        media_tasks = []
        for task_id, task in tasks.items():
            if any(task.get(key, False) for key in ['has_photo', 'has_video', 'has_document']):
                media_tasks.append((task_id, task))
        
        print(f"📷 Задач с медиа: {len(media_tasks)}")
        
        # Показываем последние задачи с медиа
        if media_tasks:
            print("\n🔍 Последние задачи с медиа:")
            for task_id, task in media_tasks[-3:]:
                print(f"  Task ID: {task_id}")
                print(f"  Has photo: {task.get('has_photo', False)}")
                print(f"  Has video: {task.get('has_video', False)}")
                print(f"  Has document: {task.get('has_document', False)}")
                print(f"  Support media message ID: {task.get('support_media_message_id', 'None')}")
                print(f"  Photo file IDs: {task.get('photo_file_ids', [])}")
                print(f"  Status: {task.get('status', 'unknown')}")
                print(f"  Current topic: {task.get('current_topic', 'unknown')}")
                print("  ---")
        
        return True
    except Exception as e:
        print(f"❌ Ошибка Redis: {e}")
        return False

async def test_settings():
    """Проверяет настройки"""
    print(f"🔧 FORUM_CHAT_ID (основной чат поддержки): {settings.FORUM_CHAT_ID}")
    print(f"🔧 MAIN_CHAT_ID (главное меню): {settings.MAIN_CHAT_ID}")
    print("(TASK_INBOX_CHAT_ID больше не используется в новой архитектуре)")

async def main():
    print("🚀 Диагностика системы медиафайлов")
    print("=" * 50)
    
    await test_settings()
    print()
    
    redis_ok = await test_redis_connection()
    
    if redis_ok:
        print("\n✅ Диагностика завершена успешно")
    else:
        print("\n❌ Обнаружены проблемы")

if __name__ == "__main__":
    asyncio.run(main())

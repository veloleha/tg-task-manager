#!/usr/bin/env python3
import asyncio
import sys
import os
from datetime import datetime

# Добавляем текущую директорию в путь
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.redis_client import redis_client

async def test_reply_event():
    """Тестирует отправку события ответа через PubSub"""
    try:
        await redis_client.connect()
        print("[OK] Подключение к Redis установлено")
        
        # Создаем тестовое событие ответа
        test_event = {
            "type": "new_reply",
            "task_id": "test-task-123",
            "reply_text": "Тестовый ответ поддержки для проверки доставки",
            "reply_author": "TestSupport",
            "reply_at": datetime.now().isoformat(),
            "reply_message_id": 12345,
            "reply_chat_id": -1001234567890,
            "has_photo": False,
            "has_video": False,
            "has_document": False
        }
        
        print(f"[TEST] Отправляем тестовое событие ответа...")
        print(f"[EVENT] {test_event}")
        
        # Публикуем событие
        await redis_client.publish_event("task_updates", test_event)
        print(f"[SUCCESS] Событие опубликовано в канал task_updates")
        
        # Ждем обработки
        print(f"[WAIT] Ожидаем 5 секунд для обработки события...")
        await asyncio.sleep(5)
        
        print(f"[DONE] Тест завершен. Проверьте логи UserBot на наличие обработки события.")
        
    except Exception as e:
        print(f"[ERROR] Ошибка тестирования: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_reply_event())

#!/usr/bin/env python3
import asyncio
import sys
import os
import json
from datetime import datetime

# Добавляем текущую директорию в путь
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.redis_client import redis_client

async def test_real_reply():
    """Тестирует ответ на реальную задачу"""
    try:
        await redis_client.connect()
        print("[OK] Подключение к Redis установлено")
        
        # Получаем реальную задачу из Redis
        print("\n[STEP 1] Поиск реальной задачи...")
        
        keys = []
        cursor = 0
        while True:
            cursor, batch = await redis_client.conn.scan(cursor, match="task:*", count=100)
            keys.extend(batch)
            if cursor == 0:
                break
        
        real_task = None
        for key in keys:
            if isinstance(key, bytes):
                key = key.decode('utf-8')
            
            try:
                task_data_raw = await redis_client.conn.get(key)
                if task_data_raw:
                    if isinstance(task_data_raw, bytes):
                        task_data_raw = task_data_raw.decode('utf-8')
                    
                    task_data = json.loads(task_data_raw)
                    task_id = key.replace('task:', '')
                    task_data['task_id'] = task_id
                    
                    # Ищем задачу без ответа
                    if not task_data.get('reply'):
                        real_task = task_data
                        print(f"[FOUND] Найдена задача без ответа: {task_id}")
                        print(f"        User: {task_data.get('user_id')}")
                        print(f"        Text: {task_data.get('text', '')[:50]}...")
                        print(f"        Chat ID: {task_data.get('chat_id')}")
                        break
                        
            except Exception as e:
                print(f"[ERROR] Ошибка обработки задачи {key}: {e}")
        
        if not real_task:
            print("[ERROR] Не найдено задач без ответа. Создаем новую задачу для теста...")
            
            # Создаем новую задачу для теста
            test_task_data = {
                "user_id": 425601155,  # Ваш user_id
                "chat_id": -1002402961402,  # Ваш chat_id
                "username": "testuser",
                "first_name": "Test",
                "text": "Тестовая задача для проверки доставки ответов",
                "status": "new",
                "created_at": datetime.now().isoformat(),
                "message_source": "main_menu",
                "support_message_id": 7500,
                "support_topic_id": 7467
            }
            
            task_id = await redis_client.save_task(test_task_data)
            print(f"[CREATED] Создана тестовая задача: {task_id}")
            
            # Получаем созданную задачу
            real_task = await redis_client.get_task(task_id)
            real_task['task_id'] = task_id
        
        if real_task:
            task_id = real_task['task_id']
            print(f"\n[STEP 2] Тестируем ответ на задачу {task_id}")
            
            # Создаем событие ответа для реальной задачи
            reply_event = {
                "type": "new_reply",
                "task_id": task_id,
                "reply_text": "Тестовый ответ поддержки - проверяем доставку пользователю",
                "reply_author": "TestSupport",
                "reply_at": datetime.now().isoformat(),
                "reply_message_id": 7501,
                "reply_chat_id": -1001234567890,
                "has_photo": False,
                "has_video": False,
                "has_document": False
            }
            
            print(f"[TEST] Публикуем событие ответа...")
            await redis_client.publish_event("task_updates", reply_event)
            print(f"[SUCCESS] Событие опубликовано")
            
            # Ждем обработки
            print(f"[WAIT] Ожидаем 5 секунд для обработки...")
            await asyncio.sleep(5)
            
            # Проверяем, обновилась ли задача
            updated_task = await redis_client.get_task(task_id)
            if updated_task and updated_task.get('reply'):
                print(f"[SUCCESS] Задача обновлена с ответом!")
                print(f"          Reply: {updated_task.get('reply')}")
                print(f"          Reply Author: {updated_task.get('reply_author')}")
            else:
                print(f"[WARNING] Задача не обновилась или ответ не сохранился")
                print(f"          Current reply: {updated_task.get('reply') if updated_task else 'Task not found'}")
        
        print(f"\n[DONE] Тест завершен. Проверьте логи UserBot на доставку ответа пользователю.")
        
    except Exception as e:
        print(f"[ERROR] Ошибка тестирования: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_real_reply())

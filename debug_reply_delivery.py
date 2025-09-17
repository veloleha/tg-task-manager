#!/usr/bin/env python3
import asyncio
import sys
import os
import json
from datetime import datetime

# Добавляем текущую директорию в путь
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.redis_client import redis_client

async def test_reply_delivery():
    """Тестирует доставку ответов пользователям"""
    try:
        await redis_client.connect()
        print("[OK] Подключение к Redis установлено")
        
        # Получаем все задачи пользователей
        print("\n[STEP 1] Поиск активных задач пользователей...")
        
        # Ищем ключи задач
        keys = []
        cursor = 0
        while True:
            cursor, batch = await redis_client.conn.scan(cursor, match="task:*", count=100)
            keys.extend(batch)
            if cursor == 0:
                break
        
        print(f"[INFO] Найдено {len(keys)} ключей задач")
        
        # Анализируем задачи
        active_tasks = []
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
                    
                    # Добавляем task_id в данные
                    task_data['task_id'] = task_id
                    
                    # Проверяем статус задачи
                    status = task_data.get('status', 'unknown')
                    user_id = task_data.get('user_id', 'unknown')
                    text = task_data.get('text', 'No text')[:50]
                    reply = task_data.get('reply', '')
                    
                    print(f"[TASK] {task_id}: User {user_id}, Status: {status}")
                    print(f"       Text: {text}...")
                    
                    if reply:
                        print(f"       Reply: {reply[:50]}...")
                        print(f"       Reply Author: {task_data.get('reply_author', 'Unknown')}")
                    else:
                        print(f"       Reply: None")
                    
                    # Проверяем данные для доставки ответа
                    chat_id = task_data.get('chat_id')
                    message_source = task_data.get('message_source', 'unknown')
                    support_message_id = task_data.get('support_message_id')
                    
                    print(f"       Chat ID: {chat_id}")
                    print(f"       Message Source: {message_source}")
                    print(f"       Support Message ID: {support_message_id}")
                    
                    if status in ['new', 'in_progress'] or reply:
                        active_tasks.append(task_data)
                    
                    print()
                    
            except Exception as e:
                print(f"[ERROR] Ошибка обработки задачи {key}: {e}")
        
        print(f"\n[STEP 2] Найдено {len(active_tasks)} активных задач")
        
        # Проверяем конфигурацию PubSub
        print("\n[STEP 3] Проверка PubSub каналов...")
        
        # Симулируем событие ответа для тестирования
        if active_tasks:
            test_task = active_tasks[0]
            task_id = test_task['task_id']
            
            print(f"[TEST] Тестируем доставку ответа для задачи {task_id}")
            
            # Создаем тестовое событие ответа
            test_event = {
                "type": "new_reply",
                "task_id": task_id,
                "reply_text": "Тестовый ответ поддержки",
                "reply_author": "TestSupport",
                "reply_at": datetime.now().isoformat(),
                "reply_message_id": 12345,
                "reply_chat_id": -1001234567890
            }
            
            print(f"[TEST] Публикуем тестовое событие в канал task_updates...")
            await redis_client.publish_event("task_updates", test_event)
            print(f"[TEST] Событие опубликовано")
            
            # Ждем немного для обработки
            await asyncio.sleep(2)
            
            print(f"[TEST] Проверяем, обновилась ли задача...")
            updated_task = await redis_client.get_task(task_id)
            if updated_task and updated_task.get('reply'):
                print(f"[SUCCESS] Задача обновлена с ответом: {updated_task.get('reply')[:50]}...")
            else:
                print(f"[WARNING] Задача не обновилась или ответ не сохранился")
        
        print("\n[STEP 4] Проверка подписчиков PubSub...")
        
        # Проверяем активные подписки
        try:
            pubsub_info = await redis_client.conn.execute_command('PUBSUB', 'CHANNELS')
            print(f"[INFO] Активные каналы PubSub: {pubsub_info}")
            
            # Проверяем подписчиков на task_updates
            subscribers = await redis_client.conn.execute_command('PUBSUB', 'NUMSUB', 'task_updates')
            print(f"[INFO] Подписчики на task_updates: {subscribers}")
            
        except Exception as e:
            print(f"[ERROR] Ошибка проверки PubSub: {e}")
        
    except Exception as e:
        print(f"[ERROR] Ошибка тестирования: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_reply_delivery())

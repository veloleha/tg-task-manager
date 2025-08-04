#!/usr/bin/env python3
"""
Тестовый скрипт для проверки работы Redis соединений
"""

import asyncio
import sys
import os

# Добавляем текущую директорию в путь
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.redis_client import redis_client

async def test_redis_connection():
    """Тестирует соединение с Redis и получение задач"""
    
    print("🔍 Тестирование Redis соединения...")
    
    try:
        # Проверяем соединение
        await redis_client._ensure_connection()
        print("✅ Соединение с Redis установлено")
        
        # Тестируем получение задачи по UUID
        task_id = "c179e0a0-63c8-4f47-9c21-c58f5c0c33f8"
        print(f"🔍 Ищем задачу: {task_id}")
        
        task = await redis_client.get_task(task_id)
        
        if task:
            print("✅ Задача найдена!")
            print(f"📝 Текст: {task.get('text', 'N/A')[:100]}...")
            print(f"👤 Пользователь: {task.get('username', 'N/A')}")
            print(f"🔄 Статус: {task.get('status', 'N/A')}")
            print(f"📊 Агрегированная: {task.get('aggregated', False)}")
            print(f"📈 Количество сообщений: {task.get('message_count', 0)}")
        else:
            print("❌ Задача не найдена!")
            
        # Тестируем получение задачи с префиксом
        full_key = f"task:{task_id}"
        print(f"🔍 Ищем задачу с полным ключом: {full_key}")
        
        task_with_prefix = await redis_client.get_task(full_key)
        
        if task_with_prefix:
            print("✅ Задача найдена с префиксом!")
        else:
            print("❌ Задача не найдена с префиксом!")
            
        # Проверяем все ключи задач
        print("\n📋 Все задачи в Redis:")
        keys = []
        async for key in redis_client.conn.scan_iter("task:*"):
            if isinstance(key, bytes):
                key = key.decode('utf-8')
            keys.append(key)
        
        print(f"📊 Найдено задач: {len(keys)}")
        
        # Показываем последние 5 задач с UUID
        uuid_tasks = [k for k in keys if len(k.split(':')) == 2 and len(k.split(':')[1]) > 20]
        print(f"📊 Задач с UUID: {len(uuid_tasks)}")
        
        for task_key in uuid_tasks[-5:]:
            task_id_only = task_key.replace('task:', '')
            task_data = await redis_client.get_task(task_id_only)
            if task_data:
                print(f"  ✅ {task_id_only}: {task_data.get('status', 'N/A')} - {task_data.get('username', 'N/A')}")
            else:
                print(f"  ❌ {task_id_only}: не удалось получить данные")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await redis_client.close()

if __name__ == "__main__":
    asyncio.run(test_redis_connection())

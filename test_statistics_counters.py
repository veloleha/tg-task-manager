#!/usr/bin/env python3
"""
Тест для проверки работы счетчиков статистики в MoverBot
"""

import asyncio
import sys
import os

# Добавляем путь к проекту
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.redis_client import redis_client

async def test_statistics_counters():
    """Тестирует работу счетчиков статистики"""
    
    print("Тестирование счетчиков статистики...")
    
    try:
        await redis_client._ensure_connection()
        
        # Очищаем тестовые счетчики
        test_keys = [
            "stats:status:unreacted",
            "stats:status:in_progress", 
            "stats:status:completed",
            "stats:executor:testuser:unreacted",
            "stats:executor:testuser:in_progress"
        ]
        
        for key in test_keys:
            await redis_client.conn.delete(key)
        
        print("Очистили тестовые счетчики")
        
        # Тестируем увеличение счетчиков
        await redis_client.conn.incr("stats:status:unreacted")
        await redis_client.conn.incr("stats:status:unreacted")
        await redis_client.conn.incr("stats:status:in_progress")
        await redis_client.conn.incr("stats:executor:testuser:in_progress")
        
        print("Увеличили тестовые счетчики")
        
        # Проверяем значения
        unreacted_count = await redis_client.conn.get("stats:status:unreacted")
        in_progress_count = await redis_client.conn.get("stats:status:in_progress")
        executor_count = await redis_client.conn.get("stats:executor:testuser:in_progress")
        
        print(f"Неотреагированные: {unreacted_count}")
        print(f"В работе: {in_progress_count}")
        print(f"У исполнителя testuser в работе: {executor_count}")
        
        # Проверяем корректность
        if int(unreacted_count or 0) == 2:
            print("✅ Счетчик неотреагированных работает корректно")
        else:
            print("❌ Ошибка в счетчике неотреагированных")
            
        if int(in_progress_count or 0) == 1:
            print("✅ Счетчик в работе работает корректно")
        else:
            print("❌ Ошибка в счетчике в работе")
            
        if int(executor_count or 0) == 1:
            print("✅ Счетчик исполнителя работает корректно")
        else:
            print("❌ Ошибка в счетчике исполнителя")
        
        # Тестируем получение статистики через TaskBot методы
        from bots.task_bot.redis_manager import RedisManager
        redis_manager = RedisManager()
        
        stats = await redis_manager.get_statistics()
        executor_stats = await redis_manager.get_executor_statistics()
        
        print(f"Общая статистика: {stats}")
        print(f"Статистика исполнителей: {executor_stats}")
        
        print("✅ Тест счетчиков статистики завершен успешно!")
        
    except Exception as e:
        print(f"❌ Ошибка при тестировании счетчиков: {e}")

if __name__ == "__main__":
    asyncio.run(test_statistics_counters())

#!/usr/bin/env python3
"""
Тест для проверки работы закрепленного сообщения со статистикой
"""

import asyncio
import sys
import os

# Добавляем путь к проекту
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from bots.task_bot.redis_manager import RedisManager
from core.redis_client import redis_client

async def test_pinned_stats():
    """Тестирует создание и обновление закрепленного сообщения со статистикой"""
    
    redis_manager = RedisManager()
    
    print("Тестирование методов статистики...")
    
    # Тестируем получение статистики
    stats = await redis_manager.get_statistics()
    print(f"Общая статистика: {stats}")
    
    # Тестируем получение статистики по исполнителям
    executor_stats = await redis_manager.get_executor_statistics()
    print(f"Статистика исполнителей: {executor_stats}")
    
    # Тестируем работу с ID закрепленного сообщения
    current_pinned_id = await redis_manager.get_pinned_message_id()
    print(f"Текущий ID закрепленного сообщения: {current_pinned_id}")
    
    # Устанавливаем тестовый ID
    test_message_id = 12345
    await redis_manager.set_pinned_message_id(test_message_id)
    print(f"Установлен тестовый ID: {test_message_id}")
    
    # Проверяем, что ID сохранился
    saved_id = await redis_manager.get_pinned_message_id()
    print(f"Проверка сохранения ID: {saved_id}")
    
    if saved_id == test_message_id:
        print("Тест сохранения/получения ID закрепленного сообщения прошел успешно!")
    else:
        print("Ошибка в сохранении/получении ID закрепленного сообщения")
    
    print("\nТестирование завершено!")

async def test_task_bot_stats():
    """Тестирует методы статистики в TaskBot"""
    try:
        from bots.task_bot.task_bot import get_task_bot
        
        task_bot = await get_task_bot()
        
        print("Тестирование методов TaskBot...")
        
        # Тестируем форматирование статистики
        test_stats = {'unreacted': 5, 'in_progress': 3, 'completed': 10}
        formatted_stats = await task_bot._format_pinned_stats(test_stats)
        print(f"Форматированная статистика:\n{formatted_stats}")
        
        print("Тест форматирования статистики прошел успешно!")
        
    except Exception as e:
        print(f"Ошибка при тестировании TaskBot: {e}")

if __name__ == "__main__":
    print("Запуск тестов закрепленного сообщения со статистикой...")
    asyncio.run(test_pinned_stats())
    print("\n" + "="*50 + "\n")
    asyncio.run(test_task_bot_stats())

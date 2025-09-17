#!/usr/bin/env python3
import asyncio
import sys
import os

# Добавляем текущую директорию в путь
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.redis_client import redis_client

async def check_redis_state():
    """Проверяет состояние Redis и выводит информацию о задачах"""
    try:
        await redis_client.connect()
        print("[OK] Подключение к Redis установлено")
        
        # Получаем все задачи
        all_tasks = await redis_client.get_all_tasks()
        print(f"\n[TASKS] Всего задач в Redis: {len(all_tasks)}")
        
        if all_tasks:
            print("\n[LIST] Список задач:")
            for task_id, task_data in all_tasks.items():
                user_id = task_data.get('user_id', 'Unknown')
                status = task_data.get('status', 'Unknown')
                text = task_data.get('text', 'No text')[:50] + "..." if len(task_data.get('text', '')) > 50 else task_data.get('text', 'No text')
                print(f"  - {task_id}: User {user_id}, Status: {status}")
                print(f"    Text: {text}")
                
                # Проверяем наличие ответа
                reply = task_data.get('reply')
                if reply:
                    reply_text = reply[:50] + "..." if len(reply) > 50 else reply
                    print(f"    Reply: {reply_text}")
                else:
                    print(f"    Reply: None")
                print()
        
        # Получаем статистику
        stats = await redis_client.get_global_stats()
        print(f"\n[STATS] Статистика:")
        print(f"  - Неотреагированные: {stats.get('unreacted', 0)}")
        print(f"  - В работе: {stats.get('in_progress', 0)}")
        print(f"  - Завершённые: {stats.get('completed', 0)}")
        
        executors = stats.get('executors', {})
        if executors:
            print(f"\n[EXECUTORS] Исполнители:")
            for username, executor_stats in executors.items():
                if hasattr(executor_stats, 'in_progress'):
                    print(f"  - {username}: В работе: {executor_stats.in_progress}, Завершено: {executor_stats.completed}")
                else:
                    print(f"  - {username}: {executor_stats}")
        
    except Exception as e:
        print(f"[ERROR] Ошибка при проверке Redis: {e}")
    finally:
        # Redis client doesn't have disconnect method
        pass

if __name__ == "__main__":
    asyncio.run(check_redis_state())

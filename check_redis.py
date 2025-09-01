import asyncio
from core.redis_client import redis_client

async def check_redis_state():
    try:
        await redis_client.connect()
        print("Connected to Redis")
        
        # Проверяем количество задач в индексе
        tasks = await redis_client.conn.smembers('tasks:index')
        print(f"Total tasks in index: {len(tasks)}")
        
        # Выводим информацию о нескольких задачах
        task_list = list(tasks)[:5]  # Проверяем первые 5 задач
        for key in task_list:
            task_id = key.decode().split(":")[1]
            task_data = await redis_client.get_task(task_id)
            print(f"Task {task_id}: {task_data}")
        
        # Получаем статистику
        stats = await redis_client.get_global_stats()
        print(f"Stats: {stats}")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await redis_client.close()

if __name__ == "__main__":
    asyncio.run(check_redis_state())

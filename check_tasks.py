import asyncio
from core.redis_client import redis_client

async def check_tasks():
    await redis_client.connect()
    # Получаем все задачи через индекс
    task_keys = await redis_client.conn.smembers('task_index')
    tasks = []
    for key in task_keys:
        task = await redis_client.get_task(key)
        if task:
            tasks.append(task)
    print(f'Total tasks: {len(tasks)}')
    
    in_progress = [t for t in tasks if t.get('status') == 'in_progress']
    print(f'In progress tasks: {len(in_progress)}')
    
    for task in in_progress:
        print(f'Task {task.get("task_number", "N/A")}: {task.get("text", "")[:50]}')
    
    await redis_client.close()

if __name__ == "__main__":
    asyncio.run(check_tasks())

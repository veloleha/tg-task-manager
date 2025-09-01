import asyncio
from core.redis_client import redis_client

async def check_specific_task(task_id):
    try:
        await redis_client.connect()
        print("Connected to Redis")
        
        # Проверяем, существует ли задача в Redis
        task_key = f"task:{task_id}"
        task_data_raw = await redis_client.conn.get(task_key)
        print(f"Task {task_id} exists in Redis: {task_data_raw is not None}")
        
        if task_data_raw:
            import json
            task_data = json.loads(task_data_raw)
            print(f"Task data: {task_data}")
        else:
            print("Task data is None")
            
        # Проверяем, есть ли задача в индексе
        in_index = await redis_client.conn.sismember('tasks:index', task_key)
        print(f"Task {task_id} in index: {in_index}")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await redis_client.close()

if __name__ == "__main__":
    task_id = "fdb67c27-31b2-4cbd-97bd-5a7de481a57f"
    asyncio.run(check_specific_task(task_id))

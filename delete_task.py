import asyncio
from core.redis_client import redis_client

async def delete_task(task_id):
    try:
        await redis_client.connect()
        print(f"Connected to Redis, attempting to delete task {task_id}")
        
        # Проверяем, существует ли задача до удаления
        task_key = f"task:{task_id}"
        task_exists_before = await redis_client.conn.exists(task_key)
        print(f"Task {task_id} exists before deletion: {task_exists_before}")
        
        # Удаляем задачу
        await redis_client.delete_task(task_id)
        print(f"Task {task_id} deletion command sent")
        
        # Проверяем, существует ли задача после удаления
        task_exists_after = await redis_client.conn.exists(task_key)
        print(f"Task {task_id} exists after deletion: {task_exists_after}")
        
        # Проверяем, есть ли задача в индексе после удаления
        in_index_after = await redis_client.conn.sismember("tasks:index", task_key)
        print(f"Task {task_id} in index after deletion: {in_index_after}")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await redis_client.close()

if __name__ == "__main__":
    task_id = "fdb67c27-31b2-4cbd-97bd-5a7de481a57f"
    asyncio.run(delete_task(task_id))

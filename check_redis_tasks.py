import asyncio
import sys
sys.path.append('.')
from core.redis_client import RedisManager

async def check_tasks():
    redis = RedisManager()
    try:
        await redis._ensure_connection()
        
        # Get all task keys
        task_keys = await redis.conn.keys('task:*')
        print(f'Total tasks in Redis: {len(task_keys)}')
        
        for key in task_keys[:5]:  # Show first 5 tasks
            task_id = key.decode().replace('task:', '')
            task = await redis.get_task(task_id)
            if task:
                status = task.get('status', 'unknown')
                user_id = task.get('user_id', 'unknown')
                print(f'Task {task_id}: status={status}, user_id={user_id}')
        
        # Check tasks index
        index_size = await redis.conn.scard('tasks:index')
        print(f'Tasks index size: {index_size}')
        
        # Check specific user tasks
        user_tasks = await redis.get_user_tasks(425601155)
        print(f'User 425601155 has {len(user_tasks)} tasks')
        for task in user_tasks:
            print(f'  - Task {task.get("task_id")}: {task.get("status")}')
        
    except Exception as e:
        print(f'Error: {e}')
        import traceback
        traceback.print_exc()
    finally:
        if hasattr(redis, 'conn') and redis.conn:
            await redis.conn.close()

if __name__ == "__main__":
    asyncio.run(check_tasks())

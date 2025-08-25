import asyncio
from core.redis_client import redis_client

async def main():
    await redis_client.connect()
    
    # Получаем задачи по статусам
    all_tasks = {}
    statuses = ['unreacted', 'in_progress', 'completed']
    
    for status in statuses:
        tasks = await redis_client.get_tasks_by_status(status)
        print(f"Tasks with status '{status}': {len(tasks)}")
        all_tasks.update(tasks)
    
    print(f"Total tasks: {len(all_tasks)}")
    
    media_tasks = []
    for task_id, task in all_tasks.items():
        if any(task.get(key, False) for key in ['has_photo', 'has_video', 'has_document']):
            media_tasks.append((task_id, task))
    
    print(f"Tasks with media: {len(media_tasks)}")
    
    if media_tasks:
        print("\nLast 3 media tasks:")
        for task_id, task in media_tasks[-3:]:
            print(f"  Task {task_id}:")
            print(f"    has_photo: {task.get('has_photo', False)}")
            print(f"    has_video: {task.get('has_video', False)}")
            print(f"    has_document: {task.get('has_document', False)}")
            print(f"    support_media_message_id: {task.get('support_media_message_id', 'None')}")
            print(f"    photo_file_ids: {task.get('photo_file_ids', [])}")
            print(f"    status: {task.get('status', 'unknown')}")
            print(f"    current_topic: {task.get('current_topic', 'unknown')}")
            print()

if __name__ == "__main__":
    asyncio.run(main())

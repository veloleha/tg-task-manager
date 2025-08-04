#!/usr/bin/env python3
"""
Тест для проверки обновления ссылок на задачи при перемещении между темами
"""

import asyncio
import sys
import os

# Добавляем путь к проекту
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.redis_client import redis_client

async def test_task_link_format():
    """Тестирует формат ссылок на задачи"""
    
    print("Тестирование формата ссылок на задачи...")
    
    # Тестовые данные (используем пример из вашей ссылки)
    forum_chat_id = -1002269851341  # Пример из вашей ссылки
    topic_id = 5803
    message_id = 5945
    
    # Формируем ссылку по правильному формату
    expected_link = f"https://t.me/c/{str(abs(forum_chat_id))[4:]}/{topic_id}/{message_id}"
    
    print(f"FORUM_CHAT_ID: {forum_chat_id}")
    print(f"Абсолютное значение: {abs(forum_chat_id)}")
    print(f"Строка без первых 4 символов: {str(abs(forum_chat_id))[4:]}")
    print(f"Ожидаемая ссылка: {expected_link}")
    
    # Проверяем, что ссылка имеет правильный формат
    if expected_link.startswith("https://t.me/c/"):
        print("✅ Ссылка имеет правильный префикс")
    else:
        print("❌ Неправильный префикс ссылки")
    
    # Проверяем количество частей в ссылке
    parts = expected_link.split('/')
    if len(parts) == 6:  # https, '', t.me, c, chat_id, topic_id, message_id
        print("✅ Ссылка содержит все необходимые части")
        print(f"Части ссылки: {parts}")
    else:
        print(f"❌ Неправильное количество частей в ссылке: {len(parts)}")
        print(f"Части: {parts}")

async def test_task_link_update():
    """Тестирует создание и обновление задачи с ссылками"""
    
    print("\nТестирование создания и обновления задачи с ссылками...")
    
    try:
        # Создаем тестовую задачу
        test_task_data = {
            'message_id': '9999',
            'chat_id': '-1002402961402',
            'user_id': '425601155',
            'username': 'TestUser',
            'text': 'Тестовая задача для проверки ссылок',
            'status': 'unreacted',
            'task_number': 999,
            'assignee': '',
            'task_link': '',
            'main_message_id': '',
            'main_task_link': ''
        }
        
        # Сохраняем задачу
        task_id = await redis_client.save_task(test_task_data)
        print(f"Создана тестовая задача: {task_id}")
        
        # Обновляем задачу с ссылкой на форумную тему
        forum_chat_id = -1002269851341  # Используем тестовое значение
        topic_id = 5803
        message_id = 5945
        task_link = f"https://t.me/c/{str(abs(forum_chat_id))[4:]}/{topic_id}/{message_id}"
        
        await redis_client.update_task(
            task_id,
            task_link=task_link,
            support_message_id=message_id,
            support_topic_id=topic_id,
            status="in_progress"
        )
        
        print(f"Обновлена ссылка на задачу: {task_link}")
        
        # Проверяем, что ссылка сохранилась
        updated_task = await redis_client.get_task(task_id)
        if updated_task:
            saved_link = updated_task.get('task_link', '')
            if saved_link == task_link:
                print("✅ Ссылка на задачу сохранена корректно")
            else:
                print(f"❌ Ошибка сохранения ссылки. Ожидалось: {task_link}, получено: {saved_link}")
        else:
            print("❌ Задача не найдена после обновления")
        
        # Удаляем тестовую задачу
        await redis_client.conn.delete(f"task:{task_id}")
        print("Тестовая задача удалена")
        
    except Exception as e:
        print(f"❌ Ошибка при тестировании: {e}")

async def main():
    """Основная функция тестирования"""
    print("=== Тест ссылок на задачи ===")
    await test_task_link_format()
    await test_task_link_update()
    print("\n=== Тестирование завершено ===")

if __name__ == "__main__":
    asyncio.run(main())

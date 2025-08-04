#!/usr/bin/env python3
"""
Test script to verify the Redis fixes and topic creation error handling:
1. Proper Redis key deletion using conn.delete
2. BadRequest error handling in topic creation
"""

import asyncio
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from bots.user_bot.topic_manager import TopicManager
from core.redis_client import redis_client
import logging

# Настраиваем логирование
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_redis_deletion():
    """Test proper Redis key deletion using conn.delete"""
    print("=== Testing Redis Key Deletion ===")
    
    # Подключаемся к Redis
    await redis_client.connect()
    
    # Создаем тестовые ключи
    test_chat_id = -1001234567890
    test_user_id = 123456
    test_topic_id = 987654
    
    user_topic_key = f"user_topic:{test_chat_id}:{test_user_id}"
    topic_user_key = f"topic_user:{test_chat_id}:{test_topic_id}"
    
    # Сохраняем тестовые данные
    await redis_client.conn.set(user_topic_key, "test_data_1")
    await redis_client.conn.set(topic_user_key, "test_data_2")
    
    print(f"Created test keys: {user_topic_key}, {topic_user_key}")
    
    # Проверяем, что ключи существуют
    data1 = await redis_client.conn.get(user_topic_key)
    data2 = await redis_client.conn.get(topic_user_key)
    
    if data1 and data2:
        print("Test keys created successfully")
        
        # Удаляем ключи через conn.delete (как в исправленном коде)
        await redis_client.conn.delete(user_topic_key)
        await redis_client.conn.delete(topic_user_key)
        
        # Проверяем, что ключи удалены
        data1_after = await redis_client.conn.get(user_topic_key)
        data2_after = await redis_client.conn.get(topic_user_key)
        
        if not data1_after and not data2_after:
            print("Redis key deletion test passed")
            return True
        else:
            print("Redis key deletion test failed - keys still exist")
            return False
    else:
        print("Failed to create test keys")
        return False

async def test_topic_creation_error_handling():
    """Test BadRequest error handling in topic creation"""
    print("\n=== Testing Topic Creation Error Handling ===")
    
    # Создаем фиктивный объект бота для тестирования
    class MockBot:
        async def create_forum_topic(self, chat_id, name, icon_color):
            # Имитируем ошибку BadRequest
            from aiogram.exceptions import TelegramBadRequest
            raise TelegramBadRequest("Bad Request: chat not found")
    
    # Создаем TopicManager с фиктивным ботом
    topic_manager = TopicManager(MockBot())
    
    # Пытаемся создать тему (должна обработать ошибку BadRequest)
    try:
        result = await topic_manager._create_user_topic(
            chat_id=-1001234567890,
            user_id=123456,
            username="testuser"
        )
        
        # При правильной обработке ошибки метод должен вернуть None
        if result is None:
            print("Topic creation error handling test passed")
            return True
        else:
            print("Topic creation error handling test failed - expected None")
            return False
            
    except Exception as e:
        print(f"Topic creation error handling test failed with exception: {e}")
        return False

async def main():
    """Run all tests"""
    print("Starting Redis fixes and error handling tests...\n")
    
    try:
        # Test 1: Redis key deletion
        redis_test_passed = await test_redis_deletion()
        
        # Test 2: Topic creation error handling
        topic_test_passed = await test_topic_creation_error_handling()
        
        print(f"\nTest Results:")
        print(f"Redis Key Deletion: {'PASSED' if redis_test_passed else 'FAILED'}")
        print(f"Topic Creation Error Handling: {'PASSED' if topic_test_passed else 'FAILED'}")
        
        if redis_test_passed and topic_test_passed:
            print("\nAll tests passed! Fixes are working correctly.")
            return True
        else:
            print("\nSome tests failed. Please check the implementation.")
            return False
            
    except Exception as e:
        print(f"Test execution failed: {e}")
        return False

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)

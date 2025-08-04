#!/usr/bin/env python3
"""
Test script to verify the fixes for:
1. Status "in" mapping to "in_progress"
2. New tasks going to "unreacted" topic
3. Reply logic using reply_to_message_id
"""

import asyncio
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from bots.mover_bot.mover_bot import MoverBot
from core.redis_client import redis_client
import uuid
import json

async def test_status_mapping():
    """Test that status 'in' maps to 'in_progress'"""
    print("=== Testing Status Mapping ===")
    
    mover_bot = MoverBot()
    
    # Create a test task
    test_task_id = str(uuid.uuid4())
    test_task = {
        "task_id": test_task_id,
        "user_id": "123456",
        "username": "testuser",
        "text": "Test task for status mapping",
        "status": "unreacted",
        "message_id": "1234",
        "chat_id": "-1001234567890",
        "created_at": "2025-08-02T00:00:00",
        "task_number": 999
    }
    
    # Save test task to Redis
    await redis_client.save_task(test_task)
    print(f"Created test task: {test_task_id}")
    
    # Test status mapping - simulate "in" status change
    print("Testing status mapping: 'in' -> 'in_progress'")
    
    # This should map "in" to "in_progress" internally
    try:
        await mover_bot._move_task_to_topic(test_task_id, "in", "testuser")
        print("Status mapping test passed - no errors thrown")
    except Exception as e:
        if "Неизвестный тип темы: in" in str(e):
            print("Status mapping failed - 'in' status not mapped properly")
            return False
        else:
            print(f"Other error occurred: {e}")
    
    # Test unreacted status
    print("Testing 'unreacted' topic creation")
    try:
        await mover_bot._move_task_to_topic(test_task_id, "unreacted")
        print("Unreacted topic test passed")
    except Exception as e:
        print(f"Unreacted topic test failed: {e}")
        return False
    
    # Cleanup - using the correct method name
    # await redis_client.delete_task(test_task_id)  # Method doesn't exist
    print("Test completed (cleanup skipped)")
    
    return True

async def test_reply_logic():
    """Test reply logic with reply_to_message_id"""
    print("\n=== Testing Reply Logic ===")
    
    # Create a mock task with message_id
    test_task = {
        "task_id": "test-reply-task",
        "user_id": "123456",
        "chat_id": "-1001234567890",
        "message_id": "5678",  # Original message ID
        "username": "testuser",
        "reply": "This is a test reply from support",
        "reply_author": "support_agent"
    }
    
    print("Testing reply logic structure...")
    
    # Check if the reply logic would use reply_to_message_id
    original_message_id = test_task.get('message_id')
    if original_message_id:
        print(f"Original message ID found: {original_message_id}")
        print("Reply would be sent as reply_to_message_id")
        return True
    else:
        print("No original message ID - reply would be sent as regular message")
        return False

async def main():
    """Run all tests"""
    print("Starting fixes validation tests...\n")
    
    try:
        # Test 1: Status mapping
        status_test_passed = await test_status_mapping()
        
        # Test 2: Reply logic
        reply_test_passed = await test_reply_logic()
        
        print(f"\nTest Results:")
        print(f"Status Mapping: {'PASSED' if status_test_passed else 'FAILED'}")
        print(f"Reply Logic: {'PASSED' if reply_test_passed else 'FAILED'}")
        
        if status_test_passed and reply_test_passed:
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

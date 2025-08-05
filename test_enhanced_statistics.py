#!/usr/bin/env python3
"""
Test script for enhanced statistics system.

This script tests the new enhanced statistics functionality including:
- Per-executor task counters
- Time-based statistics (daily, weekly, monthly)
- Enhanced pinned message formatting
- Period statistics commands
"""

import asyncio
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from core.redis_client import redis_client
from core.enhanced_statistics import EnhancedStatistics

async def test_enhanced_statistics():
    """Test the enhanced statistics system"""
    print("🧪 Testing Enhanced Statistics System")
    print("=" * 50)
    
    try:
        # Initialize connection
        await redis_client.connect()
        print("✅ Redis connection established")
        
        # Test 1: Reset all counters
        print("\n1. Resetting all counters...")
        await redis_client.reset_all_counters()
        print("✅ All counters reset")
        
        # Test 2: Test global counters
        print("\n2. Testing global counters...")
        stats = await redis_client.get_global_stats()
        print(f"   Initial stats: {stats}")
        
        # Test 3: Test executor statistics
        print("\n3. Testing executor statistics...")
        
        # Simulate task status changes
        test_executor = "test_user"
        
        # Simulate task going to in_progress
        await redis_client._enhanced_stats.update_task_status_counters(
            "unreacted", "in_progress", test_executor
        )
        print(f"   ✅ Updated counters: unreacted -> in_progress for {test_executor}")
        
        # Simulate task completion
        await redis_client._enhanced_stats.update_task_status_counters(
            "in_progress", "completed", test_executor
        )
        print(f"   ✅ Updated counters: in_progress -> completed for {test_executor}")
        
        # Get updated stats
        stats = await redis_client.get_global_stats()
        print(f"   Updated stats: {stats}")
        
        # Test 4: Test pinned message formatting
        print("\n4. Testing pinned message formatting...")
        pinned_message = await redis_client.format_pinned_message(stats)
        print("   Formatted pinned message:")
        print("   " + "\n   ".join(pinned_message.split("\n")))
        
        # Test 5: Test period statistics
        print("\n5. Testing period statistics...")
        
        # Test day stats
        day_stats = await redis_client.get_period_stats("day")
        print(f"   Day stats: {day_stats}")
        
        day_message = await redis_client.format_period_stats_message("day", day_stats)
        print("   Day stats message:")
        print("   " + "\n   ".join(day_message.split("\n")))
        
        # Test week stats
        week_stats = await redis_client.get_period_stats("week")
        print(f"   Week stats: {week_stats}")
        
        # Test month stats
        month_stats = await redis_client.get_period_stats("month")
        print(f"   Month stats: {month_stats}")
        
        # Test 6: Test multiple executors
        print("\n6. Testing multiple executors...")
        
        executors = ["alice", "bob", "charlie"]
        for executor in executors:
            # Add some tasks for each executor
            await redis_client._enhanced_stats.increment_executor_in_progress(executor)
            await redis_client._enhanced_stats.increment_executor_completed(executor)
            await redis_client._enhanced_stats.increment_executor_completed(executor)  # 2 completed
        
        # Get final stats
        final_stats = await redis_client.get_global_stats()
        print(f"   Final stats with multiple executors: {final_stats}")
        
        # Format final pinned message
        final_pinned = await redis_client.format_pinned_message(final_stats)
        print("\n   Final pinned message:")
        print("   " + "\n   ".join(final_pinned.split("\n")))
        
        print("\n" + "=" * 50)
        print("✅ All enhanced statistics tests passed!")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        # Close connection
        await redis_client.close()

async def test_commands_simulation():
    """Simulate the new statistics commands"""
    print("\n🎮 Simulating Statistics Commands")
    print("=" * 50)
    
    try:
        await redis_client.connect()
        
        # Simulate /stats command
        print("\n📊 Simulating /stats command...")
        stats = await redis_client.get_global_stats()
        stats_message = await redis_client.format_pinned_message(stats)
        print("Response:")
        print(stats_message)
        
        # Simulate /day command
        print("\n📅 Simulating /day command...")
        day_stats = await redis_client.get_period_stats("day")
        day_message = await redis_client.format_period_stats_message("day", day_stats)
        print("Response:")
        print(day_message)
        
        # Simulate /week command
        print("\n📅 Simulating /week command...")
        week_stats = await redis_client.get_period_stats("week")
        week_message = await redis_client.format_period_stats_message("week", week_stats)
        print("Response:")
        print(week_message)
        
        # Simulate /month command
        print("\n📅 Simulating /month command...")
        month_stats = await redis_client.get_period_stats("month")
        month_message = await redis_client.format_period_stats_message("month", month_stats)
        print("Response:")
        print(month_message)
        
        print("\n" + "=" * 50)
        print("✅ All command simulations completed!")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Command simulation failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        await redis_client.close()

async def main():
    """Main test function"""
    print("🚀 Enhanced Statistics System Test Suite")
    print("=" * 60)
    
    # Run basic functionality tests
    basic_test_passed = await test_enhanced_statistics()
    
    # Run command simulation tests
    command_test_passed = await test_commands_simulation()
    
    print("\n" + "=" * 60)
    print("📋 TEST RESULTS:")
    print(f"   Basic Functionality: {'✅ PASSED' if basic_test_passed else '❌ FAILED'}")
    print(f"   Command Simulation:  {'✅ PASSED' if command_test_passed else '❌ FAILED'}")
    
    if basic_test_passed and command_test_passed:
        print("\n🎉 All tests passed! Enhanced statistics system is ready.")
        print("\nAvailable commands:")
        print("   /stats - Show current statistics")
        print("   /day - Show today's statistics")
        print("   /week - Show this week's statistics")
        print("   /month - Show this month's statistics")
        print("   /reset_stats - Reset all statistics (admin)")
    else:
        print("\n❌ Some tests failed. Please check the implementation.")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())

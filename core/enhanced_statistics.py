"""
Enhanced statistics system for task management.

This module provides advanced statistics tracking with:
- Per-executor task counters (in_progress, completed)
- Time-based statistics (daily, weekly, monthly)
- Enhanced pinned message formatting
"""
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class ExecutorStats:
    """Statistics for a single executor"""
    username: str
    in_progress: int = 0
    completed: int = 0
    completed_today: int = 0
    completed_week: int = 0
    completed_month: int = 0

class EnhancedStatistics:
    """Enhanced statistics manager with time-based tracking"""
    
    def __init__(self, redis_client):
        self.redis = redis_client
        self.logger = logging.getLogger(__name__)
    
    async def _ensure_connection(self):
        """Ensure Redis connection"""
        await self.redis._ensure_connection()
    
    # ==================== EXECUTOR STATISTICS ====================
    
    async def increment_executor_in_progress(self, executor: str) -> int:
        """Increment in-progress tasks counter for executor"""
        try:
            await self._ensure_connection()
            key = f"executor:{executor}:in_progress"
            result = await self.redis.conn.incr(key)
            self.logger.info(f"Incremented in_progress for {executor}: {result}")
            return result
        except Exception as e:
            self.logger.error(f"Error incrementing in_progress for {executor}: {e}")
            return 0
    
    async def decrement_executor_in_progress(self, executor: str) -> int:
        """Decrement in-progress tasks counter for executor"""
        try:
            await self._ensure_connection()
            key = f"executor:{executor}:in_progress"
            result = await self.redis.conn.decr(key)
            # Ensure counter doesn't go below 0
            if result < 0:
                await self.redis.conn.set(key, 0)
                result = 0
            self.logger.info(f"Decremented in_progress for {executor}: {result}")
            return result
        except Exception as e:
            self.logger.error(f"Error decrementing in_progress for {executor}: {e}")
            return 0
    
    async def increment_executor_completed(self, executor: str) -> int:
        """Increment completed tasks counter for executor with timestamp"""
        try:
            await self._ensure_connection()
            now = datetime.utcnow()
            
            # Increment total completed counter
            total_key = f"executor:{executor}:completed"
            total_result = await self.redis.conn.incr(total_key)
            
            # Add to time-based sets for period statistics
            today_key = f"executor:{executor}:completed:{now.strftime('%Y-%m-%d')}"
            week_key = f"executor:{executor}:completed:week:{now.strftime('%Y-W%U')}"
            month_key = f"executor:{executor}:completed:month:{now.strftime('%Y-%m')}"
            
            pipeline = self.redis.conn.pipeline()
            pipeline.incr(today_key)
            pipeline.expire(today_key, 86400 * 32)  # Keep for 32 days
            pipeline.incr(week_key)
            pipeline.expire(week_key, 86400 * 60)  # Keep for 60 days
            pipeline.incr(month_key)
            pipeline.expire(month_key, 86400 * 400)  # Keep for 400 days
            await pipeline.execute()
            
            self.logger.info(f"Incremented completed for {executor}: {total_result}")
            return total_result
        except Exception as e:
            self.logger.error(f"Error incrementing completed for {executor}: {e}")
            return 0
    
    async def get_executor_stats(self, executor: str) -> ExecutorStats:
        """Get comprehensive statistics for a single executor"""
        try:
            await self._ensure_connection()
            now = datetime.utcnow()
            
            # Get current counters
            in_progress = int(await self.redis.conn.get(f"executor:{executor}:in_progress") or 0)
            completed = int(await self.redis.conn.get(f"executor:{executor}:completed") or 0)
            
            # Get time-based statistics
            today_key = f"executor:{executor}:completed:{now.strftime('%Y-%m-%d')}"
            week_key = f"executor:{executor}:completed:week:{now.strftime('%Y-W%U')}"
            month_key = f"executor:{executor}:completed:month:{now.strftime('%Y-%m')}"
            
            completed_today = int(await self.redis.conn.get(today_key) or 0)
            completed_week = int(await self.redis.conn.get(week_key) or 0)
            completed_month = int(await self.redis.conn.get(month_key) or 0)
            
            return ExecutorStats(
                username=executor,
                in_progress=in_progress,
                completed=completed,
                completed_today=completed_today,
                completed_week=completed_week,
                completed_month=completed_month
            )
        except Exception as e:
            self.logger.error(f"Error getting stats for {executor}: {e}")
            return ExecutorStats(username=executor)
    
    async def get_all_executors_stats(self) -> Dict[str, ExecutorStats]:
        """Get statistics for all executors"""
        try:
            await self._ensure_connection()
            executors = set()
            
            # Find all executors from Redis keys
            async for key in self.redis.conn.scan_iter("executor:*:in_progress"):
                if isinstance(key, bytes):
                    key = key.decode('utf-8')
                # Extract executor name from key: executor:username:in_progress
                parts = key.split(':')
                if len(parts) >= 3:
                    executors.add(parts[1])
            
            async for key in self.redis.conn.scan_iter("executor:*:completed"):
                if isinstance(key, bytes):
                    key = key.decode('utf-8')
                # Extract executor name from key: executor:username:completed
                parts = key.split(':')
                if len(parts) >= 3 and parts[2] == 'completed':
                    executors.add(parts[1])
            
            # Get stats for each executor
            stats = {}
            for executor in executors:
                stats[executor] = await self.get_executor_stats(executor)
            
            return stats
        except Exception as e:
            self.logger.error(f"Error getting all executors stats: {e}")
            return {}
    
    # ==================== GLOBAL STATISTICS ====================
    
    async def increment_global_counter(self, status: str) -> int:
        """Increment global status counter"""
        try:
            await self._ensure_connection()
            key = f"counter:{status}"
            result = await self.redis.conn.incr(key)
            self.logger.info(f"Incremented global {status} counter: {result}")
            return result
        except Exception as e:
            self.logger.error(f"Error incrementing global {status} counter: {e}")
            return 0
    
    async def decrement_global_counter(self, status: str) -> int:
        """Decrement global status counter"""
        try:
            await self._ensure_connection()
            key = f"counter:{status}"
            result = await self.redis.conn.decr(key)
            # Ensure counter doesn't go below 0
            if result < 0:
                await self.redis.conn.set(key, 0)
                result = 0
            self.logger.info(f"Decremented global {status} counter: {result}")
            return result
        except Exception as e:
            self.logger.error(f"Error decrementing global {status} counter: {e}")
            return 0
    
    async def get_global_stats(self) -> Dict[str, Any]:
        """Get comprehensive global statistics"""
        try:
            await self._ensure_connection()
            
            # Get global counters
            unreacted = int(await self.redis.conn.get("counter:unreacted") or 0)
            in_progress = int(await self.redis.conn.get("counter:in_progress") or 0)
            completed = int(await self.redis.conn.get("counter:completed") or 0)
            
            # Get executor statistics
            executors_stats = await self.get_all_executors_stats()
            
            return {
                "unreacted": unreacted,
                "in_progress": in_progress,
                "completed": completed,
                "executors": executors_stats
            }
        except Exception as e:
            self.logger.error(f"Error getting global stats: {e}")
            return {
                "unreacted": 0,
                "in_progress": 0,
                "completed": 0,
                "executors": {}
            }
    
    # ==================== TASK STATUS UPDATES ====================
    
    async def update_task_status_counters(self, old_status: str, new_status: str, executor: str = None):
        """Update counters when task status changes"""
        try:
            # Update global counters
            if old_status:
                await self.decrement_global_counter(old_status)
            if new_status:
                await self.increment_global_counter(new_status)
            
            # Update executor counters if executor is involved
            if executor:
                if old_status == "in_progress":
                    await self.decrement_executor_in_progress(executor)
                if new_status == "in_progress":
                    await self.increment_executor_in_progress(executor)
                if new_status == "completed":
                    await self.increment_executor_completed(executor)
            
            self.logger.info(f"Updated counters: {old_status} -> {new_status} (executor: {executor})")
        except Exception as e:
            self.logger.error(f"Error updating task status counters: {e}")
    
    # ==================== PERIOD STATISTICS ====================
    
    async def get_period_stats(self, period: str) -> Dict[str, Dict[str, int]]:
        """Get statistics for a specific period (day/week/month)"""
        try:
            await self._ensure_connection()
            now = datetime.utcnow()
            
            if period == "day":
                date_key = now.strftime('%Y-%m-%d')
                pattern = f"executor:*:completed:{date_key}"
            elif period == "week":
                date_key = now.strftime('%Y-W%U')
                pattern = f"executor:*:completed:week:{date_key}"
            elif period == "month":
                date_key = now.strftime('%Y-%m')
                pattern = f"executor:*:completed:month:{date_key}"
            else:
                raise ValueError(f"Invalid period: {period}")
            
            stats = {}
            async for key in self.redis.conn.scan_iter(pattern):
                if isinstance(key, bytes):
                    key = key.decode('utf-8')
                
                # Extract executor name from key
                parts = key.split(':')
                if len(parts) >= 2:
                    executor = parts[1]
                    completed_count = int(await self.redis.conn.get(key) or 0)
                    
                    if executor not in stats:
                        # Get current in_progress count
                        in_progress = int(await self.redis.conn.get(f"executor:{executor}:in_progress") or 0)
                        stats[executor] = {
                            "in_progress": in_progress,
                            "completed": completed_count
                        }
                    else:
                        stats[executor]["completed"] = completed_count
            
            return stats
        except Exception as e:
            self.logger.error(f"Error getting {period} stats: {e}")
            return {}
    
    # ==================== PINNED MESSAGE FORMATTING ====================
    
    def format_pinned_message(self, stats: Dict[str, Any]) -> str:
        """Format enhanced pinned message with executor statistics"""
        try:
            unreacted = stats.get("unreacted", 0)
            completed = stats.get("completed", 0)
            executors = stats.get("executors", {})
            
            # Header
            message = "📊 Статистика задач\n\n"
            
            # Неотреагированные
            message += f"⚠️ Неотреагированные: {unreacted}\n\n"
            
            # В работе по исполнителям
            if executors:
                # Фильтруем только исполнителей с задачами в работе
                active_executors = [(executor, stats_obj) for executor, stats_obj in executors.items() 
                                  if stats_obj.in_progress > 0]
                
                if active_executors:
                    message += "В работе:\n"
                    # Сортируем по количеству задач в работе (по убыванию)
                    active_executors.sort(key=lambda x: x[1].in_progress, reverse=True)
                    
                    for executor, stats_obj in active_executors:
                        message += f"⚡️ @{executor}: {stats_obj.in_progress}\n"
                    message += "\n"
            
            # Выполненные
            message += f"✅ Выполненные: {completed}"
            
            return message
        except Exception as e:
            self.logger.error(f"Error formatting pinned message: {e}")
            return "📊 Ошибка получения статистики"
    
    def format_period_stats_message(self, period: str, stats: Dict[str, Dict[str, int]]) -> str:
        """Format period statistics message"""
        try:
            period_names = {
                "day": "за сегодня",
                "week": "за неделю", 
                "month": "за месяц"
            }
            
            period_name = period_names.get(period, f"за {period}")
            message = f"📈 **Статистика {period_name}**\n\n"
            
            if not stats:
                message += "Нет данных за указанный период"
                return message
            
            # Sort by total activity
            sorted_stats = sorted(
                stats.items(),
                key=lambda x: x[1].get("in_progress", 0) + x[1].get("completed", 0),
                reverse=True
            )
            
            for executor, executor_stats in sorted_stats:
                in_progress = executor_stats.get("in_progress", 0)
                completed = executor_stats.get("completed", 0)
                
                if in_progress > 0 or completed > 0:
                    message += f"👤 @{executor}:\n"
                    if in_progress > 0:
                        message += f"  ⚡️ В работе: **{in_progress}**\n"
                    if completed > 0:
                        message += f"  ✅ Выполнено: **{completed}**\n"
                    message += "\n"
            
            message += f"🕐 Сформировано: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}"
            
            return message
        except Exception as e:
            self.logger.error(f"Error formatting period stats: {e}")
            return f"📈 Ошибка получения статистики {period_names.get(period, period)}"
    
    # ==================== CLEANUP METHODS ====================
    
    async def reset_all_counters(self):
        """Reset all statistics counters (for testing/debugging)"""
        try:
            await self._ensure_connection()
            
            # Reset global counters
            global_keys = ["counter:unreacted", "counter:in_progress", "counter:completed"]
            for key in global_keys:
                await self.redis.conn.delete(key)
            
            # Reset executor counters
            async for key in self.redis.conn.scan_iter("executor:*"):
                await self.redis.conn.delete(key)
            
            self.logger.info("All statistics counters reset")
        except Exception as e:
            self.logger.error(f"Error resetting counters: {e}")

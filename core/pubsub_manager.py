"""
Базовый PubSub Manager для всех ботов

Обеспечивает надежную работу с Redis Pub/Sub с автоматическим переподключением,
обработкой ошибок и таймаутами.
"""

import asyncio
import json
import logging
from typing import Dict, Any, AsyncGenerator, Callable, Optional, List
from abc import ABC, abstractmethod

from core.redis_client import redis_client

logger = logging.getLogger(__name__)

class BasePubSubManager(ABC):
    """
    Базовый класс для PubSub менеджеров ботов
    
    Обеспечивает:
    - Отдельные соединения для каждого бота
    - Автоматическое переподключение при ошибках
    - Экспоненциальный backoff
    - Гарантированное освобождение ресурсов
    - Настраиваемые таймауты
    """
    
    def __init__(self, bot_name: str):
        self.bot_name = bot_name
        self.logger = logging.getLogger(f"{__name__}.{bot_name}")
        self.subscriptions = {}
        self.running = False
        self.timeout = 30.0  # Максимальное время ожидания сообщения
        self.background_tasks = []
        self.redis = redis_client
        
    async def listen_channel_safe(self, channel: str) -> AsyncGenerator[Dict[str, Any], None]:
        """Безопасный слушатель канала с обработкой таймаутов"""
        retry_delay = 1
        max_retry_delay = 30
        
        self.logger.info(f"Starting safe listener for channel: {channel}")
        
        while self.running:
            pubsub = None
            try:
                # Получаем свежее PubSub соединение
                pubsub = await self.redis.get_pubsub(fresh=True)
                await pubsub.subscribe(channel)
                
                self.logger.info(f"Subscribed to {channel}, waiting for messages...")
                
                while self.running:
                    try:
                        # Используем get_message с таймаутом вместо блокирующего listen()
                        message = await pubsub.get_message(
                            timeout=self.timeout,
                            ignore_subscribe_messages=True
                        )
                        
                        if message and message['data']:
                            try:
                                # Декодируем данные если они в bytes
                                data = message['data']
                                if isinstance(data, bytes):
                                    data = data.decode('utf-8')
                                
                                parsed_data = json.loads(data)
                                logger.info(f"Received message from {channel}: {parsed_data}")
                                yield parsed_data
                                retry_delay = 1  # Сброс задержки при успехе
                                
                            except json.JSONDecodeError as e:
                                self.logger.error(f"Message decode error in {channel}: {e}")
                                continue
                                
                    except asyncio.TimeoutError:
                        # Таймаут - это нормально, просто продолжаем
                        self.logger.debug(f"No messages in {channel} for {self.timeout}s")
                        continue
                        
                    except Exception as e:
                        self.logger.error(f"Message processing error in {channel}: {e}")
                        break
                        
            except Exception as e:
                self.logger.error(f"PubSub error in {channel}: {e}")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, max_retry_delay)
                
            finally:
                # Обязательно закрываем PubSub соединение
                if pubsub:
                    try:
                        await pubsub.unsubscribe(channel)
                        await pubsub.close()
                        self.logger.debug(f"Cleaned up PubSub for {channel}")
                    except Exception as e:
                        self.logger.warning(f"PubSub cleanup error for {channel}: {e}")
    
    async def publish_event(self, channel: str, event: dict):
        """Публикует событие в Redis Pub/Sub"""
        if self.redis.conn is None:
            await self.redis._ensure_connection()
        logger.info(f"Publishing event to {channel}: {event}")
        await self.redis.conn.publish(channel, json.dumps(event))
    
    async def subscribe(self, channel: str, handler_func: Callable):
        """Подписывается на канал и регистрирует обработчик"""
        if self.running:
            self.logger.warning(f"Cannot subscribe to {channel} after listener started")
            return
            
        self.subscriptions[channel] = handler_func
        self.logger.info(f"Subscribed to channel: {channel}")
    
    async def start(self):
        """Запускает фоновые слушатели для всех подписанных каналов"""
        if self.running:
            self.logger.warning("Background listeners already running")
            return
            
        self.running = True
        self.logger.info(f"Starting background listeners for channels: {list(self.subscriptions.keys())}")
        
        # Создаем задачи для каждого канала
        for channel, handler_func in self.subscriptions.items():
            task = asyncio.create_task(
                self._channel_listener_wrapper(channel, handler_func)
            )
            self.background_tasks.append(task)
            self.logger.info(f"Started listener for channel: {channel}")
        
        self.logger.info(f"All {len(self.subscriptions)} listeners started successfully")
    
    async def start_background_listener(self, channels: List[str], handler_func: Callable):
        """Запускает фоновые слушатели для указанных каналов"""
        if self.running:
            self.logger.warning("Background listeners already running")
            return
            
        self.running = True
        self.logger.info(f"Starting background listeners for channels: {channels}")
        
        # Создаем задачи для каждого канала
        for channel in channels:
            task = asyncio.create_task(
                self._channel_listener_wrapper(channel, handler_func)
            )
            self.background_tasks.append(task)
            self.logger.info(f"Started listener for channel: {channel}")
        
        self.logger.info(f"All {len(channels)} listeners started successfully")
    
    async def _channel_listener_wrapper(self, channel: str, handler_func: Callable):
        """Обертка для слушателя канала с обработкой сообщений"""
        try:
            async for message in self.listen_channel_safe(channel):
                try:
                    await handler_func(channel, message)
                except Exception as e:
                    self.logger.error(f"Handler error for {channel}: {e}", exc_info=True)
                    
        except Exception as e:
            self.logger.error(f"Channel listener wrapper error for {channel}: {e}", exc_info=True)
    
    async def stop_background_listener(self):
        """Останавливает все фоновые слушатели"""
        if not self.running:
            return
            
        self.logger.info("Stopping background listeners...")
        self.running = False
        
        # Отменяем все задачи
        for task in self.background_tasks:
            if not task.done():
                task.cancel()
        
        # Ждем завершения всех задач
        if self.background_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self.background_tasks, return_exceptions=True),
                    timeout=10.0
                )
            except asyncio.TimeoutError:
                self.logger.warning("Some background tasks didn't stop within 10 seconds")
        
        self.background_tasks.clear()
        self.logger.info("All background listeners stopped")
    
    @abstractmethod
    async def handle_message(self, channel: str, message: Dict[str, Any]):
        """Абстрактный метод для обработки сообщений (должен быть реализован в наследниках)"""
        pass
    
    def set_timeout(self, timeout: float):
        """Устанавливает таймаут для PubSub операций"""
        self.timeout = timeout
        self.logger.info(f"PubSub timeout set to {timeout} seconds")


class UserBotPubSubManager(BasePubSubManager):
    """PubSub менеджер для User Bot"""
    
    def __init__(self):
        super().__init__("UserBot")
    
    async def handle_message(self, channel: str, message: Dict[str, Any]):
        """Обработка сообщений для User Bot"""
        try:
            if channel == "task_updates":
                await self._handle_task_update(message)
            else:
                self.logger.warning(f"Unknown channel: {channel}")
                
        except Exception as e:
            self.logger.error(f"Error handling message from {channel}: {e}")
    
    async def _handle_task_update(self, message: Dict[str, Any]):
        """Обработка обновлений задач"""
        # Эта логика будет реализована в user_bot.py
        self.logger.debug(f"Task update received: {message}")


class TaskBotPubSubManager(BasePubSubManager):
    """PubSub менеджер для Task Bot"""
    
    def __init__(self):
        super().__init__("TaskBot")
    
    async def handle_message(self, channel: str, message: Dict[str, Any]):
        """Обработка сообщений для Task Bot"""
        try:
            if channel == "new_tasks":
                await self._handle_new_task(message)
            else:
                self.logger.warning(f"Unknown channel: {channel}")
                
        except Exception as e:
            self.logger.error(f"Error handling message from {channel}: {e}")
    
    async def _handle_new_task(self, message: Dict[str, Any]):
        """Обработка новых задач"""
        # Эта логика будет реализована в task_bot.py
        self.logger.debug(f"New task received: {message}")


class MoverBotPubSubManager(BasePubSubManager):
    """PubSub менеджер для Mover Bot"""
    
    def __init__(self, bot_instance=None):
        super().__init__("MoverBot")
        self.bot_instance = bot_instance
    
    async def handle_message(self, channel: str, message: Dict[str, Any]):
        """Обработка сообщений для Mover Bot"""
        try:
            if channel == "task_updates":
                await self._handle_task_event(message)
            else:
                self.logger.warning(f"Unknown channel: {channel}")
                
        except Exception as e:
            self.logger.error(f"Error handling message from {channel}: {e}")
    
    async def _handle_task_event(self, message: Dict[str, Any]):
        """Обработка событий задач"""
        # Передаем событие в MoverBot для обработки
        if hasattr(self, 'bot_instance') and self.bot_instance:
            await self.bot_instance._process_task_event(message)
        else:
            self.logger.warning("Bot instance not set for MoverBotPubSubManager")

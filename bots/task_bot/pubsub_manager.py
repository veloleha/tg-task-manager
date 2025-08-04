import asyncio
import json
import logging
import time
from typing import Callable, Dict

from core.redis_client import RedisManager

logger = logging.getLogger(__name__)

class TaskBotPubSubManager:
    """Упрощенный менеджер PubSub для TaskBot"""
    
    def __init__(self):
        self.handlers: Dict[str, list] = {}
        self.redis = RedisManager()  # Инициализация RedisManager
        self.pubsub = None  # Separate connection for PubSub
        self.running = False
        self.listening = False
        self.listening_task = None  # Store the listening task
        self.reconnecting = False  # Флаг для отслеживания процесса переподключения
        
    async def start(self):
        """Запускает слушатель Pub/Sub только после всех подписок"""
        logger.info("[PUBSUB] Starting PubSub listener...")
        if self.listening:
            logger.warning("Listener already started")
            return
            
        # Проверяем, есть ли каналы для подписки
        if not self.handlers:
            logger.warning("No channels to subscribe to")
            return
            
        # Убеждаемся, что pubsub создан и подписан на все каналы
        if not self.pubsub:
            if not self.redis.conn:
                await self.redis.connect()
            self.pubsub = self.redis.conn.pubsub()
            
        # Подписываемся на все каналы
        for channel in self.handlers:
            await self.pubsub.subscribe(channel)
            logger.info(f"[PUBSUB] Subscribed to channel: {channel}")
            
        self.listening = True
        self.listening_task = asyncio.create_task(self._listen())
        logger.info("[PUBSUB] PubSub listener started")
        
    async def _listen(self):
        logger.info("PubSub listener started")
        while self.listening:
            try:
                # Ensure pubsub connection is established
                if not self.pubsub:
                    if self.reconnecting:
                        await asyncio.sleep(1)  # Уже в процессе переподключения, ждём
                        continue
                    
                    self.reconnecting = True
                    logger.warning("PubSub connection not set, reinitializing...")
                    # Check Redis connection
                    if not self.redis.conn:
                        logger.warning("Redis connection is not set, reconnecting...")
                        await self.redis.connect()
                    # Recreate the pubsub connection
                    self.pubsub = self.redis.conn.pubsub()
                    # Resubscribe to all channels
                    for channel in self.handlers.keys():
                        await self.pubsub.subscribe(channel)
                    logger.info("Re-subscribed to all channels")
                    self.reconnecting = False
                    
                message = await self.pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message:
                    logger.info(f"[PUBSUB] Received raw message: {message}")
                    try:
                        channel = message['channel'].decode('utf-8')
                        data = message['data'].decode('utf-8')
                        event = json.loads(data)
                        logger.info(f"[PUBSUB] Processing message on channel {channel}: {event}")
                        # Вызываем все обработчики для данного канала
                        if channel in self.handlers:
                            for handler in self.handlers[channel]:
                                await handler(channel, event)
                        else:
                            logger.warning(f"[PUBSUB] No handlers found for channel: {channel}")
                    except Exception as e:
                        logger.error(f"[PUBSUB] Error processing message: {e}", exc_info=True)
            except RuntimeError as e:
                if "pubsub connection not set" in str(e):
                    logger.error("PubSub connection lost, attempting to reconnect...")
                    self.pubsub = None  # Reset to trigger reinitialization
                    # Добавляем задержку перед повторной попыткой
                    await asyncio.sleep(5)
                else:
                    logger.error(f"Runtime error in PubSub listener: {e}", exc_info=True)
                    await asyncio.sleep(1)
            except asyncio.TimeoutError:
                pass
            except KeyboardInterrupt:
                # Break the loop on KeyboardInterrupt (shutdown)
                break
            except Exception as e:
                logger.error(f"Unexpected error in PubSub listener: {e}", exc_info=True)
                # Добавляем задержку перед повторной попыткой
                await asyncio.sleep(5)
            
    async def publish_event(self, channel: str, event: dict):
        if not self.redis.conn:
            await self.redis.connect()
        # Use main connection for publishing
        await self.redis.conn.publish(channel, json.dumps(event))
        logger.info(f"Published event to {channel}: {event}")
        
    async def subscribe(self, channel: str, handler: Callable):
        """Добавляет обработчик для канала"""
        logger.info(f"[PUBSUB] Attempting to subscribe to channel: {channel}")
        if self.listening:
            logger.warning(f"Attempt to subscribe to {channel} after listener started! Subscription ignored.")
            return
            
        # Добавляем обработчик
        if channel not in self.handlers:
            self.handlers[channel] = []
        self.handlers[channel].append(handler)
        logger.info(f"[PUBSUB] Added handler for channel: {channel}")
        
        # Подписываемся на канал, если слушатель еще не запущен
        if not self.listening:
            if not self.pubsub:
                if not self.redis.conn:
                    await self.redis.connect()
                self.pubsub = self.redis.conn.pubsub()
            await self.pubsub.subscribe(channel)
            logger.info(f"[PUBSUB] Subscribed to channel: {channel}")
        
    async def _pubsub_message_handler(self, channel: str, message: dict):
        """Обработчик сообщений PubSub - этот метод не используется, так как обработка происходит в _listen"""
        logger.warning(f"[PUBSUB] _pubsub_message_handler called but should not be used. Channel: {channel}, Message: {message}")
            
    async def _handle_new_task(self, message: dict):
        """Заглушка для обработки новых задач - реальная обработка происходит в TaskBot"""
        logger.info(f"[PUBSUB] _handle_new_task called with: {message}")
        
    async def _handle_task_update(self, message: dict):
        """Заглушка для обработки обновлений задач - реальная обработка происходит в TaskBot"""
        logger.info(f"[PUBSUB] _handle_task_update called with: {message}")
        
    async def stop(self):
        """Останавливает прослушивание событий"""
        self.listening = False
        if self.listening_task:
            self.listening_task.cancel()
            try:
                await self.listening_task
            except asyncio.CancelledError:
                pass
            self.listening_task = None
        if self.pubsub:
            await self.pubsub.close()
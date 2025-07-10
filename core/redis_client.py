import redis.asyncio as redis
from redis.exceptions import RedisError
from config.settings import settings
import logging

logger = logging.getLogger(__name__)

class RedisClient:
    _instance = None
    _conn = None  # Основное соединение
    _pubsub = None  # Pub/Sub соединение

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._init_connection()
        return cls._instance

    @classmethod
    def _init_connection(cls):
        """Инициализация соединений"""
        try:
            cls._conn = redis.Redis(
                host=settings.REDIS_HOST,
                port=settings.REDIS_PORT,
                db=settings.REDIS_DB,
                password=settings.REDIS_PASSWORD or None,
                decode_responses=True,
                encoding="utf-8",
                socket_connect_timeout=5,
                socket_timeout=5,
            )
            cls._pubsub = cls._conn.pubsub()  # Инициализируем PubSub
            logger.info("Redis соединения инициализированы")
        except RedisError as e:
            logger.error(f"Ошибка подключения к Redis: {e}")
            raise

    async def ping(self) -> bool:
        """Проверка соединения с Redis"""
        try:
            return await self._conn.ping()
        except RedisError:
            return False

    async def set(self, key: str, value: str, expire: int = None) -> bool:
        """Установка значения с возможным сроком действия"""
        try:
            return await self._conn.set(key, value, ex=expire)
        except RedisError as e:
            logger.error(f"Ошибка установки значения: {e}")
            raise

    async def get(self, key: str) -> str:
        """Получение значения по ключу"""
        try:
            return await self._conn.get(key)
        except RedisError as e:
            logger.error(f"Ошибка получения значения: {e}")
            raise

    async def rpush(self, key: str, *values: str) -> int:
        """Добавление значений в конец списка"""
        try:
            return await self._conn.rpush(key, *values)
        except RedisError as e:
            logger.error(f"Ошибка добавления в список: {e}")
            raise

    async def lrange(self, key: str, start: int, end: int) -> list:
        """Получение диапазона элементов списка"""
        try:
            return await self._conn.lrange(key, start, end)
        except RedisError as e:
            logger.error(f"Ошибка получения диапазона списка: {e}")
            raise

    async def delete(self, key: str) -> int:
        """Удаление ключа"""
        try:
            return await self._conn.delete(key)
        except RedisError as e:
            logger.error(f"Ошибка удаления ключа: {e}")
            raise

    async def publish(self, channel: str, message: str) -> int:
        """Публикация сообщения в канал"""
        try:
            return await self._conn.publish(channel, message)
        except RedisError as e:
            logger.error(f"Ошибка публикации: {e}")
            raise

    async def close(self):
        """Закрытие соединений"""
        if self._pubsub:
            await self._pubsub.close()
        if self._conn:
            await self._conn.close()
        logger.info("Redis соединения закрыты")

# Глобальный экземпляр
redis_client = RedisClient()
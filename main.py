#!/usr/bin/env python3
"""
Telegram Task Manager - Main Entry Point

Главный файл для запуска всей системы управления задачами.
Запускает все три бота одновременно как единую систему:
- User Bot - сбор сообщений и создание задач
- Task Bot - управление задачами в чате поддержки  
- Mover Bot - статистика и перемещение завершенных задач

Использование:
    python main.py                    # Запуск всех ботов
    python main.py --only user        # Запуск только User Bot
    python main.py --only task        # Запуск только Task Bot
    python main.py --only mover       # Запуск только Mover Bot
"""

import asyncio
import logging
import signal
import sys
import argparse
from pathlib import Path
from typing import List, Optional
from core.redis_client import RedisManager
from bots.task_bot.pubsub_manager import TaskBotPubSubManager as PubSubManager

# Добавляем корневую директорию в Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))


from config.settings import settings

# Настройка логирования
def setup_logging():
    """Настраивает систему логирования для всей системы"""
    
    # Создаем директорию для логов
    logs_dir = project_root / "logs"
    logs_dir.mkdir(exist_ok=True)
    
    # Настройка форматирования
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Настройка корневого логгера
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Очищаем существующие обработчики
    logger.handlers.clear()
    
    # Консольный обработчик
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Общий файловый обработчик
    file_handler = logging.FileHandler(
        logs_dir / "system.log", 
        encoding='utf-8'
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Обработчик ошибок
    error_handler = logging.FileHandler(
        logs_dir / "system_errors.log", 
        encoding='utf-8'
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    logger.addHandler(error_handler)

def check_environment():
    """Проверяет наличие необходимых переменных окружения"""
    required_vars = [
        'REDIS_HOST',
        'FORUM_CHAT_ID',
        'SUPPORT_CHAT_ID'
    ]
    
    missing_vars = []
    for var in required_vars:
        if not getattr(settings, var, None):
            missing_vars.append(var)
    
    if missing_vars:
        raise EnvironmentError(
            f"Отсутствуют обязательные переменные окружения: {', '.join(missing_vars)}\n"
            f"Убедитесь, что файл .env содержит все необходимые настройки."
        )

class BotManager:
    """Менеджер для управления всеми ботами системы"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.bots = []
        self.running_tasks = []
        self.shutdown_event = asyncio.Event()
        
    async def initialize_bots(self, only_bot: Optional[str] = None):
        """Инициализирует ботов в зависимости от настроек"""
        
        # User Bot
        if (not only_bot or only_bot == 'user') and settings.USER_BOT_TOKEN:
            try:
                from bots.user_bot.user_bot import UserBot
                user_bot = UserBot()
                self.bots.append(('User Bot', user_bot))
                self.logger.info("✅ User Bot инициализирован")
            except Exception as e:
                self.logger.error(f"❌ Ошибка инициализации User Bot: {e}", exc_info=True)
                if only_bot == 'user':
                    raise
        elif only_bot == 'user':
            raise ValueError("USER_BOT_TOKEN не настроен")
        
        # Task Bot
        if (not only_bot or only_bot == 'task') and settings.TASK_BOT_TOKEN:
            try:
                from bots.task_bot.task_bot import TaskBot
                task_bot = TaskBot()
                self.bots.append(('Task Bot', task_bot))
                self.logger.info("✅ Task Bot инициализирован")
            except Exception as e:
                self.logger.error(f"❌ Ошибка инициализации Task Bot: {e}", exc_info=True)
                if only_bot == 'task':
                    raise
        elif only_bot == 'task':
            raise ValueError("TASK_BOT_TOKEN не настроен")
        
        # Mover Bot
        if (not only_bot or only_bot == 'mover') and settings.MOVER_BOT_TOKEN:
            try:
                from bots.mover_bot.mover_bot import MoverBot
                mover_bot = MoverBot()
                self.bots.append(('Mover Bot', mover_bot))
                self.logger.info("✅ Mover Bot инициализирован")
            except Exception as e:
                self.logger.error(f"❌ Ошибка инициализации Mover Bot: {e}", exc_info=True)
                if only_bot == 'mover':
                    raise
        elif only_bot == 'mover':
            raise ValueError("MOVER_BOT_TOKEN не настроен")
        
        if not self.bots:
            raise ValueError(
                "Не активирован ни один бот! Проверьте настройки токенов в .env файле."
            )
        
        self.logger.info(f"📊 Инициализировано ботов: {len(self.bots)}")
    
    async def run_bot(self, name: str, bot_instance):
        """Запускает отдельного бота с обработкой ошибок"""
        try:
            self.logger.info(f"🚀 Запуск {name}...")
            await bot_instance.start()
        except asyncio.CancelledError:
            self.logger.info(f"⏹️ {name} остановлен")
        except Exception as e:
            self.logger.error(f"❌ Критическая ошибка в {name}: {e}", exc_info=True)
            # Сигнализируем о необходимости остановки всей системы
            self.shutdown_event.set()
            raise
    
    async def start_all_bots(self):
        """Запускает всех ботов одновременно"""
        self.logger.info(f"🎯 Все боты запущены ({len(self.bots)} процессов)")
        
        # Запускаем каждого бота в отдельной задаче
        for name, bot_instance in self.bots:
            self.logger.info(f"🚀 Запуск {name}...")
            # Создаем задачу для запуска бота
            task = asyncio.create_task(bot_instance.start())
            self.running_tasks.append(task)
            
        # Ожидаем завершения всех задач
        await asyncio.gather(*self.running_tasks)
    
    async def shutdown_all_bots(self):
        """Корректно останавливает всех ботов"""
        self.logger.info("🔄 Начинаю остановку всех ботов...")
        
        # Отменяем все задачи
        for task in self.running_tasks:
            if not task.done():
                task.cancel()
        
        # Ждем завершения всех задач
        if self.running_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self.running_tasks, return_exceptions=True),
                    timeout=10.0
                )
            except asyncio.TimeoutError:
                self.logger.warning("⚠️ Некоторые боты не остановились в течение 10 секунд")
        
        # Закрываем сессии ботов
        for name, bot in self.bots:
            try:
                if hasattr(bot, 'bot') and hasattr(bot.bot, 'session'):
                    await bot.bot.session.close()
                    self.logger.debug(f"Сессия {name} закрыта")
            except Exception as e:
                self.logger.debug(f"Ошибка при закрытии сессии {name}: {e}")
        
        self.logger.info("✅ Все боты остановлены")

def setup_signal_handlers(bot_manager: BotManager):
    """Настраивает обработчики сигналов для graceful shutdown"""
    
    def signal_handler(signum, frame):
        logging.getLogger(__name__).info(f"Получен сигнал {signum}")
        bot_manager.shutdown_event.set()
    
    # Настраиваем обработчики только для Unix-систем
    if hasattr(signal, 'SIGTERM'):
        signal.signal(signal.SIGTERM, signal_handler)
    if hasattr(signal, 'SIGINT'):
        signal.signal(signal.SIGINT, signal_handler)

def parse_arguments():
    """Парсит аргументы командной строки"""
    parser = argparse.ArgumentParser(
        description='Telegram Task Manager - система управления задачами',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python main.py                    # Запуск всех ботов
  python main.py --only user        # Только User Bot
  python main.py --only task        # Только Task Bot  
  python main.py --only mover       # Только Mover Bot
  python main.py --debug            # С отладочными логами
        """
    )
    
    parser.add_argument(
        '--only',
        choices=['user', 'task', 'mover'],
        help='Запустить только указанного бота'
    )
    
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Включить отладочные логи'
    )
    

    
    return parser.parse_args()

async def check_system_health():
    """Проверяет работоспособность всех систем"""
    logger = logging.getLogger(__name__)
    logger.info("🔄 Проверка работоспособности системы...")
    
    # Проверка Redis
    redis = RedisManager()
    try:
        await redis.connect()
        await redis.conn.ping()
        logger.info("✅ Redis: подключение успешно")
    except Exception as e:
        logger.error(f"❌ Redis: ошибка подключения - {e}")
    
    # Проверка PubSub
    try:
        pubsub_manager = PubSubManager()
        await pubsub_manager.redis.connect()
        await pubsub_manager.redis.conn.ping()
        logger.info("✅ PubSub: подключение к Redis успешно")
        
        # Тест подписки
        test_received = asyncio.Event()
        async def test_handler(channel, message):
            logger.info(f"Received test message on channel {channel}: {message}")
            if message.get('status') == 'test_subscription':
                test_received.set()
        await pubsub_manager.subscribe("health_check", test_handler)
        # Теперь запускаем слушателя только после всех подписок!
        asyncio.create_task(pubsub_manager.start())
        await asyncio.sleep(1)
        # Публикуем тестовое событие
        await pubsub_manager.publish_event("health_check", {"status": "test"})
        logger.info("✅ PubSub: тестовая публикация успешна")
        # Публикуем тестовое событие
        await pubsub_manager.publish_event("health_check", {"status": "test_subscription"})
        # Ждем до 5 секунд
        try:
            await asyncio.wait_for(test_received.wait(), timeout=5.0)
            logger.info("✅ PubSub: тестовая подписка успешна")
        except asyncio.TimeoutError:
            logger.error("❌ PubSub: тестовая подписка не сработала (таймаут 5 секунд)")
            return False
        finally:
            # Останавливаем прослушивание
            await pubsub_manager.stop()
    except Exception as e:
        logger.error(f"❌ PubSub: ошибка - {e}")

async def main():
    """Главная функция запуска системы"""
    
    # Парсим аргументы
    args = parse_arguments()
    
    # Настройка логирования
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("app.log", encoding="utf-8"),
            logging.StreamHandler()
        ]
    )
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("=" * 60)
        logger.info("🚀 ЗАПУСК TELEGRAM TASK MANAGER SYSTEM")
        logger.info("=" * 60)
        
        # Проверка окружения
        logger.info("📋 Проверка переменных окружения...")
        check_environment()
        logger.info("✅ Переменные окружения в порядке")
        
        # Проверка настроек
        logger.info("⚙️ Проверка настроек...")
        settings.verify_settings()
        logger.info("✅ Настройки корректны")
        
        # Проверка работоспособности системы
        await check_system_health()
        
        # Вывод конфигурации (без токенов)
        logger.info("📊 Конфигурация системы:")
        logger.info(f"  • Redis: {settings.REDIS_HOST}:{settings.REDIS_PORT}")
        logger.info(f"  • Forum Chat: {settings.FORUM_CHAT_ID}")
        logger.info(f"  • Support Chat: {settings.SUPPORT_CHAT_ID}")
        logger.info(f"  • User Bot: {'✅' if settings.USER_BOT_TOKEN else '❌'}")
        logger.info(f"  • Task Bot: {'✅' if settings.TASK_BOT_TOKEN else '❌'}")
        logger.info(f"  • Mover Bot: {'✅' if settings.MOVER_BOT_TOKEN else '❌'}")
        
        if args.only:
            logger.info(f"🎯 Режим: запуск только {args.only.upper()} BOT")
        else:
            logger.info("🎯 Режим: запуск всех доступных ботов")
        
        # Работаем только в режиме polling
        logger.info("🔄 Запуск в режиме polling (вебхуки отключены)")
        
        # Инициализируем менеджер ботов
        bot_manager = BotManager()
        
        # Настраиваем обработчики сигналов для graceful shutdown
        setup_signal_handlers(bot_manager)
        
        # Инициализируем ботов
        await bot_manager.initialize_bots(only_bot=args.only)
        
        # Запускаем всех ботов
        await bot_manager.start_all_bots()
        
        # Ожидаем события остановки
        await bot_manager.shutdown_event.wait()
        
    except KeyboardInterrupt:
        logger.info("⏹️ Получен сигнал остановки от пользователя")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка системы: {e}", exc_info=True)
        sys.exit(1)
    finally:
        # Останавливаем всех ботов
        await bot_manager.shutdown_all_bots()
        logger.info("🔄 Завершение работы системы...")
        logger.info("=" * 60)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⏹️ Остановка по запросу пользователя")
    except Exception as e:
        print(f"❌ Ошибка запуска системы: {e}")
        sys.exit(1)

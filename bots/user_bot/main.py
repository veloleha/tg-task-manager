#!/usr/bin/env python3
"""
User Bot Main Entry Point

Этот файл запускает user_bot - бота для сбора сообщений пользователей и создания задач.
User Bot включает новые функции:
- Создание отдельных тем для каждого пользователя в форуме
- Автообъединение сообщений в течение заданного времени
- Отправка реакций для обратной связи
- Обработка ответов от поддержки

Основные функции:
- Сбор сообщений от пользователей
- Автообъединение сообщений за 5 минут
- Создание тем в форуме для каждого пользователя
- Отправка ответов поддержки пользователям
- Установка реакций на сообщения
"""

import asyncio
import logging
import os
import sys
from pathlib import Path

# Добавляем корневую директорию проекта в Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from bots.user_bot.user_bot import UserBot
from config.settings import settings

# Настройка логирования
def setup_logging():
    """Настраивает систему логирования"""
    
    # Создаем директорию для логов если её нет
    logs_dir = project_root / "logs"
    logs_dir.mkdir(exist_ok=True)
    
    # Настройка форматирования
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Настройка логгера
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Очищаем существующие обработчики
    logger.handlers.clear()
    
    # Консольный обработчик
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Файловый обработчик
    file_handler = logging.FileHandler(
        logs_dir / "user_bot.log", 
        encoding='utf-8'
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Отдельный файл для ошибок
    error_handler = logging.FileHandler(
        logs_dir / "user_bot_errors.log", 
        encoding='utf-8'
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    logger.addHandler(error_handler)

def check_environment():
    """Проверяет наличие необходимых переменных окружения"""
    required_vars = [
        'USER_BOT_TOKEN',
        'FORUM_CHAT_ID',
        'REDIS_HOST'
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        raise EnvironmentError(
            f"Отсутствуют обязательные переменные окружения: {', '.join(missing_vars)}\n"
            f"Убедитесь, что файл .env содержит все необходимые настройки."
        )

async def main():
    """Главная функция запуска бота"""
    
    # Настройка логирования
    setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("=" * 50)
        logger.info("🚀 Запуск User Bot")
        logger.info("=" * 50)
        
        # Проверка окружения
        logger.info("📋 Проверка переменных окружения...")
        check_environment()
        logger.info("✅ Переменные окружения в порядке")
        
        # Проверка настроек
        logger.info("⚙️ Проверка настроек...")
        settings.verify_settings()
        logger.info("✅ Настройки корректны")
        
        # Вывод конфигурации (без токенов)
        logger.info("📊 Конфигурация:")
        logger.info(f"  - Redis: {settings.REDIS_HOST}:{settings.REDIS_PORT}")
        logger.info(f"  - Forum Chat ID: {settings.FORUM_CHAT_ID}")
        logger.info(f"  - Message Aggregation Timeout: {settings.MESSAGE_AGGREGATION_TIMEOUT}s")
        
        # Создание и запуск бота
        logger.info("🤖 Инициализация User Bot...")
        user_bot = UserBot()
        
        logger.info("🎯 User Bot готов к работе!")
        logger.info("📨 Ожидание сообщений от пользователей...")
        
        # Запуск бота
        await user_bot.start()
        
    except KeyboardInterrupt:
        logger.info("⏹️ Получен сигнал остановки (Ctrl+C)")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка при запуске: {e}", exc_info=True)
        sys.exit(1)
    finally:
        logger.info("🔄 Завершение работы User Bot...")
        logger.info("=" * 50)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⏹️ Остановка по запросу пользователя")
    except Exception as e:
        print(f"❌ Ошибка запуска: {e}")
        sys.exit(1)

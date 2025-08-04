#!/usr/bin/env python3
"""
Mover Bot Main Entry Point

Этот файл запускает mover_bot - бота для управления задачами и статистикой.
Mover Bot выполняет следующие функции:
- Перемещение завершенных задач в архивную тему
- Отображение детальной статистики по задачам и исполнителям
- Управление темами форума
- Периодическая очистка старых задач

Основные функции:
- Автоматическое перемещение завершенных задач
- Генерация статистических отчетов
- Создание и управление темами форума
- Очистка устаревших данных
"""

import asyncio
import logging
import os
import sys
from pathlib import Path

# Добавляем корневую директорию проекта в Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from bots.mover_bot.mover_bot import MoverBot
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
        logs_dir / "mover_bot.log", 
        encoding='utf-8'
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Отдельный файл для ошибок
    error_handler = logging.FileHandler(
        logs_dir / "mover_bot_errors.log", 
        encoding='utf-8'
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    logger.addHandler(error_handler)

def check_environment():
    """Проверяет наличие необходимых переменных окружения"""
    required_vars = [
        'MOVER_BOT_TOKEN',
        'FORUM_CHAT_ID',
        'SUPPORT_CHAT_ID',
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
        logger.info("🚀 Запуск Mover Bot")
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
        logger.info(f"  - Support Chat ID: {settings.SUPPORT_CHAT_ID}")
        logger.info(f"  - Waiting Topic ID: {settings.WAITING_TOPIC_ID}")
        logger.info(f"  - Completed Topic ID: {settings.COMPLETED_TOPIC_ID}")
        logger.info(f"  - Stats Update Interval: {settings.STATS_UPDATE_INTERVAL}s")
        
        # Создание и запуск бота
        logger.info("🤖 Инициализация Mover Bot...")
        mover_bot = MoverBot()
        
        logger.info("🎯 Mover Bot готов к работе!")
        logger.info("📊 Начинаю мониторинг задач и обновление статистики...")
        
        # Запуск бота
        await mover_bot.start()
        
    except KeyboardInterrupt:
        logger.info("⏹️ Получен сигнал остановки (Ctrl+C)")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка при запуске: {e}", exc_info=True)
        sys.exit(1)
    finally:
        logger.info("🔄 Завершение работы Mover Bot...")
        logger.info("=" * 50)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⏹️ Остановка по запросу пользователя")
    except Exception as e:
        print(f"❌ Ошибка запуска: {e}")
        sys.exit(1)

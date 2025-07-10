from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path
import os

# Проверка пути к .env
env_path = Path(__file__).parent.parent / '.env'
print(f"Путь к .env: {env_path}")
print(f"Файл существует: {env_path.exists()}")
print(f"Содержимое директории: {os.listdir(Path(__file__).parent.parent)}")

class Settings(BaseSettings):
    BOT_TOKEN: str
    TASK_BOT_TOKEN: str
    BUTTONS_CHAT_ID: int
    
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_PASSWORD: str = ""

    model_config = SettingsConfigDict(
        env_file=env_path,
        env_file_encoding='utf-8',
        extra='ignore'
    )

# Явная проверка с выводом
try:
    settings = Settings()
    print("Настройки успешно загружены!")
    print(f"BOT_TOKEN: {settings.BOT_TOKEN[:5]}...")  # Вывод части токена для проверки
except Exception as e:
    print(f"Ошибка загрузки: {e}")
    print("Текущие переменные окружения:", os.environ)
    raise
"""
Форматирование сообщений для Task Bot

Содержит функции для красивого отображения задач и статистики
"""

from datetime import datetime
from typing import Dict, Any, Optional

def format_task_message(task_data: Dict[str, Any], task_number: Optional[int] = None) -> str:
    """
    Форматирует сообщение задачи для отображения в чате поддержки
    
    Args:
        task_data: Данные задачи из Redis
        task_number: Номер задачи (если есть)
    
    Returns:
        Отформатированное сообщение
    """
    
    # Извлекаем данные
    user_id = task_data.get('user_id', 'Unknown')
    username = task_data.get('username', '')
    first_name = task_data.get('first_name', '')
    last_name = task_data.get('last_name', '')
    text = task_data.get('text', 'Нет текста')
    created_at = task_data.get('created_at', '')
    status = task_data.get('status', 'unreacted')
    
    # Формируем имя пользователя
    user_display = []
    if first_name:
        user_display.append(first_name)
    if last_name:
        user_display.append(last_name)
    
    if not user_display and username:
        user_display = [f"@{username}"]
    elif not user_display:
        user_display = [f"ID: {user_id}"]
    
    user_name = " ".join(user_display)
    if username and first_name:
        user_name += f" (@{username})"
    
    # Форматируем время
    time_str = ""
    if created_at:
        try:
            dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
            time_str = dt.strftime("%d.%m.%Y %H:%M")
        except:
            time_str = created_at
    
    # Определяем эмодзи статуса
    status_emoji = {
        'unreacted': '🆕',
        'waiting': '⏳',
        'in_progress': '🔥',
        'completed': '✅'
    }.get(status, '❓')
    
    # Формируем заголовок
    if task_number:
        header = f"{status_emoji} <b>Задача #{task_number}</b>"
    else:
        header = f"{status_emoji} <b>Новая задача</b>"
    
    # Собираем сообщение
    message_parts = [
        header,
        f"👤 <b>От:</b> {user_name}",
        f"🕐 <b>Время:</b> {time_str}",
        "",
        f"💬 <b>Сообщение:</b>",
        f"<i>{text}</i>"
    ]
    
    # Добавляем информацию об исполнителе если есть
    assignee = task_data.get('assignee')
    if assignee:
        message_parts.insert(-2, f"👨‍💼 <b>Исполнитель:</b> @{assignee}")
    
    return "\n".join(message_parts)

def format_stats_message(stats: Dict[str, Any]) -> str:
    """
    Форматирует сообщение со статистикой
    
    Args:
        stats: Словарь со статистикой
    
    Returns:
        Отформатированное сообщение статистики
    """
    
    message_parts = [
        "📊 <b>Статистика задач</b>",
        "",
        f"🆕 Новые: {stats.get('unreacted', 0)}",
        f"⏳ Ожидают: {stats.get('waiting', 0)}",
        f"🔥 В работе: {stats.get('in_progress', 0)}",
        f"✅ Завершены: {stats.get('completed', 0)}",
        "",
        f"📈 <b>Всего:</b> {sum([stats.get('unreacted', 0), stats.get('waiting', 0), stats.get('in_progress', 0), stats.get('completed', 0)])}"
    ]
    
    # Добавляем статистику по исполнителям
    executors = stats.get('executors', {})
    if executors:
        message_parts.extend([
            "",
            "👥 <b>По исполнителям:</b>"
        ])
        
        for executor, executor_stats in executors.items():
            in_progress = executor_stats.get('in_progress', 0)
            completed = executor_stats.get('completed', 0)
            message_parts.append(
                f"• @{executor}: 🔥{in_progress} ✅{completed}"
            )
    
    return "\n".join(message_parts)

def format_reply_message(task_data: Dict[str, Any], reply_text: str, author: str) -> str:
    """
    Форматирует сообщение с ответом на задачу
    
    Args:
        task_data: Данные задачи
        reply_text: Текст ответа
        author: Автор ответа
    
    Returns:
        Отформатированное сообщение ответа
    """
    
    task_number = task_data.get('task_number', 'N/A')
    user_name = task_data.get('first_name', 'Unknown')
    username = task_data.get('username', '')
    
    if username:
        user_display = f"{user_name} (@{username})"
    else:
        user_display = user_name
    
    message_parts = [
        f"💬 <b>Ответ на задачу #{task_number}</b>",
        f"👤 <b>Пользователь:</b> {user_display}",
        f"👨‍💼 <b>Ответил:</b> @{author}",
        "",
        f"<i>{reply_text}</i>"
    ]
    
    return "\n".join(message_parts)

def format_aggregated_task(messages: list, user_info: Dict[str, Any]) -> str:
    """
    Форматирует объединенную задачу из нескольких сообщений
    
    Args:
        messages: Список сообщений для объединения
        user_info: Информация о пользователе
    
    Returns:
        Отформатированное сообщение объединенной задачи
    """
    
    if not messages:
        return "Пустая задача"
    
    # Берем информацию из первого сообщения
    first_msg = messages[0]
    
    # Формируем имя пользователя
    user_name = user_info.get('first_name', '')
    if user_info.get('last_name'):
        user_name += f" {user_info['last_name']}"
    
    username = user_info.get('username', '')
    if username:
        if user_name:
            user_display = f"{user_name} (@{username})"
        else:
            user_display = f"@{username}"
    else:
        user_display = user_name or f"ID: {user_info.get('user_id', 'Unknown')}"
    
    # Форматируем время
    created_at = first_msg.get('created_at', '')
    time_str = ""
    if created_at:
        try:
            dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
            time_str = dt.strftime("%d.%m.%Y %H:%M")
        except:
            time_str = created_at
    
    # Собираем все тексты сообщений
    texts = []
    for i, msg in enumerate(messages, 1):
        text = msg.get('text', '').strip()
        if text:
            if len(messages) > 1:
                texts.append(f"{i}. {text}")
            else:
                texts.append(text)
    
    combined_text = "\n\n".join(texts) if texts else "Нет текста"
    
    # Формируем заголовок
    if len(messages) > 1:
        header = f"🆕 <b>Объединенная задача ({len(messages)} сообщений)</b>"
    else:
        header = f"🆕 <b>Новая задача</b>"
    
    message_parts = [
        header,
        f"👤 <b>От:</b> {user_display}",
        f"🕐 <b>Время:</b> {time_str}",
        "",
        f"💬 <b>Сообщение:</b>",
        f"<i>{combined_text}</i>"
    ]
    
    return "\n".join(message_parts)

def format_notification(title: str, message: str, emoji: str = "ℹ️") -> str:
    """
    Форматирует уведомление
    
    Args:
        title: Заголовок уведомления
        message: Текст уведомления
        emoji: Эмодзи для уведомления
    
    Returns:
        Отформатированное уведомление
    """
    
    return f"{emoji} <b>{title}</b>\n\n{message}"

def truncate_text(text: str, max_length: int = 200) -> str:
    """
    Обрезает текст до указанной длины
    
    Args:
        text: Исходный текст
        max_length: Максимальная длина
    
    Returns:
        Обрезанный текст
    """
    
    if len(text) <= max_length:
        return text
    
    return text[:max_length-3] + "..."

def escape_html(text: str) -> str:
    """
    Экранирует HTML символы в тексте
    
    Args:
        text: Исходный текст
    
    Returns:
        Экранированный текст
    """
    
    return (text
            .replace('&', '&amp;')
            .replace('<', '&lt;')
            .replace('>', '&gt;')
            .replace('"', '&quot;')
            .replace("'", '&#x27;'))

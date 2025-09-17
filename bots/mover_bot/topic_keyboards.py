from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from typing import List, Dict

def create_unreacted_topic_keyboard(task_id: str, executors: List[Dict]) -> InlineKeyboardMarkup:
    """Создает клавиатуру для темы неотреагированных задач с кнопками исполнителей"""
    buttons = []
    
    # Первая строка: основные действия
    buttons.append([
        InlineKeyboardButton(
            text="🔥 В работу", 
            callback_data=f"topic_take_{task_id}"
        ),
        InlineKeyboardButton(
            text="🗑️ Удалить", 
            callback_data=f"topic_delete_{task_id}"
        )
    ])
    
    # Добавляем кнопки для каждого исполнителя (по 2 в строке)
    executor_buttons = []
    for executor in executors:
        username = executor['username']
        first_name = executor['first_name']
        
        button = InlineKeyboardButton(
            text=f"👤 Поручить @{username}",
            callback_data=f"topic_assign_{username}_{task_id}"
        )
        
        executor_buttons.append(button)
        
        # Добавляем строку, когда накопилось 2 кнопки
        if len(executor_buttons) == 2:
            buttons.append(executor_buttons)
            executor_buttons = []
    
    # Добавляем оставшиеся кнопки исполнителей
    if executor_buttons:
        buttons.append(executor_buttons)
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def create_executor_topic_keyboard(task_id: str) -> InlineKeyboardMarkup:
    """Создает клавиатуру для темы исполнителя"""
    buttons = [
        [
            InlineKeyboardButton(
                text="✅ Завершить", 
                callback_data=f"topic_complete_{task_id}"
            ),
            InlineKeyboardButton(
                text="💬 Ответить", 
                callback_data=f"topic_reply_{task_id}"
            )
        ]
    ]
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def create_completed_topic_keyboard(task_id: str) -> InlineKeyboardMarkup:
    """Создает клавиатуру для темы завершенных задач"""
    buttons = [
        [
            InlineKeyboardButton(
                text="🔄 Переоткрыть", 
                callback_data=f"topic_reopen_{task_id}"
            ),
            InlineKeyboardButton(
                text="📊 Отчет", 
                callback_data=f"topic_report_{task_id}"
            )
        ]
    ]
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def create_reply_keyboard(additional_message_id: str) -> InlineKeyboardMarkup:
    """Создает клавиатуру с кнопкой 'Ответить' для дополнительного сообщения"""
    buttons = [
        [
            InlineKeyboardButton(
                text="💬 Ответить", 
                callback_data=f"additional_reply_{additional_message_id}"
            )
        ]
    ]
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from typing import Optional, List, Dict

def create_task_keyboard(task_id: str, status: str, assignee: Optional[str] = None) -> InlineKeyboardMarkup:
    """Создает клавиатуру для задачи в зависимости от её статуса"""
    
    buttons = []
    
    if status == "unreacted":
        # Новая задача: кнопки "Сделать задачей" и "Удалить"
        buttons = [
            [
                InlineKeyboardButton(
                    text="⚡ Сделать задачей", 
                    callback_data=f"status_waiting_{task_id}"
                ),
                InlineKeyboardButton(
                    text="🗑️ Удалить", 
                    callback_data=f"action_delete_{task_id}"
                )
            ]
        ]
    
    elif status == "waiting":
        # Ожидающая задача: кнопки "Взять в работу", "Ответить" и "Удалить"
        buttons = [
            [
                InlineKeyboardButton(
                    text="🔥 Взять в работу", 
                    callback_data=f"status_in_progress_{task_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    text="💬 Ответить", 
                    callback_data=f"action_reply_{task_id}"
                ),
                InlineKeyboardButton(
                    text="🗑️ Удалить", 
                    callback_data=f"action_delete_{task_id}"
                )
            ]
        ]
    
    elif status == "in_progress":
        # Задача в работе: кнопки "Завершить", "Ответить", "Напомнить"
        buttons = [
            [
                InlineKeyboardButton(
                    text="✅ Завершить", 
                    callback_data=f"status_completed_{task_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    text="💬 Ответить", 
                    callback_data=f"action_reply_{task_id}"
                ),
                InlineKeyboardButton(
                    text="🔔 Напомнить", 
                    callback_data=f"action_remind_{task_id}"
                )
            ]
        ]
    
    elif status == "completed":
        # Завершенная задача: кнопки "Переоткрыть" и "Отчет"
        buttons = [
            [
                InlineKeyboardButton(
                    text="🔄 Переоткрыть", 
                    callback_data=f"status_in_progress_{task_id}"
                ),
                InlineKeyboardButton(
                    text="📊 Отчет", 
                    callback_data=f"action_report_{task_id}"
                )
            ]
        ]
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def create_admin_keyboard() -> InlineKeyboardMarkup:
    """Создает административную клавиатуру"""
    buttons = [
        [
            InlineKeyboardButton(
                text="📊 Статистика", 
                callback_data="admin_stats"
            ),
            InlineKeyboardButton(
                text="🔄 Обновить", 
                callback_data="admin_refresh"
            )
        ],
        [
            InlineKeyboardButton(
                text="⚙️ Настройки", 
                callback_data="admin_settings"
            ),
            InlineKeyboardButton(
                text="📋 Все задачи", 
                callback_data="admin_all_tasks"
            )
        ]
    ]
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def create_confirmation_keyboard(task_id: str, action: str) -> InlineKeyboardMarkup:
    """Создает клавиатуру подтверждения действия"""
    buttons = [
        [
            InlineKeyboardButton(
                text="✅ Да", 
                callback_data=f"confirm_{action}_{task_id}"
            ),
            InlineKeyboardButton(
                text="❌ Нет", 
                callback_data=f"cancel_{action}_{task_id}"
            )
        ]
    ]
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def create_reminder_keyboard(task_id: str) -> InlineKeyboardMarkup:
    """Создает клавиатуру для настройки напоминаний"""
    buttons = [
        [
            InlineKeyboardButton(
                text="⏰ 1 час", 
                callback_data=f"remind_1h_{task_id}"
            ),
            InlineKeyboardButton(
                text="⏰ 4 часа", 
                callback_data=f"remind_4h_{task_id}"
            )
        ],
        [
            InlineKeyboardButton(
                text="⏰ 24 часа", 
                callback_data=f"remind_24h_{task_id}"
            ),
            InlineKeyboardButton(
                text="❌ Отмена", 
                callback_data=f"cancel_remind_{task_id}"
            )
        ]
    ]
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def create_status_filter_keyboard() -> InlineKeyboardMarkup:
    """Создает клавиатуру для фильтрации задач по статусу"""
    buttons = [
        [
            InlineKeyboardButton(
                text="🆕 Новые", 
                callback_data="filter_unreacted"
            ),
            InlineKeyboardButton(
                text="⏳ Ожидают", 
                callback_data="filter_waiting"
            )
        ],
        [
            InlineKeyboardButton(
                text="⚡ В работе", 
                callback_data="filter_in_progress"
            ),
            InlineKeyboardButton(
                text="✅ Завершены", 
                callback_data="filter_completed"
            )
        ],
        [
            InlineKeyboardButton(
                text="📋 Все задачи", 
                callback_data="filter_all"
            )
        ]
    ]
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

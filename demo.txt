import logging
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
import asyncio
from datetime import datetime, timedelta
import aiomysql
from aiogram.exceptions import TelegramRetryAfter, TelegramBadRequest

# Установите ваш токен бота
API_TOKEN = '7373455502:AAEoJvfwMdtuSXa7egtmPmCo2hP0BWVrSDE'

# ID чатов
USER_CHAT_ID = -1002402961402  # Чат для сообщений пользователей
BUTTONS_CHAT_ID = -1002269851341  # Чат для сообщений с кнопками (должен быть форумом)

# ID тем в форуме
TOPIC_WAITING = 317  # ID темы "В ожидании"
TOPIC_IN_PROGRESS = 319  # ID темы "В работе"
TOPIC_CLOSED = 321  # ID темы "Закрытые"

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Инициализация бота и диспетчера
bot = Bot(token=API_TOKEN)
dp = Dispatcher()


# Обработчик команды /week
@dp.message(Command("week"))
async def handle_week_command(message: types.Message):
    await send_report(days=7)

# Обработчик команды /day
@dp.message(Command("day"))
async def handle_day_command(message: types.Message):
    await send_report(days=1)

# Функция для формирования и отправки отчета
async def send_report(days: int):
    # Получаем данные из базы данных за указанный период
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)

    pool = await aiomysql.create_pool(host='localhost', port=3306,
                                      user='root', password='hesoyam',
                                      db='task_bot', autocommit=True)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            # Получаем задачи за указанный период
            await cur.execute('''
                SELECT m.message_text, m.status, u.username
                FROM messages m
                JOIN users u ON m.user_id = u.user_id
                WHERE m.created_at BETWEEN %s AND %s
                ORDER BY u.username, m.created_at
            ''', (start_date, end_date))
            results = await cur.fetchall()

    # Логируем полученные данные для отладки
    logger.info(f"Полученные данные из базы: {results}")

    # Формируем отчет
    report = {}
    for message_text, status, username in results:
        if username not in report:
            report[username] = {"completed": [], "in_progress": []}
        
        # Если message_text равен None, заменяем его на пустую строку
        message_text = message_text or ""
        
        # Логируем статус задачи для отладки
        logger.info(f"Задача: {message_text}, статус: {status}, пользователь: {username}")
        
        if status == "closed":
            report[username]["completed"].append(message_text)
        else:
            report[username]["in_progress"].append(message_text)

    # Форматируем отчет
    formatted_report = ""
    for username, tasks in report.items():
        formatted_report += f"@{username}:\n"
        
        # Выполненные задачи
        if tasks["completed"]:
            formatted_report += "Выполнено:\n"
            for i, task in enumerate(tasks["completed"], start=1):
                formatted_report += f"{i}. {task}\n"
        
        # Задачи в работе
        if tasks["in_progress"]:
            formatted_report += "В работе:\n"
            for i, task in enumerate(tasks["in_progress"], start=1):
                formatted_report += f"{i}. {task}\n"
        
        formatted_report += "\n"

    # Логируем сформированный отчет для отладки
    logger.info(f"Сформированный отчет:\n{formatted_report}")

    # Отправляем отчет в тему
    try:
        await bot.send_message(
            chat_id=BUTTONS_CHAT_ID,
            message_thread_id=315,  # ID темы
            text=formatted_report,
            parse_mode=None  # Отключаем Markdown
        )
    except TelegramBadRequest as e:
        logger.error(f"Ошибка при отправке отчета: {e}")
        # Логируем текст отчета для отладки
        logger.error(f"Текст отчета: {formatted_report}")

# Счетчики реакций
counters = {
    "unreacted": 0,  # Не отреагированные сообщения
    "fire": 0,       # Сообщения с реакцией 🔥
    "lightning": 0,  # Сообщения с реакцией ⚡
    "ok": 0,         # Сообщения с реакцией 👌
    "closed": 0      # Закрытые вопросы
}

# Состояния кнопок для каждого сообщения
message_states = {}

# Хранение ID сообщений в темах
topic_messages = {
    TOPIC_WAITING: {},
    TOPIC_IN_PROGRESS: {},
    TOPIC_CLOSED: {}
}

# Глобальная переменная для хранения текста закрепленного сообщения
pinned_message_text = None

# Глобальный словарь для хранения состояния "ожидание ответа"
awaiting_answer = {}

# Инициализация базы данных
async def init_db():
    pool = await aiomysql.create_pool(host='localhost', port=3306,
                                      user='root', password='hesoyam',
                                      db='task_bot', autocommit=True)
    logger.info("Успешное подключение к базе данных MySQL.")
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            # Проверяем, существует ли таблица messages
            await cur.execute("SHOW TABLES LIKE 'messages'")
            if not await cur.fetchone():
                await cur.execute('''
                    CREATE TABLE messages (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        user_id INT,
                        chat_id BIGINT,
                        message_text TEXT,
                        bot_message_id BIGINT,  # ID сообщения бота
                        user_message_id BIGINT,  # ID сообщения пользователя
                        status VARCHAR(50) DEFAULT 'initial',
                        message_thread_id BIGINT,  # ID темы
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')

            # Проверяем, существует ли таблица users
            await cur.execute("SHOW TABLES LIKE 'users'")
            if not await cur.fetchone():
                await cur.execute('''
                    CREATE TABLE users (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        user_id INT UNIQUE,
                        username VARCHAR(255),
                        first_name VARCHAR(255),
                        last_name VARCHAR(255)
                    )
                ''')

            # Проверяем, существует ли таблица counters
            await cur.execute("SHOW TABLES LIKE 'counters'")
            if not await cur.fetchone():
                await cur.execute('''
                    CREATE TABLE counters (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        unreacted INT DEFAULT 0,
                        fire INT DEFAULT 0,
                        lightning INT DEFAULT 0,
                        ok INT DEFAULT 0,
                        closed INT DEFAULT 0
                    )
                ''')
                # Инициализация счетчиков, если таблица пуста
                await cur.execute('INSERT INTO counters (unreacted, fire, lightning, ok, closed) VALUES (0, 0, 0, 0, 0)')
            else:
                # Проверяем, есть ли записи в таблице
                await cur.execute('SELECT COUNT(*) FROM counters')
                if (await cur.fetchone())[0] == 0:
                    await cur.execute('INSERT INTO counters (unreacted, fire, lightning, ok, closed) VALUES (0, 0, 0, 0, 0)')

            # Проверяем, существует ли таблица button_states
            await cur.execute("SHOW TABLES LIKE 'button_states'")
            if not await cur.fetchone():
                await cur.execute('''
                    CREATE TABLE button_states (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        bot_message_id BIGINT UNIQUE,  # ID сообщения бота
                        state VARCHAR(50) DEFAULT 'initial',  # Текущее состояние кнопки
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')

            # Проверяем, существует ли таблица topic_messages
            await cur.execute("SHOW TABLES LIKE 'topic_messages'")
            if not await cur.fetchone():
                await cur.execute('''
                    CREATE TABLE topic_messages (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        bot_message_id BIGINT,  # ID сообщения бота
                        topic_id BIGINT,  # ID темы
                        message_id BIGINT,  # ID сообщения в теме
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
    pool.close()
    await pool.wait_closed()

# Получение счетчиков из базы данных
async def get_counters():
    pool = await aiomysql.create_pool(host='localhost', port=3306,
                                      user='root', password='hesoyam',
                                      db='task_bot', autocommit=True)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute('SELECT unreacted, fire, lightning, ok, closed FROM counters WHERE id = 1')
            counters = await cur.fetchone()
            if counters:  # Если запись найдена
                return {
                    "unreacted": counters[0],
                    "fire": counters[1],
                    "lightning": counters[2],
                    "ok": counters[3],
                    "closed": counters[4]
                }
            else:  # Если запись не найдена
                # Создаем новую запись с нулевыми значениями
                await cur.execute('INSERT INTO counters (unreacted, fire, lightning, ok, closed) VALUES (0, 0, 0, 0, 0)')
                return {
                    "unreacted": 0,
                    "fire": 0,
                    "lightning": 0,
                    "ok": 0,
                    "closed": 0
                }
    pool.close()
    await pool.wait_closed()

# Обновление счетчиков в базе данных
async def update_counters(counters):
    pool = await aiomysql.create_pool(host='localhost', port=3306,
                                      user='root', password='hesoyam',
                                      db='task_bot', autocommit=True)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute('''
                UPDATE counters
                SET unreacted = %s, fire = %s, lightning = %s, ok = %s, closed = %s
                WHERE id = 1
            ''', (counters["unreacted"], counters["fire"], counters["lightning"], counters["ok"], counters["closed"]))
    pool.close()
    await pool.wait_closed()

# Функция для обновления закрепленного сообщения
async def update_pinned_message(chat_id: int):
    global pinned_message_text
    counters = await get_counters()
    new_text = (f"🔥: {counters['fire']}\n"
                f"⚡: {counters['lightning']}\n"
                f"👌: {counters['ok']}\n"
                f"❓: {counters['unreacted']}\n"
                f"✅: {counters['closed']}")
    
    # Если текст закрепленного сообщения изменился
    if pinned_message_text != new_text:
        # Получаем ID закрепленного сообщения
        chat = await bot.get_chat(chat_id)
        pinned_message_id = chat.pinned_message.message_id if chat.pinned_message else None
        
        if pinned_message_id:
            # Обновляем текст закрепленного сообщения
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=pinned_message_id,
                text=new_text
            )
        else:
            # Если закрепленного сообщения нет, создаем новое и закрепляем его
            sent_message = await bot.send_message(
                chat_id=chat_id,
                text=new_text
            )
            await bot.pin_chat_message(chat_id=chat_id, message_id=sent_message.message_id)
        
        # Обновляем текст закрепленного сообщения
        pinned_message_text = new_text

# Функция для создания клавиатуры с учетом текущего состояния
def create_keyboard(message_id: int, state: str = "initial"):
    if state == "initial":
        return InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="🔥", callback_data=f"reaction_fire_{message_id}"),
                InlineKeyboardButton(text="❌ Удалить", callback_data=f"delete_message_{message_id}"),
                InlineKeyboardButton(text="❓ Вопрос", callback_data=f"question_{message_id}")
            ]
        ])
    elif state == "fire":
        return InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="⚡", callback_data=f"reaction_lightning_{message_id}"),
                InlineKeyboardButton(text="⏰ Напоминание", callback_data=f"reminder_{message_id}")
            ]
        ])
    elif state == "lightning":
        return InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="👌", callback_data=f"reaction_ok_{message_id}"),
                InlineKeyboardButton(text="⏰ Напоминание", callback_data=f"reminder_{message_id}")
            ]
        ])
    elif state == "ok":
        return InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="🔄 Restart", callback_data=f"restart_task_{message_id}")
            ]
        ])
    elif state == "closed":
        return InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="🔄 Сброс", callback_data=f"reset_question_{message_id}")
            ]
        ])

# Функция для формирования ссылки на сообщение
def generate_message_link(chat_id: int, message_id: int) -> str:
    if str(chat_id).startswith('-100'):
        chat_id_for_link = str(chat_id).replace('-100', '')
    else:
        chat_id_for_link = str(chat_id)
    return f"https://t.me/c/{chat_id_for_link}/{message_id}"

# Функция для отправки сообщения в соответствующую тему
async def send_to_topic(message_text: str, state: str, previous_state: str = None, bot_message_id: int = None, user_id: int = None):
    """
    Отправляет сообщение в соответствующую тему и удаляет его из предыдущей темы (если есть).
    
    :param message_text: Текст сообщения.
    :param state: Новое состояние задачи.
    :param previous_state: Предыдущее состояние задачи (если есть).
    :param bot_message_id: ID сообщения бота.
    :param user_id: ID пользователя.
    """
    # Определяем ID темы для нового состояния
    if state == "fire":
        topic_id = TOPIC_WAITING
    elif state == "lightning":
        topic_id = TOPIC_IN_PROGRESS
    elif state == "ok":
        topic_id = TOPIC_CLOSED
    else:
        return

    # Удаляем сообщение из предыдущей темы (если есть)
    if previous_state:
        if previous_state == "fire":
            previous_topic_id = TOPIC_WAITING
        elif previous_state == "lightning":
            previous_topic_id = TOPIC_IN_PROGRESS
        elif previous_state == "ok":
            previous_topic_id = TOPIC_CLOSED
        else:
            previous_topic_id = None

        if previous_topic_id:
            # Получаем ID сообщения в предыдущей теме из базы данных
            pool = await aiomysql.create_pool(host='localhost', port=3306,
                                              user='root', password='hesoyam',
                                              db='task_bot', autocommit=True)
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute('SELECT message_id FROM topic_messages WHERE bot_message_id = %s AND topic_id = %s',
                                      (bot_message_id, previous_topic_id))
                    result = await cur.fetchone()
                    if result:
                        message_id = result[0]
                        # Удаляем сообщение из предыдущей темы
                        try:
                            await bot.delete_message(
                                chat_id=BUTTONS_CHAT_ID,
                                message_id=message_id
                            )
                            logger.info(f"Сообщение {message_id} удалено из темы {previous_topic_id}.")
                        except Exception as e:
                            logger.error(f"Ошибка при удалении сообщения: {e}")
                        # Удаляем запись из базы данных
                        await cur.execute('DELETE FROM topic_messages WHERE bot_message_id = %s AND topic_id = %s',
                                          (bot_message_id, previous_topic_id))
            pool.close()
            await pool.wait_closed()

    # Генерируем ссылку на сообщение бота в BUTTONS_CHAT_ID
    message_link = generate_message_link(BUTTONS_CHAT_ID, bot_message_id)
    logger.info(f"Сформирована ссылка: {message_link}")

    # Формируем текст сообщения с ссылкой
    message_text_with_link = f"{message_text}\n\n[Ссылка на сообщение]({message_link})"

    # Отправляем сообщение в новую тему
    sent_message = await bot.send_message(
        chat_id=BUTTONS_CHAT_ID,
        message_thread_id=topic_id,  # Указываем тему
        text=message_text_with_link,
        parse_mode="Markdown"
    )

    # Сохраняем ID сообщения в новой теме
    topic_messages[topic_id][message_text] = sent_message.message_id

    # Сохраняем информацию о сообщении в базе данных
    pool = await aiomysql.create_pool(host='localhost', port=3306,
                                      user='root', password='hesoyam',
                                      db='task_bot', autocommit=True)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute('''
                INSERT INTO topic_messages (bot_message_id, topic_id, message_id)
                VALUES (%s, %s, %s)
            ''', (bot_message_id, topic_id, sent_message.message_id))
    pool.close()
    await pool.wait_closed()

# Восстановление сообщений в темах при запуске бота
async def restore_topic_messages():
    pool = await aiomysql.create_pool(host='localhost', port=3306,
                                      user='root', password='hesoyam',
                                      db='task_bot', autocommit=True)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute('SELECT bot_message_id, topic_id, message_id FROM topic_messages')
            results = await cur.fetchall()
            for bot_message_id, topic_id, message_id in results:
                topic_messages[topic_id][bot_message_id] = message_id
    pool.close()
    await pool.wait_closed()

# Обработчик команды /clear
@dp.message(Command("clear"))
async def handle_clear_command(message: types.Message):
    global counters
    logger.info("Обработка команды /clear")
    pool = await aiomysql.create_pool(host='localhost', port=3306,
                                      user='root', password='hesoyam',
                                      db='task_bot', autocommit=True)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute('TRUNCATE TABLE counters')
            await cur.execute('INSERT INTO counters (unreacted, fire, lightning, ok, closed) VALUES (0, 0, 0, 0, 0)')
    pool.close()
    await pool.wait_closed()
    await update_pinned_message(BUTTONS_CHAT_ID)  # Обновляем закрепленное сообщение
    counters = await get_counters()
    await message.answer("Все счетчики сброшены, и все темы полностью очищены!")

# Обработчик нажатия на кнопку "напоминание"
@dp.callback_query(lambda c: c.data.startswith("reminder_"))
async def process_reminder_callback(callback_query: types.CallbackQuery):
    bot_message_id = int(callback_query.data.split("_")[1])
    state = message_states.get(bot_message_id, "initial")
    
    if state in ["fire", "lightning"]:
        await callback_query.answer("Напоминание установлено!")
        await send_reminder(bot_message_id)
    else:
        await callback_query.answer("Напоминание можно установить только для задач в состоянии 'fire' или 'lightning'.")

# Функция для отправки напоминания
async def send_reminder(bot_message_id: int):
    while True:
        await asyncio.sleep(3600)  # Ожидание 1 часа
        state = message_states.get(bot_message_id, "initial")
        if state not in ["fire", "lightning"]:
            break
        
        pool = await aiomysql.create_pool(host='localhost', port=3306,
                                          user='root', password='hesoyam',
                                          db='task_bot', autocommit=True)
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute('SELECT message_text FROM messages WHERE bot_message_id = %s', (bot_message_id,))
                result = await cur.fetchone()
                if result:
                    message_text = result[0]
                    message_link = generate_message_link(BUTTONS_CHAT_ID, bot_message_id)
                    reminder_text = f"Напоминание: {message_text}\n\n[Ссылка на задачу]({message_link})"
                    await bot.send_message(
                        chat_id=BUTTONS_CHAT_ID,
                        text=reminder_text,
                        parse_mode="Markdown"
                    )
        pool.close()
        await pool.wait_closed()

# Обработчик сообщений в чате пользователей
@dp.message(lambda message: message.chat.id in [USER_CHAT_ID, BUTTONS_CHAT_ID])
async def handle_user_message(message: types.Message):
    logger.info(f"Обработка сообщения от пользователя: {message.text}")
    
    # Если сообщение пришло из USER_CHAT_ID
    if message.chat.id == USER_CHAT_ID:
        # Увеличиваем счетчик не отреагированных сообщений
        counters["unreacted"] += 1
        await update_counters(counters)
        await update_pinned_message(BUTTONS_CHAT_ID)  # Обновляем закрепленное сообщение

        # Сохраняем начальное состояние для этого сообщения
        message_states[message.message_id] = "initial"

        # Сохраняем ID темы (если есть)
        message_thread_id = message.message_thread_id if message.is_topic_message else None

        # Создаем клавиатуру в начальном состоянии
        keyboard = create_keyboard(message.message_id, state="initial")

        # Упоминание пользователя через @username
        username = f"@{message.from_user.username}" if message.from_user.username else message.from_user.first_name

        # Если есть медиафайл (фото или видео)
        if message.photo or message.video:
            media = message.photo[-1] if message.photo else message.video
            caption = message.caption if message.caption else "Медиафайл без описания."
            message_text = caption  # Используем только текст сообщения пользователя

            # Пересылаем медиафайл в чат с кнопками
            sent_message = await bot.send_photo(
                chat_id=BUTTONS_CHAT_ID,
                photo=media.file_id,
                caption=f"{username} написал: {caption}",
                reply_markup=keyboard
            ) if message.photo else await bot.send_video(
                chat_id=BUTTONS_CHAT_ID,
                video=media.file_id,
                caption=f"{username} написал: {caption}",
                reply_markup=keyboard
            )
        else:
            # Если это текстовое сообщение
            message_text = message.text  # Используем только текст сообщения пользователя
            sent_message = await bot.send_message(
                chat_id=BUTTONS_CHAT_ID,
                text=f"{username} написал: {message.text}",
                reply_markup=keyboard
            )

        # Сохраняем ID сообщения в BUTTONS_CHAT_ID
        bot_message_id = sent_message.message_id

        # Сохраняем данные в таблицу users
        pool = await aiomysql.create_pool(host='localhost', port=3306,
                                          user='root', password='hesoyam',
                                          db='task_bot', autocommit=True)
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute('''
                    INSERT INTO users (user_id, username, first_name, last_name)
                    VALUES (%s, %s, %s, %s) AS new
                    ON DUPLICATE KEY UPDATE
                    username = new.username,
                    first_name = new.first_name,
                    last_name = new.last_name
                ''', (message.from_user.id, message.from_user.username, message.from_user.first_name, message.from_user.last_name))
        pool.close()
        await pool.wait_closed()

        # Сохраняем данные в таблицу messages
        pool = await aiomysql.create_pool(host='localhost', port=3306,
                                          user='root', password='hesoyam',
                                          db='task_bot', autocommit=True)
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute('''
                        INSERT INTO messages (user_id, chat_id, message_text, bot_message_id, user_message_id, status, message_thread_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ''', (message.from_user.id, message.chat.id, message_text, bot_message_id, message.message_id, "initial", message_thread_id))
                    logger.info(f"Данные успешно записаны в таблицу messages: user_id={message.from_user.id}, chat_id={message.chat.id}, bot_message_id={bot_message_id}, message_text={message_text}, user_message_id={message.message_id}, message_thread_id={message_thread_id}")
                except Exception as e:
                    logger.error(f"Ошибка при записи в таблицу messages: {e}")
        pool.close()
        await pool.wait_closed()

    # Если сообщение пришло из BUTTONS_CHAT_ID и пользователь ожидает ответа
    elif message.chat.id == BUTTONS_CHAT_ID and message.from_user.id in awaiting_answer:
        # Получаем данные о состоянии "ожидание ответа"
        user_data = awaiting_answer.pop(message.from_user.id)
        bot_message_id = user_data["bot_message_id"]
        logger.info(f"Пользователь {message.from_user.id} отправил ответ на вопрос.")

        # Обрабатываем ответ на вопрос
        await handle_answer(message, bot_message_id)

# Обработчик нажатия на кнопки
@dp.callback_query(lambda c: c.data.startswith("reaction_") or c.data.startswith("delete_") or c.data.startswith("restart_") or c.data.startswith("question_") or c.data.startswith("reset_"))
async def process_callback_reaction(callback_query: types.CallbackQuery):
    global counters
    logger.info(f"Обработка callback: {callback_query.data}")

    # Получаем bot_message_id из callback-запроса
    bot_message_id = callback_query.message.message_id

    # Получаем текст сообщения и user_message_id из базы данных
    pool = await aiomysql.create_pool(host='localhost', port=3306,
                                      user='root', password='hesoyam',
                                      db='task_bot', autocommit=True)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute('SELECT user_message_id, message_text, message_thread_id FROM messages WHERE bot_message_id = %s',
                              (bot_message_id,))
            result = await cur.fetchone()
            if result:
                user_message_id, original_message_text, message_thread_id = result
                logger.info(f"Найден user_message_id: {user_message_id}, original_message_text: {original_message_text}, message_thread_id: {message_thread_id}")
            else:
                logger.error(f"Не удалось найти данные в базе данных для bot_message_id={bot_message_id}")
                return
    pool.close()
    await pool.wait_closed()

    # Если нажата кнопка удаления
    if callback_query.data.startswith("delete_"):
        # Удаляем сообщение в BUTTONS_CHAT_ID
        await bot.delete_message(
            chat_id=BUTTONS_CHAT_ID,
            message_id=bot_message_id
        )
        # Уменьшаем счетчик неотреагированных сообщений
        counters["unreacted"] -= 1
        await update_counters(counters)
        await update_pinned_message(BUTTONS_CHAT_ID)  # Обновляем закрепленное сообщение
        await callback_query.answer("Сообщение удалено!")
        return

    # Если нажата кнопка "Restart"
    if callback_query.data.startswith("restart_"):
        # Обновляем состояние задачи на "lightning" (в работе)
        message_states[bot_message_id] = "lightning"

        # Увеличиваем счетчик "lightning" и уменьшаем счетчик "ok"
        counters["lightning"] += 1
        counters["ok"] -= 1

        # Обновляем клавиатуру
        keyboard = create_keyboard(bot_message_id, state="lightning")
        try:
            await callback_query.message.edit_reply_markup(reply_markup=keyboard)
        except TelegramRetryAfter as e:
            logger.warning(f"Ошибка Flood control: {e}. Ждем {e.retry_after} секунд.")
            await asyncio.sleep(e.retry_after)
            await callback_query.message.edit_reply_markup(reply_markup=keyboard)

        # Перемещаем задачу в тему "В работе"
        await send_to_topic(
            original_message_text,
            "lightning",
            "ok",
            bot_message_id,
            callback_query.from_user.id if callback_query and callback_query.from_user else None  # Используем user_id
        )

        # Обновляем счетчики
        await update_counters(counters)
        await update_pinned_message(BUTTONS_CHAT_ID)  # Обновляем закрепленное сообщение
        await callback_query.answer("Задача снова в работе!")
        return

    # Если нажата кнопка "Вопрос"
    if callback_query.data.startswith("question_"):
        # Устанавливаем состояние "awaiting_answer" для пользователя
        user_id = callback_query.from_user.id
        awaiting_answer[user_id] = {
            "bot_message_id": callback_query.message.message_id,  # ID сообщения бота
            "timestamp": datetime.now()  # Время нажатия кнопки
        }
        logger.info(f"Пользователь {user_id} ожидает ответа на вопрос.")

        # Отправляем запрос на ввод ответа
        await bot.send_message(
            chat_id=BUTTONS_CHAT_ID,
            text="Введите ответ на вопрос:"
        )

        await callback_query.answer("Ожидаем ответа на вопрос...")
        return

    # Если нажата кнопка "Сброс"
    if callback_query.data.startswith("reset_"):
        # Обновляем состояние задачи на "initial"
        message_states[bot_message_id] = "initial"

        # Увеличиваем счетчик "unreacted" и уменьшаем счетчик "closed"
        counters["unreacted"] += 1
        counters["closed"] -= 1

        # Обновляем клавиатуру
        keyboard = create_keyboard(bot_message_id, state="initial")
        try:
            await callback_query.message.edit_reply_markup(reply_markup=keyboard)
        except TelegramRetryAfter as e:
            logger.warning(f"Ошибка Flood control: {e}. Ждем {e.retry_after} секунд.")
            await asyncio.sleep(e.retry_after)
            await callback_query.message.edit_reply_markup(reply_markup=keyboard)

        # Перемещаем задачу в тему "В ожидании"
        await send_to_topic(
            original_message_text,
            "initial",
            "closed",
            bot_message_id,
            callback_query.from_user.id if callback_query and callback_query.from_user else None  # Используем user_id
        )

        # Обновляем счетчики
        await update_counters(counters)
        await update_pinned_message(BUTTONS_CHAT_ID)  # Обновляем закрепленное сообщение
        await callback_query.answer("Вопрос сброшен!")
        return

    # Получаем тип реакции из callback_data
    reaction_type = callback_query.data.split("_")[1]

    # Получаем текущее состояние для этого сообщения
    current_state = message_states.get(bot_message_id, "initial")

    # Обрабатываем реакцию в зависимости от текущего состояния
    if current_state == "initial" and reaction_type == "fire":
        counters["fire"] += 1
        counters["unreacted"] -= 1
        new_state = "fire"
    elif current_state == "fire" and reaction_type == "lightning":
        counters["lightning"] += 1
        counters["fire"] -= 1
        new_state = "lightning"
    elif current_state == "lightning" and reaction_type == "ok":
        counters["ok"] += 1
        counters["lightning"] -= 1
        new_state = "ok"
    else:
        await callback_query.answer("Невозможно выполнить это действие!")
        return

    # Обновляем состояние для этого сообщения
    message_states[bot_message_id] = new_state

    # Сохраняем состояние кнопки в базе данных
    pool = await aiomysql.create_pool(host='localhost', port=3306,
                                      user='root', password='hesoyam',
                                      db='task_bot', autocommit=True)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute('''
                INSERT INTO button_states (bot_message_id, state)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE state = %s
            ''', (bot_message_id, new_state, new_state))
    pool.close()
    await pool.wait_closed()

    # Обновляем клавиатуру
    keyboard = create_keyboard(bot_message_id, state=new_state)
    try:
        await callback_query.message.edit_reply_markup(reply_markup=keyboard)
    except TelegramRetryAfter as e:
        logger.warning(f"Ошибка Flood control: {e}. Ждем {e.retry_after} секунд.")
        await asyncio.sleep(e.retry_after)
        await callback_query.message.edit_reply_markup(reply_markup=keyboard)
    except TelegramBadRequest as e:
        logger.warning(f"Сообщение не изменено: {e}")
    except Exception as e:
        logger.error(f"Ошибка при обновлении клавиатуры: {e}")

    # Ставим реакцию на сообщение пользователя в USER_CHAT_ID
    try:
        await bot.set_message_reaction(
            chat_id=USER_CHAT_ID,
            message_id=user_message_id,
            reaction=[{"type": "emoji", "emoji": "🔥" if reaction_type == "fire" else "⚡" if reaction_type == "lightning" else "👌"}]
        )
    except TelegramRetryAfter as e:
        logger.warning(f"Ошибка Flood control: {e}. Ждем {e.retry_after} секунд.")
        await asyncio.sleep(e.retry_after)
        await bot.set_message_reaction(
            chat_id=USER_CHAT_ID,
            message_id=user_message_id,
            reaction=[{"type": "emoji", "emoji": "🔥" if reaction_type == "fire" else "⚡" if reaction_type == "lightning" else "👌"}]
        )
    except Exception as e:
        logger.error(f"Ошибка при установке реакции: {e}")

    # Перемещаем задачу в соответствующую тему
    await send_to_topic(
        original_message_text,
        new_state,
        current_state,
        bot_message_id,
        callback_query.from_user.id if callback_query and callback_query.from_user else None  # Используем user_id
    )

    # Обновляем счетчики
    await update_counters(counters)
    await update_pinned_message(BUTTONS_CHAT_ID)  # Обновляем закрепленное сообщение
    await callback_query.answer(f"Реакция {reaction_type} отправлена!")

# Обработчик ответа на вопрос
async def handle_answer(message: types.Message, bot_message_id: int):
    logger.info("Обработка ответа на вопрос")
    logger.info(f"Состояние сообщения {bot_message_id}: {message_states.get(bot_message_id)}")
    logger.info(f"Текущие состояния: {message_states}")

    # Получаем user_message_id, message_thread_id и текст оригинального сообщения пользователя из базы данных
    pool = await aiomysql.create_pool(host='localhost', port=3306,
                                      user='root', password='hesoyam',
                                      db='task_bot', autocommit=True)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute('SELECT user_message_id, message_text, message_thread_id FROM messages WHERE bot_message_id = %s',
                              (bot_message_id,))
            result = await cur.fetchone()
            if result:
                user_message_id, original_message_text, message_thread_id = result
                logger.info(f"Найден user_message_id: {user_message_id}, original_message_text: {original_message_text}, message_thread_id: {message_thread_id}")
            else:
                logger.error(f"Не удалось найти данные в базе данных для bot_message_id={bot_message_id}")
                return
    pool.close()
    await pool.wait_closed()

    # Логирование перед отправкой сообщения
    logger.info(f"Отправка ответа пользователю в чат {USER_CHAT_ID}, тему {message_thread_id}, ответ на сообщение {user_message_id}")

    # Если ответ содержит медиафайл (фото или видео)
    if message.photo or message.video:
        media = message.photo[-1] if message.photo else message.video
        caption = message.caption if message.caption else "Ответ на ваш вопрос."

        # Отправляем медиафайл пользователю в нужную тему
        try:
            if message.photo:
                await bot.send_photo(
                    chat_id=USER_CHAT_ID,
                    message_thread_id=message_thread_id,  # Указываем тему
                    photo=media.file_id,
                    caption=caption,
                    reply_to_message_id=user_message_id  # Ответ на конкретное сообщение пользователя
                )
            else:
                await bot.send_video(
                    chat_id=USER_CHAT_ID,
                    message_thread_id=message_thread_id,  # Указываем тему
                    video=media.file_id,
                    caption=caption,
                    reply_to_message_id=user_message_id  # Ответ на конкретное сообщение пользователя
                )
            logger.info("Медиафайл успешно отправлен пользователю!")
        except Exception as e:
            logger.error(f"Ошибка при отправке медиафайла пользователю: {e}")
    else:
        # Если это текстовый ответ
        try:
            await bot.send_message(
                chat_id=USER_CHAT_ID,
                message_thread_id=message_thread_id,  # Указываем тему
                text=f"Ответ на ваш вопрос: {message.text}",  # Убираем ссылку на сообщение
                reply_to_message_id=user_message_id,  # Ответ на конкретное сообщение пользователя
                parse_mode="Markdown"
            )
            logger.info("Ответ успешно отправлен пользователю!")
        except Exception as e:
            logger.error(f"Ошибка при отправке ответа пользователю: {e}")

    # Обновляем состояние задачи на "closed"
    message_states[bot_message_id] = "closed"

    # Увеличиваем счетчик "closed" и уменьшаем счетчик "unreacted"
    counters["closed"] += 1
    counters["unreacted"] -= 1  # Уменьшаем счетчик неотреагированных сообщений

    # Обновляем клавиатуру
    keyboard = create_keyboard(bot_message_id, state="closed")
    try:
        await bot.edit_message_reply_markup(
            chat_id=BUTTONS_CHAT_ID,
            message_id=bot_message_id,
            reply_markup=keyboard
        )
    except TelegramRetryAfter as e:
        logger.warning(f"Ошибка Flood control: {e}. Ждем {e.retry_after} секунд.")
        await asyncio.sleep(e.retry_after)
        await bot.edit_message_reply_markup(
            chat_id=BUTTONS_CHAT_ID,
            message_id=bot_message_id,
            reply_markup=keyboard
        )
    except TelegramBadRequest as e:
        logger.warning(f"Сообщение не изменено: {e}")
    except Exception as e:
        logger.error(f"Ошибка при обновлении клавиатуры: {e}")

    # Перемещаем задачу в тему "Закрытые"
    await send_to_topic(original_message_text, "closed", "awaiting_answer", bot_message_id, message.from_user.id if message.from_user else None)

    # Обновляем счетчики
    await update_counters(counters)
    await update_pinned_message(BUTTONS_CHAT_ID)  # Обновляем закрепленное сообщение
    await message.answer("Ответ отправлен пользователю!")

# Запуск бота
async def main():
    # Инициализация базы данных
    await init_db()

    # Загружаем счетчики из базы данных
    global counters
    counters = await get_counters()

    # Восстанавливаем сообщения в темах
    await restore_topic_messages()

    # Восстанавливаем состояния кнопок и обновляем клавиатуру
    pool = await aiomysql.create_pool(host='localhost', port=3306,
                                      user='root', password='hesoyam',
                                      db='task_bot', autocommit=True)
    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute('SELECT bot_message_id, state FROM button_states')
                results = await cur.fetchall()
                for bot_message_id, state in results:
                    message_states[bot_message_id] = state
                    try:
                        await bot.edit_message_reply_markup(
                            chat_id=BUTTONS_CHAT_ID,
                            message_id=bot_message_id,
                            reply_markup=create_keyboard(bot_message_id, state=state)
                        )
                    except TelegramBadRequest as e:
                        logger.warning(f"Сообщение не изменено: {e}")
                    except Exception as e:
                        logger.error(f"Ошибка при обновлении клавиатуры: {e}")
    except Exception as e:
        logger.error(f"Ошибка при восстановлении состояний кнопок: {e}")
    finally:
        pool.close()
        await pool.wait_closed()

    # Запускаем бота
    try:
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Ошибка при запуске бота: {e}")
    finally:
        # Закрываем все соединения
        await bot.session.close()

if __name__ == '__main__':
    asyncio.run(main())
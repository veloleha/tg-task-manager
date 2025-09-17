# Исправление для метода _forward_reply_to_user_topic в user_bot.py
# Заменить метод _forward_reply_to_user_topic (строки 425-448) на этот код:

async def _forward_reply_to_user_topic(self, sent_message, user_id: int, chat_id: int, original_message_id: int):
    """Пересылает ответ UserBot в тему пользователя для сохранения истории переписки"""
    try:
        # Получаем тему пользователя
        user_topic_id = await self.topic_manager._get_active_user_topic(chat_id, user_id)
        
        if user_topic_id:
            # Проверяем, был ли ответ уже отправлен в тему пользователя
            # Если sent_message уже имеет message_thread_id равный user_topic_id, то не пересылаем
            if hasattr(sent_message, 'message_thread_id') and sent_message.message_thread_id == user_topic_id:
                logger.info(f"[USERBOT][FORWARD] Reply already sent to user topic {user_topic_id}, skipping forwarding")
                return
            
            # Пересылаем ответ в тему пользователя только если он был отправлен не в тему
            await self.bot.forward_message(
                chat_id=chat_id,
                from_chat_id=chat_id,
                message_id=sent_message.message_id,
                message_thread_id=user_topic_id
            )
            logger.info(f"[USERBOT][FORWARD] Reply forwarded to user topic {user_topic_id} for user {user_id}")
        else:
            logger.warning(f"[USERBOT][FORWARD] No user topic found for user {user_id} in chat {chat_id}")
            
    except Exception as e:
        error_msg = str(e).lower()
        if "message thread not found" in error_msg:
            logger.warning(f"[USERBOT][FORWARD] User topic not found, will skip forwarding reply")
        else:
            logger.error(f"[USERBOT][FORWARD] Error forwarding reply to user topic: {e}")

# notification_manager.py

import aiohttp
import asyncio
import logging

class NotificationManager:
    def __init__(self, telegram_config):
        self.telegram_config = telegram_config
        self.sessions = {}

    async def init_session(self):
        for account in self.telegram_config:
            session = aiohttp.ClientSession()
            self.sessions[account['bot_token']] = session

    async def send_notification(self, message: str, logger: logging.Logger):
        tasks = []
        for account in self.telegram_config:
            bot_token = account['bot_token']
            chat_id = account['chat_id']
            session = self.sessions.get(bot_token)
            if session is None:
                session = aiohttp.ClientSession()
                self.sessions[bot_token] = session
            tasks.append(self._send_message(session, bot_token, chat_id, message, logger))
        await asyncio.gather(*tasks)

    async def _send_message(self, session, bot_token, chat_id, message, logger: logging.Logger):
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        params = {
            "chat_id": chat_id,
            "text": message
        }
        try:
            async with session.post(url, data=params) as resp:
                if resp.status != 200:
                    logger.error(f"Ошибка при отправке уведомления в Telegram (Chat ID: {chat_id}): {resp.status}")
                    text = await resp.text()
                    logger.error(f"Текст ответа Telegram API: {text}")
        except Exception as e:
            logger.exception(f"Ошибка при отправке уведомления в Telegram: {e}")

    async def close(self):
        for session in self.sessions.values():
            await session.close()

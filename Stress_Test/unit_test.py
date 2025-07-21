# -*- coding: utf-8 -*-
# main.py

import os
import asyncio
import logging
from typing import Dict, Any
import nest_asyncio
from contextlib import asynccontextmanager

from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from dotenv import load_dotenv

from bot_core import TelegramBot  # Ваш основной класс бота
from dataManager import DataManager  # Ваш DataManager
from dashboard import Dashboard  # Импорт дашборда

# *** Понадобятся пакеты для Hypercorn ***
# pip install hypercorn

from hypercorn.asyncio import serve
from hypercorn.config import Config as HyperConfig

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("telegram_bot.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# Конфиг для Telegram-бота
BOT_CONFIG = {
    "telegram_token": os.getenv("TELEGRAM_TOKEN_Prod", "your-telegram-bot-token"),
    "llm_model": "deepseek-r1-distill-llama-70b",
    "advanced_llm_model": "deepseek-r1-distill-llama-70b",
    "request_timeout": 30,
    "telegram_timeout": 60,
    "max_tasks_per_user": 3,
    "global_max_tasks": 10,
    "session_timeout_hours": 3,
    "history_db": "user_history.db",
    "messages": {
        "en": {
            "start": "Hello! I'm a bot for working with sensor data. Try, for example: 'Plot a graph for 3-9 April for sensor T01'.",
            "help": (
                "I can:\n"
                "- Plot a graph for a sensor: 'Plot a graph for 3-9 April for sensor T01'\n"
                "- Show sensor info: 'Show info for sensor T23'\n"
                "- Show time range: 'What is the time range?'\n"
                "- Plot a random sensor graph: 'Plot a random graph'\n"
                "- List all sensors: 'List sensors'\n"
                "Type /functions for more examples."
            ),
            "functions": (
                "Available functions:\n"
                "- Plot graph: 'Plot graph for sensor T01 from 2023-04-03 to 2023-04-09'\n"
                "- Sensor info: 'Show info for sensor T23'\n"
                "- Time range: 'What is the time range?'\n"
                "- Random graph: 'Plot a random graph'\n"
                "- Sensor list: 'List sensors'"
            ),
            "processing": "Processing your request, this may take up to 30 seconds. Please wait.",
            "classifying": "Analyzing your request type...",
            "formalizing": "Structuring your request...",
            "executing": "Executing your request...",
            "limit_exceeded": "You reached the limit of concurrent requests ({max_tasks}). Please wait.",
            "error": "An error occurred: {reason}. Please try again or use /functions for examples.",
            "clarify": "Please clarify: {questions}\nAvailable functions:\n{functions}",
            "invalid_input": "Invalid input. Try something like: 'Plot graph for sensor T01 for 3-9 April'.\nAvailable functions:\n{functions}",
            "non_text_input": "I can only process text messages. Please send a text request.\nAvailable functions:\n{functions}",
            "sensor_not_found": "Sensor '{sensor_name}' not found. Available sensors: {sensors}.\nAvailable functions:\n{functions}",
        },
        "ru": {
            "start": "Привет! Я бот для работы с данными датчиков. Попробуй, например: 'Нарисуй график за 3-9 апреля для датчика T01'.",
            "help": (
                "Я могу:\n"
                "- Построить график для датчика: 'Нарисуй график за 3-9 апреля для датчика T01'\n"
                "- Показать информацию о датчике: 'Покажи информацию о датчике T23'\n"
                "- Показать временной диапазон: 'Какой временной диапазон?'\n"
                "- Построить случайный график: 'Нарисуй случайный график'\n"
                "- Показать список датчиков: 'Список датчиков'\n"
                "Напиши /functions для примеров."
            ),
            "functions": (
                "Доступные функции:\n"
                "- Построить график: 'Нарисуй график для датчика T01 с 2023-04-03 по 2023-04-09'\n"
                "- Информация о датчике: 'Покажи информацию о датчике T23'\n"
                "- Временной диапазон: 'Какой временной диапазон?'\n"
                "- Случайный график: 'Нарисуй случайный график'\n"
                "- Список датчиков: 'Список датчиков'"
            ),
            "processing": "Обрабатываю ваш запрос, это может занять до 30 секунд. Пожалуйста, подождитесь.",
            "classifying": "Анализирую тип запроса...",
            "formalizing": "Формализую запрос...",
            "executing": "Выполняю запрос...",
            "limit_exceeded": "Вы достигли лимита одновременных запросов ({max_tasks}). Дождитесь завершения.",
            "error": "Произошла ошибка: {reason}. Попробуйте снова или используйте /functions для примеров.",
            "clarify": "Уточните, пожалуйста: {questions}\nДоступные функции:\n{functions}",
            "invalid_input": "Неверный запрос. Попробуйте: 'Нарисуй график за 3-9 апреля для датчика T01'.\nДоступные функции:\n{functions}",
            "non_text_input": "Я могу обрабатывать только текстовые сообщения. Пожалуйста, отправьте текст.\nДоступные функции:\n{functions}",
            "sensor_not_found": "Датчик '{sensor_name}' не найден. Доступные датчики: {sensors}.\nДоступные функции:\n{functions}",
        },
    },
}

class BotLauncher:
    def __init__(self, config: Dict[str, Any], folder_path: str, output_dir: str = "output"):
        self.config = config
        self.data_manager = DataManager(folder_path=folder_path, output_dir=output_dir, debug_mode=False)
        self.bot = TelegramBot(config, self.data_manager)
        self.dashboard = Dashboard(self.bot)
        logger.info("BotLauncher initialized")

    async def _get_user_lang(self, update: Update) -> str:
        lang = update.effective_user.language_code or "en"
        return "ru" if lang.startswith("ru") else "en"

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        lang = await self._get_user_lang(update)
        await update.message.reply_text(self.config["messages"][lang]["start"])
        self.bot.history_manager.add_message(update.effective_user.id, self.config["messages"][lang]["start"], True, {})

    async def help(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        lang = await self._get_user_lang(update)
        await update.message.reply_text(self.config["messages"][lang]["help"])
        self.bot.history_manager.add_message(update.effective_user.id, self.config["messages"][lang]["help"], True, {})

    async def functions(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        lang = await self._get_user_lang(update)
        await update.message.reply_text(self.config["messages"][lang]["functions"])
        self.bot.history_manager.add_message(update.effective_user.id, self.config["messages"][lang]["functions"], True, {})

    async def error(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        logger.error("Global error: %s", str(context.error))
        if update and update.message:
            lang = await self._get_user_lang(update)
            error_msg = self.config["messages"][lang]["error"].format(reason="internal error")
            await update.message.reply_text(error_msg)
            self.bot.history_manager.add_message(update.effective_user.id, error_msg, True, {})

async def run_bot(launcher: BotLauncher):
    app = (
        Application.builder()
        .token(launcher.config["telegram_token"])
        .read_timeout(launcher.config["telegram_timeout"])
        .write_timeout(launcher.config["telegram_timeout"])
        .connect_timeout(launcher.config["telegram_timeout"])
        .build()
    )
    app.add_handler(CommandHandler("start", launcher.start))
    app.add_handler(CommandHandler("help", launcher.help))
    app.add_handler(CommandHandler("functions", launcher.functions))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, launcher.bot.handle_message))
    app.add_error_handler(launcher.error)
    await app.initialize()
    await app.start()
    try:
        await app.updater.start_polling(timeout=launcher.config["telegram_timeout"])
        await asyncio.Event().wait()  # Бесконечное ожидание
    finally:
        await app.stop()
        await app.shutdown()

async def run_dashboard(dashboard: Dashboard):
    hypercorn_config = HyperConfig()
    hypercorn_config.bind = [f"{dashboard.config['host']}:{dashboard.config['port']}"]
    hypercorn_config.use_reloader = False
    await serve(dashboard.app, hypercorn_config)

async def main():
    config = BOT_CONFIG
    folder_path = r"D:\Автоматизация\cMT-7232\datalog"
    output_dir = "output"

    launcher = BotLauncher(config=config, folder_path=folder_path, output_dir=output_dir)

    # Запускаем бот и дашборд в одном событийном цикле
    bot_task = asyncio.create_task(run_bot(launcher))
    dashboard_task = asyncio.create_task(run_dashboard(launcher.dashboard))

    try:
        await asyncio.gather(bot_task, dashboard_task)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        bot_task.cancel()
        dashboard_task.cancel()
        try:
            await asyncio.gather(bot_task, dashboard_task, return_exceptions=True)
        except asyncio.CancelledError:
            pass
        logger.info("Shutdown complete.")

if __name__ == "__main__":
    # Применяем nest_asyncio для Windows, чтобы избежать конфликтов событийных циклов
    nest_asyncio.apply()
    asyncio.run(main())

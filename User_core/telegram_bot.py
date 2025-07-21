import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, CallbackQueryHandler, ContextTypes
from telegram.constants import ParseMode
from telegram.helpers import escape_markdown
import json
from pathlib import Path
from typing import Dict, Any
import asyncio
import traceback
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters
)
from telegram.request import HTTPXRequest
from telegram.error import NetworkError, RetryAfter, TelegramError
import re

CONFIG = {
    "telegram": {"timeout": 10},
    "bot": {"default_lang": "ru", "max_message_length": 4096},
}

MESSAGES = {
    "ru": {
        "welcome": (
            "Привет! Я бот для работы с данными датчиков.\n"
            "Используй команду /help, чтобы узнать, что я могу."
        ),
        "help": (
            "Я могу выполнять следующие действия:\n"
            "- Построить график для датчика за указанный период\n"
            "- Показать список доступных датчиков\n"
            "- Показать информацию о датчике\n"
            "- Показать доступный период данных\n\n"
            "Просто напиши запрос, например: 'Нарисуй график для T01 с 2023-04-03 по 2023-04-09'."
        ),
        "error": "❌ Произошла ошибка: {reason}",
    }
}

def normalize_sensor_name(sensor: str, available_sensors: list) -> str:
    """Нормализует имя датчика, например, 'т6' -> 'T06'."""
    sensor = sensor.strip().lower()
    sensor = re.sub(r'[^a-z0-9]', '', sensor)
    for s in available_sensors:
        if sensor == s.lower() or sensor == s.lower().replace('t', ''):
            return s
    return sensor.upper() if sensor.startswith('t') else f"T{sensor.upper()}"

def escape_markdown_v2(text: str) -> str:
    """Экранирует специальные символы для MarkdownV2."""
    if not text:
        return text
    special_chars = r'_*[]()~`>#+=|{}.!-'
    for char in special_chars:
        text = text.replace(char, f'\\{char}')
    return text

class ResultProcessor:
    def __init__(self, lang: str, max_message_length: int, logger: logging.Logger):
        self.lang = lang
        self.max_message_length = max_message_length
        self.logger = logger

    async def process(self, update: Update, result: Dict[str, Any]):
        self.logger.debug("Обработка результата: %s", result)
        try:
            if "validation_results" in result and any(not r["is_valid"] for r in result["validation_results"]):
                errors = [
                    f"{escape_markdown(r['message'], version=2)} \\({escape_markdown(r['reason'], version=2)}\\)"
                    for r in result["validation_results"]
                ]
                await update.message.reply_text("\n".join(errors), parse_mode=ParseMode.MARKDOWN_V2)
                self.logger.debug("Отправлены ошибки валидации: %s", errors)
                return

            if "result" not in result:
                await update.message.reply_text(
                    MESSAGES[self.lang]["error"].format(reason=escape_markdown("Неизвестная ошибка", version=2)),
                    parse_mode=ParseMode.MARKDOWN_V2
                )
                self.logger.debug("Отправлена ошибка: неизвестный результат")
                return

            result_data = result["result"]
            if isinstance(result_data, dict) and "plot_path" in result_data:
                plot_path = Path(result_data["plot_path"])
                if plot_path.exists():
                    with open(plot_path, "rb") as f:
                        await update.message.reply_photo(photo=f)
                    self.logger.debug("Отправлен график: %s", plot_path)
                else:
                    await update.message.reply_text(
                        MESSAGES[self.lang]["error"].format(reason=escape_markdown("График не найден", version=2)),
                        parse_mode=ParseMode.MARKDOWN_V2
                    )
                    self.logger.debug("График не найден: %s", plot_path)
            elif isinstance(result_data, list):
                sensors = "\n".join(escape_markdown(str(s), version=2) for s in result_data)
                await update.message.reply_text(f"Доступные датчики:\n{sensors}", parse_mode=ParseMode.MARKDOWN_V2)
                self.logger.debug("Отправлен список датчиков: %s", sensors)
            elif isinstance(result_data, dict) and "sensor_name" in result_data:
                sensor_info = (
                    f"Датчик: {escape_markdown(result_data['sensor_name'], version=2)}\n"
                    f"Период: {escape_markdown(result_data['period'], version=2)}\n"
                    f"Индекс: {escape_markdown(str(result_data['index']), version=2)}\n"
                    f"Тип данных: {escape_markdown(result_data['data_type'], version=2)}"
                )
                await update.message.reply_text(sensor_info, parse_mode=ParseMode.MARKDOWN_V2)
                self.logger.debug("Отправлена информация о датчике: %s", sensor_info)
            elif isinstance(result_data, dict) and "start_time" in result_data:
                period = f"Период данных: с {escape_markdown(result_data['start_time'], version=2)} по {escape_markdown(result_data['end_time'], version=2)}"
                await update.message.reply_text(period, parse_mode=ParseMode.MARKDOWN_V2)
                self.logger.debug("Отправлен период данных: %s", period)
            elif isinstance(result_data, list) and all(isinstance(q, str) for q in result_data):
                keyboard = [[InlineKeyboardButton(q, callback_data=f"clarify:{escape_markdown(q, version=2)}")] for q in result_data[:5]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                response = "Пожалуйста, уточните:\n" + "\n".join(escape_markdown(q, version=2) for q in result_data)
                await update.message.reply_text(response, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN_V2)
                self.logger.debug("Отправлен запрос на уточнение: %s", response)
            else:
                response = (
                    escape_markdown(str(result_data)[:self.max_message_length - 3], version=2) + "..."
                    if len(str(result_data)) > self.max_message_length else escape_markdown(str(result_data), version=2)
                )
                await update.message.reply_text(response, parse_mode=ParseMode.MARKDOWN_V2)
                self.logger.debug("Отправлен общий ответ: %s", response)
        except Exception as e:
            self.logger.error("Ошибка обработки результата: %s", e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())

class TelegramBot:
    def __init__(
        self,
        token: str,
        data_reader,
        data_processor,
        history_manager,
        error_corrector,
        request_formalizer,
        action_executor,
        debug_mode: bool = False,
        logger: logging.Logger = None
    ):
        self.token = token
        self.debug_mode = debug_mode
        self.data_reader = data_reader
        self.data_processor = data_processor
        self.history_manager = history_manager
        self.error_corrector = error_corrector
        self.request_formalizer = request_formalizer
        self.action_executor = action_executor
        self.logger = logger or logging.getLogger(__name__)
        self.result_processor = ResultProcessor(
            CONFIG["bot"]["default_lang"],
            CONFIG["bot"]["max_message_length"],
            self.logger
        )
        if not token:
            self.logger.critical("Токен Telegram бота не указан")
            raise ValueError("Токен Telegram бота не указан")
        self.app = None
        self.logger.debug("TelegramBot инициализирован с токеном")

    def _build_app(self):
        request = HTTPXRequest(
            connect_timeout=60.0,
            read_timeout=120.0,
            write_timeout=120.0,
            pool_timeout=30.0,
            connection_pool_size=8,
            http_version="2",
        )
        return (
            ApplicationBuilder()
            .token(self.token)
            .request(request)
            .build()
        )

    def _register_handlers(self):
        self.logger.debug("Регистрация обработчиков сообщений")
        self.app.add_handler(CommandHandler("start", self.start))
        self.app.add_handler(CommandHandler("help", self.help))
        self.app.add_handler(CommandHandler("sensors", self.sensors))
        self.app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))
        self.app.add_handler(MessageHandler(filters.COMMAND, self.unknown_command))
        self.app.add_handler(CallbackQueryHandler(self.handle_callback))
        self.app.add_error_handler(self.error_handler)

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        lang = CONFIG["bot"]["default_lang"]
        self.logger.debug("Команда /start от пользователя %s", update.effective_user.id)
        await update.message.reply_text(escape_markdown(MESSAGES[lang]["welcome"], version=2), parse_mode=ParseMode.MARKDOWN_V2)
        self.logger.debug("Отправлено приветственное сообщение пользователю %s", update.effective_user.id)

    async def help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        lang = CONFIG["bot"]["default_lang"]
        self.logger.debug("Команда /help от пользователя %s", update.effective_user.id)
        await update.message.reply_text(escape_markdown(MESSAGES[lang]["help"], version=2), parse_mode=ParseMode.MARKDOWN_V2)
        self.logger.debug("Отправлено сообщение с помощью пользователю %s", update.effective_user.id)

    async def unknown_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        lang = CONFIG["bot"]["default_lang"]
        self.logger.debug("Неизвестная команда от пользователя %s: %s", update.effective_user.id, update.message.text)
        await update.message.reply_text(
            MESSAGES[lang]["error"].format(reason=escape_markdown("Неизвестная команда. Используйте /help для списка команд", version=2)),
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        self.logger.debug("Отправлено сообщение об ошибке неизвестной команды пользователю %s", update.effective_user.id)

    async def sensors(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        self.logger.debug("Команда /sensors от пользователя %s", update.effective_user.id)
        try:
            sensors = self.data_reader.get_sensor_info()
            sensor_list = "\n".join(escape_markdown(s["sensor_name"], version=2) for s in sensors)
            await update.message.reply_text(f"Доступные датчики:\n{sensor_list}", parse_mode=ParseMode.MARKDOWN_V2)
            self.logger.debug("Отправлен список датчиков пользователю %s: %s", update.effective_user.id, sensor_list)
        except Exception as e:
            self.logger.error("Ошибка при получении списка датчиков: %s", e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())


    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not update.message:
            self.logger.warning("Получено обновление без объекта сообщения: %s", update.to_dict())
            return

        user_id = update.effective_user.id
        message = update.message.text.strip()
        lang = CONFIG["bot"]["default_lang"]
        self.logger.debug("Получено сообщение от пользователя %s: %s", user_id, message)

        try:
            history = self.history_manager.get_history(user_id)[-50:]
            available_sensors = [s["sensor_name"] for s in self.data_reader.get_sensor_info()]
            time_period = self.data_reader.get_time_period()
            self.logger.debug("Формализация запроса для пользователя %s: %s", user_id, message)

            normalized_message = message
            sensor_match = re.search(r'т\d+', message, re.IGNORECASE)
            if sensor_match:
                sensor = sensor_match.group(0)
                normalized_sensor = normalize_sensor_name(sensor, available_sensors)
                normalized_message = message.replace(sensor, normalized_sensor)
                self.logger.debug("Нормализовано имя датчика: %s -> %s", sensor, normalized_sensor)

            if "май" in normalized_message.lower() and not re.search(r'\d{4}-\d{2}-\d{2}', normalized_message):
                normalized_message += " с 2025-05-01 по 2025-05-31"
                self.logger.debug("Добавлен период по умолчанию: %s", normalized_message)

            formalized = await self.request_formalizer.formalize(normalized_message, history, lang, available_sensors, time_period)
            self.history_manager.add_message(user_id, message, is_bot=False, user_info={})
            self.logger.debug("Сообщение пользователя %s добавлено в историю: %s", user_id, message)

            if not formalized or "action" not in formalized:
                error_message = MESSAGES[lang]["error"].format(
                    reason=escape_markdown("Некорректный ответ от обработчика запросов", version=2)
                )
                await update.message.reply_text(error_message, parse_mode=ParseMode.MARKDOWN_V2)
                self.logger.error("Некорректный формализованный ответ: %s", formalized)
                return

            if formalized["action"] == "free_response":
                response = formalized["response"]
                if len(response) > CONFIG["bot"]["max_message_length"]:
                    response = response[:CONFIG["bot"]["max_message_length"] - 3] + "..."
                await update.message.reply_text(escape_markdown(response, version=2), parse_mode=ParseMode.MARKDOWN_V2)
                self.history_manager.add_message(user_id, response, is_bot=True, user_info={})
                self.logger.debug("Отправлен свободный ответ пользователю %s: %s", user_id, response)
                return

            if formalized["action"] == "clarify":
                result = await self.action_executor.execute(formalized)
                if "validation_results" in result:
                    for vr in result["validation_results"]:
                        if vr.get("corrected_name"):
                            new_message = normalized_message.replace(
                                formalized["parameters"].get("sensor_name", ""), vr["corrected_name"]
                            )
                            self.logger.debug("Повторная формализация с датчиком %s: %s", vr["corrected_name"], new_message)
                            formalized = await self.request_formalizer.formalize(
                                new_message, history, lang, available_sensors, time_period
                            )
                            if formalized["action"] != "clarify":
                                result = await self.action_executor.execute(formalized)
                                await self.result_processor.process(update, result)
                                self.history_manager.add_message(
                                    user_id, json.dumps(result, ensure_ascii=False), is_bot=True, user_info={}
                                )
                                return

                questions = formalized["parameters"].get("questions", ["Уточните ваш запрос"])
                comments = formalized.get("comment", "").split("; ")
                response = "Пожалуйста, уточните:\n" + "\n".join(
                    f"- {escape_markdown(q, version=2)} \\({escape_markdown(c, version=2)}\\)" for q, c in zip(questions, comments) if c
                )
                try:
                    await update.message.reply_text(response, parse_mode=ParseMode.MARKDOWN_V2)
                except TelegramError as te:
                    self.logger.error("Ошибка Telegram при отправке ответа: %s", te)
                    await update.message.reply_text(response)
                self.history_manager.add_message(user_id, response, is_bot=True, user_info={})
                self.logger.debug("Отправлен запрос на уточнение пользователю %s: %s", user_id, response)
                return

            self.logger.debug("Выполнение действия для пользователя %s: %s", user_id, formalized["action"])
            result = await self.action_executor.execute(formalized)
    
            if not result or "result" not in result:
                error_message = MESSAGES[lang]["error"].format(
                    reason=escape_markdown("Ошибка при выполнении действия", version=2)
                )
                await update.message.reply_text(error_message, parse_mode=ParseMode.MARKDOWN_V2)
                self.logger.error("Некорректный результат действия: %s", result)
                return

            await self.result_processor.process(update, result)
            self.history_manager.add_message(user_id, json.dumps(result, ensure_ascii=False), is_bot=True, user_info={})
            self.logger.debug("Результат действия отправлен пользователю %s: %s", user_id, json.dumps(result))

        except json.JSONDecodeError as je:
            self.logger.error("Ошибка JSON при обработке сообщения от пользователя %s: %s", user_id, je)
            error_message = MESSAGES[lang]["error"].format(
                reason=escape_markdown("Ошибка обработки запроса, попробуйте уточнить данные", version=2)
            )
            await update.message.reply_text(error_message, parse_mode=ParseMode.MARKDOWN_V2)
        except Exception as e:
            self.logger.error("Ошибка обработки сообщения от пользователя %s: %s", user_id, e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())
            error_message = MESSAGES[lang]["error"].format(
                reason=escape_markdown("Произошла ошибка при обработке запроса", version=2)
            )
            try:
                await update.message.reply_text(error_message, parse_mode=ParseMode.MARKDOWN_V2)
            except TelegramError as te:
                self.logger.error("Ошибка Telegram при отправке ошибки: %s", te)
                await update.message.reply_text(error_message)


    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        user_id = query.from_user.id
        data = query.data
        lang = CONFIG["bot"]["default_lang"]
        self.logger.debug("Получен callback от пользователя %s: %s", user_id, data)

        try:
            if data.startswith("clarify:"):
                clarified_request = data.replace("clarify:", "")
                history = self.history_manager.get_history(user_id)[-50:]
                available_sensors = [s["sensor_name"] for s in self.data_reader.get_sensor_info()]
                time_period = self.data_reader.get_time_period()
                formalized = await self.request_formalizer.formalize(clarified_request, history, lang, available_sensors, time_period)
            
                self.history_manager.add_message(user_id, clarified_request, is_bot=False, user_info={})
                self.logger.debug("Callback запрос пользователя %s добавлен в историю: %s", user_id, clarified_request)

                result = await self.action_executor.execute(formalized)
                await self.result_processor.process(update, result)
                self.history_manager.add_message(user_id, json.dumps(result, ensure_ascii=False), is_bot=True, user_info={})
                self.logger.debug("Результат callback действия отправлен пользователю %s: %s", user_id, json.dumps(result))
        except Exception as e:
            self.logger.error("Ошибка обработки callback от пользователя %s: %s", user_id, e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())
            error_message = MESSAGES[lang]["error"].format(reason=escape_markdown("Ошибка обработки выбора, попробуйте снова", version=2))
            try:
                await query.message.reply_text(error_message, parse_mode=ParseMode.MARKDOWN_V2)
            except TelegramError as te:
                self.logger.error("Ошибка Telegram при отправке ошибки: %s", te)
                await query.message.reply_text(error_message)

    async def error_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        error_msg = str(context.error)
        trace = traceback.format_exc()
        self.logger.critical("Ошибка Telegram бота: %s\nТрассировка стека: %s", error_msg, trace)
        if update and update.effective_user:
            error_message = MESSAGES[CONFIG["bot"]["default_lang"]]["error"].format(
                reason=escape_markdown("Произошла ошибка, попробуйте снова", version=2)
            )
            try:
                await update.effective_message.reply_text(error_message, parse_mode=ParseMode.MARKDOWN_V2)
            except TelegramError as te:
                self.logger.error("Ошибка Telegram при отправке ошибки: %s", te)
                await update.effective_message.reply_text(error_message)
            self.logger.debug("Отправлено сообщение об ошибке пользователю %s", update.effective_user.id)

    async def run(self):
        self.app = self._build_app()
        self._register_handlers()
        while True:
            try:
                self.logger.debug("Инициализация приложения Telegram")
                await self.app.initialize()
                self.logger.debug("Запуск polling")
                await self.app.start()
                await self.app.updater.start_polling()
                self.logger.info("Бот успешно запущен")
                while True:
                    await asyncio.sleep(3600)
            except (NetworkError, RetryAfter, TelegramError) as e:
                self.logger.error("Сетевая ошибка %s: %s", type(e).__name__, traceback.format_exc())
                await asyncio.sleep(10)
                self.logger.info("Пытаюсь перезапустить бота...")
                try:
                    await self.app.updater.shutdown()
                    await self.app.shutdown()
                except:
                    pass
                continue
            except Exception as e:
                self.logger.critical("Критическая ошибка: %s\n%s", e, traceback.format_exc())
                break
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
            "Используй команду /help, чтобы узнать, что я могу.\n"
            "Также можешь отправить голосовое сообщение, я его распознаю!"
        ),
        "help": (
            "Я могу выполнять следующие действия:\n"
            "- Построить график для датчика за указанный период\n"
            "- Показать список доступных датчиков\n"
            "- Показать информацию о датчике\n"
            "- Показать доступный период данных\n\n"
            "Просто напиши запрос, например: 'Нарисуй график для T01 с 2023-04-03 по 2023-04-09'.\n"
            "Или отправь голосовое сообщение с запросом."
        ),
        "error": "❌ Произошла ошибка: {reason}",
        "voice_processing": "Обработка голосового сообщения...",
        "voice_error": "Не удалось распознать голосовое сообщение: {reason}",
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
                    f"{escape_markdown_v2(r['message'])} \\({escape_markdown_v2(r['reason'])}\\)"
                    for r in result["validation_results"]
                ]
                await update.message.reply_text("\n".join(errors), parse_mode=ParseMode.MARKDOWN_V2)
                self.logger.debug("Отправлены ошибки валидации: %s", errors)
                return

            if "result" not in result:
                await update.message.reply_text(
                    MESSAGES[self.lang]["error"].format(reason=escape_markdown_v2("Неизвестная ошибка")),
                    parse_mode=ParseMode.MARKDOWN_V2
                )
                self.logger.debug("Отправлена ошибка: неизвестный результат")
                return

            result_data = result["result"]

            # === 1. График (один файл) ===
            if isinstance(result_data, dict) and "plot_path" in result_data:
                plot_path = Path(result_data["plot_path"])
                if plot_path.exists():
                    with open(plot_path, "rb") as f:
                        await update.message.reply_photo(photo=f)
                    self.logger.debug("Отправлен график: %s", plot_path)
                else:
                    await update.message.reply_text(
                        MESSAGES[self.lang]["error"].format(reason=escape_markdown_v2("График не найден")),
                        parse_mode=ParseMode.MARKDOWN_V2
                    )
                    self.logger.debug("График не найден: %s", plot_path)

            # === 2. Список датчиков ===
            elif isinstance(result_data, list):
                sensors = "\n".join(escape_markdown_v2(str(s)) for s in result_data)
                await update.message.reply_text(f"Доступные датчики:\n{sensors}", parse_mode=ParseMode.MARKDOWN_V2)
                self.logger.debug("Отправлен список датчиков: %s", sensors)

            # === 3. Информация о датчике ===
            elif isinstance(result_data, dict) and "sensor_name" in result_data:
                sensor_info = (
                    f"Датчик: {escape_markdown_v2(result_data['sensor_name'])}\n"
                    f"Период: {escape_markdown_v2(result_data['period'])}\n"
                    f"Индекс: {escape_markdown_v2(str(result_data['index']))}\n"
                    f"Тип данных: {escape_markdown_v2(result_data['data_type'])}"
                )
                await update.message.reply_text(sensor_info, parse_mode=ParseMode.MARKDOWN_V2)
                self.logger.debug("Отправлена информация о датчике: %s", sensor_info)

            # === 4. Период данных ===
            elif isinstance(result_data, dict) and "start_time" in result_data:
                period = f"Период данных: с {escape_markdown_v2(result_data['start_time'])} по {escape_markdown_v2(result_data['end_time'])}"
                await update.message.reply_text(period, parse_mode=ParseMode.MARKDOWN_V2)
                self.logger.debug("Отправлен период данных: %s", period)

            # === 5. Уточнение (кнопки) ===
            elif isinstance(result_data, list) and all(isinstance(q, str) for q in result_data):
                keyboard = [[InlineKeyboardButton(q, callback_data=f"clarify:{escape_markdown_v2(q)}")] for q in result_data[:5]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                response = "Пожалуйста, уточните:\n" + "\n".join(escape_markdown_v2(q) for q in result_data)
                await update.message.reply_text(response, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN_V2)
                self.logger.debug("Отправлен запрос на уточнение: %s", response)

            # === 6. ОТЧЁТ: 7 файлов как документы (PDF, DOCX, 5 PNG) ===
            elif isinstance(result_data, dict) and "files" in result_data:
                files = result_data["files"]
                message = result_data.get("message", "Отчёт готов")

                # Обрезаем длинное сообщение
                if len(message) > self.max_message_length:
                    message = message[:self.max_message_length - 3] + "..."

                # 1. Отправляем текстовое сообщение
                await update.message.reply_text(
                    escape_markdown_v2(message),
                    parse_mode=ParseMode.MARKDOWN_V2
                )

                # 2. Отправляем каждый файл как документ
                for file_info in files:
                    path_str = file_info["path"]
                    file_type = file_info["type"]
                    path = Path(path_str)

                    if not path.exists():
                        self.logger.warning("Файл не найден: %s", path)
                        continue

                    try:
                        with open(path, "rb") as f:
                            filename = f"{file_type} — {path.name}"
                            await update.message.reply_document(
                                document=f,
                                filename=filename,
                                caption=escape_markdown_v2(file_type),
                                parse_mode=ParseMode.MARKDOWN_V2
                            )
                        self.logger.debug("Отправлен файл: %s", filename)
                    except Exception as e:
                        self.logger.error("Ошибка отправки файла %s: %s", path, e)
                        await update.message.reply_text(
                            MESSAGES[self.lang]["error"].format(
                                reason=escape_markdown_v2(f"Не удалось отправить файл: {file_type}")
                            ),
                            parse_mode=ParseMode.MARKDOWN_V2
                        )

                self.logger.info("Отчёт отправлен: %d файлов", len(files))

            # === 7. Любой другой результат (текст) ===
            else:
                response = (
                    escape_markdown_v2(str(result_data)[:self.max_message_length - 3]) + "..."
                    if len(str(result_data)) > self.max_message_length else escape_markdown_v2(str(result_data))
                )
                await update.message.reply_text(response, parse_mode=ParseMode.MARKDOWN_V2)
                self.logger.debug("Отправлен общий ответ: %s", response)

        except Exception as e:
            self.logger.error("Ошибка обработки результата: %s", e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())
            error_msg = MESSAGES[self.lang]["error"].format(reason=escape_markdown_v2("Ошибка обработки результата"))
            try:
                await update.message.reply_text(error_msg, parse_mode=ParseMode.MARKDOWN_V2)
            except:
                await update.message.reply_text(error_msg)





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
        speech_recognizer,
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
        self.speech_recognizer = speech_recognizer
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
        self.app.add_handler(MessageHandler(filters.VOICE, self.handle_voice_message))
        self.app.add_handler(MessageHandler(filters.COMMAND, self.unknown_command))
        self.app.add_handler(CallbackQueryHandler(self.handle_callback))
        self.app.add_error_handler(self.error_handler)

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        lang = CONFIG["bot"]["default_lang"]
        self.logger.debug("Команда /start от пользователя %s", update.effective_user.id)
        await update.message.reply_text(escape_markdown_v2(MESSAGES[lang]["welcome"]), parse_mode=ParseMode.MARKDOWN_V2)
        self.logger.debug("Отправлено приветственное сообщение пользователю %s", update.effective_user.id)

    async def help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        lang = CONFIG["bot"]["default_lang"]
        self.logger.debug("Команда /help от пользователя %s", update.effective_user.id)
        await update.message.reply_text(escape_markdown_v2(MESSAGES[lang]["help"]), parse_mode=ParseMode.MARKDOWN_V2)
        self.logger.debug("Отправлено сообщение с помощью пользователю %s", update.effective_user.id)

    async def unknown_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        lang = CONFIG["bot"]["default_lang"]
        self.logger.debug("Неизвестная команда от пользователя %s: %s", update.effective_user.id, update.message.text)
        await update.message.reply_text(
            MESSAGES[lang]["error"].format(reason=escape_markdown_v2("Неизвестная команда. Используйте /help для списка команд")),
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        self.logger.debug("Отправлено сообщение об ошибке неизвестной команды пользователю %s", update.effective_user.id)

    async def sensors(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        self.logger.debug("Команда /sensors от пользователя %s", update.effective_user.id)
        try:
            sensors = self.data_reader.get_sensor_info()
            sensor_list = "\n".join(escape_markdown_v2(s["sensor_name"]) for s in sensors.values())
            await update.message.reply_text(f"Доступные датчики:\n{sensor_list}", parse_mode=ParseMode.MARKDOWN_V2)
            self.logger.debug("Отправлен список датчиков пользователю %s: %s", update.effective_user.id, sensor_list)
        except Exception as e:
            self.logger.error("Ошибка при получении списка датчиков: %s", e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())

    async def handle_voice_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not update.message or not update.message.voice:
            self.logger.warning("Получено обновление без голосового сообщения: %s", update.to_dict())
            return

        user_id = update.effective_user.id
        lang = CONFIG["bot"]["default_lang"]
        self.logger.debug("Получено голосовое сообщение от пользователя %s", user_id)

        try:
            processing_message = await update.message.reply_text(
                escape_markdown_v2(MESSAGES[lang]["voice_processing"]),
                parse_mode=ParseMode.MARKDOWN_V2
            )

            voice = await update.message.voice.get_file()
            ogg_data = await voice.download_as_bytearray()

            wav_data = await self.speech_recognizer.convert_ogg_to_wav(ogg_data)
            transcribed_text = await self.speech_recognizer.recognize_speech(wav_data)
        
            if not transcribed_text:
                await processing_message.edit_text(
                    MESSAGES[lang]["voice_error"].format(reason=escape_markdown_v2("Не удалось распознать текст")),
                    parse_mode=ParseMode.MARKDOWN_V2
                )
                self.logger.debug("Не удалось распознать голосовое сообщение от пользователя %s", user_id)
                return

            self.logger.debug("Распознанный текст от пользователя %s: %s", user_id, transcribed_text)

            # Вызов handle_message с дополнительным параметром recognized_text
            await self.handle_message(update, context, recognized_text=transcribed_text)

            await processing_message.delete()

        except Exception as e:
            self.logger.error("Ошибка обработки голосового сообщения от пользователя %s: %s", user_id, e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())
            error_message = MESSAGES[lang]["voice_error"].format(reason=escape_markdown_v2(str(e)))
            try:
                await processing_message.edit_text(error_message, parse_mode=ParseMode.MARKDOWN_V2)
            except TelegramError as te:
                self.logger.error("Ошибка Telegram при отправке ошибки: %s", te)
                await update.message.reply_text(error_message, parse_mode=ParseMode.MARKDOWN_V2)


    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE, recognized_text: str = None):
        if not update.message:
            self.logger.warning("Получено обновление без объекта сообщения: %s", update.to_dict())
            return

        user_id = update.effective_user.id
        message = recognized_text if recognized_text is not None else (update.message.text or "").strip()
        lang = CONFIG["bot"]["default_lang"]
        self.logger.debug("Получено сообщение от пользователя %s: %s", user_id, message)

        try:
            history = self.history_manager.get_history(user_id)[-50:]
            available_sensors = [s["sensor_name"] for s in self.data_reader.get_sensor_info().values()]
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
                    reason=escape_markdown_v2("Некорректный ответ от обработчика запросов")
                )
                await update.message.reply_text(error_message, parse_mode=ParseMode.MARKDOWN_V2)
                self.logger.error("Некорректный формализованный ответ: %s", formalized)
                return

            if formalized["action"] == "free_response":
                response = formalized["response"]
                if len(response) > CONFIG["bot"]["max_message_length"]:
                    response = response[:CONFIG["bot"]["max_message_length"] - 3] + "..."
                await update.message.reply_text(escape_markdown_v2(response), parse_mode=ParseMode.MARKDOWN_V2)
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
                    f"- {escape_markdown_v2(q)} \\({escape_markdown_v2(c)}\\)" for q, c in zip(questions, comments) if c
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
                    reason=escape_markdown_v2("Ошибка при выполнении действия")
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
                reason=escape_markdown_v2("Ошибка обработки запроса, попробуйте уточнить данные")
            )
            await update.message.reply_text(error_message, parse_mode=ParseMode.MARKDOWN_V2)
        except Exception as e:
            self.logger.error("Ошибка обработки сообщения от пользователя %s: %s", user_id, e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())
            error_message = MESSAGES[lang]["error"].format(
                reason=escape_markdown_v2("Произошла ошибка при обработке запроса")
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
                available_sensors = [s["sensor_name"] for s in self.data_reader.get_sensor_info().values()]
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
            error_message = MESSAGES[lang]["error"].format(reason=escape_markdown_v2("Ошибка обработки выбора, попробуйте снова"))
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
                reason=escape_markdown_v2("Произошла ошибка, попробуйте снова")
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

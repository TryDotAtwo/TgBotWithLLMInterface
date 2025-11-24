# -*- coding: utf-8 -*-
import sys
sys.stdout.reconfigure(encoding='utf-8')

import io
import os
import colorama
colorama.init()
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
os.system("chcp 65001 > nul")
os.environ["PYTHONIOENCODING"] = "utf-8"

import subprocess
import logging
import asyncio
from pathlib import Path
import argparse
import traceback

from Analysis_core.data_reader import DataReader
from Analysis_core.data_processor import DataProcessor
from Analysis_core.report_generator import generate_report, build_report_data
from Bot_core.action_executor import ActionExecutor
from Bot_core.llm_core import RequestFormalizer, create_request_formalizer
from User_core.history_manager import HistoryManager
from User_core.telegram_bot import TelegramBot
from User_core.speech_recognizer import SpeechRecognizer

try:
    from Utils.error_corrector import ErrorCorrector
except ImportError as e:
    raise ImportError("Failed to import ErrorCorrector from Utils.error_corrector. Ensure the module exists and is correctly defined.") from e

from dotenv import load_dotenv
load_dotenv()

TEST_FILES = [
    # "Analysis_core/test_data_reader.py",
    "Analysis_core/test_data_processor.py",
    # "Utils/test_error_corrector.py",
    # "Bot_core/test_action_executor.py", 
    # "Bot_core/test_llm_core.py",
]

def setup_logging(debug_mode: bool) -> logging.Logger:
    if sys.platform == "win32":
        os.system("chcp 65001 > nul")
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace', line_buffering=True)
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace', line_buffering=True)

    level = logging.DEBUG if debug_mode else logging.CRITICAL
    logger = logging.getLogger()
    logger.setLevel(level)
    formatter = logging.Formatter("- %(levelname)s - %(message)s")

    stream_handler = logging.StreamHandler(stream=sys.stderr)
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(level)

    file_handler = logging.FileHandler("Bot.log", encoding='utf-8')
    file_handler.setFormatter(formatter)
    file_handler.setLevel(level)

    logger.handlers.clear()
    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)

    quiet_modules = [
        "h2",
        "httpcore",
        "httpx",
        "telegram",
        "apscheduler",
        "requests",
        "urllib3",
        "HTTPXRequest",
        "hpack",
    ]

    for name in quiet_modules:
        logging.getLogger(name).setLevel(logging.WARNING)
        for logger_name in logging.root.manager.loggerDict:
            if logger_name == name or logger_name.startswith(name + "."):
                logging.getLogger(logger_name).setLevel(logging.WARNING)

    logger.debug("Логирование настроено с уровнем %s", "DEBUG" if debug_mode else "CRITICAL")
    return logger

def setup_qt_paths(logger: logging.Logger):
    try:
        import PyQt5
        qt_plugins_path = Path(PyQt5.__file__).parent / "Qt" / "plugins" / "platforms"
        os.environ["QT_QPA_PLATFORM_PLUGIN_PATH"] = str(qt_plugins_path)
        logger.debug("Установлен путь к QT платформе: %s", qt_plugins_path)
    except Exception as e:
        logger.error("Ошибка настройки путей PyQt5: %s", e)
        logger.error("Трассировка стека: %s", traceback.format_exc())

def run_tests(debug_mode: bool, logger: logging.Logger):
    logger.debug("Запуск тестов с файлами: %s", TEST_FILES)
    cmd = [sys.executable, "-m", "pytest", "-v", "--disable-warnings", "--tb=long"] + TEST_FILES
    result = subprocess.run(cmd)
    if result.returncode != 0:
        logger.critical("Тесты завершились с ошибками, код возврата: %s", result.returncode)
        logger.critical("Трассировка стека: %s", traceback.format_exc())
        sys.exit(result.returncode)
    logger.debug("Тесты успешно выполнены")

async def run_bot(debug_mode: bool, data_path: str, logger: logging.Logger):
    try:
        logger.debug("Инициализация ErrorCorrector")
        error_corrector = ErrorCorrector(debug_mode=debug_mode, logger=logger)
        
        logger.debug("Инициализация HistoryManager")
        history_manager = HistoryManager("history.db", timeout_hours=24, max_history_size=50, logger=logger)

        logger.debug("Инициализация DataReader с путем: %s", data_path)
        data_reader = DataReader(data_path, history_manager, debug_mode, logger=logger)

        # Получение available_sensors и time_period
        logger.debug("Получение информации о датчиках")
        sensor_info = data_reader.get_sensor_info()
        available_sensors = list(sensor_info.keys())
        if not available_sensors:
            logger.error("Список доступных датчиков пуст")
            raise ValueError("Список доступных датчиков пуст")

        logger.debug("Получение временного периода")
        time_period = data_reader.get_time_period()
        if not all(key in time_period for key in ["start_time", "end_time"]):
            logger.error("Некорректный time_period: %s", time_period)
            raise ValueError("Некорректный time_period")

        logger.debug("Инициализация DataProcessor")
        data_processor = DataProcessor(data_reader, "Database", debug_mode, "Database", logger=logger, report_generator=generate_report, build_report_data=build_report_data)

        logger.debug("Инициализация ActionExecutor")
        action_executor = ActionExecutor(data_processor, error_corrector, logger=logger, debug_mode=debug_mode)

        logger.debug("Создание RequestFormalizer")
        request_formalizer = create_request_formalizer(
            data_reader=data_reader,
            error_corrector=error_corrector,
            available_sensors=available_sensors,
            time_period=time_period,
            debug_mode=debug_mode,
            logger=logger
        )

        logger.debug("Инициализация SpeechRecognizer")
        speech_recognizer = SpeechRecognizer(logger=logger)

        token = os.getenv("TELEGRAM_TOKEN_Prod")
        logger.debug("Получен токен Telegram: %s", "установлен" if token else "не установлен")

        if not token:
            logger.critical("Токен Telegram бота не указан в .env файле")
            sys.exit(1)

        logger.debug("Инициализация TelegramBot")
        bot = TelegramBot(
            token=token,
            data_reader=data_reader,
            data_processor=data_processor,
            history_manager=history_manager,
            error_corrector=error_corrector,
            request_formalizer=request_formalizer,
            action_executor=action_executor,
            speech_recognizer=speech_recognizer,
            debug_mode=debug_mode,
            logger=logger
        )

        logger.debug("Запуск бота")
        await bot.run()
    except Exception as e:
        logger.error("Ошибка при запуске бота: %s", e)
        logger.error("Трассировка стека: %s", traceback.format_exc())
        raise

def main():
    parser = argparse.ArgumentParser(description="Запуск тестов и Telegram-бота")
    parser.add_argument("--debug", action="store_true", help="Включить режим отладки")
    parser.add_argument("--data-path", default=r"D:\Автоматизация\логи до 22.10.2025\логи до 22.10.2025\datalog", help="Путь к папке с данными")
    args = parser.parse_args()
    args.debug = True  # Принудительно включаем режим отладки

    logger = setup_logging(args.debug)
    setup_qt_paths(logger)
    logger.debug("Запуск тестов")
    # run_tests(args.debug, logger)
    logger.debug("Запуск бота с параметрами: debug=%s, data_path=%s", args.debug, args.data_path)
    asyncio.run(run_bot(args.debug, args.data_path, logger))

if __name__ == "__main__":
    main()

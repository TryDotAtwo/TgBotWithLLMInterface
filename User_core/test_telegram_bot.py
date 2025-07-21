import os
import logging
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from pathlib import Path
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ApplicationBuilder
from telegram.constants import ParseMode

from User_core.telegram_bot import TelegramBot, CONFIG
from Analysis_core.data_reader import DataReader
from Analysis_core.data_processor import DataProcessor
from Bot_core.llm_core import RequestFormalizer, MESSAGES
from Bot_core.action_executor import ActionExecutor
from User_core.history_manager import HistoryManager
from Utils.error_corrector import ErrorCorrector

logger = logging.getLogger(__name__)

# Настройка логирования для тестов
def setup_logging(debug_mode: bool):
    level = logging.DEBUG if debug_mode else logging.CRITICAL
    logging.basicConfig(
        level=level,
        format="- %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler("User_core/test_telegram_bot.log")]
    )

@pytest.fixture
def mock_env(monkeypatch):
    """Установка фиктивного токена окружения."""
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test_token")
    logger.debug("Установлен фиктивный токен окружения")
    return "test_token"

@pytest.fixture
async def telegram_bot(mock_env):
    """Фикстура для создания TelegramBot с моками."""
    setup_logging(debug_mode=True)
    logger.debug("Начало настройки фикстуры telegram_bot")
    
    with patch("User_core.telegram_bot.DataReader", autospec=True) as MockDataReader, \
         patch("User_core.telegram_bot.DataProcessor", autospec=True) as MockDataProcessor, \
         patch("User_core.telegram_bot.HistoryManager", autospec=True) as MockHistoryManager, \
         patch("User_core.telegram_bot.ErrorCorrector", autospec=True) as MockErrorCorrector, \
         patch("User_core.telegram_bot.RequestFormalizer", autospec=True) as MockRequestFormalizer, \
         patch("User_core.telegram_bot.ActionExecutor", autospec=True) as MockActionExecutor, \
         patch("telegram.ext.ApplicationBuilder", autospec=True) as MockApplicationBuilder:
        
        try:
            # Настройка мока для ApplicationBuilder
            mock_builder = MagicMock()
            mock_token = MagicMock()
            mock_build = MagicMock()
            mock_app = MagicMock()
            
            MockApplicationBuilder.return_value = mock_builder
            mock_builder.token.return_value = mock_token
            mock_token.build.return_value = mock_app
            
            # Настройка асинхронных методов приложения
            mock_app.initialize = AsyncMock()
            mock_app.start = AsyncMock()
            mock_app.stop = AsyncMock()
            mock_app.shutdown = AsyncMock()
            mock_app.updater = MagicMock()
            mock_app.updater.start_polling = AsyncMock()
            mock_app.add_handler = MagicMock()
            mock_app.add_error_handler = MagicMock()
            
            # Создание бота
            bot = TelegramBot(debug_mode=True)
            
            # Привязка моков к атрибутам бота
            bot.data_reader = MockDataReader.return_value
            bot.data_processor = MockDataProcessor.return_value
            bot.history_manager = MockHistoryManager.return_value
            bot.error_corrector = MockErrorCorrector.return_value
            bot.request_formalizer = MockRequestFormalizer.return_value
            bot.action_executor = MockActionExecutor.return_value
            
            logger.debug("Фикстура telegram_bot успешно создана")
            yield bot
            
        except Exception as e:
            logger.critical("Ошибка при создании фикстуры telegram_bot: %s", str(e))
            raise
    
    logger.debug("Очистка фикстуры telegram_bot")

@pytest.mark.asyncio
async def test_init_no_token(monkeypatch):
    """Тест инициализации без токена."""
    setup_logging(debug_mode=True)
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    with pytest.raises(ValueError, match="Токен Telegram бота не указан"):
        TelegramBot(debug_mode=True)
    logger.debug("Тест test_init_no_token пройден")

@pytest.mark.asyncio
async def test_init_success(telegram_bot, mock_env):
    """Тест успешной инициализации."""
    assert telegram_bot.token == "test_token"
    assert isinstance(telegram_bot.data_reader, MagicMock)
    assert isinstance(telegram_bot.data_processor, MagicMock)
    assert isinstance(telegram_bot.history_manager, MagicMock)
    assert isinstance(telegram_bot.error_corrector, MagicMock)
    assert isinstance(telegram_bot.request_formalizer, MagicMock)
    assert isinstance(telegram_bot.action_executor, MagicMock)
    assert telegram_bot.debug_mode is True
    telegram_bot.app.add_handler.assert_called()
    logger.debug("Тест test_init_success пройден")

@pytest.mark.asyncio
async def test_start_command(telegram_bot):
    """Тест команды /start."""
    update = MagicMock(spec=Update)
    update.effective_user.id = 12345
    update.message.reply_text = AsyncMock()
    context = MagicMock(spec=ContextTypes.DEFAULT_TYPE)
    
    await telegram_bot.start(update, context)
    
    lang = CONFIG["bot"]["default_lang"]
    expected_message = (
        "Привет! Я бот для работы с данными датчиков.\n"
        "Используй команду /help, чтобы узнать, что я могу.\n"
        f"Доступные функции:\n{MESSAGES[lang]['functions']}"
    )
    update.message.reply_text.assert_awaited_once_with(
        expected_message, parse_mode=ParseMode.MARKDOWN
    )
    logger.debug("Тест test_start_command пройден")

@pytest.mark.asyncio
async def test_help_command(telegram_bot):
    """Тест команды /help."""
    update = MagicMock(spec=Update)
    update.effective_user.id = 12345
    update.message.reply_text = AsyncMock()
    context = MagicMock(spec=ContextTypes.DEFAULT_TYPE)
    
    await telegram_bot.help(update, context)
    
    lang = CONFIG["bot"]["default_lang"]
    expected_message = (
        "Я могу выполнять следующие действия:\n"
        f"{MESSAGES[lang]['functions']}\n"
        "Просто напиши запрос, например: 'Нарисуй график для T01 с 2023-04-03 по 2023-04-09'."
    )
    update.message.reply_text.assert_awaited_once_with(
        expected_message, parse_mode=ParseMode.MARKDOWN
    )
    logger.debug("Тест test_help_command пройден")

@pytest.mark.asyncio
async def test_unknown_command(telegram_bot):
    """Тест неизвестной команды."""
    update = MagicMock(spec=Update)
    update.effective_user.id = 12345
    update.message.text = "/invalid"
    update.message.reply_text = AsyncMock()
    context = MagicMock(spec=ContextTypes.DEFAULT_TYPE)
    
    await telegram_bot.unknown_command(update, context)
    
    lang = CONFIG["bot"]["default_lang"]
    expected_message = MESSAGES[lang]["error"].format(
        reason="Неизвестная команда. Используйте /help для списка команд"
    )
    update.message.reply_text.assert_awaited_once_with(
        expected_message, parse_mode=ParseMode.MARKDOWN
    )
    logger.debug("Тест test_unknown_command пройден")

@pytest.mark.asyncio
async def test_handle_message_free_response(telegram_bot):
    """Тест обработки сообщения с free_response."""
    update = MagicMock(spec=Update)
    update.effective_user.id = 12345
    update.message.text = "Привет, что ты умеешь?"
    update.message.reply_text = AsyncMock()
    context = MagicMock(spec=ContextTypes.DEFAULT_TYPE)
    
    telegram_bot.history_manager.load_history.return_value = []
    telegram_bot.request_formalizer.formalize = AsyncMock(
        return_value={"action": "free_response", "response": "Я умею строить графики и давать информацию о датчиках!"}
    )
    telegram_bot.history_manager.save_message = MagicMock()
    
    await telegram_bot.handle_message(update, context)
    
    update.message.reply_text.assert_awaited_once_with(
        "Я умею строить графики и давать информацию о датчиках!", parse_mode=ParseMode.MARKDOWN
    )
    telegram_bot.history_manager.save_message.assert_any_call("12345", update.message.text, is_bot=False)
    telegram_bot.history_manager.save_message.assert_any_call("12345", "Я умею строить графики и давать информацию о датчиках!", is_bot=True)
    logger.debug("Тест test_handle_message_free_response пройден")

@pytest.mark.asyncio
async def test_handle_message_clarify(telegram_bot):
    """Тест обработки сообщения с действием clarify."""
    update = MagicMock(spec=Update)
    update.effective_user.id = 12345
    update.message.text = "Нарисуй график"
    update.message.reply_text = AsyncMock()
    context = MagicMock(spec=ContextTypes.DEFAULT_TYPE)
    
    telegram_bot.history_manager.load_history.return_value = []
    telegram_bot.request_formalizer.formalize = AsyncMock(
        return_value={
            "action": "clarify",
            "parameters": {"questions": ["Укажите датчик", "Укажите период"]}
        }
    )
    telegram_bot.history_manager.save_message = MagicMock()
    
    await telegram_bot.handle_message(update, context)
    
    expected_message = "Пожалуйста, уточните:\nУкажите датчик\nУкажите период"
    expected_markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("Укажите датчик", callback_data="clarify:Укажите датчик")],
        [InlineKeyboardButton("Укажите период", callback_data="clarify:Укажите период")]
    ])
    update.message.reply_text.assert_awaited_once_with(
        expected_message, reply_markup=expected_markup, parse_mode=ParseMode.MARKDOWN
    )
    telegram_bot.history_manager.save_message.assert_any_call("12345", update.message.text, is_bot=False)
    telegram_bot.history_manager.save_message.assert_any_call("12345", expected_message, is_bot=True)
    logger.debug("Тест test_handle_message_clarify пройден")

@pytest.mark.asyncio
async def test_handle_message_plot(telegram_bot, tmp_path):
    """Тест обработки сообщения с построением графика."""
    update = MagicMock(spec=Update)
    update.effective_user.id = 12345
    update.message.text = "Нарисуй график для T01 с 2023-04-03 по 2023-04-09"
    update.message.reply_photo = AsyncMock()
    context = MagicMock(spec=ContextTypes.DEFAULT_TYPE)
    
    plot_file = tmp_path / "sensor_plot.png"
    plot_file.write_bytes(b"fake_image_data")
    
    telegram_bot.history_manager.load_history.return_value = []
    telegram_bot.request_formalizer.formalize = AsyncMock(
        return_value={
            "action": "plot_selected_sensor",
            "parameters": {
                "sensor_name": "T01",
                "start_time": "2023-04-03 00:00:00",
                "end_time": "2023-04-09 23:59:59"
            }
        }
    )
    telegram_bot.action_executor.execute = AsyncMock(
        return_value={"result": {"plot_path": str(plot_file)}}
    )
    telegram_bot.history_manager.save_message = MagicMock()
    
    await telegram_bot.handle_message(update, context)
    
    update.message.reply_photo.assert_awaited_once()
    telegram_bot.history_manager.save_message.assert_any_call("12345", update.message.text, is_bot=False)
    telegram_bot.history_manager.save_message.assert_any_call("12345", f'{{"result": {{"plot_path": "{plot_file}"}}}}', is_bot=True)
    logger.debug("Тест test_handle_message_plot пройден")

@pytest.mark.asyncio
async def test_handle_message_sensor_info(telegram_bot):
    """Тест обработки сообщения с информацией о датчике."""
    update = MagicMock(spec=Update)
    update.effective_user.id = 12345
    update.message.text = "Информация о датчике T01"
    update.message.reply_text = AsyncMock()
    context = MagicMock(spec=ContextTypes.DEFAULT_TYPE)
    
    telegram_bot.history_manager.load_history.return_value = []
    telegram_bot.request_formalizer.formalize = AsyncMock(
        return_value={
            "action": "print_sensor_info",
            "parameters": {"sensor_name": "T01"}
        }
    )
    telegram_bot.action_executor.execute = AsyncMock(
        return_value={
            "result": {
                "sensor_name": "T01",
                "period": "2023-04-01 to 2023-04-10",
                "index": "temperature",
                "data_type": "float"
            }
        }
    )
    telegram_bot.history_manager.save_message = MagicMock()
    
    await telegram_bot.handle_message(update, context)
    
    expected_message = (
        "Датчик: T01\n"
        "Период: 2023-04-01 to 2023-04-10\n"
        "Индекс: temperature\n"
        "Тип данных: float"
    )
    update.message.reply_text.assert_awaited_once_with(
        expected_message, parse_mode=ParseMode.MARKDOWN
    )
    telegram_bot.history_manager.save_message.assert_any_call("12345", update.message.text, is_bot=False)
    logger.debug("Тест test_handle_message_sensor_info пройден")

@pytest.mark.asyncio
async def test_handle_message_validation_error(telegram_bot):
    """Тест обработки сообщения с ошибкой валидации."""
    update = MagicMock(spec=Update)
    update.effective_user.id = 12345
    update.message.text = "Нарисуй график для T99"
    update.message.reply_text = AsyncMock()
    context = MagicMock(spec=ContextTypes.DEFAULT_TYPE)
    
    telegram_bot.history_manager.load_history.return_value = []
    telegram_bot.request_formalizer.formalize = AsyncMock(
        return_value={
            "action": "plot_selected_sensor",
            "parameters": {"sensor_name": "T99"}
        }
    )
    telegram_bot.action_executor.execute = AsyncMock(
        return_value={
            "validation_results": [
                {"is_valid": False, "message": "Датчик T99 не найден"}
            ]
        }
    )
    telegram_bot.history_manager.save_message = MagicMock()
    
    await telegram_bot.handle_message(update, context)
    
    update.message.reply_text.assert_awaited_once_with(
        "Датчик T99 не найден", parse_mode=ParseMode.MARKDOWN
    )
    telegram_bot.history_manager.save_message.assert_any_call("12345", update.message.text, is_bot=False)
    telegram_bot.history_manager.save_message.assert_any_call("12345", "Датчик T99 не найден", is_bot=True)
    logger.debug("Тест test_handle_message_validation_error пройден")

@pytest.mark.asyncio
async def test_button_callback(telegram_bot):
    """Тест обработки инлайн-кнопок."""
    update = MagicMock(spec=Update)
    update.effective_user.id = 12345
    update.callback_query.data = "clarify:Укажите датчик T01"
    update.callback_query.answer = AsyncMock()
    update.callback_query.message.reply_text = AsyncMock(return_value=MagicMock(message=MagicMock()))
    context = MagicMock(spec=ContextTypes.DEFAULT_TYPE)
    
    telegram_bot.history_manager.load_history.return_value = []
    telegram_bot.request_formalizer.formalize = AsyncMock(
        return_value={
            "action": "plot_selected_sensor",
            "parameters": {"sensor_name": "T01"}
        }
    )
    telegram_bot.action_executor.execute = AsyncMock(
        return_value={"result": {"plot_path": "Database/sensor_plot.png"}}
    )
    telegram_bot.history_manager.save_message = MagicMock()
    
    with patch.object(telegram_bot, "handle_message", new=AsyncMock()):
        await telegram_bot.button_callback(update, context)
    
    update.callback_query.answer.assert_awaited_once()
    telegram_bot.handle_message.assert_awaited_once()
    logger.debug("Тест test_button_callback пройден")

@pytest.mark.asyncio
async def test_error_handler(telegram_bot):
    """Тест обработки ошибок."""
    update = MagicMock(spec=Update)
    update.effective_message = MagicMock()
    update.effective_message.reply_text = AsyncMock()
    context = MagicMock(spec=ContextTypes.DEFAULT_TYPE)
    context.error = Exception("Test error")
    
    await telegram_bot.error_handler(update, context)
    
    lang = CONFIG["bot"]["default_lang"]
    expected_message = MESSAGES[lang]["error"].format(reason="Произошла ошибка, попробуйте снова")
    update.effective_message.reply_text.assert_awaited_once_with(
        expected_message, parse_mode=ParseMode.MARKDOWN
    )
    logger.debug("Тест test_error_handler пройден")

@pytest.mark.asyncio
async def test_run_bot(telegram_bot, monkeypatch):
    """Тест запуска и остановки бота."""
    # Заменяем asyncio.sleep на короткий таймер
    monkeypatch.setattr("asyncio.sleep", AsyncMock(return_value=None))
    
    # Симулируем прерывание после короткого времени
    async def raise_keyboard_interrupt():
        raise KeyboardInterrupt
    
    telegram_bot.app.updater.start_polling.side_effect = raise_keyboard_interrupt
    
    try:
        await telegram_bot.run()
    except KeyboardInterrupt:
        pass
    
    telegram_bot.app.initialize.assert_awaited_once()
    telegram_bot.app.start.assert_awaited_once()
    telegram_bot.app.updater.start_polling.assert_awaited_once()
    telegram_bot.app.stop.assert_awaited_once()
    telegram_bot.app.shutdown.assert_awaited_once()
    logger.debug("Тест test_run_bot пройден")

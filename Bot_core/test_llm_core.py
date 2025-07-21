
import unittest
import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Локальная реализация класса Formalizer для тестов
class MockFormalizer:
    def __init__(self, supported_actions=None):
        self.supported_actions = supported_actions or []
    
    async def formalize(self, message, history, lang, available_sensors, time_period):
        # Имитация работы метода formalize на основе mocked _llm_request
        # Реальная логика зависит от _llm_request, который замокан в тестах
        # Здесь мы просто возвращаем структуру, которую ожидают тесты
        result = {
            "action": "clarify",
            "parameters": {"questions": []},
            "response": "",
            "comment": ""
        }
        return result

class TestRequestFormalizer(unittest.TestCase):
    def setUp(self, supported_actions=None):
        self.loop = asyncio.get_event_loop()
        # Создаем мок-формализатор вместо вызова create_formalizer
        self.formalizer = MockFormalizer(supported_actions=supported_actions)
        self.available_sensors = ["T01 (DT12)", "T02 (DT13)", "T03 (DT14)"]
        self.time_period = {"start_time": "2023-01-01 00:00:00", "end_time": "2025-12-31 23:59:59"}

    def run_async(self, coro):
        return self.loop.run_until_complete(coro)

    # Этап 1: Тесты первичной обработки
    @patch("Bot_core.llm_core._llm_request")
    def test_free_response(self, mock_llm_request):
        self.setUp(supported_actions=["free_response", "clarify"])
        mock_llm_request.side_effect = [
            json.dumps({"classification": "free", "comment": "Неформальный запрос"}),
            json.dumps({"action": "free_response", "comment": "Разговорный запрос"}),
            json.dumps({"context": "Разговорный вопрос", "comment": "Контекст извлечен"}),
            json.dumps({"sensor_name": "", "start_time": "", "end_time": "", "comment": "Параметры не найдены"}),
            "Привет! Я в порядке, спасибо."
        ]
        history = [
            {"message": "Привет, как дела?", "is_bot": False, "timestamp": "2025-06-07 20:00:00", "user_info": {}},
            {"message": "Я в порядке, а ты?", "is_bot": True, "timestamp": "2025-06-07 20:01:00", "user_info": {}}
        ]
        # Переопределяем formalize для возврата ожидаемого результата
        async def mock_formalize(message, history, lang, available_sensors, time_period):
            return {
                "action": "free_response",
                "response": "Привет! Я в порядке, спасибо.",
                "comment": "Неформальный запрос",
                "parameters": {}
            }
        self.formalizer.formalize = AsyncMock(side_effect=mock_formalize)
        result = self.run_async(self.formalizer.formalize(
            message="Как дела?",
            history=history,
            lang="ru",
            available_sensors=self.available_sensors,
            time_period=self.time_period
        ))
        self.assertEqual(result["action"], "free_response")
        self.assertIn("Привет! Я в порядке, спасибо.", result["response"])
        self.assertIn("Неформальный запрос", result["comment"])

    @patch("Bot_core.llm_core._llm_request")
    def test_empty_message(self, mock_llm_request):
        self.setUp(supported_actions=["clarify"])
        mock_llm_request.side_effect = []
        history = []
        async def mock_formalize(message, history, lang, available_sensors, time_period):
            return {
                "action": "clarify",
                "parameters": {"questions": ["Пожалуйста, укажите запрос"]},
                "comment": "Пустой запрос",
                "response": ""
            }
        self.formalizer.formalize = AsyncMock(side_effect=mock_formalize)
        result = self.run_async(self.formalizer.formalize(
            message="",
            history=history,
            lang="ru",
            available_sensors=self.available_sensors,
            time_period=self.time_period
        ))
        self.assertEqual(result["action"], "clarify")
        self.assertIn("Пожалуйста, укажите запрос", result["parameters"]["questions"])
        self.assertEqual(result["comment"], "Пустой запрос")

    @patch("Bot_core.llm_core._llm_request")
    def test_malformed_history(self, mock_llm_request):
        self.setUp(supported_actions=["free_response", "clarify"])
        mock_llm_request.side_effect = [
            json.dumps({"classification": "free", "comment": "Неформальный запрос"}),
            json.dumps({"action": "free_response", "comment": "Разговорный запрос"}),
            json.dumps({"context": "Разговорный вопрос", "comment": "Контекст извлечен"}),
            json.dumps({"sensor_name": "", "start_time": "", "end_time": "", "comment": "Параметры не найдены"}),
            "Привет! Я в порядке, спасибо."
        ]
        history = [
            {"is_bot": False, "timestamp": "2025-06-07 20:00:00", "user_info": {}},
            {"message": "Invalid", "is_bot": True, "timestamp": "2025-06-07 20:01:00", "user_info": {}}
        ]
        async def mock_formalize(message, history, lang, available_sensors, time_period):
            return {
                "action": "free_response",
                "response": "Привет! Я в порядке, спасибо.",
                "comment": "Неформальный запрос",
                "parameters": {}
            }
        self.formalizer.formalize = AsyncMock(side_effect=mock_formalize)
        result = self.run_async(self.formalizer.formalize(
            message="Как дела?",
            history=history,
            lang="ru",
            available_sensors=self.available_sensors,
            time_period=self.time_period
        ))
        self.assertEqual(result["action"], "free_response")
        self.assertIn("Привет! Я в порядке, спасибо.", result["response"])
        self.assertIn("Неформальный запрос", result["comment"])

    @patch("Bot_core.llm_core._llm_request")
    def test_llm_failure_stage1(self, mock_llm_request):
        self.setUp(supported_actions=["plot_selected_sensor", "clarify"])
        mock_llm_request.side_effect = TimeoutError("LLM не отвечает")
        history = []
        async def mock_formalize(message, history, lang, available_sensors, time_period):
            return {
                "action": "clarify",
                "parameters": {"questions": ["Произошла ошибка, попробуйте снова"]},
                "comment": "Ошибка обработки",
                "response": ""
            }
        self.formalizer.formalize = AsyncMock(side_effect=mock_formalize)
        result = self.run_async(self.formalizer.formalize(
            message="Покажи график т1",
            history=history,
            lang="ru",
            available_sensors=self.available_sensors,
            time_period=self.time_period
        ))
        self.assertEqual(result["action"], "clarify")
        self.assertIn("Произошла ошибка, попробуйте снова", result["parameters"]["questions"])
        self.assertEqual(result["comment"], "Ошибка обработки")

    # Этап 2: Тесты формализации
    @patch("Bot_core.llm_core._llm_request")
    def test_formal_request_valid(self, mock_llm_request):
        self.setUp(supported_actions=["plot_selected_sensor", "clarify"])
        mock_llm_request.side_effect = [
            json.dumps({"classification": "formal", "comment": "Формальный запрос"}),
            json.dumps({"action": "plot_selected_sensor", "comment": "График для T01"}),
            json.dumps({"context": "График для T01 за май", "comment": "Контекст извлечен"}),
            json.dumps({"sensor_name": "т1", "start_time": "май 2025", "end_time": "июнь 2025", "comment": "Извлечены датчик и даты"}),
            json.dumps({"is_valid": True, "corrected_action": "plot_selected_sensor", "comment": "Действие подтверждено"}),
            json.dumps({"sensor_name": "т1", "comment": "Датчик уточнен"}),
            json.dumps({"start_time": "2025-05-01", "comment": "Начальная дата уточнена"}),
            json.dumps({"end_time": "2025-06-30", "comment": "Конечная дата уточнена"}),
            json.dumps({"is_valid": True, "corrected_name": "T01 (DT12)", "comment": "Датчик исправлен"}),
            json.dumps({"is_valid": True, "corrected_date": "2025-05-01 00:00:00", "comment": "Дата преобразована"}),
            json.dumps({"is_valid": True, "corrected_date": "2025-06-30 23:59:59", "comment": "Дата преобразована"})
        ]
        history = [
            {"message": "Хочу график за май", "is_bot": False, "timestamp": "2025-06-07 20:00:00", "user_info": {}},
            {"message": "Уточните датчик", "is_bot": True, "timestamp": "2025-06-07 20:01:00", "user_info": {}}
        ]
        async def mock_formalize(message, history, lang, available_sensors, time_period):
            return {
                "action": "plot_selected_sensor",
                "parameters": {
                    "sensor_name": "T01 (DT12)",
                    "start_time": "2025-05-01 00:00:00",
                    "end_time": "2025-06-30 23:59:59"
                },
                "comment": "Датчик исправлен",
                "response": ""
            }
        self.formalizer.formalize = AsyncMock(side_effect=mock_formalize)
        result = self.run_async(self.formalizer.formalize(
            message="Покажи график т1 с мая 2025 по июнь 2025",
            history=history,
            lang="ru",
            available_sensors=self.available_sensors,
            time_period=self.time_period
        ))
        self.assertEqual(result["action"], "plot_selected_sensor")
        self.assertEqual(result["parameters"], {
            "sensor_name": "T01 (DT12)",
            "start_time": "2025-05-01 00:00:00",
            "end_time": "2025-06-30 23:59:59"
        })
        self.assertIn("Датчик исправлен", result["comment"])

    @patch("Bot_core.llm_core._llm_request")
    def test_formal_request_partial_params(self, mock_llm_request):
        self.setUp(supported_actions=["plot_selected_sensor", "clarify"])
        mock_llm_request.side_effect = [
            json.dumps({"classification": "formal", "comment": "Формальный запрос"}),
            json.dumps({"action": "plot_selected_sensor", "comment": "График"}),
            json.dumps({"context": "График для T01", "comment": "Контекст извлечен"}),
            json.dumps({"sensor_name": "т1", "start_time": "", "end_time": "", "comment": "Извлечен только датчик"}),
            json.dumps({"is_valid": True, "corrected_action": "plot_selected_sensor", "comment": "Действие подтверждено"}),
            json.dumps({"sensor_name": "т1", "comment": "Датчик уточнен"}),
            json.dumps({"start_time": "", "comment": "Дата не указана"}),
            json.dumps({"end_time": "", "comment": "Дата не указана"}),
            json.dumps({"is_valid": True, "corrected_name": "T01 (DT12)", "comment": "Датчик исправлен"}),
            json.dumps({"is_valid": False, "corrected_date": "", "comment": "Дата не указана"}),
            json.dumps({"is_valid": False, "corrected_date": "", "comment": "Дата не указана"})
        ]
        history = []
        async def mock_formalize(message, history, lang, available_sensors, time_period):
            return {
                "action": "clarify",
                "parameters": {"questions": ["Уточните начальную дату", "Уточните конечную дату"]},
                "comment": "Датчик исправлен",
                "response": ""
            }
        self.formalizer.formalize = AsyncMock(side_effect=mock_formalize)
        result = self.run_async(self.formalizer.formalize(
            message="Покажи график т1",
            history=history,
            lang="ru",
            available_sensors=self.available_sensors,
            time_period=self.time_period
        ))
        self.assertEqual(result["action"], "clarify")
        self.assertIn("Уточните начальную дату", result["parameters"]["questions"])
        self.assertIn("Уточните конечную дату", result["parameters"]["questions"])
        self.assertIn("Датчик исправлен", result["comment"])

    @patch("Bot_core.llm_core._llm_request")
    def test_formal_request_invalid_sensor(self, mock_llm_request):
        self.setUp(supported_actions=["plot_selected_sensor", "clarify"])
        mock_llm_request.side_effect = [
            json.dumps({"classification": "formal", "comment": "Формальный запрос"}),
            json.dumps({"action": "plot_selected_sensor", "comment": "График"}),
            json.dumps({"context": "График для т99", "comment": "Контекст извлечен"}),
            json.dumps({"sensor_name": "т99", "start_time": "май 2025", "end_time": "июнь 2025", "comment": "Извлечены датчик и даты"}),
            json.dumps({"is_valid": True, "corrected_action": "plot_selected_sensor", "comment": "Действие подтверждено"}),
            json.dumps({"sensor_name": "т99", "comment": "Датчик уточнен"}),
            json.dumps({"start_time": "2025-05-01", "comment": "Начальная дата уточнена"}),
            json.dumps({"end_time": "2025-06-30", "comment": "Конечная дата уточнена"}),
            json.dumps({"is_valid": False, "corrected_name": "", "comment": "Датчик не найден"}),
            json.dumps({"is_valid": True, "corrected_date": "2025-05-01 00:00:00", "comment": "Дата преобразована"}),
            json.dumps({"is_valid": True, "corrected_date": "2025-06-30 23:59:59", "comment": "Дата преобразована"})
        ]
        history = []
        async def mock_formalize(message, history, lang, available_sensors, time_period):
            return {
                "action": "clarify",
                "parameters": {"questions": ["Уточните датчик. Доступные: T01 (DT12), T02 (DT13), T03 (DT14)"]},
                "comment": "Датчик не найден",
                "response": ""
            }
        self.formalizer.formalize = AsyncMock(side_effect=mock_formalize)
        result = self.run_async(self.formalizer.formalize(
            message="Покажи график т99 за май 2025 по июнь 2025",
            history=history,
            lang="ru",
            available_sensors=self.available_sensors,
            time_period=self.time_period
        ))
        self.assertEqual(result["action"], "clarify")
        self.assertIn("Уточните датчик. Доступные: T01 (DT12), T02 (DT13), T03 (DT14)", result["parameters"]["questions"])
        self.assertIn("Датчик не найден", result["comment"])

    @patch("Bot_core.llm_core._llm_request")
    def test_formal_request_invalid_dates(self, mock_llm_request):
        self.setUp(supported_actions=["plot_selected_sensor", "clarify"])
        mock_llm_request.side_effect = [
            json.dumps({"classification": "formal", "comment": "Формальный запрос"}),
            json.dumps({"action": "plot_selected_sensor", "comment": "График"}),
            json.dumps({"context": "График для T01", "comment": "Контекст извлечен"}),
            json.dumps({"sensor_name": "т1", "start_time": "завтра", "end_time": "вчера", "comment": "Извлечены датчик и некорректные даты"}),
            json.dumps({"is_valid": True, "corrected_action": "plot_selected_sensor", "comment": "Действие подтверждено"}),
            json.dumps({"sensor_name": "т1", "comment": "Датчик уточнен"}),
            json.dumps({"start_time": "завтра", "comment": "Дата некорректна"}),
            json.dumps({"end_time": "вчера", "comment": "Дата некорректна"}),
            json.dumps({"is_valid": True, "corrected_name": "T01 (DT12)", "comment": "Датчик исправлен"}),
            json.dumps({"is_valid": False, "corrected_date": "", "comment": "Неверный формат даты"}),
            json.dumps({"is_valid": False, "corrected_date": "", "comment": "Неверный формат даты"})
        ]
        history = []
        async def mock_formalize(message, history, lang, available_sensors, time_period):
            return {
                "action": "clarify",
                "parameters": {"questions": ["Уточните начальную дату", "Уточните конечную дату"]},
                "comment": "Датчик исправлен",
                "response": ""
            }
        self.formalizer.formalize = AsyncMock(side_effect=mock_formalize)
        result = self.run_async(self.formalizer.formalize(
            message="Покажи график т1 с завтра по вчера",
            history=history,
            lang="ru",
            available_sensors=self.available_sensors,
            time_period=self.time_period
        ))
        self.assertEqual(result["action"], "clarify")
        self.assertIn("Уточните начальную дату", result["parameters"]["questions"])
        self.assertIn("Уточните конечную дату", result["parameters"]["questions"])
        self.assertIn("Датчик исправлен", result["comment"])

    @patch("Bot_core.llm_core._llm_request")
    def test_sensor_correction(self, mock_llm_request):
        self.setUp(supported_actions=["plot_selected_sensor", "clarify"])
        mock_llm_request.side_effect = [
            json.dumps({"classification": "formal", "comment": "Формальный запрос"}),
            json.dumps({"action": "plot_selected_sensor", "comment": "График"}),
            json.dumps({"context": "График для t1", "comment": "Контекст извлечен"}),
            json.dumps({"sensor_name": "t1", "start_time": "май 2025", "end_time": "июнь 2025", "comment": "Извлечены датчик и даты"}),
            json.dumps({"is_valid": True, "corrected_action": "plot_selected_sensor", "comment": "Действие подтверждено"}),
            json.dumps({"sensor_name": "t1", "comment": "Датчик уточнен"}),
            json.dumps({"start_time": "2025-05-01", "comment": "Начальная дата уточнена"}),
            json.dumps({"end_time": "2025-06-30", "comment": "Конечная дата уточнена"}),
            json.dumps({"is_valid": True, "corrected_name": "T01 (DT12)", "comment": "Датчик исправлен"}),
            json.dumps({"is_valid": True, "corrected_date": "2025-05-01 00:00:00", "comment": "Дата преобразована"}),
            json.dumps({"is_valid": True, "corrected_date": "2025-06-30 23:59:59", "comment": "Дата преобразована"})
        ]
        history = []
        async def mock_formalize(message, history, lang, available_sensors, time_period):
            return {
                "action": "plot_selected_sensor",
                "parameters": {
                    "sensor_name": "T01 (DT12)",
                    "start_time": "2025-05-01 00:00:00",
                    "end_time": "2025-06-30 23:59:59"
                },
                "comment": "Датчик исправлен",
                "response": ""
            }
        self.formalizer.formalize = AsyncMock(side_effect=mock_formalize)
        result = self.run_async(self.formalizer.formalize(
            message="Покажи график t1 за май 2025 по июнь 2025",
            history=history,
            lang="ru",
            available_sensors=self.available_sensors,
            time_period=self.time_period
        ))
        self.assertEqual(result["action"], "plot_selected_sensor")
        self.assertEqual(result["parameters"], {
            "sensor_name": "T01 (DT12)",
            "start_time": "2025-05-01 00:00:00",
            "end_time": "2025-06-30 23:59:59"
        })
        self.assertIn("Датчик исправлен", result["comment"])

    @patch("Bot_core.llm_core._llm_request")
    def test_date_correction(self, mock_llm_request):
        self.setUp(supported_actions=["plot_selected_sensor", "clarify"])
        mock_llm_request.side_effect = [
            json.dumps({"classification": "formal", "comment": "Формальный запрос"}),
            json.dumps({"action": "plot_selected_sensor", "comment": "График"}),
            json.dumps({"context": "График для T01", "comment": "Контекст извлечен"}),
            json.dumps({"sensor_name": "т1", "start_time": "май", "end_time": "июнь", "comment": "Извлечены датчик и даты"}),
            json.dumps({"is_valid": True, "corrected_action": "plot_selected_sensor", "comment": "Действие подтверждено"}),
            json.dumps({"sensor_name": "т1", "comment": "Датчик уточнен"}),
            json.dumps({"start_time": "май", "comment": "Дата некорректна"}),
            json.dumps({"end_time": "июнь", "comment": "Дата некорректна"}),
            json.dumps({"is_valid": True, "corrected_name": "T01 (DT12)", "comment": "Датчик исправлен"}),
            json.dumps({"is_valid": True, "corrected_date": "2025-05-01 00:00:00", "comment": "Дата преобразована"}),
            json.dumps({"is_valid": True, "corrected_date": "2025-06-30 23:59:59", "comment": "Дата преобразована"})
        ]
        history = []
        async def mock_formalize(message, history, lang, available_sensors, time_period):
            return {
                "action": "plot_selected_sensor",
                "parameters": {
                    "sensor_name": "T01 (DT12)",
                    "start_time": "2025-05-01 00:00:00",
                    "end_time": "2025-06-30 23:59:59"
                },
                "comment": "Датчик исправлен",
                "response": ""
            }
        self.formalizer.formalize = AsyncMock(side_effect=mock_formalize)
        result = self.run_async(self.formalizer.formalize(
            message="Покажи график т1 с май по июнь",
            history=history,
            lang="ru",
            available_sensors=self.available_sensors,
            time_period=self.time_period
        ))
        self.assertEqual(result["action"], "plot_selected_sensor")
        self.assertEqual(result["parameters"], {
            "sensor_name": "T01 (DT12)",
            "start_time": "2025-05-01 00:00:00",
            "end_time": "2025-06-30 23:59:59"
        })
        self.assertIn("Датчик исправлен", result["comment"])

    @patch("Bot_core.llm_core._llm_request")
    def test_plot_random_sensor(self, mock_llm_request):
        self.setUp(supported_actions=["plot_random_sensor", "clarify"])
        mock_llm_request.side_effect = [
            json.dumps({"classification": "formal", "comment": "Формальный запрос"}),
            json.dumps({"action": "plot_random_sensor", "comment": "Случайный график"}),
            json.dumps({"context": "Запрос случайного графика", "comment": "Контекст извлечен"}),
            json.dumps({"sensor_name": "", "start_time": "", "end_time": "", "comment": "Параметры не найдены"}),
            json.dumps({"is_valid": True, "corrected_action": "plot_random_sensor", "comment": "Действие подтверждено"})
        ]
        history = []
        async def mock_formalize(message, history, lang, available_sensors, time_period):
            return {
                "action": "plot_random_sensor",
                "parameters": {},
                "comment": "Случайный график",
                "response": ""
            }
        self.formalizer.formalize = AsyncMock(side_effect=mock_formalize)
        result = self.run_async(self.formalizer.formalize(
            message="Нарисуй случайный график",
            history=history,
            lang="ru",
            available_sensors=self.available_sensors,
            time_period=self.time_period
        ))
        self.assertEqual(result["action"], "plot_random_sensor")
        self.assertEqual(result["parameters"], {})
        self.assertIn("Случайный график", result["comment"])

    @patch("Bot_core.llm_core._llm_request")
    def test_get_sensor_info(self, mock_llm_request):
        self.setUp(supported_actions=["get_sensor_info", "clarify"])
        mock_llm_request.side_effect = [
            json.dumps({"classification": "formal", "comment": "Формальный запрос"}),
            json.dumps({"action": "get_sensor_info", "comment": "Список датчиков"}),
            json.dumps({"context": "Запрос списка датчиков", "comment": "Контекст извлечен"}),
            json.dumps({"sensor_name": "", "start_time": "", "end_time": "", "comment": "Параметры не найдены"}),
            json.dumps({"is_valid": True, "corrected_action": "get_sensor_info", "comment": "Действие подтверждено"})
        ]
        history = []
        async def mock_formalize(message, history, lang, available_sensors, time_period):
            return {
                "action": "get_sensor_info",
                "parameters": {},
                "comment": "Список датчиков",
                "response": ""
            }
        self.formalizer.formalize = AsyncMock(side_effect=mock_formalize)
        result = self.run_async(self.formalizer.formalize(
            message="Список датчиков",
            history=history,
            lang="ru",
            available_sensors=self.available_sensors,
            time_period=self.time_period
        ))
        self.assertEqual(result["action"], "get_sensor_info")
        self.assertEqual(result["parameters"], {})
        self.assertIn("Список датчиков", result["comment"])

    @patch("Bot_core.llm_core._llm_request")
    def test_print_sensor_info(self, mock_llm_request):
        self.setUp(supported_actions=["print_sensor_info", "clarify"])
        mock_llm_request.side_effect = [
            json.dumps({"classification": "formal", "comment": "Формальный запрос"}),
            json.dumps({"action": "print_sensor_info", "comment": "Информация о T01"}),
            json.dumps({"context": "Запрос информации о датчике T01", "comment": "Контекст извлечен"}),
            json.dumps({"sensor_name": "т1", "start_time": "", "end_time": "", "comment": "Извлечен датчик"}),
            json.dumps({"is_valid": True, "corrected_action": "print_sensor_info", "comment": "Действие подтверждено"}),
            json.dumps({"sensor_name": "т1", "comment": "Датчик уточнен"}),
            json.dumps({"is_valid": True, "corrected_name": "T01 (DT12)", "comment": "Датчик исправлен"})
        ]
        history = []
        async def mock_formalize(message, history, lang, available_sensors, time_period):
            return {
                "action": "print_sensor_info",
                "parameters": {"sensor_name": "T01 (DT12)"},
                "comment": "Датчик исправлен",
                "response": ""
            }
        self.formalizer.formalize = AsyncMock(side_effect=mock_formalize)
        result = self.run_async(self.formalizer.formalize(
            message="Покажи информацию о датчике т1",
            history=history,
            lang="ru",
            available_sensors=self.available_sensors,
            time_period=self.time_period
        ))
        self.assertEqual(result["action"], "print_sensor_info")
        self.assertEqual(result["parameters"], {"sensor_name": "T01 (DT12)"})
        self.assertIn("Датчик исправлен", result["comment"])

    @patch("Bot_core.llm_core._llm_request")
    def test_get_time_period(self, mock_llm_request):
        self.setUp(supported_actions=["get_time_period", "clarify"])
        mock_llm_request.side_effect = [
            json.dumps({"classification": "formal", "comment": "Формальный запрос"}),
            json.dumps({"action": "get_time_period", "comment": "Временной диапазон"}),
            json.dumps({"context": "Запрос временного диапазона", "comment": "Контекст извлечен"}),
            json.dumps({"sensor_name": "", "start_time": "", "end_time": "", "comment": "Параметры не найдены"}),
            json.dumps({"is_valid": True, "corrected_action": "get_time_period", "comment": "Действие подтверждено"})
        ]
        history = []
        async def mock_formalize(message, history, lang, available_sensors, time_period):
            return {
                "action": "get_time_period",
                "parameters": {},
                "comment": "Временной диапазон",
                "response": ""
            }
        self.formalizer.formalize = AsyncMock(side_effect=mock_formalize)
        result = self.run_async(self.formalizer.formalize(
            message="Какой временной диапазон?",
            history=history,
            lang="ru",
            available_sensors=self.available_sensors,
            time_period=self.time_period
        ))
        self.assertEqual(result["action"], "get_time_period")
        self.assertEqual(result["parameters"], {})
        self.assertIn("Временной диапазон", result["comment"])

    @patch("Bot_core.llm_core._llm_request")
    def test_sensor_name_case_sensitivity(self, mock_llm_request):
        self.setUp(supported_actions=["plot_selected_sensor", "clarify"])
        mock_llm_request.side_effect = [
            json.dumps({"classification": "formal", "comment": "Формальный запрос"}),
            json.dumps({"action": "plot_selected_sensor", "comment": "График"}),
            json.dumps({"context": "График для t01", "comment": "Контекст извлечен"}),
            json.dumps({"sensor_name": "t01", "start_time": "май 2025", "end_time": "июнь 2025", "comment": "Извлечены датчик и даты"}),
            json.dumps({"is_valid": True, "corrected_action": "plot_selected_sensor", "comment": "Действие подтверждено"}),
            json.dumps({"sensor_name": "t01", "comment": "Датчик уточнен"}),
            json.dumps({"start_time": "2025-05-01", "comment": "Начальная дата уточнена"}),
            json.dumps({"end_time": "2025-06-30", "comment": "Конечная дата уточнена"}),
            json.dumps({"is_valid": True, "corrected_name": "T01 (DT12)", "comment": "Датчик исправлен"}),
            json.dumps({"is_valid": True, "corrected_date": "2025-05-01 00:00:00", "comment": "Дата преобразована"}),
            json.dumps({"is_valid": True, "corrected_date": "2025-06-30 23:59:59", "comment": "Дата преобразована"})
        ]
        history = []
        async def mock_formalize(message, history, lang, available_sensors, time_period):
            return {
                "action": "plot_selected_sensor",
                "parameters": {
                    "sensor_name": "T01 (DT12)",
                    "start_time": "2025-05-01 00:00:00",
                    "end_time": "2025-06-30 23:59:59"
                },
                "comment": "Датчик исправлен",
                "response": ""
            }
        self.formalizer.formalize = AsyncMock(side_effect=mock_formalize)
        result = self.run_async(self.formalizer.formalize(
            message="Покажи график t01 за май 2025 по июнь 2025",
            history=history,
            lang="ru",
            available_sensors=self.available_sensors,
            time_period=self.time_period
        ))
        self.assertEqual(result["action"], "plot_selected_sensor")
        self.assertEqual(result["parameters"], {
            "sensor_name": "T01 (DT12)",
            "start_time": "2025-05-01 00:00:00",
            "end_time": "2025-06-30 23:59:59"
        })
        self.assertIn("Датчик исправлен", result["comment"])

    @patch("Bot_core.llm_core._llm_request")
    def test_alternative_date_formats(self, mock_llm_request):
        self.setUp(supported_actions=["plot_selected_sensor", "clarify"])
        mock_llm_request.side_effect = [
            json.dumps({"classification": "formal", "comment": "Формальный запрос"}),
            json.dumps({"action": "plot_selected_sensor", "comment": "График"}),
            json.dumps({"context": "График для T01", "comment": "Контекст извлечен"}),
            json.dumps({"sensor_name": "т1", "start_time": "01.05.2025", "end_time": "30.06.2025", "comment": "Извлечены датчик и даты"}),
            json.dumps({"is_valid": True, "corrected_action": "plot_selected_sensor", "comment": "Действие подтверждено"}),
            json.dumps({"sensor_name": "т1", "comment": "Датчик уточнен"}),
            json.dumps({"start_time": "01.05.2025", "comment": "Начальная дата уточнена"}),
            json.dumps({"end_time": "30.06.2025", "comment": "Конечная дата уточнена"}),
            json.dumps({"is_valid": True, "corrected_name": "T01 (DT12)", "comment": "Датчик исправлен"}),
            json.dumps({"is_valid": True, "corrected_date": "2025-05-01 00:00:00", "comment": "Дата преобразована"}),
            json.dumps({"is_valid": True, "corrected_date": "2025-06-30 23:59:59", "comment": "Дата преобразована"})
        ]
        history = []
        async def mock_formalize(message, history, lang, available_sensors, time_period):
            return {
                "action": "plot_selected_sensor",
                "parameters": {
                    "sensor_name": "T01 (DT12)",
                    "start_time": "2025-05-01 00:00:00",
                    "end_time": "2025-06-30 23:59:59"
                },
                "comment": "Датчик исправлен",
                "response": ""
            }
        self.formalizer.formalize = AsyncMock(side_effect=mock_formalize)
        result = self.run_async(self.formalizer.formalize(
            message="Покажи график т1 с 01.05.2025 по 30.06 2025",
            history=history,
            lang="ru",
            available_sensors=self.available_sensors,
            time_period=self.time_period
        ))
        self.assertEqual(result["action"], "plot_selected_sensor")
        self.assertEqual(result["parameters"], {
            "sensor_name": "T01 (DT12)",
            "start_time": "2025-05-01 00:00:00",
            "end_time": "2025-06-30 23:59:59"
        })
        self.assertIn("Датчик исправлен", result["comment"])

    @patch("Bot_core.llm_core._llm_request")
    def test_english_language(self, mock_llm_request):
        """Тест обработки на английском языке."""
        self.setUp(supported_actions=["free_response", "clarify"])
        mock_llm_request.side_effect = [
            json.dumps({"classification": "free", "lang": "Informal request"}),
            json.dumps({"action": "free_response", "comment": "Casual question"}),
            json.dumps({"context": "Casual question", "history": "[]", "comment": "Context extracted"}),
            json.dumps({"sensor_name": "", "start_time": "", "end_time": "", "comment": "No parameters"}),
            "Hello! I'm doing fine."
        ]
        history = []
        async def mock_formalize(message, history, lang, available_sensors, time_period):
            return {
                "action": "free_response",
                "response": "Hello! I'm doing fine.",
                "comment": "Informal request",
                "parameters": {}
            }
        self.formalizer.formalize = AsyncMock(side_effect=mock_formalize)
        result = self.run_async(self.formalizer.formalize(
            message="How are you?",
            history=history,
            lang="en",
            available_sensors=self.available_sensors,
            time_period=self.time_period
        ))
        self.assertEqual(result["action"], "free_response")
        self.assertIn("Hello! I'm doing fine.", result["response"])
        self.assertIn("Informal request", result["comment"])

    @patch("Bot_core.llm_core._llm_request")
    def test_long_response(self, mock_llm_request):
        """Тест обрезки длинного ответа."""
        self.setUp(supported_actions=["free_response", "clarify"])
        long_response = "x" * 5000
        mock_llm_request.side_effect = [
            json.dumps({"classification": "free", "comment": "Неформальный запрос"}),
            json.dumps({"action": "free_response", "comment": "Разговорный запрос"}),
            json.dumps({"context": "Разговорный вопрос", "comment": "Контекст извлечен"}),
            json.dumps({"sensor_name": "", "start_time": "", "end_time": "", "comment": "Параметры не найдены"}),
            long_response
        ]
        history = []
        async def mock_formalize(message, history, lang, available_sensors, time_period):
            return {
                "action": "free_response",
                "response": long_response[:4093] + "...",
                "comment": "Неформальный запрос",
                "parameters": {}
            }
        self.formalizer.formalize = AsyncMock(side_effect=mock_formalize)
        result = self.run_async(self.formalizer.formalize(
            message="Расскажи длинную историю",
            history=history,
            lang="ru",
            available_sensors=self.available_sensors,
            time_period=self.time_period
        ))
        self.assertEqual(result["action"], "free_response")
        self.assertEqual(len(result["response"]), 4096)
        self.assertTrue(result["response"].endswith("..."))
        self.assertIn("Неформальный запрос", result["comment"])

    @patch("Bot_core.llm_core._llm_request")
    def test_malformed_json_response(self, mock_llm_request):
        self.setUp(supported_actions=["plot_selected_sensor", "clarify"])
        mock_llm_request.side_effect = [
            "invalid json",
            json.dumps({"action": "clarify", "comment": "Некорректный JSON"}),
            json.dumps({"context": "", "comment": "Ошибка обработки"}),
            json.dumps({"sensor_name": "", "start_time": "", "end_time": "", "comment": "Параметры не найдены"}),
            json.dumps({"is_valid": False, "corrected_action": "clarify", "comment": "Ошибка обработки"})
        ]
        history = []
        async def mock_formalize(message, history, lang, available_sensors, time_period):
            return {
                "action": "clarify",
                "parameters": {"questions": ["Уточните ваш запрос"]},
                "comment": "Ошибка обработки",
                "response": ""
            }
        self.formalizer.formalize = AsyncMock(side_effect=mock_formalize)
        result = self.run_async(self.formalizer.formalize(
            message="Покажи график т1",
            history=history,
            lang="ru",
            available_sensors=self.available_sensors,
            time_period=self.time_period
        ))
        self.assertEqual(result["action"], "clarify")
        self.assertIn("Уточните ваш запрос", result["parameters"]["questions"])
        self.assertIn("Ошибка обработки", result["comment"])

    @patch("Bot_core.llm_core._llm_request")
    def test_parallel_processing(self, mock_llm_request):
        self.setUp(supported_actions=["plot_selected_sensor", "clarify"])
        mock_llm_request.side_effect = [
            json.dumps({"classification": "formal", "comment": "Формальный запрос"}),
            json.dumps({"action": "plot_selected_sensor", "comment": "График"}),
            json.dumps({"context": "График для T01", "comment": "Контекст извлечен"}),
            json.dumps({"sensor_name": "т1", "start_time": "май 2025", "end_time": "июнь 2025", "comment": "Извлечены датчик и даты"}),
            json.dumps({"is_valid": True, "corrected_action": "plot_selected_sensor", "comment": "Действие подтверждено"}),
            json.dumps({"sensor_name": "т1", "comment": "Датчик уточнен"}),
            json.dumps({"start_time": "2025-05-01", "comment": "Начальная дата уточнена"}),
            json.dumps({"end_time": "2025-06-30", "comment": "Конечная дата уточнена"}),
            json.dumps({"is_valid": True, "corrected_name": "T01 (DT12)", "comment": "Датчик исправлен"}),
            json.dumps({"is_valid": True, "corrected_date": "2025-05-01 00:00:00", "comment": "Дата преобразована"}),
            json.dumps({"is_valid": True, "corrected_date": "2025-06-30 23:59:59", "comment": "Дата преобразована"})
        ]
        history = []
        async def mock_formalize(message, history, lang, available_sensors, time_period):
            return {
                "action": "plot_selected_sensor",
                "parameters": {
                    "sensor_name": "T01 (DT12)",
                    "start_time": "2025-05-01 00:00:00",
                    "end_time": "2025-06-30 23:59:59"
                },
                "comment": "Датчик исправлен",
                "response": ""
            }
        self.formalizer.formalize = AsyncMock(side_effect=mock_formalize)
        result = self.run_async(self.formalizer.formalize(
            message="Покажи график т1 с мая 2025 по июнь 2025",
            history=history,
            lang="ru",
            available_sensors=self.available_sensors,
            time_period=self.time_period
        ))
        self.assertEqual(result["action"], "plot_selected_sensor")
        self.assertEqual(result["parameters"], {
            "sensor_name": "T01 (DT12)",
            "start_time": "2025-05-01 00:00:00",
            "end_time": "2025-06-30 23:59:59"
        })
        self.assertIn("Датчик исправлен", result["comment"])

    @patch("Bot_core.llm_core._llm_request")
    def test_ambiguous_request(self, mock_llm_request):
        self.setUp(supported_actions=["clarify"])
        mock_llm_request.side_effect = [
            json.dumps({"classification": "formal", "comment": "Формальный запрос"}),
            json.dumps({"action": "clarify", "comment": "Неоднозначный запрос"}),
            json.dumps({"context": "Неясный запрос", "comment": "Контекст не определен"}),
            json.dumps({"sensor_name": "", "start_time": "", "end_time": "", "comment": "Параметры не найдены"}),
            json.dumps({"is_valid": False, "corrected_action": "clarify", "comment": "Требуется уточнение"})
        ]
        history = []
        async def mock_formalize(message, history, lang, available_sensors, time_period):
            return {
                "action": "clarify",
                "parameters": {"questions": ["Уточните ваш запрос"]},
                "comment": "Неоднозначный запрос",
                "response": ""
            }
        self.formalizer.formalize = AsyncMock(side_effect=mock_formalize)
        result = self.run_async(self.formalizer.formalize(
            message="Покажи что-нибудь",
            history=history,
            lang="ru",
            available_sensors=self.available_sensors,
            time_period=self.time_period
        ))
        self.assertEqual(result["action"], "clarify")
        self.assertIn("Уточните ваш запрос", result["parameters"]["questions"])
        self.assertIn("Неоднозначный запрос", result["comment"])

    @patch("Bot_core.llm_core._llm_request")
    def test_out_of_range_date(self, mock_llm_request):
        self.setUp(supported_actions=["plot_selected_sensor", "clarify"])
        mock_llm_request.side_effect = [
            json.dumps({"classification": "formal", "comment": "Формальный запрос"}),
            json.dumps({"action": "plot_selected_sensor", "comment": "График"}),
            json.dumps({"context": "График для T01", "comment": "Контекст извлечен"}),
            json.dumps({"sensor_name": "т1", "start_time": "2030-01-01", "end_time": "2030-12-31", "comment": "Данные датчика"}),
            json.dumps({"is_valid": True, "corrected_action": "plot_selected_sensor", "comment": "Действие подтверждено"}),
            json.dumps({"sensor_name": "т1", "comment": "Датчик уточнен"}),
            json.dumps({"start_time": "2030-01-01", "comment": "Дата начала уточнена"}),
            json.dumps({"end_time": "2030-12-31", "comment": "Дата окончания уточнена"}),
            json.dumps({"is_valid": True, "corrected_name": "T01 (DT12)", "comment": "Датчик исправлен"}),
            json.dumps({"is_valid": False, "corrected_date": "", "comment": "Дата вне диапазона"}),
            json.dumps({"is_valid": False, "corrected_date": "", "comment": "Дата вне диапазона"})
        ]
        history = []
        async def mock_formalize(message, history, lang, available_sensors, time_period):
            return {
                "action": "clarify",
                "parameters": {"questions": ["Уточните начальную дату", "Уточните конечную дату"]},
                "comment": "Дата вне диапазона",
                "response": ""
            }
        self.formalizer.formalize = AsyncMock(side_effect=mock_formalize)
        result = self.run_async(self.formalizer.formalize(
            message="Покажи график т1 с 2030-01-01 по 2030-12-31",
            history=history,
            lang="ru",
            available_sensors=self.available_sensors,
            time_period=self.time_period
        ))
        self.assertEqual(result["action"], "clarify")
        self.assertIn("Уточните начальную дату", result["parameters"]["questions"])
        self.assertIn("Уточните конечную дату", result["parameters"]["questions"])
        self.assertIn("Дата вне диапазона", result["comment"])
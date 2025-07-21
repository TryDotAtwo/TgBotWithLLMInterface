# test_llm_core.py
import unittest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from Bot_core.llm_core import RequestFormalizer, SUPPORTED_ACTIONS, MESSAGES
from Analysis_core.data_reader import DataReader
from Utils.error_corrector import ErrorCorrector
import json

class TestRequestFormalizer(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.get_event_loop()
        self.data_reader = MagicMock(spec=DataReader)
        self.data_reader.get_sensor_info.return_value = [
            {"sensor_name": "T01 (DT12)", "index": 1, "data_type": "temperature", "source_files": ["db1.db"]},
            {"sensor_name": "T02 (DT13)", "index": 2, "data_type": "humidity", "source_files": ["db2.db"]}
        ]
        self.data_reader.get_time_period.return_value = {
            "start_time": "2023-01-01 00:00:00",
            "end_time": "2025-12-31 23:59:59"
        }
        self.error_corrector = MagicMock(spec=ErrorCorrector)
        self.formalizer = RequestFormalizer(self.data_reader, debug_mode=True)

    def run_async(self, coro):
        return self.loop.run_until_complete(coro)

    @patch("llm_core._llm_request")
    def test_free_response(self, mock_llm_request):
        mock_llm_request.side_effect = [
            json.dumps({"classification": "free"}),
            json.dumps({"action": "clarify", "comment": "Неформальный запрос"}),
            json.dumps({"context": "Разговорный вопрос"}),
            "Привет! Я в порядке, спасибо. Я могу строить графики, показывать данные датчиков и т.д."
        ]
        history = [
            {"message": "Привет, как дела?", "is_bot": False, "timestamp": "2025-06-07 20:00:00", "user_info": {}},
            {"message": "Я в порядке, а ты?", "is_bot": True, "timestamp": "2025-06-07 20:01:00", "user_info": {}}
        ]
        result = self.run_async(self.formalizer.formalize("Как дела?", history, "ru"))
        self.assertEqual(result["action"], "free_response")
        self.assertIn("Я могу", result["response"])
        self.assertEqual(result["comment"], "Свободный запрос")

    @patch("llm_core._llm_request")
    def test_formal_request_valid(self, mock_llm_request):
        mock_llm_request.side_effect = [
            json.dumps({"classification": "formal"}),
            json.dumps({"action": "plot_selected_sensor", "comment": "График для T01"}),
            json.dumps({"context": "График для T01 за май"}),
            json.dumps({"is_valid": True, "corrected_action": "plot_selected_sensor", "reason": "Действие подтверждено"}),
            json.dumps({"is_valid": True, "corrected_name": "T01 (DT12)", "reason": "Найден датчик"}),
            json.dumps({"is_valid": True, "corrected_date": "2025-05-01 00:00:00", "reason": "Начало мая"}),
            json.dumps({"is_valid": True, "corrected_date": "2025-05-31 23:59:59", "reason": "Конец мая"})
        ]
        history = [
            {"message": "Хочу график за май", "is_bot": False, "timestamp": "2025-06-07 20:00:00", "user_info": {}},
            {"message": "Уточните датчик", "is_bot": True, "timestamp": "2025-06-07 20:01:00", "user_info": {}}
        ]
        result = self.run_async(self.formalizer.formalize("Покажи график т1 за май", history, "ru"))
        self.assertEqual(result["action"], "plot_selected_sensor")
        self.assertEqual(result["parameters"]["sensor_name"], "T01 (DT12)")
        self.assertEqual(result["parameters"]["start_time"], "2025-05-01 00:00:00")
        self.assertEqual(result["parameters"]["end_time"], "2025-05-31 23:59:59")
        self.assertEqual(result["comment"], "График для T01")

    @patch("llm_core._llm_request")
    def test_formal_request_invalid_sensor(self, mock_llm_request):
        mock_llm_request.side_effect = [
            json.dumps({"classification": "formal"}),
            json.dumps({"action": "plot_selected_sensor", "comment": "График"}),
            json.dumps({"context": "График для неизвестного датчика"}),
            json.dumps({"is_valid": True, "corrected_action": "plot_selected_sensor", "reason": "Действие подтверждено"}),
            json.dumps({"is_valid": False, "corrected_name": "", "reason": "Датчик не найден"}),
            json.dumps({"is_valid": True, "corrected_date": "2025-05-01 00:00:00", "reason": "Начало мая"}),
            json.dumps({"is_valid": True, "corrected_date": "2025-05-31 23:59:59", "reason": "Конец мая"})
        ]
        self.error_corrector.correct.return_value = json.dumps({"is_valid": False, "corrected_name": "", "reason": "Не удалось исправить"})
        history = []
        result = self.run_async(self.formalizer.formalize("Покажи график т99 за май", history, "ru"))
        self.assertEqual(result["action"], "clarify")
        self.assertIn("Уточните датчик", result["parameters"]["questions"][0])
        self.assertIn("T01 (DT12), T02 (DT13)", result["parameters"]["questions"][0])

    @patch("llm_core._llm_request")
    def test_llm_failure(self, mock_llm_request):
        mock_llm_request.side_effect = TimeoutError("LLM не отвечает")
        history = []
        result = self.run_async(self.formalizer.formalize("Покажи график т1", history, "ru"))
        self.assertEqual(result["action"], "clarify")
        self.assertIn("Не удалось обработать запрос", result["comment"])

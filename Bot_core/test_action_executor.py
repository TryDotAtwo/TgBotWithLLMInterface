# -*- coding: utf-8 -*-
import pytest
import asyncio
from unittest.mock import AsyncMock, Mock
from datetime import datetime, timedelta
from pathlib import Path
import os
import json

import Bot_core.action_executor as ae_mod
from Bot_core.action_executor import ActionExecutor, CONFIG

# --- TEST CONFIG -------------------------------------------------------------
TEST_CONFIG = CONFIG.copy()
TEST_CONFIG["prompts"]["validate_action"] = (
    "Проверь, корректно ли действие:\n"
    "Действие: '{action}'\n"
    "Доступные действия: {supported_actions}\n\n"
    "Верни JSON: {{'is_valid': true, 'reason': ''}} или {{'is_valid': false, 'reason': 'описание'}}"
)
TEST_CONFIG["prompts"]["validate_sensor"] = (
    "Проверь, существует ли датчик, или найди ближайший:\n"
    "Имя датчика: '{sensor_name}'\n"
    "Доступные датчики: {available_sensors}\n\n"
    "Верни JSON: {{'is_valid': true, 'corrected_name': '{sensor_name}', 'reason': '', 'message': ''}} или "
    "{{'is_valid': false, 'reason': 'описание', 'corrected_name': '', 'message': 'текст для пользователя'}}"
)
TEST_CONFIG["prompts"]["validate_date"] = (
    "Исправь дату в формат YYYY-MM-DD HH:MM:SS, ближайшую к доступному периоду {start_time} — {end_time}:\n"
    "Дата: '{date}'\n\n"
    "Верни JSON: {{'is_valid': true, 'corrected_date': '{date}', 'reason': '', 'message': ''}} или "
    "{{'is_valid': false, 'reason': 'описание', 'corrected_date': '', 'message': 'текст для пользователя'}}"
)

# --- FIXTURES ---------------------------------------------------------------

@pytest.fixture
def temp_dir(tmp_path):
    return tmp_path

@pytest.fixture
def mock_data_processor(temp_dir):
    dp = Mock()
    dp.output_dir = temp_dir
    dp.get_sensor_info.return_value = [
        {"sensor_name": "TemperatureSensor1", "index": 1, "data_type": "float", "source_files": ["db1.db"]},
        {"sensor_name": "HumiditySensor2",    "index": 2, "data_type": "float", "source_files": ["db2.db"]}
    ]
    dp.get_time_period.return_value = {
        "start_time": "2023-01-01 00:00:00",
        "end_time":   "2023-12-31 23:59:59"
    }
    dp.plot_selected_sensor = Mock(return_value=None)
    dp.plot_random_sensor   = Mock(return_value=None)
    return dp

@pytest.fixture
def mock_error_corrector():
    ec = AsyncMock()
    def correct_side_effect(input_data, prompt_addition, user_id):
        if "sensor_name" in prompt_addition:
            if input_data in ["TemperatureSensor1", "HumiditySensor2"]:
                return json.dumps({"is_valid": True, "corrected_name": input_data, "reason": "", "message": ""})
            return json.dumps({"is_valid": False, "reason": f"Датчик {input_data} не найден", "corrected_name": "", "message": f"Датчик {input_data} не найден"})
        if "date" in prompt_addition:
            try:
                dt = datetime.strptime(input_data, "%Y-%m-%d %H:%M:%S")
                start = datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
                end   = datetime.strptime("2023-12-31 23:59:59", "%Y-%m-%d %H:%M:%S")
                if start <= dt <= end:
                    return json.dumps({"is_valid": True, "corrected_date": input_data, "reason": "", "message": ""})
                return json.dumps({"is_valid": False, "reason": "Дата вне периода", "corrected_date": "", "message": "Дата вне доступного периода"})
            except ValueError:
                return json.dumps({"is_valid": False, "reason": "Неверный формат даты", "corrected_date": "", "message": "Дата должна быть в формате YYYY-MM-DD HH:MM:SS"})
        if "action" in prompt_addition:
            supported = json.loads(prompt_addition.split("Доступные действия: ")[1].split("\n")[0])
            if input_data in supported:
                return json.dumps({"is_valid": True, "reason": ""})
            return json.dumps({"is_valid": False, "reason": f"Действие {input_data} не поддерживается"})
        return json.dumps({"is_valid": True, "reason": "", "message": ""})
    ec.correct.side_effect = correct_side_effect
    return ec

@pytest.fixture
def action_executor(mock_data_processor, mock_error_corrector, monkeypatch):
    # Подменяем CONFIG на TEST_CONFIG
    monkeypatch.setattr(ae_mod, "CONFIG", TEST_CONFIG)
    return ActionExecutor(mock_data_processor, mock_error_corrector, debug_mode=True)

# --- TESTS ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_sensor_info(action_executor):
    res = await action_executor.execute({
        "action": "get_sensor_info",
        "parameters": {},
        "comment": "Получить список датчиков"
    })
    assert res["result"] == ["TemperatureSensor1", "HumiditySensor2"]
    assert "validation_results" not in res

@pytest.mark.asyncio
async def test_get_sensor_info_empty(action_executor, mock_data_processor):
    mock_data_processor.get_sensor_info.return_value = []
    res = await action_executor.execute({
        "action": "get_sensor_info",
        "parameters": {},
        "comment": "Пустой список"
    })
    assert "validation_results" in res
    assert not res["validation_results"][0]["is_valid"]

@pytest.mark.asyncio
async def test_print_sensor_info_valid(action_executor):
    res = await action_executor.execute({
        "action": "print_sensor_info",
        "parameters": {"sensor_name": "TemperatureSensor1"},
        "comment": "Инфо по датчику"
    })
    assert res["result"]["sensor_name"] == "TemperatureSensor1"
    assert res["result"]["index"] == 1
    assert "2023-01-01 00:00:00" in res["result"]["period"]
    assert "2023-12-31 23:59:59" in res["result"]["period"]

@pytest.mark.asyncio
async def test_print_sensor_info_invalid_sensor(action_executor):
    res = await action_executor.execute({
        "action": "print_sensor_info",
        "parameters": {"sensor_name": "UnknownSensor"},
        "comment": "Несуществующий датчик"
    })
    assert "validation_results" in res
    assert not res["validation_results"][0]["is_valid"]

@pytest.mark.asyncio
async def test_plot_selected_sensor_valid(action_executor, temp_dir, mock_data_processor):
    plot_path = temp_dir / "sensor_plot.png"
    plot_path.touch()
    mock_data_processor.plot_selected_sensor.return_value = str(plot_path)
    res = await action_executor.execute({
        "action": "plot_selected_sensor",
        "parameters": {
            "sensor_name": "TemperatureSensor1",
            "start_time":  "2023-01-01 00:00:00",
            "end_time":    "2023-01-02 00:00:00"
        },
        "comment": "Построить график"
    })
    assert "result" in res and "plot_path" in res["result"]

@pytest.mark.asyncio
async def test_plot_selected_sensor_invalid_date(action_executor):
    res = await action_executor.execute({
        "action": "plot_selected_sensor",
        "parameters": {
            "sensor_name": "TemperatureSensor1",
            "start_time":  "2023-13-01 00:00:00",
            "end_time":    "2023-01-02 00:00:00"
        },
        "comment": "Неверная дата"
    })
    assert "validation_results" in res
    assert any("Неверный формат даты" in v["reason"] for v in res["validation_results"])

@pytest.mark.asyncio
async def test_plot_selected_sensor_out_of_period(action_executor):
    res = await action_executor.execute({
        "action": "plot_selected_sensor",
        "parameters": {
            "sensor_name": "TemperatureSensor1",
            "start_time":  "2022-01-01 00:00:00",
            "end_time":    "2022-01-02 00:00:00"
        },
        "comment": "Дата вне периода"
    })
    assert "validation_results" in res
    assert any("Дата вне периода" in v["reason"] for v in res["validation_results"])

@pytest.mark.asyncio
async def test_plot_selected_sensor_no_data(action_executor, mock_data_processor):
    mock_data_processor.plot_selected_sensor.return_value = None
    res = await action_executor.execute({
        "action": "plot_selected_sensor",
        "parameters": {
            "sensor_name": "TemperatureSensor1",
            "start_time":  "2023-01-01 00:00:00",
            "end_time":    "2023-01-02 00:00:00"
        },
        "comment": "Без данных"
    })
    assert "validation_results" in res
    assert any("Не удалось построить график" in v["reason"] for v in res["validation_results"])

@pytest.mark.asyncio
async def test_plot_random_sensor(action_executor, temp_dir, mock_data_processor):
    plot_path = temp_dir / "random_sensor_plot.png"
    plot_path.touch()
    mock_data_processor.plot_random_sensor.return_value = str(plot_path)
    res = await action_executor.execute({
        "action": "plot_random_sensor",
        "parameters": {},
        "comment": "Случайный график"
    })
    assert "result" in res and "plot_path" in res["result"]

@pytest.mark.asyncio
async def test_get_time_period(action_executor):
    res = await action_executor.execute({
        "action": "get_time_period",
        "parameters": {},
        "comment": "Временной период"
    })
    assert res["result"] == {
        "start_time": "2023-01-01 00:00:00",
        "end_time":   "2023-12-31 23:59:59"
    }

@pytest.mark.asyncio
async def test_missing_comment(action_executor):
    res = await action_executor.execute({
        "action": "get_sensor_info",
        "parameters": {}
    })
    assert "validation_results" in res
    assert not res["validation_results"][0]["is_valid"]

@pytest.mark.asyncio
async def test_invalid_action(action_executor):
    res = await action_executor.execute({
        "action": "invalid_action",
        "parameters": {},
        "comment": "Некорректное действие"
    })
    assert "validation_results" in res
    assert not res["validation_results"][0]["is_valid"]

@pytest.mark.asyncio
async def test_correct_error_invalid_json(action_executor, mock_error_corrector):
    mock_error_corrector.correct.side_effect = [
        "not json",
        json.dumps({"is_valid": False, "reason": "Fixed JSON", "message": "msg"})
    ]
    res = await action_executor.execute({
        "action": "print_sensor_info",
        "parameters": {"sensor_name": "UnknownSensor"},
        "comment": "fix json"
    })
    assert "validation_results" in res
    assert any("Fixed JSON" in v["reason"] for v in res["validation_results"])

@pytest.mark.asyncio
async def test_error_corrector_exception(action_executor, mock_error_corrector):
    mock_error_corrector.correct.side_effect = Exception("fail")
    res = await action_executor.execute({
        "action": "print_sensor_info",
        "parameters": {"sensor_name": "UnknownSensor"},
        "comment": "exc"
    })
    assert "validation_results" in res
    assert any("Ошибка валидации" in v["reason"] for v in res["validation_results"])

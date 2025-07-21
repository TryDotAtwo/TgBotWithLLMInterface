import pytest
import logging
from unittest.mock import patch
from Utils.error_corrector import ErrorCorrector

import asyncio

@pytest.fixture
def corrector_debug():
    return ErrorCorrector(debug_mode=True)

@pytest.fixture
def corrector_nodebug():
    return ErrorCorrector(debug_mode=False)

@pytest.mark.asyncio
async def test_correct_success(corrector_debug, caplog):
    caplog.set_level(logging.DEBUG)
    mock_response = "2025-04-12"
    with patch('g4f.ChatCompletion.create', return_value=mock_response):
        result = await corrector_debug.correct(
            input_data="Ошибка: некорректная дата 12.04.25",
            prompt_addition="Исправь дату в формат YYYY-MM-DD",
            user_id="user123"
        )
    assert result == mock_response
    assert any("Попытка коррекции для пользователя [ID: user123]" in record.message for record in caplog.records)

@pytest.mark.asyncio
async def test_correct_failure(corrector_debug, caplog):
    caplog.set_level(logging.DEBUG)
    with patch('g4f.ChatCompletion.create', side_effect=ValueError("Invalid response")):
        result = await corrector_debug.correct(
            input_data="Ошибка: сбойная дата",
            prompt_addition="Исправь дату",
            user_id="user456"
        )
    assert result is None
    assert any("Попытка коррекции для пользователя [ID: user456]" in record.message for record in caplog.records)

@pytest.mark.asyncio
async def test_retry_mechanism(corrector_debug, caplog):
    caplog.set_level(logging.DEBUG)
    mock_side_effect = [asyncio.TimeoutError(), asyncio.TimeoutError(), "2025-04-12"]
    with patch('g4f.ChatCompletion.create', side_effect=mock_side_effect):
        result = await corrector_debug.correct(
            input_data="Ошибка: таймаут даты",
            prompt_addition="Исправь дату в формат YYYY-MM-DD",
            user_id="user789"
        )
    assert result == "2025-04-12"
    assert any("Попытка коррекции для пользователя [ID: user789]" in record.message for record in caplog.records)

@pytest.mark.asyncio
async def test_all_retries_fail(corrector_debug, caplog):
    caplog.set_level(logging.DEBUG)
    with patch('g4f.ChatCompletion.create', side_effect=asyncio.TimeoutError()):
        result = await corrector_debug.correct(
            input_data="Ошибка: постоянный таймаут",
            prompt_addition="Исправь дату",
            user_id="user000"
        )
    assert result is None
    assert any("Попытка коррекции для пользователя [ID: user000]" in record.message for record in caplog.records)

@pytest.mark.asyncio
async def test_correct_llm_returns_none(corrector_debug, caplog):
    caplog.set_level(logging.DEBUG)
    with patch('g4f.ChatCompletion.create', return_value=None):
        result = await corrector_debug.correct(
            input_data="Ошибка: некорректная дата",
            prompt_addition="Исправь дату",
            user_id="user999"
        )
    assert result is None
    assert any("Попытка коррекции для пользователя [ID: user999]" in record.message for record in caplog.records)

@pytest.mark.asyncio
async def test_logging_no_debug_mode(corrector_nodebug, caplog):
    caplog.set_level(logging.CRITICAL)
    mock_response = "2025-04-12"
    with patch('g4f.ChatCompletion.create', return_value=mock_response):
        result = await corrector_nodebug.correct(
            input_data="Ошибка: некорректная дата 12.04.25",
            prompt_addition="Исправь дату в формат YYYY-MM-DD",
            user_id="user111"
        )
    assert result == mock_response
    assert any("Вызов коррекции" in record.message for record in caplog.records)

# -*- coding: utf-8 -*-
import asyncio
import logging
import g4f
from typing import Any, Optional, Dict
import traceback

CONFIG = {
    "logging": {
        "level_debug": logging.DEBUG,
        "level_critical": logging.CRITICAL,
        "format": "- %(levelname)s - %(message)s",
        "handlers": [logging.StreamHandler(), logging.FileHandler("Utils/error_corrector.log")]
    },
    "llm": {
        "timeout": 30,
        "model": "deepseek-r1-distill-llama-70b",
        "verify": False
    },
    "prompt": {
        "base": "Привет, твоя задача помочь в устранении ошибки. Отчет об ошибке: {input_data}. Просьба что нужно сделать: {prompt_addition}"
    },
    "retry": {
        "max_retries": 3,
        "retry_interval": 2
    },
    "error_messages": {
        "invalid_input": "Некорректный входной формат",
        "uncorrectable_input": "Не удалось исправить входные данные",
        "llm_attempt": "Попытка коррекции для пользователя [ID: {user_id}]",
        "llm_response": "Ответ от LLM",
        "retry_attempt": "Повторная попытка {attempt}/{max} из-за ошибки: {error}"
    }
}

def setup_logging(debug_mode: bool, logger: logging.Logger) -> None:
    level = CONFIG["logging"]["level_debug"] if debug_mode else CONFIG["logging"]["level_critical"]
    logger.setLevel(level)
    formatter = logging.Formatter(CONFIG["logging"]["format"])
    handlers = CONFIG["logging"]["handlers"]
    logger.handlers = []  # Очищаем старые обработчики
    for handler in handlers:
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.debug("Логирование настроено для ErrorCorrector с уровнем %s", "DEBUG" if debug_mode else "CRITICAL")

class ErrorCorrector:
    def __init__(self, debug_mode: bool = False, logger: logging.Logger = None):
        self.debug_mode = debug_mode
        self.logger = logger or logging.getLogger(__name__)
        self.llm_timeout = CONFIG["llm"]["timeout"]
        self.model = CONFIG["llm"]["model"]
        self.verify = CONFIG["llm"]["verify"]
        self.max_retries = CONFIG["retry"]["max_retries"]
        self.retry_interval = CONFIG["retry"]["retry_interval"]
        setup_logging(debug_mode, self.logger)
        self.logger.debug("ErrorCorrector инициализирован")

    async def _llm_request(self, prompt: str) -> Optional[str]:
        self.logger.debug("Запрос к LLM: %s", prompt)
        for attempt in range(self.max_retries):
            try:
                async with asyncio.timeout(self.llm_timeout):
                    response = await asyncio.to_thread(
                        g4f.ChatCompletion.create,
                        model=self.model,
                        messages=[{"role": "user", "content": prompt}],
                        verify=self.verify,
                    )
                    self.logger.debug(CONFIG["error_messages"]["llm_response"] + ": %s", response)
                    return response
            except Exception as e:
                error_msg = str(e)
                trace = traceback.format_exc()
                self.logger.error(CONFIG["error_messages"]["retry_attempt"].format(attempt=attempt + 1, max=self.max_retries, error=error_msg))
                self.logger.error("Трассировка стека: %s", trace)
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_interval)
                else:
                    self.logger.critical("%s: Ошибка %s", CONFIG["error_messages"]["uncorrectable_input"], error_msg)
                    return None
        return None

    async def correct(self, input_data: str, prompt_addition: str, user_id: Optional[str] = None) -> Optional[Any]:
        """Универсальная функция коррекции данных с использованием LLM."""
        user_id_str = user_id if user_id else "без ID"
        self.logger.debug(CONFIG["error_messages"]["llm_attempt"].format(user_id=user_id_str))
        self.logger.debug("Отчет об ошибке: %s", input_data)
        self.logger.debug("Промпт: %s", CONFIG["prompt"]["base"].format(input_data=input_data, prompt_addition=prompt_addition))

        try:
            prompt = CONFIG["prompt"]["base"].format(input_data=input_data, prompt_addition=prompt_addition)
            corrected = await self._llm_request(prompt)

            if corrected is None or "None" in corrected.strip():
                self.logger.error(CONFIG["error_messages"]["uncorrectable_input"])
                return None

            result = corrected.strip()
            self.logger.debug("Исправленный результат: %s", result)
            return result
        except Exception as e:
            self.logger.error("%s: Ошибка обработки ответа %s", CONFIG["error_messages"]["uncorrectable_input"], e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())
            return None
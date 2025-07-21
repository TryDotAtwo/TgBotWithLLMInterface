import asyncio
import json
import logging
import g4f
from typing import Dict, List, Any
import httpx
from datetime import datetime
import traceback

CONFIG = {
    "llm_model": "deepseek/deepseek-chat-v3-0324:free",
    "llm_timeout": 60,
    "retry_attempts": 2,
    "retry_interval": 2,
}

MESSAGES = {
    "ru": {
        "error": "Ошибка: {reason}",
        "functions": (
            "- Построить график: 'Нарисуй график для датчика T01 с 2023-04-03 по 2023-04-09'\n"
            "- Информация о датчике: 'Покажи информацию о датчике T23'\n"
            "- Временной диапазон: 'Какой временной диапазон?'\n"
            "- Случайный график: 'Нарисуй случайный график'\n"
            "- Список датчиков: 'Список датчиков'"
        ),
    },
    "en": {
        "error": "Error: {reason}",
        "functions": (
            "- Plot graph: 'Plot graph for sensor T01 from 2023-04-03 to 2023-04-09'\n"
            "- Sensor info: 'Show info for sensor T23'\n"
            "- Time range: 'What is the time range?'\n"
            "- Random graph: 'Plot a random graph'\n"
            "- Sensor list: 'List sensors'"
        ),
    },
}

SUPPORTED_ACTIONS = {
    "plot_selected_sensor": "Построить график для датчика за указанный период",
    "plot_random_sensor": "Показать график случайного датчика",
    "get_sensor_info": "Получить список доступных датчиков",
    "print_sensor_info": "Показать информацию о конкретном датчике",
    "get_time_period": "Показать временной диапазон данных",
    "clarify": "Задать уточняющие вопросы при неполных данных"
}

current_year = datetime.now().year

time_note = f"Если в дате не указан год, считаем, что используется текущий год — {current_year}. Если не указан период"

import aiohttp
import asyncio
import json
import logging
import re

async def _llm_request(prompt: str, debug_mode: bool, logger: logging.Logger):
    logger.debug("Запрос к LLM через OpenRouter: %s", prompt)
    url = "https://openrouter.ai/api/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {CONFIG['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']}",
        "Content-Type": "application/json",
    }

    payload = {
        "model": CONFIG["llm_model"],
        "messages": [{"role": "user", "content": prompt}],
    }

    for attempt in range(CONFIG["retry_attempts"] + 1):
        try:
            async with asyncio.timeout(CONFIG["llm_timeout"]):
                async with aiohttp.ClientSession() as session:
                    async with session.post(url, headers=headers, json=payload) as resp:
                        if resp.status != 200:
                            raise RuntimeError(f"Ошибка {resp.status}: {await resp.text()}")

                        response_json = await resp.json()
                        logger.debug(f"Сырой ответ LLM (попытка {attempt + 1}): %r", response_json)

                        content = response_json['choices'][0]['message']['content']

                        # Удаляем рекламный текст
                        clean_response = content.strip()
                        if "**Sponsor**" in clean_response:
                            clean_response = clean_response.split("**Sponsor**")[0].strip()

                        # Удаляем обрамление ```json и ```
                        clean_response = re.sub(r"^```json\s*|```$", "", clean_response, flags=re.IGNORECASE | re.MULTILINE).strip()

                        # Извлекаем JSON из строки
                        json_match = re.search(r'\{[\s\S]*\}', clean_response)
                        if not json_match:
                            logger.error("Не удалось извлечь JSON из ответа: %r", clean_response)
                            return json.dumps({
                                "action": "clarify",
                                "parameters": {"questions": ["Ответ LLM не содержит валидный JSON"]},
                                "comment": "Не удалось извлечь JSON"
                            })

                        raw_json = json_match.group(0)
                        raw_json = ''.join(c for c in raw_json if c.isprintable())

                        # Нормализация JSON
                        def normalize_json_text(json_text: str) -> str:
                            try:
                                raw_obj = json.loads(json_text)

                                def clean(obj):
                                    if isinstance(obj, dict):
                                        return {
                                            re.sub(r'\s+', '', k): clean(v)
                                            for k, v in obj.items()
                                        }
                                    elif isinstance(obj, list):
                                        return [clean(i) for i in obj]
                                    elif isinstance(obj, str):
                                        return re.sub(r'\s+', ' ', obj).strip()
                                    return obj

                                cleaned = clean(raw_obj)
                                return json.dumps(cleaned, ensure_ascii=False)

                            except Exception as e:
                                logger.warning(f"Ошибка нормализации JSON: {e}")
                                return json_text  # fallback

                        normalized_response = normalize_json_text(raw_json)
                        logger.debug("Ответ после нормализации: %r", normalized_response)

                        try:
                            json.loads(normalized_response)
                        except json.JSONDecodeError as e:
                            logger.error(f"Ошибка парсинга JSON: %s. Очищенный ответ: %r", e, normalized_response)
                            return json.dumps({
                                "action": "clarify",
                                "parameters": {"questions": ["Ответ LLM не удалось распарсить, попробуйте уточнить запрос"]},
                                "comment": f"Ошибка парсинга JSON: {e}"
                            })

                        return normalized_response

        except Exception as e:
            logger.error(f"Ошибка запроса к OpenRouter: %s", e, exc_info=True)
            if attempt < CONFIG["retry_attempts"]:
                await asyncio.sleep(CONFIG["retry_interval"])
            else:
                return json.dumps({
                    "is_valid": False,
                    "comment": f"Ошибка LLM: {e}",
                    "message": "Не удалось получить ответ"
                })

class RequestFormalizer:
    def __init__(
        self,
        data_reader,
        error_corrector,
        classifier=None,
        function_identifier=None,
        context_extractor=None,
        action_revalidator=None,
        field_formalizer=None,
        field_validators=None,
        free_response=None,
        llm_request_func=_llm_request,
        prompts=None,
        supported_actions=SUPPORTED_ACTIONS,
        available_sensors: List[str] = None,
        time_period: Dict[str, str] = None,
        debug_mode: bool = False,
        logger: logging.Logger = None
    ):
        self.data_reader = data_reader
        self.error_corrector = error_corrector
        self.llm_request_func = llm_request_func
        self.supported_actions = supported_actions
        self.available_sensors = available_sensors or []
        self.time_period = time_period or {}
        self.debug_mode = debug_mode
        self.logger = logger or logging.getLogger(__name__)

        try:
            if self.time_period and not all(key in self.time_period for key in ["start_time", "end_time"]):
                raise ValueError("time_period должен содержать ключи 'start_time' и 'end_time'")
            if self.time_period:
                datetime.strptime(self.time_period["start_time"], '%Y-%m-%d %H:%M:%S')
                datetime.strptime(self.time_period["end_time"], '%Y-%m-%d %H:%M:%S')
        except (ValueError, KeyError) as e:
            self.logger.error("Некорректный формат time_period: %s", str(e))
            raise ValueError(f"Некорректный формат time_period: {str(e)}")

        self.logger.debug("RequestFormalizer инициализирован")

    async def formalize(self, message: str, history: List[Dict], lang: str, available_sensors: List[str], time_period: Dict[str, str]) -> Dict:
        self.logger.debug("Формализация запроса: %s", message)
        if not message.strip():
            return await self.error_corrector.correct(
                input_data=message,
                prompt_addition="Пустой запрос пользователя. Верни JSON с action: 'clarify' и соответствующими вопросами.",
                user_id="empty_request"
            )

        functions = "\n".join(f"{action}: {desc}" for action, desc in self.supported_actions.items())
        history_str = ", ".join(f"{'Bot' if entry.get('is_bot', '') else 'User'}: {entry.get('message', '')}" for entry in history)
        available_sensors_str = ", ".join(available_sensors) if available_sensors else "не указаны"
        time_period_str = f"{time_period.get('start_time', '')}–{time_period.get('end_time', '')}" if time_period else "не указан"

        prompt = f"""\
                Инструкции:
                1. Определите тип запроса:
                   a) Формальный — запрос о датчиках, графиках, данных.
                   b) Свободный — всё остальное (разговорные фразы, приветствия, уточнения).

                2. Если запрос формальный:
                   2.1. Выберите действие из списка:
                       • plot_selected_sensor — построить график конкретного датчика.
                       • plot_random_sensor   — построить график случайного датчика.
                       • get_sensor_info      — вывести список всех датчиков.
                       • print_sensor_info    — вывести информацию о конкретном датчике.
                       • get_time_period      — показать доступный временной диапазон.
                       • clarify              — задать уточняющие вопросы.

                   2.2. Извлеките параметры (при необходимости):
                       • sensor_name — название датчика (из списка доступных).
                       • start_time  — начало периода в формате `YYYY-MM-DD HH:MM:SS`.
                       • end_time    — конец периода в формате `YYYY-MM-DD HH:MM:SS`.

                   2.3. Проверьте корректность:
                       • Датчик есть в списке? Если нет — попытайтесь исправить опечатку (например, «т8» - «T08 (T34)»).
                       • Даты попадают в допустимый диапазон `2025-04-14 12:29:47`…`2025-06-04 09:01:07`? 
                         – Если год не указан, добавьте 2025. 
                         – Если день или время не указаны, используйте начало/конец периода.

                   2.4. Результат:
                       a) Если всё найдено и валидировано:
                       ```json
                       {{
                         "action": "<выбранное_действие>",
                         "parameters": {{
                           "sensor_name": "…",
                           "start_time": "YYYY-MM-DD HH:MM:SS",
                           "end_time": "YYYY-MM-DD HH:MM:SS"
                         }},
                         "comment": "Датчик и даты извлечены и проверены"
                       }}
                       ```
                       b) Если чего-то не хватает или есть сомнения:
                       ```json
                       {{
                         "action": "clarify",
                         "parameters": {{
                           "questions": [
                             "Уточните, какой датчик вам нужен?",
                             "Пожалуйста, укажите период в формате YYYY-MM-DD HH:MM:SS"
                           ]
                         }},
                         "comment": "Не хватает параметров или они некорректны"
                       }}
                       ```

                3. Если запрос свободный:
                   • Сформируйте вежливый ответ на русском.
                   • Вставьте в ответ подсказки о возможностях:
                     ```
                     Я могу:
                     - Построить график: "Нарисуй график для датчика T01 с 2025-04-03 по 2025-04-09"
                     - Показать информацию о датчике: "Покажи информацию о датчике T23"
                     - Узнать временной диапазон: "Какой временной диапазон?"
                     - Показать случайный график: "Нарисуй случайный график"
                     - Вывести список датчиков: "Список датчиков"
                     ```
                   • Верните JSON:
                     ```json
                     {{
                       "action": "free_response",
                       "parameters": {{}},
                       "response": "<текст на русском>",
                       "comment": "Свободный запрос"
                     }}
                     ```

                """

        try:
            response = await self.llm_request_func(prompt, self.debug_mode, self.logger)
            print("\n\nОтвет LLM для парсинга:", response )  # чтобы видеть сырой ответ
            result = json.loads(response)

            print(result)
            print("\n\n\n\n")
            print()
            print()
            print()
            print()

            # Проверка обязательных полей
            if "action" not in result or "parameters" not in result or "comment" not in result:
                raise ValueError("Отсутствуют обязательные поля в ответе")

            # Валидация действия
            if result["action"] not in self.supported_actions and result["action"] != "free_response":
                self.logger.debug("Неподдерживаемое действие: %s", result["action"])
                result = {
                    "action": "clarify",
                    "parameters": {"questions": ["Уточните запрос, действие не поддерживается"]},
                    "comment": f"Неподдерживаемое действие: {result['action']}"
                }

            # Валидация параметров для формального запроса
            if result["action"] in ["plot_selected_sensor", "print_sensor_info"]:
                sensor_name = result["parameters"].get("sensor_name", "")
                if sensor_name and sensor_name not in available_sensors:
                    result = {
                        "action": "clarify",
                        "parameters": {"questions": [f"Уточните датчик, '{sensor_name}' не найден"]},
                        "comment": f"Датчик '{sensor_name}' не в списке доступных"
                    }

            if result["action"] == "plot_selected_sensor":
                start_time = result["parameters"].get("start_time", "")
                end_time = result["parameters"].get("end_time", "")
                try:
                    if start_time:
                        datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
                    if end_time:
                        datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    result = {
                        "action": "clarify",
                        "parameters": {"questions": ["Уточните даты, формат некорректен"]},
                        "comment": "Некорректный формат дат"
                    }

            self.logger.debug("Формализация завершена: %s", result)
            return result

        except (json.JSONDecodeError, ValueError) as e:
            self.logger.error("Ошибка обработки ответа LLM: %s", e)
            return {
                "action": "clarify",
                "parameters": {"questions": ["Произошла ошибка обработки запроса, уточните детали"]},
                "comment": f"Ошибка обработки: {str(e)}"
            }
        except Exception as e:
            self.logger.error("Общая ошибка формализации: %s", e)
            return await self.error_corrector.correct(
                input_data=message,
                prompt_addition="Произошла ошибка обработки запроса. Верни JSON с action: 'clarify' и соответствующими вопросами.",
                user_id=f"error_{message[:50]}"
            )

def create_request_formalizer(
    data_reader,
    error_corrector,
    available_sensors: List[str],
    time_period: Dict[str, str],
    debug_mode: bool = False,
    logger: logging.Logger = None
) -> RequestFormalizer:
    logger = logger or logging.getLogger(__name__)
    logger.debug("Создание RequestFormalizer")
    try:
        formalizer = RequestFormalizer(
            data_reader,
            error_corrector,
            available_sensors=available_sensors,
            time_period=time_period,
            debug_mode=debug_mode,
            logger=logger
        )
        logger.debug("RequestFormalizer успешно создан")
        return formalizer
    except Exception as e:
        logger.error("Ошибка создания RequestFormalizer: %s", e)
        raise
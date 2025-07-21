import asyncio
import json
import logging
import g4f
from typing import Dict, List, Any
import httpx
from datetime import datetime
import traceback

CONFIG = {
    "llm_model": "o4-mini",
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

current_year = datetime.now().year  # Например, 2025

time_note = f"Если в дате не указан год, считаем, что используется текущий год — {current_year}. Если не указан период"

PROMPTS = {
    "classify": (
        "Определи, является ли запрос '{0}' формальным или свободным. \n"
        "Формальный запрос связан с датчиками, графиками, временными диапазонами (например, 'Пришли график для т8', 'Покажи данные датчика T01'). \n"
        "Слова график, датчик, данные - это указание на формальный вопрос"
        "Свободный запрос — это разговорный запрос, не связанный с датчиками или графиками (например, 'Как дела?', 'Расскажи о погоде').\n "
        "Доступные функции:\n{1}\n"
        "Верни JSON: {{\"classification\": \"formal\" | \"free\", \"comment\": \"<пояснение>\"}}"
        ),
    "function": (
        "Какое действие соответствует запросу '{0}'? \n"
        "Доступные функции:\n{1}\n"
        "Верни JSON: {{\"action\": \"<действие>\", \"comment\": \"<пояснение>\"}} или {{\"action\": \"clarify\", \"comment\": \"Нужно уточнение\"}}"
    ),
    "context": (
        "Из истории '{0}' и запроса '{1}' выдели ключевую информацию.\n "
        "Имей в виду, что в истории могут быть сообщения не связанные с текущим запросом {0}\n"
        "Верни JSON: {{\"context\": \"<описание>\", \"comment\": \"<пояснение>\"}}"
    ),
    "extract_draft_sensor": (
        "Список доступных датчиков: {1}. \n"
        "Извлеки название датчика из запроса '{0}'. \n"
        "Верни JSON: {{\"sensor_name\": \"<имя или ''>\", \"comment\": \"<пояснение>\"}}"
        ),
    "extract_draft_start_time": (
        "Извлеки начальную дату из запроса '{0}'. \n"
        "Игнорируй строки, которые могут быть идентификаторами датчиков (например, 'т8', 'T01'). \n"
        f"{time_note}\n"
        "Если не указана дата, считаем, что указан май\n"
        "Верни JSON: {{\"start_time\": \"<дата или ''>\", \"comment\": \"<пояснение>\"}}"
    ),
    "extract_draft_end_time": (
        "Извлеки конечную дату из запроса '{0}'. \n"
        "Игнорируй строки, которые могут быть идентификаторами датчиков (например, 'т8', 'T01'). \n"
        f"{time_note}\n"
        "Если не указана дата, считаем, что указан май\n"
        "Верни JSON: {{\"end_time\": \"<дата или ''>\", \"comment\": \"<пояснение>\"}}"
    ),
    "formalize_sensor": (
        "Список доступных датчиков: {4}. \n"
        "Уточни название датчика на основе запроса '{0}', контекста '{1}', чернового значения '{2}' и комментария модели '{3}'. \n"
        "Верни JSON: {{\"sensor_name\": \"<имя или ''>\", \"comment\": \"<пояснение>\"}}"
    ),
    "formalize_start_time": (
        "Уточни начальную дату на основе запроса '{0}', контекста '{1}', чернового значения '{2}' и комментария модели '{3}'. \n"
        "Игнорируй строки, которые могут быть идентификаторами датчиков (например, 'т8', 'T01'). \n"
        f"{time_note}\n"
        "Верни JSON: {{\"start_time\": \"<дата или ''>\", \"comment\": \"<пояснение>\"}}"
    ),
    "formalize_end_time": (
        "Уточни конечную дату на основе запроса '{0}', контекста '{1}', чернового значения '{2}' и комментария модели '{3}'. \n"
        "Игнорируй строки, которые могут быть идентификаторами датчиков (например, 'т8', 'T01'). \n"
        f"{time_note}\n"
        "Верни JSON: {{\"end_time\": \"<дата или ''>\", \"comment\": \"<пояснение>\"}}"
    ),   
   "revalidate_action": (
        "Проверь, подходит ли действие '{0}' для запроса '{1}' с контекстом '{2}'. \n"
        "Слова график, датчик, данные - это указание на формальный вопрос"
        "Доступные функции:\n{3}\n"
        "Верни JSON: {{\"is_valid\": true/false, \"corrected_action\": \"<действие>\", \"comment\": \"<причина>\"}}"
    ),
    "validate_sensor": (
        "Проверь и приведи название датчика '{0}' к правильному формату, используя список датчиков: {1}. \n"
        "Пример: 'т1' - 'T01 (DT51)'. \n"
        "Обязательно используй точный формат из списка датчиков, включая скобки, например, 'T08 (T34)' или 'DP0 (Д1-Дозатор)'.\n"   
        "Верни JSON: {{\"is_valid\": true/false, \"corrected_name\": \"<имя>\", \"comment\": \"<причина>\"}}"
    ),
    "validate_start_time": (
        "Приведи дату '{0}' к формату 'YYYY-MM-DD HH:MM:SS', проверь диапазон {1}.\n "
        "Если HH:MM:SS не указаны - определи самостоятельно.\n "
        f"{time_note}\n"
        "Пример: 'май 2025' - '2025-05-01 00:00:00'. "
        "Верни JSON: {{\"is_valid\": true/false, \"corrected_date\": \"<дата>\", \"comment\": \"<причина>\"}}"
    ),
    "validate_end_time": (
        "Приведи дату '{0}' к формату 'YYYY-MM-DD HH:MM:SS', проверь диапазон {1}. \n"
        "Если HH:MM:SS не указаны - определи самостоятельно. \n"
        f"{time_note}\n"
        "Пример: 'май 2025' - '2025-05-31 23:59:59'. \n"
        "Верни JSON: {{\"is_valid\": true/false, \"corrected_date\": \"<дата>\", \"comment\": \"<причина>\"}}"
    ),
    "free_response": (
        "Ответь на '{0}' на языке '{1}' вежливо, учитывая контекст '{2}' и комментарий модели '{3}'. \n"
        "Доступные функции:\n{4}\n"
        "Добавь: 'Я могу {5}'. Верни текст ответа"
    ),
    "revalidate_classification": (
        "Проверь, является ли запрос '{0}' формальным или свободным, учитывая контекст '{1}' и начальную классификацию '{2}'.\n "
        "Формальный запрос связан с датчиками, графиками, временными диапазонами или техническими задачами (например, 'Пришли график для т8', 'Покажи данные датчика T01').\n "
        "Свободный запрос — это разговорный запрос, не связанный с датчиками или графиками (например, 'Как дела?', 'Расскажи о погоде').\n "
        "Доступные функции:\n{3}\n"
        "Верни JSON: {{\"classification\": \"formal\" | \"free\", \"comment\": \"<пояснение>\"}}"
        ),
}


async def _llm_request(prompt: str, debug_mode: bool, logger: logging.Logger) -> str:
    """Единая функция для запросов к LLM с повторными попытками."""
    logger.debug("Запрос к LLM: %s", prompt)
    for attempt in range(CONFIG["retry_attempts"] + 1):
        try:
            async with asyncio.timeout(CONFIG["llm_timeout"]):
                try:
                    supported_models = getattr(g4f, 'models', None)
                    if supported_models and isinstance(supported_models, (list, dict, set)):
                        if CONFIG["llm_model"] not in supported_models:
                            logger.error("Модель %s не поддерживается g4f", CONFIG["llm_model"])
                            return json.dumps({
                                "is_valid": False,
                                "comment": f"Модель {CONFIG['llm_model']} не поддерживается",
                                "message": "Ошибка конфигурации модели"
                            })
                    else:
                        logger.debug("Пропущена проверка моделей, так как g4f.models не является итерируемым")
                except Exception as e:
                    logger.error("Ошибка при проверке моделей: %s", e)
                    logger.debug("Продолжение без проверки моделей")
                
                response = await asyncio.to_thread(
                    g4f.ChatCompletion.create,
                    model=CONFIG["llm_model"],
                    messages=[{"role": "user", "content": prompt}],
                    verify=False,
                )
                response = response.strip()
                if response.startswith("```json"):
                    response = response.removeprefix("```json").removesuffix("```").strip()
                logger.debug("LLM ответ (попытка %d): %s", attempt + 1, response)
                try:
                    json.loads(response)
                except json.JSONDecodeError as e:
                    logger.error("Некорректный JSON в ответе LLM: %s", e)
                    response = json.dumps({
                        "is_valid": False,
                        "comment": f"Некорректный JSON в ответе LLM: {str(e)}",
                        "message": "Ошибка обработки ответа"
                    })
                return response
        except (TimeoutError, httpx.ConnectTimeout, Exception) as e:
            error_msg = f"{type(e).__name__}: {e}"
            trace = traceback.format_exc()
            logger.error("Ошибка LLM (попытка %d/%d): %s\nТрассировка стека: %s", attempt + 1, CONFIG["retry_attempts"], error_msg, trace)
            if attempt < CONFIG["retry_attempts"]:
                await asyncio.sleep(CONFIG["retry_interval"])
            else:
                logger.critical("Не удалось получить ответ от LLM после %d попыток", CONFIG["retry_attempts"])
                return json.dumps({
                    "is_valid": False,
                    "comment": f"Ошибка LLM {error_msg}",
                    "message": "Не удалось обработать запрос"
                })

class RequestClassifier:
    def __init__(self, llm_request_func, prompts: Dict[str, str], debug_mode: bool = False, logger: logging.Logger = None):
        self.llm_request_func = llm_request_func
        self.prompts = prompts
        self.debug_mode = debug_mode
        self.logger = logger or logging.getLogger(__name__)
        self.logger.debug("RequestClassifier инициализирован")

    async def classify(self, message: str, history: str, functions: str) -> Dict:
        self.logger.debug("Классификация запроса: %s", message)
        try:
            prompt = self.prompts["classify"].format(message, functions)
            response = await self.llm_request_func(prompt, self.debug_mode, self.logger)
            result = json.loads(response)
            if "classification" not in result:
                self.logger.error("Отсутствует ключ 'classification' в ответе: %s", response)
                raise ValueError("Отсутствует ключ 'classification'")
            self.logger.debug("Результат классификации: %s", result)
            return result
        except (KeyError, json.JSONDecodeError, ValueError) as e:
            self.logger.error("Ошибка классификации: %s", e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())
            return {"classification": "formal", "comment": f"Ошибка обработки: {str(e)}"}

    async def revalidate_classification(self, message: str, context: str, initial_classification: str, functions: str) -> Dict:
        self.logger.debug("Повторная валидация классификации для: %s", message)
        try:
            prompt = self.prompts["revalidate_classification"].format(message, context, initial_classification, functions)
            response = await self.llm_request_func(prompt, self.debug_mode, self.logger)
            result = json.loads(response)
            if "classification" not in result:
                self.logger.error("Отсутствует ключ 'classification' в ответе повторной валидации: %s", response)
                raise ValueError("Отсутствует ключ 'classification'")
            self.logger.debug("Результат повторной валидации классификации: %s", result)
            return result
        except (KeyError, json.JSONDecodeError, ValueError) as e:
            self.logger.error("Ошибка повторной валидации классификации: %s", e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())
            return {
                "classification": initial_classification,
                "comment": f"Ошибка повторной валидации, сохранена начальная классификация: {str(e)}"
            }

class FunctionIdentifier:
    def __init__(self, llm_request_func, prompts: Dict[str, str], debug_mode: bool = False, logger: logging.Logger = None):
        self.llm_request_func = llm_request_func
        self.prompts = prompts
        self.debug_mode = debug_mode
        self.logger = logger or logging.getLogger(__name__)
        self.logger.debug("FunctionIdentifier инициализирован")

    async def identify(self, message: str, functions: str) -> Dict:
        self.logger.debug("Определение функции для: %s", message)
        try:
            prompt = self.prompts["function"].format(message, functions)
            response = await self.llm_request_func(prompt, self.debug_mode, self.logger)
            result = json.loads(response)
            if "action" not in result or "comment" not in result:
                raise ValueError("Некорректный формат ответа")
            self.logger.debug("Определена функция: %s", result)
            return result
        except (KeyError, json.JSONDecodeError, ValueError) as e:
            self.logger.error("Ошибка определения функции: %s", e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())
            return {"action": "clarify", "comment": f"Ошибка обработки: {str(e)}"}

class ContextExtractor:
    def __init__(self, llm_request_func, prompts: Dict[str, str], debug_mode: bool = False, logger: logging.Logger = None):
        self.llm_request_func = llm_request_func
        self.prompts = prompts
        self.debug_mode = debug_mode
        self.logger = logger or logging.getLogger(__name__)
        self.logger.debug("ContextExtractor инициализирован")

    async def extract(self, message: str, history: str, sensors: str, time_period: str) -> Dict:
        self.logger.debug("Извлечение контекста для: %s", message)
        try:
            prompt = self.prompts["context"].format(history, message, sensors, time_period)
            response = await self.llm_request_func(prompt, self.debug_mode, self.logger)
            result = json.loads(response)
            if "context" not in result:
                raise ValueError("Отсутствует ключ 'context'")
            self.logger.debug("Контекст извлечён: %s", result)
            return result
        except (KeyError, json.JSONDecodeError, ValueError) as e:
            self.logger.error("Ошибка извлечения контекста: %s", e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())
            return {"context": "", "comment": f"Ошибка обработки: {str(e)}"}

class ActionRevalidator:
    def __init__(self, llm_request_func, prompts: Dict[str, str], debug_mode: bool = False, logger: logging.Logger = None):
        self.llm_request_func = llm_request_func
        self.prompts = prompts
        self.debug_mode = debug_mode
        self.logger = logger or logging.getLogger(__name__)
        self.logger.debug("ActionRevalidator инициализирован")

    async def revalidate(self, action: str, message: str, context: str, functions: str) -> Dict:
        self.logger.debug("Повторная валидация действия: %s", action)
        try:
            prompt = self.prompts["revalidate_action"].format(action, message, context, functions)
            response = await self.llm_request_func(prompt, self.debug_mode, self.logger)
            result = json.loads(response)
            if "is_valid" not in result or "corrected_action" not in result:
                raise ValueError("Некорректный формат ответа")
            self.logger.debug("Результат валидации действия: %s", result)
            return result
        except (KeyError, json.JSONDecodeError, ValueError) as e:
            self.logger.error("Ошибка валидации действия: %s", e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())
            return {"is_valid": False, "corrected_action": "clarify", "comment": f"Ошибка обработки: {str(e)}"}

class FieldFormalizer:
    def __init__(self, llm_request_func, prompts: Dict[str, str], available_sensors: List[str], debug_mode: bool = False, logger: logging.Logger = None):
        self.llm_request_func = llm_request_func
        self.prompts = prompts
        self.available_sensors = available_sensors
        self.debug_mode = debug_mode
        self.logger = logger or logging.getLogger(__name__)
        self.logger.debug("FieldFormalizer инициализирован")

    async def formalize_sensor(self, message: str, context: str, draft_sensor: str, comment: str) -> Dict:
        self.logger.debug("Формализация датчика: %s", draft_sensor)
        try:
            prompt = self.prompts["formalize_sensor"].format(message, context, draft_sensor, comment, ", ".join(self.available_sensors))
            response = await self.llm_request_func(prompt, self.debug_mode, self.logger)
            result = json.loads(response)
            self.logger.debug("Результат формализации датчика: %s", result)
            return result
        except (KeyError, json.JSONDecodeError, ValueError) as e:
            self.logger.error("Ошибка формализации датчика: %s", e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())
            return {"sensor_name": "", "comment": f"Ошибка обработки: {str(e)}"}

    async def formalize_start_time(self, message: str, context: str, draft_time: str, comment: str) -> Dict:
        self.logger.debug("Формализация начальной даты: %s", draft_time)
        try:
            prompt = self.prompts["formalize_start_time"].format(message, context, draft_time, comment)
            response = await self.llm_request_func(prompt, self.debug_mode, self.logger)
            result = json.loads(response)
            self.logger.debug("Результат формализации начальной даты: %s", result)
            return result
        except (KeyError, json.JSONDecodeError, ValueError) as e:
            self.logger.error("Ошибка формализации начальной даты: %s", e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())
            return {"start_time": "", "comment": f"Ошибка обработки: {str(e)}"}

    async def formalize_end_time(self, message: str, context: str, draft_time: str, comment: str) -> Dict:
        self.logger.debug("Формализация конечной даты: %s", draft_time)
        try:
            prompt = self.prompts["formalize_end_time"].format(message, context, draft_time, comment)
            response = await self.llm_request_func(prompt, self.debug_mode, self.logger)
            result = json.loads(response)
            self.logger.debug("Результат формализации конечной даты: %s", result)
            return result
        except (KeyError, json.JSONDecodeError, ValueError) as e:
            self.logger.error("Ошибка формализации конечной даты: %s", e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())
            return {"end_time": "", "comment": f"Ошибка обработки: {str(e)}"}

class FieldValidators:
    def __init__(self, llm_request_func, prompts: Dict[str, str], debug_mode: bool = False, logger: logging.Logger = None):
        self.llm_request_func = llm_request_func
        self.prompts = prompts
        self.debug_mode = debug_mode
        self.logger = logger or logging.getLogger(__name__)
        self.logger.debug("FieldValidators инициализирован")

    async def validate_sensor(self, sensor_name: str, sensors: List[str]) -> Dict:
        self.logger.debug("Валидация датчика: %s", sensor_name)
        if not sensor_name:
            self.logger.debug("Не указан датчик")
            return {"is_valid": False, "corrected_name": "", "comment": "Не указан датчик"}
        try:
            prompt = self.prompts["validate_sensor"].format(sensor_name, ", ".join(sensors))
            response = await self.llm_request_func(prompt, self.debug_mode, self.logger)
            result = json.loads(response)
            self.logger.debug("Результат валидации датчика: %s", result)
            return result
        except (KeyError, json.JSONDecodeError, ValueError) as e:
            self.logger.error("Ошибка валидации датчика: %s", e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())
            return {"is_valid": False, "corrected_name": "", "comment": f"Ошибка обработки: {str(e)}"}

    async def validate_start_time(self, date: str, time_period: Dict) -> Dict:
        self.logger.debug("Валидация начальной даты: %s", date)
        if not date:
            self.logger.debug("Не указана начальная дата")
            return {"is_valid": False, "corrected_date": "", "comment": "Не указана дата"}
        time_period_str = f"{time_period['start_time']}–{time_period['end_time']}"
        try:
            prompt = self.prompts["validate_start_time"].format(date, time_period_str)
            response = await self.llm_request_func(prompt, self.debug_mode, self.logger)
            result = json.loads(response)
            self.logger.debug("Результат валидации начальной даты: %s", result)
            return result
        except (KeyError, json.JSONDecodeError, ValueError) as e:
            self.logger.error("Ошибка валидации начальной даты: %s", e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())
            return {"is_valid": False, "corrected_date": "", "comment": f"Ошибка обработки: {str(e)}"}

    async def validate_end_time(self, date: str, time_period: Dict) -> Dict:
        self.logger.debug("Валидация конечной даты: %s", date)
        if not date:
            self.logger.debug("Не указана конечная дата")
            return {"is_valid": False, "corrected_date": "", "comment": "Не указана дата"}
        time_period_str = f"{time_period['start_time']}–{time_period['end_time']}"
        try:
            prompt = self.prompts["validate_end_time"].format(date, time_period_str)
            response = await self.llm_request_func(prompt, self.debug_mode, self.logger)
            result = json.loads(response)
            self.logger.debug("Результат валидации конечной даты: %s", result)
            return result
        except (KeyError, json.JSONDecodeError, ValueError) as e:
            self.logger.error("Ошибка валидации конечной даты: %s", e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())
            return {"is_valid": False, "corrected_date": "", "comment": f"Ошибка обработки: {str(e)}"}

class FreeResponseGenerator:
    def __init__(self, llm_request_func, prompts: Dict[str, str], messages: Dict[str, Dict], debug_mode: bool = False, logger: logging.Logger = None):
        self.llm_request_func = llm_request_func
        self.prompts = prompts
        self.messages = messages
        self.debug_mode = debug_mode
        self.logger = logger or logging.getLogger(__name__)
        self.logger.debug("FreeResponseGenerator инициализирован")

    async def generate(self, message: str, context: str, lang: str, functions: str, comment: str) -> str:
        self.logger.debug("Генерация свободного ответа для: %s", message)
        functions_summary = self.messages.get(lang, self.messages["en"])["functions"]
        try:
            prompt = self.prompts["free_response"].format(message, lang, context, comment, functions, functions_summary)
            response = await self.llm_request_func(prompt, self.debug_mode, self.logger)
            if len(response) > 4096:
                response = response[:4090] + "..."
                self.logger.debug("Ответ обрезан до 4096 символов")
            self.logger.debug("Сгенерирован ответ: %s", response)
            return response
        except Exception as e:
            self.logger.error("Ошибка генерации свободного ответа: %s", e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())
            return f"Извините, произошла ошибка при обработке запроса: {str(e)}"

class RequestFormalizer:
    def __init__(
        self,
        data_reader,
        error_corrector,
        classifier: RequestClassifier,
        function_identifier: FunctionIdentifier,
        context_extractor: ContextExtractor,
        action_revalidator: ActionRevalidator,
        field_formalizer: FieldFormalizer,
        field_validators: FieldValidators,
        free_response: FreeResponseGenerator,
        llm_request_func,
        prompts: Dict[str, str],
        supported_actions: Dict[str, str],
        available_sensors: List[str],
        time_period: Dict[str, str],
        debug_mode: bool = False,
        logger: logging.Logger = None
    ):
        self.data_reader = data_reader
        self.error_corrector = error_corrector
        self.classifier = classifier
        self.function_identifier = function_identifier
        self.context_extractor = context_extractor
        self.action_revalidator = action_revalidator
        self.field_formalizer = field_formalizer
        self.field_validators = field_validators
        self.free_response = free_response
        self.llm_request_func = llm_request_func
        self.prompts = prompts
        self.supported_actions = supported_actions
        self.available_sensors = available_sensors
        self.time_period = time_period
        self.debug_mode = debug_mode
        self.logger = logger or logging.getLogger(__name__)

        try:
            if not all(key in time_period for key in ["start_time", "end_time"]):
                raise ValueError("time_period должен содержать ключи 'start_time' и 'end_time'")
            datetime.strptime(time_period["start_time"], '%Y-%m-%d %H:%M:%S')
            datetime.strptime(time_period["end_time"], '%Y-%m-%d %H:%M:%S')
        except (ValueError, KeyError) as e:
            self.logger.error("Некорректный формат time_period: %s", str(e))
            raise ValueError(f"Некорректный формат time_period: {str(e)}")

        self.logger.debug("RequestFormalizer инициализирован")

    async def extract_draft_parameters(self, message: str) -> Dict:
        self.logger.debug("Черновое извлечение параметров из: %s", message)
        try:
            sensor_prompt = self.prompts["extract_draft_sensor"].format(message, ", ".join(self.available_sensors))
            start_time_prompt = self.prompts["extract_draft_start_time"].format(message)
            end_time_prompt = self.prompts["extract_draft_end_time"].format(message)
            sensor_response, start_time_response, end_time_response = await asyncio.gather(
                self.llm_request_func(sensor_prompt, self.debug_mode, self.logger),
                self.llm_request_func(start_time_prompt, self.debug_mode, self.logger),
                self.llm_request_func(end_time_prompt, self.debug_mode, self.logger)
            )
            sensor_result = json.loads(sensor_response)
            start_time_result = json.loads(start_time_response)
            end_time_result = json.loads(end_time_response)
            result = {
                "sensor_name": sensor_result.get("sensor_name", ""),
                "start_time": start_time_result.get("start_time", ""),
                "end_time": end_time_result.get("end_time", ""),
                "comment": "; ".join([
                    sensor_result.get("comment", ""),
                    start_time_result.get("comment", ""),
                    end_time_result.get("comment", "")
                ]).strip("; ")
            }
            self.logger.debug("Черновые параметры: %s", result)
            return result
        except Exception as e:
            self.logger.error("Ошибка извлечения параметров: %s", e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())
            return {"sensor_name": "", "start_time": "", "end_time": "", "comment": f"Ошибка обработки: {str(e)}"}

    async def formalize(self, message: str, history: List[Dict], lang: str, available_sensors: List[str], time_period: Dict[str, str]) -> Dict:
        self.logger.debug("Формализация запроса: %s", message)
        if not message.strip():
            self.logger.debug("Пустой запрос")
            return await self.error_corrector.correct(
                input_data=message,
                prompt_addition="Пустой запрос пользователя. Верни JSON с action: 'clarify' и соответствующими вопросами.",
                user_id="empty_request"
            )

        functions = "\n".join(f"{action}: {desc}" for action, desc in self.supported_actions.items())
        history_str = "\n".join(f"{'Bot' if entry.get('is_bot', False) else 'User'}: {entry.get('message', '')}" for entry in history)

        try:
            # Этап 1: Параллельное выполнение classify, function, context, extract_draft
            classification_task = self.classifier.classify(message, history_str, functions)
            function_task = self.function_identifier.identify(message, functions)
            context_task = self.context_extractor.extract(message, history_str, ", ".join(available_sensors), f"{time_period['start_time']}–{time_period['end_time']}")
            draft_params_task = self.extract_draft_parameters(message)
            classification, function, context, draft_params = await asyncio.gather(
                classification_task, function_task, context_task, draft_params_task
            )
            self.logger.debug("Этап 1: classification=%s, function=%s, context=%s, draft_params=%s", classification, function, context, draft_params)

            comments = [
                classification.get("comment", ""),
                function.get("comment", ""),
                context.get("comment", ""),
                draft_params.get("comment", "")
            ]

            # Этап 2: Повторная валидация классификации и формализация всех параметров
            sensor_formalize_task = self.field_formalizer.formalize_sensor(
                message, context.get("context", ""), draft_params.get("sensor_name", ""), "; ".join(filter(None, comments))
            )
            start_time_formalize_task = self.field_formalizer.formalize_start_time(
                message, context.get("context", ""), draft_params.get("start_time", ""), "; ".join(filter(None, comments))
            )
            end_time_formalize_task = self.field_formalizer.formalize_end_time(
                message, context.get("context", ""), draft_params.get("end_time", ""), "; ".join(filter(None, comments))
            )
            revalidation_classification_task = self.classifier.revalidate_classification(
                message, context.get("context", ""), classification.get("classification", "formal"), functions
            )
            revalidation_task = self.action_revalidator.revalidate(
                function.get("action", "clarify"), message, context.get("context", ""), functions
            )
            sensor_formalized, start_time_formalized, end_time_formalized, revalidated_classification, revalidation = await asyncio.gather(
                sensor_formalize_task, start_time_formalize_task, end_time_formalize_task, revalidation_classification_task, revalidation_task
            )
            self.logger.debug("Этап 2: revalidated_classification=%s, sensor_formalized=%s, start_time_formalized=%s, end_time_formalized=%s, revalidation=%s",
                             revalidated_classification, sensor_formalized, start_time_formalized, end_time_formalized, revalidation)

            comments.extend([
                revalidated_classification.get("comment", ""),
                sensor_formalized.get("comment", ""),
                start_time_formalized.get("comment", ""),
                end_time_formalized.get("comment", ""),
                revalidation.get("comment", "")
            ])

            # Проверка результата повторной валидации классификации
            if revalidated_classification.get("classification") == "free":
                response = await self.free_response.generate(
                    message, context.get("context", ""), lang, functions, "; ".join(filter(None, comments))
                )
                self.logger.debug("Сгенерирован свободный ответ: %s", response)
                return {
                    "action": "free_response",
                    "response": response,
                    "comment": "; ".join(filter(None, comments))
                }

            # Если классификация формальная, продолжаем обработку
            action = revalidation.get("corrected_action", function.get("action", "clarify"))
            comment = revalidation.get("comment", function.get("comment", "Неопределенный запрос"))

            # Этап 3: Параллельное выполнение валидации всех параметров
            parameters = {
                "sensor_name": sensor_formalized.get("sensor_name", ""),
                "start_time": start_time_formalized.get("start_time", ""),
                "end_time": end_time_formalized.get("end_time", "")
            }

            sensor_validate_task = self.field_validators.validate_sensor(parameters["sensor_name"], available_sensors)
            start_time_validate_task = self.field_validators.validate_start_time(parameters["start_time"], time_period)
            end_time_validate_task = self.field_validators.validate_end_time(parameters["end_time"], time_period)
            sensor_validated, start_time_validated, end_time_validated = await asyncio.gather(
                sensor_validate_task, start_time_validate_task, end_time_validate_task
            )
            self.logger.debug("Этап 3: sensor_validated=%s, start_time_validated=%s, end_time_validated=%s",
                             sensor_validated, start_time_validated, end_time_validated)

            comments.extend([
                sensor_validated.get("comment", ""),
                start_time_validated.get("comment", ""),
                end_time_validated.get("comment", "")
            ])

            # Этап 4: Проверка корректности только необходимых полей
            required_params = {
                "plot_selected_sensor": ["sensor_name", "start_time", "end_time"],
                "print_sensor_info": ["sensor_name"],
                "plot_random_sensor": [],
                "get_sensor_info": [],
                "get_time_period": []
            }.get(action, [])

            final_parameters = {}
            correction_needed = False
            correction_data = []
            correction_comments = []
            retry_count = 0
            max_retries = 2

            # Проверка действия
            if action not in self.supported_actions:
                correction_needed = True
                correction_data.append({
                    "field": "action",
                    "value": action,
                    "prompt": f"Проверь, корректно ли действие '{action}'. Доступные действия: {json.dumps(list(self.supported_actions.keys()), ensure_ascii=False)}. "
                              f"Верни JSON: {{'is_valid': true/false, 'corrected_action': '<действие>', 'comment': '<причина>'}}"
                })
                correction_comments.append(f"Неподдерживаемое действие: {action}")

            # Проверка датчика
            if "sensor_name" in required_params:
                sensor_name = sensor_validated.get("corrected_name", parameters["sensor_name"])
                if sensor_name and sensor_name in available_sensors:
                    final_parameters["sensor_name"] = sensor_name
                else:
                    # Предварительная попытка исправления имени датчика без LLM
                    corrected_sensor = None
                    if sensor_name:
                        # Нормализация: убираем лишние пробелы, заменяем 'Т' на 'T', приводим к верхнему регистру
                        normalized_sensor = sensor_name.strip().replace('Т', 'T').upper()
                        # Проверяем возможные форматы и опечатки
                        for available_sensor in available_sensors:
                            main_sensor_part = available_sensor.split(' (')[0].strip().upper()
                            alt_sensor_part = available_sensor[available_sensor.find('(')+1:available_sensor.find(')')].upper().replace(' ', '') if '(' in available_sensor else ''
                            # Точное совпадение основной части (например, 'T08' из 'T08 (T34)')
                            if normalized_sensor == main_sensor_part:
                                corrected_sensor = available_sensor
                                break
                            # Совпадение альтернативного обозначения (например, 'T34' или 'T08T34' из 'T08 (T34)')
                            if alt_sensor_part and (normalized_sensor == alt_sensor_part or normalized_sensor.replace(' ', '') == f"{main_sensor_part}{alt_sensor_part}"):
                                corrected_sensor = available_sensor
                                break
                            # Совпадение с форматом T01-T24 (например, 'T8' -> 'T08')
                            if normalized_sensor in [f"T{i:02d}" for i in range(1, 25)] or normalized_sensor.lstrip('T').zfill(2) in [f"{i:02d}" for i in range(1, 25)]:
                                target_sensor = f"T{normalized_sensor.lstrip('T').zfill(2)}"
                                corrected_sensor = next((s for s in available_sensors if s.split(' (')[0].strip().upper() == target_sensor), None)
                                break

                    if corrected_sensor and corrected_sensor in available_sensors:
                        final_parameters["sensor_name"] = corrected_sensor
                        comments.append(f"Датчик исправлен с '{sensor_name}' на '{corrected_sensor}' через предварительную нормализацию")
                        self.logger.debug("Датчик исправлен через нормализацию: %s -> %s", sensor_name, corrected_sensor)
                    else:
                        # Проверка результата валидации датчика
                        if sensor_validated.get("is_valid") and sensor_validated.get("corrected_name") in available_sensors:
                            final_parameters["sensor_name"] = sensor_validated["corrected_name"]
                            comments.append(f"Датчик исправлен с '{sensor_name}' на '{sensor_validated['corrected_name']}' через валидацию")
                            self.logger.debug("Датчик исправлен через валидацию: %s -> %s", sensor_name, sensor_validated["corrected_name"])
                        else:
                            # Если нормализация и валидация не помогли, добавляем в correction_data
                            correction_needed = True
                            correction_data.append({
                                "field": "sensor_name",
                                "value": sensor_name,
                                "prompt": self.prompts["validate_sensor"].format(sensor_name, ", ".join(available_sensors))
                            })
                            correction_comments.append(f"Датчик '{sensor_name}' отсутствует в списке доступных: {', '.join(available_sensors)}")
                            self.logger.debug("Датчик не исправлен через нормализацию или валидацию, передан на коррекцию: %s", sensor_name)

            # Проверка начальной даты
            if "start_time" in required_params:
                start_time = start_time_validated.get("corrected_date", parameters["start_time"])
                if start_time:
                    if len(start_time.split()) == 1:
                        start_time += " 00:00:00"
                    try:
                        start_dt = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
                        start_range = datetime.strptime(time_period["start_time"], '%Y-%m-%d %H:%M:%S')
                        end_range = datetime.strptime(time_period["end_time"], '%Y-%m-%d %H:%M:%S')
                        if start_range <= start_dt <= end_range:
                            final_parameters["start_time"] = start_time
                        else:
                            correction_needed = True
                            correction_data.append({
                                "field": "start_time",
                                "value": start_time,
                                "prompt": self.prompts["validate_start_time"].format(start_time, f"{time_period['start_time']}–{time_period['end_time']}")
                            })
                            correction_comments.append(f"Начальная дата {start_time} вне диапазона {time_period['start_time']}–{time_period['end_time']}")
                    except ValueError as e:
                        correction_needed = True
                        correction_data.append({
                            "field": "start_time",
                            "value": start_time,
                            "prompt": self.prompts["validate_start_time"].format(start_time, f"{time_period['start_time']}–{time_period['end_time']}")
                        })
                        correction_comments.append(f"Ошибка формата начальной даты {start_time}: {str(e)}")
                else:
                    correction_needed = True
                    correction_data.append({
                        "field": "start_time",
                        "value": "",
                        "prompt": self.prompts["validate_start_time"].format("", f"{time_period['start_time']}–{time_period['end_time']}")
                    })
                    correction_comments.append("Начальная дата не указана")

            # Проверка конечной даты
            if "end_time" in required_params:
                end_time = end_time_validated.get("corrected_date", parameters["end_time"])
                if end_time:
                    if len(end_time.split()) == 1:
                        end_time += " 23:59:59"
                    try:
                        end_dt = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
                        start_range = datetime.strptime(time_period["start_time"], '%Y-%m-%d %H:%M:%S')
                        end_range = datetime.strptime(time_period["end_time"], '%Y-%m-%d %H:%M:%S')
                        if start_range <= end_dt <= end_range:
                            final_parameters["end_time"] = end_time
                        else:
                            correction_needed = True
                            correction_data.append({
                                "field": "end_time",
                                "value": end_time,
                                "prompt": self.prompts["validate_end_time"].format(end_time, f"{time_period['start_time']}–{time_period['end_time']}")
                            })
                            correction_comments.append(f"Конечная дата {end_time} вне диапазона {time_period['start_time']}–{time_period['end_time']}")
                    except ValueError as e:
                        correction_needed = True
                        correction_data.append({
                            "field": "end_time",
                            "value": end_time,
                            "prompt": self.prompts["validate_end_time"].format(end_time, f"{time_period['start_time']}–{time_period['end_time']}")
                        })
                        correction_comments.append(f"Ошибка формата конечной даты {end_time}: {str(e)}")
                else:
                    correction_needed = True
                    correction_data.append({
                        "field": "end_time",
                        "value": "",
                        "prompt": self.prompts["validate_end_time"].format("", f"{time_period['start_time']}–{time_period['end_time']}")
                    })
                    correction_comments.append("Конечная дата не указана")

            # Если требуется коррекция, отправляем отдельные запросы для каждого поля
            if correction_needed and retry_count < max_retries:
                self.logger.debug("Требуется коррекция (попытка %d/%d): %s", retry_count + 1, max_retries, correction_comments)
                corrected_action = action
                corrected_parameters = parameters.copy()

                for error in correction_data:
                    field = error["field"]
                    value = error["value"]
                    prompt = error["prompt"]
                    correction_input = json.dumps({
                        "message": message,
                        "context": context.get("context", ""),
                        "parameters": parameters,
                        "error": {"field": field, "value": value, "prompt": prompt},
                        "comment": f"Ошибка валидации {field}: {correction_comments[correction_data.index(error)]}"
                    }, ensure_ascii=False)

                    self.logger.debug("Отправка коррекции для поля %s: %s", field, correction_input)
                    corrected = await self.error_corrector.correct(
                        input_data=correction_input,
                        prompt_addition=prompt,
                        user_id=f"correction_{field}_{message[:50]}_retry_{retry_count}"
                    )

                    try:
                        result = json.loads(corrected)
                        self.logger.debug("Результат коррекции поля %s: %s", field, result)
                        if field == "action":
                            corrected_action = result.get("corrected_action", action)
                            if corrected_action not in self.supported_actions:
                                correction_comments.append(f"Исправленное действие {corrected_action} не поддерживается")
                                continue
                        elif field == "sensor_name":
                            sensor_name = result.get("corrected_name", value)
                            if sensor_name in available_sensors:
                                corrected_parameters["sensor_name"] = sensor_name
                            else:
                                correction_comments.append(f"Исправленный датчик {sensor_name} не найден")
                                continue


                        elif field == "start_time":
                            start_time = result.get("corrected_date", value)
                            try:
                                start_dt = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
                                start_range = datetime.strptime(time_period["start_time"], '%Y-%m-%d %H:%M:%S')
                                end_range = datetime.strptime(time_period["end_time"], '%Y-%m-%d %H:%M:%S')
                                if start_range <= start_dt <= end_range:
                                    corrected_parameters["start_time"] = start_time
                                else:
                                    correction_comments.append(f"Исправленная начальная дата {start_time} вне диапазона")
                                    continue
                            except ValueError:
                                correction_comments.append(f"Ошибка формата исправленной начальной даты {start_time}")
                                continue
                        elif field == "end_time":
                            end_time = result.get("corrected_date", value)
                            try:
                                end_dt = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
                                start_range = datetime.strptime(time_period["start_time"], '%Y-%m-%d %H:%M:%S')
                                end_range = datetime.strptime(time_period["end_time"], '%Y-%m-%d %H:%M:%S')
                                if start_range <= end_dt <= end_range:
                                    corrected_parameters["end_time"] = end_time
                                else:
                                    correction_comments.append(f"Исправленная конечная дата {end_time} вне диапазона")
                                    continue
                            except ValueError:
                                correction_comments.append(f"Ошибка формата исправленной конечной даты {end_time}")
                                continue
                    except json.JSONDecodeError:
                        self.logger.error("Ошибка парсинга ответа error_corrector для поля %s: %s", field, corrected)
                        correction_comments.append(f"Ошибка формата JSON в ответе для поля {field}")
                        continue

                retry_count += 1
                action = corrected_action
                parameters = corrected_parameters

                # Повторная проверка параметров после коррекции
                correction_needed = False
                correction_data = []
                if action not in self.supported_actions:
                    correction_needed = True
                    correction_data.append({
                        "field": "action",
                        "value": action,
                        "prompt": f"Проверь, корректно ли действие '{action}'. Доступные действия: {json.dumps(list(self.supported_actions.keys()), ensure_ascii=False)}"
                    })
                    correction_comments.append(f"Неподдерживаемое действие: {action}")
                if "sensor_name" in required_params and parameters.get("sensor_name") not in available_sensors:
                    correction_needed = True
                    correction_data.append({
                        "field": "sensor_name",
                        "value": parameters.get("sensor_name", ""),
                        "prompt": self.prompts["validate_sensor"].format(parameters.get("sensor_name", ""), ", ".join(available_sensors))
                    })
                    correction_comments.append(f"Датчик {parameters.get('sensor_name', '')} отсутствует")
                if "start_time" in required_params and parameters.get("start_time"):
                    try:
                        start_dt = datetime.strptime(parameters["start_time"], '%Y-%m-%d %H:%M:%S')
                        start_range = datetime.strptime(time_period["start_time"], '%Y-%m-%d %H:%M:%S')
                        end_range = datetime.strptime(time_period["end_time"], '%Y-%m-%d %H:%M:%S')
                        if not (start_range <= start_dt <= end_range):
                            correction_needed = True
                            correction_data.append({
                                "field": "start_time",
                                "value": parameters["start_time"],
                                "prompt": self.prompts["validate_start_time"].format(parameters["start_time"], f"{time_period['start_time']}–{time_period['end_time']}")
                            })
                            correction_comments.append(f"Начальная дата {parameters['start_time']} вне диапазона")
                    except ValueError:
                        correction_needed = True
                        correction_data.append({
                            "field": "start_time",
                            "value": parameters["start_time"],
                            "prompt": self.prompts["validate_start_time"].format(parameters["start_time"], f"{time_period['start_time']}–{time_period['end_time']}")
                        })
                        correction_comments.append(f"Ошибка формата начальной даты {parameters['start_time']}")
                if "end_time" in required_params and parameters.get("end_time"):
                    try:
                        end_dt = datetime.strptime(parameters["end_time"], '%Y-%m-%d %H:%M:%S')
                        start_range = datetime.strptime(time_period["start_time"], '%Y-%m-%d %H:%M:%S')
                        end_range = datetime.strptime(time_period["end_time"], '%Y-%m-%d %H:%M:%S')
                        if not (start_range <= end_dt <= end_range):
                            correction_needed = True
                            correction_data.append({
                                "field": "end_time",
                                "value": parameters["end_time"],
                                "prompt": self.prompts["validate_end_time"].format(parameters["end_time"], f"{time_period['start_time']}–{time_period['end_time']}")
                            })
                            correction_comments.append(f"Конечная дата {parameters['end_time']} вне диапазона")
                    except ValueError:
                        correction_needed = True
                        correction_data.append({
                            "field": "end_time",
                            "value": parameters["end_time"],
                            "prompt": self.prompts["validate_end_time"].format(parameters["end_time"], f"{time_period['start_time']}–{time_period['end_time']}")
                        })
                        correction_comments.append(f"Ошибка формата конечной даты {parameters['end_time']}")

                if not correction_needed:
                    final_parameters = {k: v for k, v in parameters.items() if k in required_params}
                    self.logger.debug("Формализация завершена: action=%s, parameters=%s", action, final_parameters)
                    return {
                        "action": action,
                        "parameters": final_parameters,
                        "comment": "; ".join(filter(None, comments))
                    }

            # Если коррекция не удалась после max_retries
            if correction_needed:
                self.logger.debug("Коррекция не удалась после %d попыток", max_retries)
                return {
                    "action": "clarify",
                    "parameters": {"questions": ["Пожалуйста, уточните запрос, так как не удалось исправить ошибки в действии или параметрах."]},
                    "comment": "; ".join(filter(None, comments + ["Не удалось исправить запрос после двух попыток"]))
                }

            # Этап 5: Формирование финального результата
            self.logger.debug("Формализация завершена: action=%s, parameters=%s", action, final_parameters)
            return {
                "action": action,
                "parameters": final_parameters,
                "comment": "; ".join(filter(None, comments))
            }
        except Exception as e:
            self.logger.error("Ошибка формализации запроса: %s", e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())
            correction_input = json.dumps({
                "message": message,
                "context": "",
                "parameters": {},
                "errors": [{"field": "general", "value": "", "prompt": "Общая ошибка обработки"}],
                "comment": f"Ошибка обработки: {str(e)}"
            }, ensure_ascii=False)
            prompt_addition = (
                f"Произошла ошибка обработки запроса. Верни JSON с action, parameters и comment. "
                f"Действие должно быть одним из: {json.dumps(list(self.supported_actions.keys()), ensure_ascii=False)}."
            )
            return await self.error_corrector.correct(
                input_data=correction_input,
                prompt_addition=prompt_addition,
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
    """Создаёт и возвращает полностью инициализированный объект RequestFormalizer."""
    logger = logger or logging.getLogger(__name__)
    logger.debug("Создание RequestFormalizer")
    llm_request_func = _llm_request
    prompts = PROMPTS
    supported_actions = SUPPORTED_ACTIONS

    try:
        classifier = RequestClassifier(llm_request_func, prompts, debug_mode, logger)
        function_identifier = FunctionIdentifier(llm_request_func, prompts, debug_mode, logger)
        context_extractor = ContextExtractor(llm_request_func, prompts, debug_mode, logger)
        action_revalidator = ActionRevalidator(llm_request_func, prompts, debug_mode, logger)
        field_formalizer = FieldFormalizer(llm_request_func, prompts, available_sensors, debug_mode, logger)
        field_validators = FieldValidators(llm_request_func, prompts, debug_mode, logger)
        free_response = FreeResponseGenerator(llm_request_func, prompts, MESSAGES, debug_mode, logger)

        formalizer = RequestFormalizer(
            data_reader,
            error_corrector,
            classifier,
            function_identifier,
            context_extractor,
            action_revalidator,
            field_formalizer,
            field_validators,
            free_response,
            llm_request_func,
            prompts,
            supported_actions,
            available_sensors,
            time_period,
            debug_mode,
            logger
        )
        logger.debug("RequestFormalizer успешно создан")
        return formalizer
    except Exception as e:
        logger.error("Ошибка создания RequestFormalizer: %s", e)
        logger.error("Трассировка стека: %s", traceback.format_exc())
        raise
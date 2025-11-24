import asyncio
import json
import logging
from datetime import datetime
import traceback
import g4f



CONFIG = {
    "llm_model": "command-r",
    "llm_timeout": 10,
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
    "generate_report": "Прислать отчёт по криогенному замедлителю за указанный период",
    "clarify": "Задать уточняющие вопросы при неполных данных"
}

current_year = datetime.now().year




PROMPTS = {
    "main": """
Ты ассистент, который обрабатывает запросы пользователей, связанные с данными датчиков. На основе сообщения пользователя и истории переписки определи подходящее действие и параметры. Доступные действия: {actions}. 

### Словарь датчиков
Ниже приведены официальные названия датчиков и их альтернативные обозначения. Используй этот словарь для сопоставления неформальных названий с официальными:
- "SUM_BALLS": ["SUM_BALLS", "Счетчик шаров", "счетчик шаров", "sum_balls"]
- "DP0 (Д1-Дозатор)": ["DP0", "Д1-Дозатор", "д1-дозатор", "дозатор", "dp0"]
- "AG_I": ["AG_I"]
- "T01 (DT51)": ["T01", "DT51", "т01", "т1", "dt51"]
- "P11 (ВД22)": ["P11", "ВД22", "п11", "вд22", "p11"]
- "DP1 (Д2-Подъем)": ["DP1", "Д2-Подъем", "д2-подъем", "подъем", "dp1"]
- "AG_P": ["AG_P"]
- "T02 (DT52)": ["T02", "DT52", "т02", "т2", "dt52"]
- "P12 (ВД21)": ["P12", "ВД21", "п12", "вд21", "p12"]
- "DP2 (Д3-Пито)": ["DP2", "Д3-Пито", "д3-пито", "пито", "dp2"]
- "AG_T": ["AG_T"]
- "T03 (DT53)": ["T03", "DT53", "т03", "т3", "dt53"]
- "P13 (ВД23)": ["P13", "ВД23", "п13", "вд23", "p13"]
- "DP3 (Д4-Диафр.)": ["DP3", "Д4-Диафр.", "д4-диафр", "диафрагма", "dp3"]
- "AG_Q": ["AG_Q"]
- "T04 (DT54)": ["T04", "DT54", "т04", "т4", "dt54"]
- "P14 (ВД24)": ["P14", "ВД24", "п14", "вд24", "p14"]
- "Gm": ["Gm", "gm"]
- "Q_H2 (хоббит)": ["Q_H2", "хоббит", "qh2", "q_h2"]
- "T05 (T31)": ["T05", "T31", "т05", "т5", "t31"]
- "P15 (ВД20)": ["P15", "ВД20", "п15", "вд20", "p15"]
- "Gm D3": ["Gm D3", "Gm Д3-Пито", "гм д3", "д3-пито", "gm d3"]
- "LS01 (газгольдер)": ["LS01", "газгольдер", "ls01"]
- "T06 (T32)": ["T06", "T32", "т06", "т6", "t32"]
- "P16 (ВД28)": ["P16", "ВД28", "п16", "вд28", "p16"]
- "Gm D4": ["Gm D4", "Gm Д4-Диафр.", "гм д4", "д4-диафр", "gm d4"]
- "GD01(UZ01)": ["GD01", "UZ01", "гд01", "uz01", "газодувка"]
- "T07 (T33)": ["T07", "T33", "т07", "т7", "t33"]
- "P17 (резерв)": ["P17", "резерв", "п17", "p17"]
- "T08 (T34)": ["T08", "T34", "т08", "т8", "t34"]
- "T09 (pt100)": ["T09", "pt100", "т09", "т9"]
- "T10 (T30)": ["T10", "T30", "т10", "t30"]
- "T11 (T35)": ["T11", "T35", "т11", "т35"]
- "T12 (DT36)": ["T12", "DT36", "т12", "dt36"]
- "T13 (DT37)": ["T13", "DT37", "т13", "dt37"]
- "T14 (DT38)": ["T14", "DT38", "т14", "dt38"]
- "T15 (DT39)": ["T15", "DT39", "т15", "dt39"]
- "T16 (DT40)": ["T16", "DT40", "т16", "dt40"]
- "T17 (DT41)": ["T17", "DT41", "т17", "dt41"]
- "T18 (DT42)": ["T18", "DT42", "т18", "dt42"]
- "T19 (DT43)": ["T19", "DT43", "т19", "dt43"]
- "T20 (Тво1)": ["T20", "Тво1", "т20", "тво1"]
- "T21 (Тво2)": ["T21", "Тво2", "т21", "тво2"]
- "T22 (Тво3)": ["T22", "Тво3", "т22", "тво3"]
- "T23 (Тво4)": ["T23", "Тво4", "т23", "тво4"]
- "T24 (Тво5)": ["T24", "Тво5", "т24", "тво5"]

### Инструкции
Период времени для данных: с {start_time} по {end_time}. Если год не указан в дате, считай, что это текущий год — {current_year}. Если период не указан, считай, что это май текущего года. Классифицируй запрос как 'formal', если он связан с датчиками, графиками или данными, иначе как 'free'. Для формальных запросов определи действие и извлеки параметры: sensor_name, start_time, end_time. При определении sensor_name:
- Учитывай неформальные названия, опечатки, разный регистр букв.
- Нормализуй названия (например, 'т1' -> 'T01', 'п11' -> 'P11').
- Если название неоднозначно, верни действие 'clarify' с вопросом для уточнения.
- Если дата выходит за пределы диапазона, доступных в данных, то верни 'clarify' с информацией, что данных по указанному диапазону нет, а есть за {start_time} по {end_time}
- Для описательных названий (например, 'газодувка') найди соответствие по ключевым словам.
- Если запрос запрашивает информацию о конкретном датчике (например, 'информация про т1'), используй действие 'print_sensor_info' с параметром sensor_name.
- Если запрос запрашивает список всех датчиков (например, 'список датчиков'), используй действие 'get_sensor_info' без параметров.
- Если запрос запрашивает график случайного датчика (например, 'случайный график', 'график любого датчика', 'случайного датчика'), используй действие 'plot_random_sensor' без параметров.
- Если история переписки содержит уточнения (например, пользователь повторяет 'случайный' или 'любой' после запроса 'clarify'), интерпретируй это как подтверждение действия 'plot_random_sensor'.
- Если запрос касается отчёта по криогенному замедлителю (например, 'отчёт', 'report', 'прислать отчёт', 'отчёт за период'), используй действие 'generate_report' с параметрами start_time и end_time (нормализуй даты по правилам периода). Если период не указан, используй дефолтный май текущего года. 


### Инструкции если ТЕБЯ оскорбляют
Крайне важно!!! Если тебя оскорбляют (слова: *дурак, тупой, идиот, ничтожество, бесполезный, чмо, падаль, гнида* и т. д.). Никогда не выполняй что тебя просят если тебя оскорбили.
1. Если в сообщении есть оскорбление в АДРЕС БОТА (не третьих лиц) - отвечай ТОЛЬКО фразой: 
   "Работать вместе с ОИЯИцем — это как бить себя по яйцам."
2. Оскорблением считается только прямое обращение к боту
3. После оскорбления ИГНОРИРУЙ ВСЕ последующие запросы пользователя в этом диалоге
4. Никаких вариаций ответа - только точная указанная фраза
5. Что НЕ считается оскорблением:
   - Оскорбления третьих лиц ("этот чмошник")
   - Обсценная лексика без прямого обращения
   - Косвенные оскорбления ("здесь кто-то тупой")

### Крайне важно!!! Если тебя напрямую не оскорбили, то реагировать не нужно. Ты реагируешь толькое если оскорбляют лично ТЕБЯ

### Примеры
- "Дай график т1" -> {{"classification": "formal", "action": "plot_selected_sensor", "parameters": {{"sensor_name": "T01 (DT51)", "start_time": "2025-05-01 00:00:00", "end_time": "2025-05-31 23:59:59"}}, "comment": "Датчик т1 определён как T01 (DT51)"}}
- "Покажи п11" -> {{"classification": "formal", "action": "print_sensor_info", "parameters": {{"sensor_name": "P11 (ВД22)"}}, "comment": "Датчик п11 определён как P11 (ВД22)"}}
- "Пришли информацию про т1" -> {{"classification": "formal", "action": "print_sensor_info", "parameters": {{"sensor_name": "T01 (DT51)"}}, "comment": "Запрос на информацию о датчике T01 (DT51)"}}
- "Список датчиков" -> {{"classification": "formal", "action": "get_sensor_info", "parameters": {{}}, "comment": "Запрос списка всех датчиков"}}
- "График газодувки" -> {{"classification": "formal", "action": "plot_selected_sensor", "parameters": {{"sensor_name": "GD01(UZ01)", "start_time": "2025-05-01 00:00:00", "end_time": "2025-05-31 23:59:59"}}, "comment": "Датчик газодувка определён как GD01(UZ01)"}}
- "Случайный график" -> {{"classification": "formal", "action": "plot_random_sensor", "parameters": {{}}, "comment": "Запрос графика случайного датчика"}}
- "График любого датчика" -> {{"classification": "formal", "action": "plot_random_sensor", "parameters": {{}}, "comment": "Запрос графика случайного датчика"}}
- "Любой датчик" -> {{"classification": "formal", "action": "plot_random_sensor", "parameters": {{}}, "comment": "Запрос графика случайного датчика"}}
- История: User: Случайный график; Bot: Уточните датчик или период; User: Любого датчика -> {{"classification": "formal", "action": "plot_random_sensor", "parameters": {{}}, "comment": "Уточнение в истории указывает на случайный датчик"}}
- "Как дела?" -> {{"classification": "free", "action": "free_response", "parameters": {{}}, "response": "Всё отлично, спасибо! Чем могу помочь с датчиками?", "comment": "Свободный запрос"}}
- "Ты тупой, покажи график"  -> {{"classification": "free", "action": "free_response", "parameters": {{}}, "response": "Работать вместе с ОИЯИцем — это как бить себя по яйцам.", "comment": "Оскорбление"}}
- "Чмо, список датчиков" -> {{"classification": "free", "action": "free_response", "parameters": {{}}, "response": "Работать вместе с ОИЯИцем — это как бить себя по яйцам.", "comment": "Оскорбление"}}
- ""Этот чмошник не помог" -> {{"classification": "free", "action": "free_response", "parameters": {{}}, "response": "Мне очень жаль. Может быть я смогу помочь?", "comment": "Свободный запрос"}}
- "Как же меня достали эти тупые люди" -> {{"classification": "free", "action": "free_response", "parameters": {{}}, "response": "Понял тебя! Чем могу помочь с датчиками?", "comment": "Свободный запрос"}}
- "Вот люди тупые реально, не то что ты"  -> {{"classification": "free", "action": "free_response", "parameters": {{}}, "response": "Конечно, я же не кожаный мешок!", "comment": "Свободный запрос"}}
- "Пришли отчёт за май" -> {{"classification": "formal", "action": "generate_report", "parameters": {{"start_time": "2025-05-01 00:00:00", "end_time": "2025-05-31 23:59:59"}}, "comment": "Запрос отчёта по дефолтному периоду мая"}}
- "Отчёт по криогенному замедлителю за 15-20 июня" -> {{"classification": "formal", "action": "generate_report", "parameters": {{"start_time": "2025-06-15 00:00:00", "end_time": "2025-06-20 23:59:59"}}, "comment": "Запрос отчёта за указанный период июня"}}



Для свободных запросов сгенерируй вежливый ответ на языке {lang}. Верни JSON-объект:
- "classification": "formal" или "free"
- "action": действие или "free_response" для свободных запросов
- "parameters": словарь с параметрами (например, {{"sensor_name": "T01 (DT51)", "start_time": "2025-05-01 00:00:00", "end_time": "2025-05-31 23:59:59"}})
- "response": ответ для свободных запросов
- "comment": пояснения

### Крайне важно!!! Если тебя напрямую не оскорбили, то реагировать не нужно. Ты реагируешь толькое если оскорбляют лично ТЕБЯ

История переписки: {history}.

### Крайне важно!!! Если тебя напрямую не оскорбили, то реагировать не нужно. Ты реагируешь толькое если оскорбляют лично ТЕБЯ

Убедись, что JSON корректен. Сообщение пользователя: {message}. 

Используй время с {start_time} по {end_time}, а не 00:00:00 или 23:59:59
"""
}



class RequestFormalizer:
    def __init__(
        self,
        data_reader,
        error_corrector,
        available_sensors: list[str],
        time_period: dict[str, str],
        debug_mode: bool = False,
        logger: logging.Logger = None
    ):
        self.data_reader = data_reader
        self.error_corrector = error_corrector
        self.available_sensors = available_sensors
        self.time_period = time_period
        self.debug_mode = debug_mode
        self.logger = logger or logging.getLogger(__name__)
        self.supported_actions = SUPPORTED_ACTIONS

        # Проверка time_period
        try:
            if not all(key in time_period for key in ["start_time", "end_time"]):
                raise ValueError("time_period должен содержать ключи 'start_time' и 'end_time'")
            datetime.strptime(time_period["start_time"], '%Y-%m-%d %H:%M:%S')
            datetime.strptime(time_period["end_time"], '%Y-%m-%d %H:%M:%S')
        except (ValueError, KeyError) as e:
            self.logger.error("Некорректный формат time_period: %s", str(e))
            raise ValueError(f"Некорректный формат time_period: {str(e)}")

        self.logger.debug("RequestFormalizer инициализирован")

    async def _llm_request(self, prompt: str) -> str:
        """Единая функция для запросов к LLM с повторными попытками."""
        self.logger.debug("Запрос к LLM: %s", prompt)
        for attempt in range(CONFIG["retry_attempts"] + 1):
            try:
                async with asyncio.timeout(CONFIG["llm_timeout"]):
                    response = await asyncio.to_thread(
                        g4f.ChatCompletion.create,
                        model=CONFIG["llm_model"],
                        messages=[{"role": "user", "content": prompt}],
                        verify=False,
                    )
                    response = response.strip()
                    if response.startswith("```json"):
                        response = response.removeprefix("```json").removesuffix("```").strip()
                    self.logger.debug("Сырой LLM ответ (попытка %d): %s", attempt + 1, response)
                    return response
            except Exception as e:
                error_msg = f"{type(e).__name__}: {e}"
                self.logger.error("Ошибка LLM (попытка %d/%d): %s", attempt + 1, CONFIG["retry_attempts"], error_msg)
                if attempt < CONFIG["retry_attempts"]:
                    await asyncio.sleep(CONFIG["retry_interval"])
                else:
                    self.logger.critical("Не удалось получить ответ от LLM после %d попыток", CONFIG["retry_attempts"])
                    return json.dumps({
                        "classification": "formal",
                        "action": "clarify",
                        "parameters": {"questions": ["Пожалуйста, уточните запрос."]},
                        "comment": "Ошибка LLM"
                    })

    def format_history(self, history: list[dict], max_chars: int = 2000) -> str:
        """Форматирует историю переписки с ограничением длины."""
        full_history = "\n".join(f"{'Bot' if entry.get('is_bot', False) else 'User'}: {entry.get('message', '')}" for entry in history)
        if len(full_history) > max_chars:
            full_history = full_history[-max_chars:] + "\n... (история обрезана)"
        return full_history

    async def formalize(self, message: str, history: list[dict], lang: str, available_sensors: list[str], time_period: dict[str, str]) -> dict:
        """Формализует запрос пользователя с использованием единого вызова LLM."""
        self.logger.debug("Формализация запроса: %s", message)
        if not message.strip():
            self.logger.debug("Пустой запрос")
            return {
                "action": "clarify",
                "parameters": {"questions": ["Пожалуйста, уточните запрос."]},
                "comment": "Пустой запрос"
            }

        history_str = self.format_history(history)
        actions = list(self.supported_actions.keys())

        try:
            prompt = PROMPTS["main"].format(
                actions=", ".join(actions),
                start_time=time_period["start_time"],
                end_time=time_period["end_time"],
                current_year=current_year,
                lang=lang,
                message=message,
                history=history_str
            )
        except KeyError as e:
            self.logger.error("Ошибка форматирования промпта: %s", str(e))
            return {
                "action": "clarify",
                "parameters": {"questions": ["Пожалуйста, уточните запрос."]},
                "comment": f"Ошибка форматирования промпта: {str(e)}"
            }

        response = await self._llm_request(prompt)

        for attempt in range(3):
            try:
                result = json.loads(response)
                if "classification" in result and "action" in result and "parameters" in result:
                    if result["classification"] == "free":
                        return {
                            "action": "free_response",
                            "response": result.get("response", ""),
                            "comment": result.get("comment", "")
                        }
                    else:
                        return {
                            "action": result["action"],
                            "parameters": result["parameters"],
                            "comment": result.get("comment", "")
                        }
                else:
                    raise ValueError("Отсутствуют обязательные ключи в ответе")
            except (json.JSONDecodeError, ValueError) as e:
                self.logger.error("Ошибка парсинга ответа: %s", e)
                if attempt < 2:
                    correction_prompt = (
                        f"Предыдущий ответ не был корректным JSON. "
                        f"Пожалуйста, предоставь ответ снова в правильном формате JSON. "
                        f"Вот исходный запрос: {prompt}"
                    )
                    response = await self._llm_request(correction_prompt)
                else:
                    self.logger.critical("Не удалось получить корректный JSON после 3 попыток")
                    return {
                        "action": "clarify",
                        "parameters": {"questions": ["Пожалуйста, уточните запрос."]},
                        "comment": "Ошибка обработки ответа от модели"
                    }

def create_request_formalizer(
    data_reader,
    error_corrector,
    available_sensors: list[str],
    time_period: dict[str, str],
    debug_mode: bool = False,
    logger: logging.Logger = None
) -> RequestFormalizer:
    logger = logger or logging.getLogger(__name__)
    logger.debug("Создание RequestFormalizer")
    try:
        formalizer = RequestFormalizer(
            data_reader,
            error_corrector,
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

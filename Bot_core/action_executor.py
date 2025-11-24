# -*- coding: utf-8 -*-
import json
import logging
import asyncio
from typing import Dict, Any, Optional
import traceback
from datetime import datetime, timedelta
from datetime import timezone, timedelta

moscow_tz = timezone(timedelta(hours=3))


CONFIG = {
    "prompts": {
        "validate_action": (
            "Проверь, корректно ли действие и исправь, если возможно:\n"
            "Действие: '{action}'\n"
            "Доступные действия: {supported_actions}\n\n"
            "Верни JSON: {{'is_valid': true, 'corrected_action': '{action}', 'reason': ''}} или "
            "{{'is_valid': false, 'corrected_action': '', 'reason': 'описание', 'message': 'текст для пользователя'}}"
        ),
        "validate_sensor": (
            "Проверь, существует ли датчик, или найди ближайший:\n"
            "Имя датчика: '{sensor_name}'\n"
            "Доступные датчики: {available_sensors}\n\n"
            "Обязательно используй точный формат из списка датчиков, включая скобки, например, 'T08 (T34)' или 'DP0 (Д1-Дозатор)'.\n"
            "Верни JSON: {{'is_valid': true, 'corrected_name': '{sensor_name}', 'reason': '', 'message': ''}} или "
            "{{'is_valid': false, 'corrected_name': '', 'reason': 'описание', 'message': 'текст для пользователя'}}"
        ),
        "validate_start_time": (
            "Приведи дату '{0}' к формату 'YYYY-MM-DD HH:MM:SS', проверь диапазон {1}. "
            "Если HH:MM:SS не указаны, используй 00:00:00. "
            "Если год не указан, используй текущий год (2025). "
            "Если дата выходит за пределы диапазона, доступных в данных, то верни 'clarify' с информацией, что данных по указанному диапазону нет, а есть с {start_time}"
            "Пример: 'май 2025' - '2025-05-01 00:00:00'. "
            "Пример: 'январь 2025' - 'Данных за январь нет. Доступны данные только с {start_time}'. "
            "Верни JSON: {{'is_valid': true/false, 'corrected_date': '<дата>', 'reason': '<причина>', 'message': ''}}"
        ),
        "validate_end_time": (
            "Приведи дату '{0}' к формату 'YYYY-MM-DD HH:MM:SS', проверь диапазон {1}. "
            "Если HH:MM:SS не указаны, используй 23:59:59. "
            "Если год не указан, используй текущий год (2025). "
            "Если дата выходит за пределы диапазона, доступных в данных, то верни 'clarify' с информацией, что данных по указанному диапазону нет, а есть до {end_time}"
            "Пример: 'май 2025' - '2025-05-31 23:59:59'. "
            "Пример: 'январь 2025' - 'Данных за январь нет. Доступны данные только до {end_time}'. "
            "Верни JSON: {{'is_valid': true/false, 'corrected_date': '<дата>', 'reason': '<причина>', 'message': ''}}"
        ),
        "supported_actions": {
            "plot_selected_sensor": {
                "description": "Построить график по выбранному датчику за указанный период",
                "call_rule": "Указать имя датчика, начальную и конечную дату в формате 'YYYY-MM-DD HH:MM:SS'",
                "expected_json": {
                    "action": "plot_selected_sensor",
                    "parameters": {
                        "sensor_name": "string",
                        "start_time": "string (YYYY-MM-DD HH:MM:SS)",
                        "end_time": "string (YYYY-MM-DD HH:MM:SS)"
                    },
                    "comment": "string"
                },
                "validations": ["action", "sensor_name", "start_time", "end_time"]
            },
            "plot_random_sensor": {
                "description": "Показать график случайного датчика",
                "call_rule": "Без параметров",
                "expected_json": {
                    "action": "plot_random_sensor",
                    "parameters": {},
                    "comment": "string"
                },
                "validations": ["action"]
            },
            "get_sensor_info": {
                "description": "Показать список доступных датчиков",
                "call_rule": "Без параметров",
                "expected_json": {
                    "action": "get_sensor_info",
                    "parameters": {},
                    "comment": "string"
                },
                "validations": ["action"]
            },
            "print_sensor_info": {
                "description": "Показать информацию о конкретном датчике",
                "call_rule": "Указать имя датчика",
                "expected_json": {
                    "action": "print_sensor_info",
                    "parameters": {
                        "sensor_name": "string"
                    },
                    "comment": "string"
                },
                "validations": ["action", "sensor_name"]
            },
            "get_time_period": {
                "description": "Показать, за какой период есть данные",
                "call_rule": "Без параметров",
                "expected_json": {
                    "action": "get_time_period",
                    "parameters": {},
                    "comment": "string"
                },
                "validations": ["action"]
            },


            "generate_report": {
                "description": "Сгенерировать отчёт по криогенному замедлителю КЗ201 (5 графиков: T32, P22, DT_51, ВД21, ЛИР)",
                "call_rule": "Указать начальную и конечную дату (опционально). По умолчанию — последние 24 часа.",
                "expected_json": {
                    "action": "generate_report",
                    "parameters": {
                        "start_time": "string (YYYY-MM-DD HH:MM:SS, опционально)",
                        "end_time": "string (YYYY-MM-DD HH:MM:SS, опционально)"
                    },
                    "comment": "string"
                },
                "validations": ["action", "start_time", "end_time"]
}
        },


            "clarify": {
                "description": "Задать уточняющие вопросы",
                "call_rule": "Используется при неполных данных",
                "expected_json": {
                    "action": "clarify",
                    "parameters": {
                        "questions": ["string"]
                    },
                    "comment": "string"
                },
                "validations": ["action", "questions"]
            }


    },
    "llm_timeout": 60
}

class ActionExecutor:
    """Выполняет действия на основе формализованных запросов, возвращая JSON-ответ."""

    def __init__(self, data_processor, error_corrector, logger: logging.Logger = None, debug_mode: bool = False):
        self.data_processor = data_processor
        self.error_corrector = error_corrector
        self.logger = logger or logging.getLogger(__name__)
        self.supported_actions = list(CONFIG["prompts"]["supported_actions"].keys())
        self.debug_mode = debug_mode
        self.logger.debug("ActionExecutor инициализирован")

    async def execute(self, formalized: Dict[str, Any]) -> Dict[str, Any]:
        """Выполняет действие на основе формализованного запроса, возвращая JSON-ответ."""
        self.logger.debug("Выполнение формализованного запроса: %s", formalized)
        if not isinstance(formalized, dict) or not formalized.get("action") or not isinstance(formalized.get("parameters"), dict):
            self.logger.error("Некорректный формат JSON: %s", formalized)
            return {
                "validation_results": [{
                    "is_valid": False,
                    "reason": "Некорректный формат JSON: отсутствует 'action' или 'parameters'",
                    "message": "Пожалуйста, уточните запрос"
                }]
            }

        action = formalized.get("action")
        params = formalized.get("parameters", {})
        comment = formalized.get("comment", "")
        retry_count = 0
        max_retries = 2

        if not comment:
            self.logger.error("Отсутствует поле 'comment' в запросе")
            return {
                "validation_results": [{
                    "is_valid": False,
                    "reason": "Отсутствует поле 'comment'",
                    "message": "Пожалуйста, уточните запрос"
                }]
            }

        try:
            sensors = self.data_processor.reader.get_sensor_info()  # Убрал "s", предполагая, что параметр не нужен
            available_sensors = [s["sensor_name"] for s in sensors.values()] if sensors else []
            time_period = self.data_processor.get_time_period()
            required_params = {
                "plot_selected_sensor": ["sensor_name", "start_time", "end_time"],
                "print_sensor_info": ["sensor_name"],
                "plot_random_sensor": [],
                "get_sensor_info": [],
                "get_time_period": [],
                "clarify": ["questions"]  # Добавлено для действия clarify
            }.get(action, [])

            while retry_count <= max_retries:
                validation_results = await self._validate_action(action, params, comment, available_sensors, time_period)
                corrected_action = action
                corrected_params = params.copy()
                correction_needed = False
                correction_data = []
                correction_comments = []

                # Проверка результатов валидации
                for result in validation_results:
                    if result.get("corrected_action"):
                        corrected_action = result["corrected_action"]
                    if result.get("corrected_name"):
                        corrected_params["sensor_name"] = result["corrected_name"]
                    if result.get("corrected_date"):
                        if result.get("original_field") == "start_time":
                            corrected_params["start_time"] = result["corrected_date"]
                        elif result.get("original_field") == "end_time":
                            corrected_params["end_time"] = result["corrected_date"]
                    if not result.get("is_valid", True):
                        correction_needed = True
                        field = result.get("original_field", "action" if "corrected_action" in result else "")
                        correction_data.append({
                            "field": field,
                            "value": result.get(field, "" if field != "action" else result.get("corrected_action", "")),
                            "prompt": CONFIG["prompts"].get(f"validate_{field}", "")
                        })
                        correction_comments.append(result.get("reason", ""))

                # Если все параметры валидны, выполняем действие
                if not correction_needed:
                    self.logger.debug("Все параметры валидны, выполнение действия %s с параметрами %s", corrected_action, corrected_params)
                    return await self._run_action(corrected_action, corrected_params)

                # Если требуется коррекция, вызываем error_corrector
                if retry_count < max_retries:
                    self.logger.debug("Требуется коррекция (попытка %d/%d): %s", retry_count + 1, max_retries, correction_comments)
                    for error in correction_data:
                        field = error["field"]
                        value = error["value"]
                        prompt = error["prompt"]
                        correction_input = json.dumps({
                            "action": corrected_action,
                            "parameters": corrected_params,
                            "comment": comment,
                            "error": {"field": field, "value": value, "prompt": prompt},
                            "validation_comment": correction_comments[correction_data.index(error)]
                        }, ensure_ascii=False)

                        self.logger.debug("Отправка коррекции для поля %s: %s", field, correction_input)
                        corrected = await self.error_corrector.correct(
                            input_data=correction_input,
                            prompt_addition=prompt,
                            user_id=f"execution_correction_{field}_{comment[:50]}_retry_{retry_count}"
                        )

                        try:
                            result = json.loads(corrected)
                            self.logger.debug("Результат коррекции поля %s: %s", field, result)
                            if field == "action":
                                corrected_action = result.get("corrected_action", corrected_action)
                            elif field == "sensor_name":
                                corrected_params["sensor_name"] = result.get("corrected_name", value)
                            elif field == "start_time":
                                corrected_params["start_time"] = result.get("corrected_date", value)
                            elif field == "end_time":
                                corrected_params["end_time"] = result.get("corrected_date", value)
                        except json.JSONDecodeError:
                            self.logger.error("Ошибка парсинга ответа error_corrector для поля %s: %s", field, corrected)
                            correction_comments.append(f"Ошибка формата JSON в ответе для поля {field}")

                    retry_count += 1
                    action = corrected_action
                    params = corrected_params
                else:
                    break

            # Если коррекция не удалась
            self.logger.debug("Коррекция не удалась после %d попыток", max_retries)
            return {
                "validation_results": [{
                    "is_valid": False,
                    "reason": "Не удалось исправить действие или параметры после двух попыток",
                    "message": "Пожалуйста, уточните запрос, так как не удалось исправить ошибки."
                }]
            }
        except Exception as e:
            self.logger.error("Ошибка при выполнении действия %s: %s", action, e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())
            return {
                "validation_results": [{
                    "is_valid": False,
                    "reason": f"Ошибка выполнения: {str(e)}",
                    "message": "Произошла ошибка при обработке запроса. Попробуйте снова"
                }]
            }

    async def _validate_action(self, action: str, params: Dict[str, Any], comment: str, available_sensors: list, time_period: dict) -> list:
        """Валидирует действие и параметры, вызывая error_corrector только при необходимости."""
        self.logger.debug("Валидация действия %s с параметрами %s", action, params)
        validation_results = []
        tasks = []

        try:
            if not isinstance(time_period, dict) or "start_time" not in time_period or "end_time" not in time_period:
                validation_results.append({
                    "is_valid": False,
                    "reason": "Некорректный формат временного периода",
                    "message": "Ошибка при получении периода данных. Попробуйте снова",
                    "original_field": "time_period"
                })
                self.logger.error("Некорректный формат временного периода")
                return validation_results

            # Валидация действия
            if action not in self.supported_actions:
                prompt = CONFIG["prompts"]["validate_action"].format(
                    action=action,
                    supported_actions=json.dumps(self.supported_actions, ensure_ascii=False)
                )
                tasks.append(self._correct_error(action, prompt, comment, "action"))
            else:
                validation_results.append({
                    "is_valid": True,
                    "reason": "",
                    "message": "",
                    "original_field": "action"
                })

            validations = CONFIG["prompts"]["supported_actions"].get(action, {}).get("validations", [])

            # Валидация датчика
            if "sensor_name" in validations:
                sensor_name = params.get("sensor_name", "")
                if sensor_name not in available_sensors:
                    prompt = CONFIG["prompts"]["validate_sensor"].format(
                        sensor_name=sensor_name,
                        available_sensors=json.dumps(available_sensors, ensure_ascii=False)
                    )
                    tasks.append(self._correct_error(sensor_name, prompt, comment, "sensor_name"))
                else:
                    validation_results.append({
                        "is_valid": True,
                        "reason": "",
                        "message": "",
                        "original_field": "sensor_name"
                    })

            # Валидация начальной даты
            if "start_time" in validations:
                start_time = params.get("start_time", "")
                if start_time:
                    try:
                        start_dt = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
                        start_range = datetime.strptime(time_period["start_time"], '%Y-%m-%d %H:%M:%S')
                        end_range = datetime.strptime(time_period["end_time"], '%Y-%m-%d %H:%M:%S')
                        if not (start_range <= start_dt <= end_range):
                            prompt = CONFIG["prompts"]["validate_start_time"].format(
                                start_time,
                                f"{time_period['start_time']}–{time_period['end_time']}"
                            )
                            tasks.append(self._correct_error(start_time, prompt, comment, "start_time"))
                        else:
                            validation_results.append({
                                "is_valid": True,
                                "reason": "",
                                "message": "",
                                "original_field": "start_time"
                            })
                    except ValueError:
                        prompt = CONFIG["prompts"]["validate_start_time"].format(
                            start_time,
                            f"{time_period['start_time']}–{time_period['end_time']}"
                        )
                        tasks.append(self._correct_error(start_time, prompt, comment, "start_time"))
                else:
                    prompt = CONFIG["prompts"]["validate_start_time"].format(
                        "",
                        f"{time_period['start_time']}–{time_period['end_time']}"
                    )
                    tasks.append(self._correct_error("", prompt, comment, "start_time"))

            # Валидация конечной даты
            if "end_time" in validations:
                end_time = params.get("end_time", "")
                if end_time:
                    try:
                        end_dt = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
                        start_range = datetime.strptime(time_period["start_time"], '%Y-%m-%d %H:%M:%S')
                        end_range = datetime.strptime(time_period["end_time"], '%Y-%m-%d %H:%M:%S')
                        if not (start_range <= end_dt <= end_range):
                            prompt = CONFIG["prompts"]["validate_end_time"].format(
                                end_time,
                                f"{time_period['start_time']}–{time_period['end_time']}"
                            )
                            tasks.append(self._correct_error(end_time, prompt, comment, "end_time"))
                        else:
                            validation_results.append({
                                "is_valid": True,
                                "reason": "",
                                "message": "",
                                "original_field": "end_time"
                            })
                    except ValueError:
                        prompt = CONFIG["prompts"]["validate_end_time"].format(
                            end_time,
                            f"{time_period['start_time']}–{time_period['end_time']}"
                        )
                        tasks.append(self._correct_error(end_time, prompt, comment, "end_time"))
                else:
                    prompt = CONFIG["prompts"]["validate_end_time"].format(
                        "",
                        f"{time_period['start_time']}–{time_period['end_time']}"
                    )
                    tasks.append(self._correct_error("", prompt, comment, "end_time"))

            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for task_result in results:
                    if isinstance(task_result, Exception):
                        self.logger.error("Ошибка валидации: %s", task_result)
                        self.logger.error("Трассировка стека: %s", traceback.format_exc())
                        validation_results.append({
                            "is_valid": False,
                            "reason": f"Ошибка валидации: {str(task_result)}",
                            "message": "Произошла ошибка при проверке запроса. Попробуйте снова",
                            "original_field": "general"
                        })
                        continue
                    try:
                        result, validation_type = task_result
                        validation = json.loads(result)
                        validation["original_field"] = validation_type
                        validation_results.append(validation)
                        self.logger.debug("Успешная валидация %s: %s", validation_type, validation)
                    except (ValueError, json.JSONDecodeError) as e:
                        self.logger.error("Некорректный результат валидации: %s", e)
                        self.logger.error("Трассировка стека: %s", traceback.format_exc())
                        validation_results.append({
                            "is_valid": False,
                            "reason": f"Некорректный результат валидации: {str(e)}",
                            "message": "Ошибка обработки проверки. Попробуйте снова",
                            "original_field": "general"
                        })

            self.logger.debug("Результаты валидации: %s", validation_results)
            return validation_results
        except Exception as e:
            self.logger.error("Ошибка валидации действия: %s", e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())
            return [{
                "is_valid": False,
                "reason": f"Ошибка валидации: {str(e)}",
                "message": "Произошла ошибка при проверке запроса",
                "original_field": "general"
            }]

    async def _correct_error(self, input_data: str, prompt_addition: str, comment: str, validation_type: str) -> tuple[Optional[str], str]:
        """Исправляет ошибку с помощью ErrorCorrector, возвращает результат и тип валидации."""
        self.logger.debug("Исправление ошибки: input=%s, type=%s", input_data, validation_type)
        try:
            async with asyncio.timeout(CONFIG["llm_timeout"]):
                corrected = await self.error_corrector.correct(
                    input_data=input_data,
                    prompt_addition=prompt_addition,
                    user_id=f"action_executor_{validation_type}_{comment[:50]}"
                )
                if corrected:
                    try:
                        json.loads(corrected)
                        self.logger.debug("Правильный JSON для %s: %s", validation_type, corrected)
                        return corrected, validation_type
                    except json.JSONDecodeError:
                        self.logger.debug("Не JSON для %s: %s", validation_type, corrected)
                        second = await self.error_corrector.correct(
                            input_data=corrected,
                            prompt_addition="Исправь JSON, чтобы он был валидным",
                            user_id=f"action_executor_{validation_type}_{comment[:50]}_retry"
                        )
                        self.logger.debug("Повторная коррекция JSON: %s", second)
                        return second, validation_type
                self.logger.error("Пустой ответ от корректора %s: %s", validation_type, input_data)
                return json.dumps({
                    "is_valid": False,
                    "reason": f"Пустой ответ от корректора для {validation_type}",
                    "message": "Ошибка обработки данных. Попробуйте снова"
                }), validation_type
        except asyncio.TimeoutError as e:
            self.logger.error("Таймаут для %s: %s", validation_type, e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())
            return json.dumps({
                "is_valid": False,
                "reason": f"Таймаут при обработке {validation_type}",
                "message": "Запрос занял слишком много времени. Попробуйте снова"
            }), validation_type
        except Exception as e:
            self.logger.error("Исключение в корректоре %s: %s", validation_type, e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())
            return json.dumps({
                "is_valid": False,
                "reason": f"Ошибка валидации {validation_type}: {e}",
                "message": "Произошла ошибка при проверке запроса. Попробуйте снова"
            }), validation_type

    async def _run_action(self, action: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Выполняет действие без предварительной проверки через LLM."""
        self.logger.debug("Запуск действия %s с параметрами %s", action, params)
        try:
            if action == "get_sensor_info":
                sensors = self.data_processor.reader.get_sensor_info()
                if not sensors:
                    raise ValueError("Нет доступных датчиков")
                result = {"result": [s["sensor_name"] for s in sensors.values()]}
                self.logger.debug("Получен список датчиков: %s", result)
                return result

            if action == "clarify":
                questions = params.get("questions", ["Пожалуйста, уточните запрос"])
                if not isinstance(questions, list) or not all(isinstance(q, str) for q in questions):
                    raise ValueError("Некорректный формат параметра 'questions'")
                result = {"result": questions}
                self.logger.debug("Возвращены уточняющие вопросы: %s", result)
                return result

            if action == "plot_selected_sensor":
                path = self.data_processor.plot_selected_sensor(
                    params["sensor_name"], params["start_time"], params["end_time"]
                )
                if not path:
                    raise RuntimeError("Не удалось построить график")
                result = {"result": {"plot_path": str(path)}}
                self.logger.debug("График построен: %s", result)
                return result

            if action == "print_sensor_info":
                sensor_name = params["sensor_name"]
                sensors = self.data_processor.reader.get_sensor_info()
                sensor = next((s for s in sensors.values() if s["sensor_name"] == sensor_name), None)
                if not sensor:
                    raise ValueError(f"Датчик {sensor_name} не найден")
                period = self.data_processor.get_time_period()
                result = {
                    "result": {
                        "sensor_name": sensor_name,
                        "period": f"с {period['start_time']} по {period['end_time']}",
                        "index": sensor["index"],
                        "data_type": sensor["data_type"]
                    }
                }
                self.logger.debug("Информация о датчике: %s", result)
                return result

            if action == "get_time_period":
                result = {"result": self.data_processor.get_time_period()}
                self.logger.debug("Возвращён период данных: %s", result)
                return result

            if action == "plot_random_sensor":
                path = self.data_processor.plot_random_sensor()
                if not path:
                    raise RuntimeError("Не удалось построить график для случайного датчика")
                result = {"result": {"plot_path": str(path)}}
                self.logger.debug("Случайный график построен: %s", result)
                return result

            if action == "generate_report":
                # ───── Параметры из LLM ─────
                start_time_str = params.get("start_time")
                end_time_str   = params.get("end_time")

                # ───── Парсим даты (UTC) ─────
                start_dt = None
                end_dt   = None

                if start_time_str:
                    try:
                        start_dt = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S") \
                                   .replace(tzinfo=timezone.utc)
                    except ValueError:
                        raise ValueError(
                            f"Неверный формат start_time: '{start_time_str}'. "
                            "Ожидается: YYYY-MM-DD HH:MM:SS"
                        )

                if end_time_str:
                    try:
                        end_dt = datetime.strptime(end_time_str, "%Y-%m-%d %H:%M:%S") \
                                 .replace(tzinfo=timezone.utc)
                    except ValueError:
                        raise ValueError(
                            f"Неверный формат end_time: '{end_time_str}'. "
                            "Ожидается: YYYY-MM-DD HH:MM:SS"
                        )

                # ───── Если даты не указаны → последние 24 часа ─────
                if not start_dt or not end_dt:
                    end_dt   = datetime.now(timezone.utc)
                    start_dt = end_dt - timedelta(hours=24)

                # ───── Проверка: start < end ─────
                if start_dt >= end_dt:
                    raise ValueError("start_time должен быть раньше end_time")

                # ───── Генерация отчёта ─────
                try:
                        plot_paths, pdf_path, docx_path = self.data_processor.generate_report(
                            start_time=start_dt,
                            end_time=end_dt,
                            output_dir="Bot_Reports",
                            logger=self.logger
                        )
                except Exception as exc:
                    self.logger.error("Ошибка генерации отчёта КЗ201: %s", exc)
                    self.logger.error(traceback.format_exc())
                    raise RuntimeError(f"Не удалось сгенерировать отчёт: {exc}")

                # === Собираем ВСЕ файлы для отправки ===
                files_to_send = []

                if pdf_path and pdf_path.exists():
                    files_to_send.append(("PDF", pdf_path))
                if docx_path and docx_path.exists():
                    files_to_send.append(("DOCX", docx_path))
                for i, plot_path in enumerate(plot_paths, 1):
                    if plot_path and plot_path.exists():
                        files_to_send.append((f"График {i}", plot_path))

                # === Ответ боту ===
                result = {
                    "result": {
                        "files": [
                            {"type": file_type, "path": str(path)} for file_type, path in files_to_send
                        ],
                        "message": (
                            f"**Отчёт КЗ201 готов** (7 файлов)\n"
                            f"`{start_dt.strftime('%d.%m.%Y %H:%M')} — {end_dt.strftime('%d.%m.%Y %H:%M')}`\n"
                            f"PDF + DOCX + 5 графиков"
                        )
                    }
                }
                self.logger.info("Отчёт КЗ201: подготовлено %d файлов", len(files_to_send))
                return result


            raise ValueError(f"Неизвестное действие: {action}")
        except Exception as e:
            self.logger.error("Ошибка выполнения действия %s: %s", action, e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())
            return {
                "validation_results": [{
                    "is_valid": False,
                    "reason": f"Ошибка выполнения: {str(e)}",
                    "message": "Произошла ошибка при обработке запроса. Попробуйте снова"
                }]
            }

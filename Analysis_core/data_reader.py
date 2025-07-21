# -*- coding: utf-8 -*-
import sqlite3
from typing import List, Dict, Any, Tuple, Iterator, Optional, Union
from pathlib import Path
from datetime import datetime
from dateutil.tz import tzutc
import logging
import psutil
import traceback

class DataReader:
    """Класс для чтения данных из баз SQLite с использованием HistoryManager для кэширования."""

    def __init__(self, folder_path: str, history_manager=None, debug_mode: bool = False, logger: logging.Logger = None):
        """Инициализация DataReader."""
        self.folder_path: Path = Path(folder_path)
        self.debug_mode: bool = debug_mode
        self.logger = logger or logging.getLogger(__name__)
        self.history_manager = history_manager  # Добавляем HistoryManager
        self.db_files: List[Path] = []
        self.sensor_info: List[Dict[str, Any]] = []
        self.time_period: Dict[str, str] = {"start_time": None, "end_time": None}
        self.bytes_per_row = 32
        self.logger.debug("Инициализация DataReader с путем: %s", folder_path)
        self._initialize()

    def _initialize(self) -> None:
        """Инициализация настроек."""
        try:
            self._load_db_files()
            self._load_cached_time_period()
            self._enable_wal_mode()
            self.logger.debug("Инициализация DataReader завершена")
        except Exception as e:
            self.logger.error("Ошибка инициализации DataReader: %s", e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())
            raise  

    def _load_db_files(self) -> None:
        """Загрузка файлов .db."""
        self.logger.debug("Загрузка файлов .db из %s", self.folder_path)
        try:
            if not self.folder_path.exists():
                self.logger.error("Папка %s не существует", self.folder_path)
                raise FileNotFoundError(f"Папка {self.folder_path} не существует.")
            self.db_files = list(self.folder_path.rglob("*.db"))
            if not self.db_files:
                self.logger.error("В папке %s не найдено файлов .db", self.folder_path)
                raise FileNotFoundError(f"В папке {self.folder_path} не найдено файлов .db.")
            self.logger.debug("Найдено файлов .db: %d", len(self.db_files))
        except Exception as e:
            self.logger.error("Ошибка загрузки файлов .db: %s", e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())
            raise

    def _enable_wal_mode(self) -> None:
        """Включение режима WAL."""
        self.logger.debug("Включение режима WAL для всех баз")
        for db_file in self.db_files:
            try:
                with sqlite3.connect(db_file, timeout=10) as conn:
                    conn.execute("PRAGMA journal_mode=WAL")
                    self.logger.debug("Включён режим WAL для %s", db_file)
            except sqlite3.Error as e:
                self.logger.error("Ошибка при включении WAL для %s: %s", db_file, e)
                self.logger.error("Трассировка стека: %s", traceback.format_exc())

    def _load_cached_time_period(self) -> None:
        """Загрузка кэшированного временного периода из HistoryManager."""
        self.logger.debug("Загрузка кэша временного периода")
        if self.history_manager:
            try:
                cached_period = self.history_manager.get_cache("time_period")
                if cached_period:
                    self.time_period = cached_period
                    self.logger.debug("Загружен кэш из HistoryManager: %s", self.time_period)
            except Exception as e:
                self.logger.error("Ошибка загрузки кэша из HistoryManager: %s", e)
                self.logger.error("Трассировка стека: %s", traceback.format_exc())

    def _save_time_period_cache(self) -> None:
        """Сохранение кэша временного периода в HistoryManager."""
        self.logger.debug("Сохранение кэша временного периода")
        if self.history_manager:
            try:
                # Сохраняем на 24 часа (86400 секунд)
                self.history_manager.set_cache("time_period", self.time_period, ttl_seconds=86400)
                self.logger.debug("Сохранён кэш в HistoryManager: %s", self.time_period)
            except Exception as e:
                self.logger.error("Ошибка сохранения кэша в HistoryManager: %s", e)
                self.logger.error("Трассировка стека: %s", traceback.format_exc())

    def _calculate_batch_size(self, total_rows: int) -> int:
        """Расчёт размера батча."""
        self.logger.debug("Расчёт размера батча для %d строк", total_rows)
        try:
            available_memory = psutil.virtual_memory().available
            max_memory = available_memory * 0.25
            batch_size = int(max_memory // self.bytes_per_row)
            batch_size = max(1000, min(batch_size, total_rows // 10 + 1))
            self.logger.debug("Размер батча: %d строк", batch_size)
            return batch_size
        except Exception as e:
            self.logger.error("Ошибка расчёта батча: %s", e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())
            self.logger.debug("Использование размера батча по умолчанию: 10000")
            return 10000

    def get_time_period(self) -> Dict[str, str]:
        """Получение временного периода данных."""
        self.logger.debug("Получение временного периода")
        if self.time_period["start_time"] and self.time_period["end_time"]:
            self.logger.debug("Возвращён кэшированный период: %s", self.time_period)
            return self.time_period
        all_min_ts, all_max_ts = [], []
        for db_file in self.db_files:
            try:
                with sqlite3.connect(str(db_file), timeout=10) as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT MIN([time@timestamp]), MAX([time@timestamp]) FROM data")
                    min_ts, max_ts = cursor.fetchone()
                    if min_ts and max_ts:
                        all_min_ts.append(float(min_ts))
                        all_max_ts.append(float(max_ts))
                self.logger.debug("Получены временные метки для %s: min=%s, max=%s", db_file, min_ts, max_ts)
            except sqlite3.Error as e:
                self.logger.error("Ошибка получения периода из %s: %s", db_file, e)
                self.logger.error("Трассировка стека: %s", traceback.format_exc())
        if not all_min_ts:
            self.logger.error("Нет данных для временного периода")
            raise ValueError("Нет данных для временного периода.")
        try:
            self.time_period["start_time"] = datetime.fromtimestamp(min(all_min_ts), tz=tzutc()).strftime('%Y-%m-%d %H:%M:%S')
            self.time_period["end_time"] = datetime.fromtimestamp(max(all_max_ts), tz=tzutc()).strftime('%Y-%m-%d %H:%M:%S')
            self._save_time_period_cache()
            self.logger.debug("Рассчитан новый период: %s", self.time_period)
            return self.time_period
        except Exception as e:
            self.logger.error("Ошибка обработки временного периода: %s", e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())
            raise

    def get_sensor_info(self, deduplicate_by_index: bool = False) -> List[Dict[str, Any]]:
        """Получение информации о датчиках с кэшированием в HistoryManager."""
        self.logger.debug("Получение информации о датчиках")
        if self.history_manager:
            cached_sensor_info = self.history_manager.get_cache("sensor_info")
            if cached_sensor_info:
                self.sensor_info = cached_sensor_info
                self.logger.debug("Возвращена кэшированная информация из HistoryManager: %d датчиков", len(self.sensor_info))
                return self.sensor_info
        if self.sensor_info:
            self.logger.debug("Возвращена кэшированная информация: %d датчиков", len(self.sensor_info))
            return self.sensor_info
        sensor_dict: Dict[Tuple[str, int], Dict[str, Any]] = {}
        seen_indices = set() if deduplicate_by_index else None
        for db_file in self.db_files:
            try:
                with sqlite3.connect(db_file, timeout=10) as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT comment, data_format_index, data_type FROM data_format")
                    for row in cursor.fetchall():
                        sensor_name, index, data_type = row
                        index = int(index)
                        if deduplicate_by_index and index in seen_indices:
                            continue
                        key = (sensor_name, index)
                        if key in sensor_dict:
                            sensor_dict[key]["source_files"].append(str(db_file))
                        else:
                            sensor_dict[key] = {
                                "sensor_name": sensor_name,
                                "index": index,
                                "data_type": data_type,
                                "source_files": [str(db_file)]
                            }
                        if deduplicate_by_index:
                            seen_indices.add(index)
                self.logger.debug("Обработана информация о датчиках из %s", db_file)
            except sqlite3.Error as e:
                self.logger.error("Ошибка получения данных из %s: %s", db_file, e)
                self.logger.error("Трассировка стека: %s", traceback.format_exc())
        if not sensor_dict:
            self.logger.error("Информация о датчиках не найдена")
            raise ValueError("Информация о датчиках не найдена")
        try:
            self.sensor_info = sorted(sensor_dict.values(), key=lambda x: x["index"])
            if self.history_manager:
                self.history_manager.set_cache("sensor_info", self.sensor_info, ttl_seconds=86400)
                self.logger.debug("Сохранена информация о датчиках в HistoryManager: %d датчиков", len(self.sensor_info))
            self.logger.debug("Получено %d датчиков", len(self.sensor_info))
            return self.sensor_info
        except Exception as e:
            self.logger.error("Ошибка обработки информации о датчиках: %s", e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())
            return []

    def get_data_stream(self, sensor_index: int, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None) -> Union[Iterator[Tuple[List[datetime], List[float]]], Tuple[List[datetime], List[float]]]:
        """Получение потоков данных для датчика."""
        self.logger.debug("Получение данных для датчика с индексом %d, start_time=%s, end_time=%s", sensor_index, start_time, end_time)
        sensors = [s for s in self.get_sensor_info() if s["index"] == sensor_index]
        if not sensors:
            self.logger.error("Датчик с индексом %d не найден", sensor_index)
            raise ValueError(f"Датчик с индексом {sensor_index} не найден.")
        sensor = sensors[0]
        total_rows = 0
        for db_file in sensor["source_files"]:
            try:
                with sqlite3.connect(db_file, timeout=10) as conn:
                    cursor = conn.cursor()
                    count_query = f"SELECT COUNT(*) FROM data WHERE data_format_{sensor_index} IS NOT NULL"
                    if start_time or end_time:
                        conditions = []
                        if start_time:
                            conditions.append(f"[time@timestamp] >= {int(start_time.timestamp())}")
                        if end_time:
                            conditions.append(f"[time@timestamp] <= {int(end_time.timestamp())}")
                        count_query += " AND " + " AND ".join(conditions)
                    self.logger.debug("Выполняется запрос подсчета: %s", count_query)
                    cursor.execute(count_query)
                    rows = cursor.fetchone()[0]
                    total_rows += rows
                    self.logger.debug("Подсчитано строк для %s: %d", db_file, rows)
            except sqlite3.Error as e:
                self.logger.error("Ошибка подсчёта строк в %s: %s", db_file, e)
                self.logger.error("Трассировка стека: %s", traceback.format_exc())
        if total_rows == 0:
            self.logger.debug("Нет данных для датчика %d в указанном периоде", sensor_index)
            return [], []
        use_batches = (total_rows * self.bytes_per_row) > (psutil.virtual_memory().total * 0.25)
        self.logger.debug("Использование батчей: %s", use_batches)
        if not use_batches:
            all_times = []
            all_values = []
            for db_file in sensor["source_files"]:
                try:
                    with sqlite3.connect(str(db_file), timeout=10) as conn:
                        cursor = conn.cursor()
                        query = f"SELECT [time@timestamp], data_format_{sensor_index} FROM data WHERE data_format_{sensor_index} IS NOT NULL"
                        if start_time or end_time:
                            conditions = []
                            if start_time:
                                conditions.append(f"[time@timestamp] >= {int(start_time.timestamp())}")
                            if end_time:
                                conditions.append(f"[time@timestamp] <= {int(end_time.timestamp())}")
                            query += " AND " + " AND ".join(conditions)
                        query += " ORDER BY [time@timestamp]"
                        self.logger.debug("Выполняется запрос: %s", query)
                        cursor.execute(query)
                        rows = cursor.fetchall()
                        self.logger.debug("Получено строк из %s: %d", db_file, len(rows))
                        for row in rows:
                            all_times.append(datetime.fromtimestamp(float(row[0]), tz=tzutc()))
                            all_values.append(float(row[1]))
                    self.logger.debug("Получены данные из %s: %d записей", db_file, len(all_times))
                except sqlite3.Error as e:
                    self.logger.error("Ошибка получения данных из %s: %s", db_file, e)
                    self.logger.error("Трассировка стека: %s", traceback.format_exc())
            self.logger.debug("Возвращено %d записей без батчей", len(all_times))
            return all_times, all_values
        batch_size = self._calculate_batch_size(total_rows)
        def data_iterator():
            for db_file in sensor["source_files"]:
                try:
                    with sqlite3.connect(db_file, timeout=10) as conn:
                        cursor = conn.cursor()
                        offset = 0
                        while True:
                            query = f"SELECT [time@timestamp], data_format_{sensor_index} FROM data WHERE data_format_{sensor_index} IS NOT NULL"
                            if start_time or end_time:
                                conditions = []
                                if start_time:
                                    conditions.append(f"[time@timestamp] >= {int(start_time.timestamp())}")
                                if end_time:
                                    conditions.append(f"[time@timestamp] <= {int(end_time.timestamp())}")
                                query += " AND " + " AND ".join(conditions)
                            query += f" ORDER BY [time@timestamp] LIMIT {batch_size} OFFSET {offset}"
                            self.logger.debug("Выполняется запрос батча: %s", query)
                            cursor.execute(query)
                            times, values = [], []
                            rows_fetched = 0
                            for row in cursor:
                                times.append(datetime.fromtimestamp(float(row[0]), tz=tzutc()))
                                values.append(float(row[1]))
                                rows_fetched += 1
                            self.logger.debug("Получено строк из %s (offset %d): %d", db_file, offset, rows_fetched)
                            if rows_fetched == 0:
                                break
                            if times:
                                self.logger.debug("Уступка батча из %s: %d записей", db_file, len(times))
                                yield times, values
                            offset += batch_size
                except sqlite3.Error as e:
                    self.logger.error("Ошибка итерации данных из %s: %s", db_file, e)
                    self.logger.error("Трассировка стека: %s", traceback.format_exc())
        self.logger.debug("Возвращён итератор данных с размером батча %d", batch_size)
        return data_iterator()
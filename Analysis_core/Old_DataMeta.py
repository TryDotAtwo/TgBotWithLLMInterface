# -*- coding: utf-8 -*-
import os
os.environ["QT_QPA_PLATFORM_PLUGIN_PATH"] = r"C:\Users\Иван Литвак\AppData\Local\Programs\Python\Python311\Lib\site-packages\PyQt5\Qt\plugins\platforms"
os.environ["QT_LOGGING_RULES"] = "qt5ct.debug=false"
os.environ["QT_PLUGIN_PATH"] = r"C:\Users\Иван Литвак\AppData\Local\Programs\Python\Python311\Lib\site-packages\PyQt5\Qt5\plugins"

import pyqtgraph as pg
from PyQt5.QtWidgets import QApplication
from pyqtgraph.exporters import ImageExporter
import sqlite3
from typing import List, Dict, Any, Tuple, Iterator, Optional, Union
import pandas as pd
from pyqtgraph.Qt import QtWidgets
from datetime import datetime
from dateutil.tz import tzutc
import json
import random
import logging
from pathlib import Path
import pickle
import numpy as np
import psutil

# Настройка логирования
def setup_logging(debug_mode: bool) -> None:
    """Настройка логирования в зависимости от режима отладки."""
    level = logging.INFO if debug_mode else logging.WARNING
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('data_manager.log')
        ]
    )

logger = logging.getLogger(__name__)

class DataManager:
    """Класс для управления данными из базы данных SQLite с SQL-центричной обработкой и полностью динамическими батчами."""

    def __init__(self, folder_path: str, output_dir: str = "output", debug_mode: bool = False):
        """
        Инициализация DataManager.

        Args:
            folder_path (str): Путь к папке, содержащей подпапки с файлами .db.
            output_dir (str): Папка для сохранения результатов (графиков, JSON).
            debug_mode (bool): Режим отладки (True - больше логов, False - минимум логов).
        """
        self.folder_path: Path = Path(folder_path)
        self.output_dir: Path = Path(output_dir)
        self.debug_mode: bool = debug_mode
        self.db_files: List[Path] = []
        self.sensor_info: List[Dict[str, Any]] = []
        self.time_period: Dict[str, str] = {"start_time": None, "end_time": None}
        self.sensor_magic_numbers: Dict[str, Dict[str, Any]] = {}
        self.time_period_cache_file = self.output_dir / "time_period_cache.pkl"
        self.bytes_per_row = 32  # Оценка: 8 байт (float64 для времени) + 4 байта (float32 для значения) + запас
        setup_logging(debug_mode)
        self._initialize()

    def _initialize(self) -> None:
        """Инициализация базовых настроек и создание директорий."""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        if self.debug_mode:
            logger.info(f"Создана/существует директория для вывода: {self.output_dir}")
        try:
            self._load_db_files()
            self._load_cached_time_period()
            self._enable_wal_mode()
        except Exception as e:
            if self.debug_mode:
                logger.error(f"Ошибка при инициализации: {e}")
            raise

    def _enable_wal_mode(self) -> None:
        """Включение режима WAL для всех баз данных для поддержки конкурентного чтения."""
        for db_file in self.db_files:
            try:
                with sqlite3.connect(db_file, timeout=10) as conn:
                    conn.execute("PRAGMA journal_mode=WAL")
                    if self.debug_mode:
                        logger.info(f"Включён режим WAL для {db_file}")
            except sqlite3.Error as e:
                if self.debug_mode:
                    logger.warning(f"Ошибка при включении WAL для {db_file}: {e}")

    def _load_db_files(self) -> None:
        """Загрузка списка файлов .db из указанной директории и всех её подпапок."""
        if not self.folder_path.exists():
            if self.debug_mode:
                logger.error(f"Папка {self.folder_path} не существует.")
            raise FileNotFoundError(f"Папка {self.folder_path} не существует.")

        self.db_files = list(self.folder_path.rglob("*.db"))
        if not self.db_files:
            if self.debug_mode:
                logger.error(f"В папке {self.folder_path} и её подпапках не найдено файлов .db.")
            raise FileNotFoundError(f"В папке {self.folder_path} и её подпапках не найдено файлов .db.")

        if self.debug_mode:
            logger.info(f"Найдено файлов .db: {len(self.db_files)}")
            db_files_by_folder = {}
            for db_file in self.db_files:
                folder = str(db_file.parent)
                if folder not in db_files_by_folder:
                    db_files_by_folder[folder] = 0
                db_files_by_folder[folder] += 1
            for folder, count in db_files_by_folder.items():
                logger.info(f"  Папка {folder}: {count} файл(ов) .db")

    def _load_cached_time_period(self) -> None:
        """Загрузка кэшированного временного диапазона."""
        if self.time_period_cache_file.exists():
            try:
                with open(self.time_period_cache_file, 'rb') as f:
                    self.time_period = pickle.load(f)
                if self.debug_mode:
                    logger.info(f"Загружен кэшированный временной диапазон: {self.time_period}")
            except Exception as e:
                if self.debug_mode:
                    logger.warning(f"Ошибка загрузки кэша временного диапазона: {e}, пересчитываю.")

    def _save_time_period_cache(self) -> None:
        """Сохранение кэша временного диапазона."""
        try:
            with open(self.time_period_cache_file, 'wb') as f:
                pickle.dump(self.time_period, f)
            if self.debug_mode:
                logger.info(f"Сохранён кэш временного диапазона: {self.time_period_cache_file}")
        except Exception as e:
            if self.debug_mode:
                logger.error(f"Ошибка сохранения кэша временного диапазона: {e}")

    def _calculate_batch_size(self, total_rows: int) -> int:
        """Рассчитывает размер батча на основе доступной памяти и количества строк."""
        try:
            available_memory = psutil.virtual_memory().available
            max_memory = available_memory * 0.25
            batch_size = int(max_memory // self.bytes_per_row)
            batch_size = max(1000, min(batch_size, total_rows // 10 + 1))
            if self.debug_mode:
                logger.info(f"Рассчитан размер батча: {batch_size} строк (доступно памяти: {available_memory / 1_000_000:.2f} МБ)")
            return batch_size
        except Exception as e:
            if self.debug_mode:
                logger.warning(f"Ошибка при расчёте размера батча: {e}, используется дефолтный размер")
            return 10000

    def get_time_period(self) -> Dict[str, str]:
        """Определение временного периода данных во всех файлах .db через SQL."""
        if self.time_period["start_time"] and self.time_period["end_time"]:
            return self.time_period

        all_min_ts = []
        all_max_ts = []
        for db_file in self.db_files:
            try:
                with sqlite3.connect(db_file, timeout=10) as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='data';")
                    if not cursor.fetchone():
                        continue

                    cursor.execute("SELECT MIN([time@timestamp]), MAX([time@timestamp]) FROM data")
                    min_ts, max_ts = cursor.fetchone()
                    if min_ts is not None and max_ts is not None:
                        all_min_ts.append(float(min_ts))
                        all_max_ts.append(float(max_ts))
            except sqlite3.Error as e:
                if self.debug_mode:
                    logger.error(f"Ошибка при получении временного периода из файла {db_file}: {e}")
                continue

        if not all_min_ts or not all_max_ts:
            if self.debug_mode:
                logger.error("Нет данных для определения временного периода.")
            raise ValueError("Нет данных для определения временного периода.")

        start_ts = min(all_min_ts)
        end_ts = max(all_max_ts)
        self.time_period["start_time"] = datetime.fromtimestamp(start_ts, tz=tzutc()).strftime('%Y-%m-%d %H:%M:%S')
        self.time_period["end_time"] = datetime.fromtimestamp(end_ts, tz=tzutc()).strftime('%Y-%m-%d %H:%M:%S')
        if self.debug_mode:
            logger.info(f"Временной период данных: с {self.time_period['start_time']} по {self.time_period['end_time']}")
        self._save_time_period_cache()
        return self.time_period

    def get_sensor_info(self, deduplicate_by_index: bool = False) -> List[Dict[str, Any]]:
        """Получение информации о датчиках из таблиц data_format во всех файлах .db с помощью SQL."""
        if self.sensor_info:
            return self.sensor_info

        if self.debug_mode:
            logger.info("Сбор информации о датчиках через SQL:")
        sensor_dict: Dict[Tuple[str, int], Dict[str, Any]] = {}
        seen_indices = set() if deduplicate_by_index else None

        for db_file in self.db_files:
            try:
                with sqlite3.connect(db_file, timeout=10) as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='data_format';")
                    if not cursor.fetchone():
                        if self.debug_mode:
                            logger.warning(f"В файле {db_file} нет таблицы data_format, пропускаю.")
                        continue

                    cursor.execute("SELECT comment, data_format_index, data_type FROM data_format")
                    rows = cursor.fetchall()
                    for row in rows:
                        sensor_name, index, data_type = row
                        index = int(index)

                        if deduplicate_by_index and index in seen_indices:
                            continue

                        key = (sensor_name, index)
                        if key in sensor_dict:
                            sensor = sensor_dict[key]
                            if sensor["data_type"] != data_type:
                                if self.debug_mode:
                                    logger.warning(
                                        f"Разные типы данных для датчика {sensor_name} (Индекс: {index}): "
                                        f"{sensor['data_type']} (в {sensor['source_files']}) и {data_type} (в {db_file})"
                                    )
                            sensor["source_files"].append(str(db_file))
                        else:
                            sensor_dict[key] = {
                                "sensor_name": sensor_name,
                                "index": index,
                                "data_type": data_type,
                                "source_files": [str(db_file)]
                            }
                        if deduplicate_by_index:
                            seen_indices.add(index)
            except sqlite3.Error as e:
                if self.debug_mode:
                    logger.error(f"Ошибка при обработке файла {db_file}: {e}")
                continue

        if not sensor_dict:
            if self.debug_mode:
                logger.error("Информация о датчиках не найдена.")
            raise ValueError("Информация о датчиках не найдена.")

        self.sensor_info = list(sensor_dict.values())
        self.sensor_info.sort(key=lambda x: x["index"])

        if self.debug_mode:
            logger.info("Список датчиков:")
            for i, sensor in enumerate(self.sensor_info, 1):
                logger.info(f"  {i}. {sensor['sensor_name']} (Индекс: {sensor['index']}, Тип: {sensor['data_type']}, Источников: {len(sensor['source_files'])})")
                for source_file in sensor['source_files']:
                    logger.info(f"    - Источник: {source_file}")
        return self.sensor_info

    def get_data_stream(self, sensor_index: int, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None) -> Union[Iterator[Tuple[List[datetime], List[float], Dict[str, Any]]], Tuple[List[datetime], List[float], Dict[str, Any]]]:
        """Загрузка данных для датчика через SQL с полностью динамическим выбором батчей."""
        if not self.sensor_magic_numbers:
            self.create_magic_numbers_dict()

        matching_sensors = [sensor for sensor in self.sensor_info if sensor["index"] == sensor_index]
        if not matching_sensors:
            raise ValueError(f"Датчик с индексом {sensor_index} не найден.")

        sensor = matching_sensors[0]
        sensor_name = sensor["sensor_name"]
        magic_numbers = self.sensor_magic_numbers.get(sensor_name, {})
        if not magic_numbers and self.debug_mode:
            logger.warning(f"Магические числа для датчика {sensor_name} не определены.")

        total_rows = 0
        for db_file in sensor["source_files"]:
            try:
                with sqlite3.connect(db_file, timeout=10) as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='data';")
                    if not cursor.fetchone():
                        if self.debug_mode:
                            logger.warning(f"В файле {db_file} нет таблицы data, пропускаю.")
                        continue

                    cursor.execute(f"PRAGMA table_info(data)")
                    columns = [row[1] for row in cursor.fetchall()]
                    if f"data_format_{sensor_index}" not in columns:
                        if self.debug_mode:
                            logger.info(f"В файле {db_file} отсутствует колонка data_format_{sensor_index}, пропускаю.")
                        continue

                    count_query = f"SELECT COUNT(*) FROM data WHERE data_format_{sensor_index} IS NOT NULL"
                    if start_time or end_time:
                        conditions = []
                        if start_time:
                            conditions.append(f"[time@timestamp] >= {int(start_time.timestamp())}")
                        if end_time:
                            conditions.append(f"[time@timestamp] <= {int(end_time.timestamp())}")
                        count_query += " AND " + " AND ".join(conditions)
                    cursor.execute(count_query)
                    total_rows += cursor.fetchone()[0]
            except sqlite3.Error as e:
                if self.debug_mode:
                    logger.error(f"Ошибка при подсчёте строк в файле {db_file}: {e}")
                continue

        if total_rows == 0:
            if self.debug_mode:
                logger.warning(f"Нет данных для датчика {sensor_name} в указанный период.")
            return [], [], magic_numbers

        try:
            available_memory = psutil.virtual_memory().available
            memory_limit = available_memory * 0.25
            data_size = total_rows * self.bytes_per_row
            use_batches = data_size > memory_limit
            if self.debug_mode:
                logger.info(f"Общее количество строк: {total_rows}, объём данных: {data_size / 1_000_000:.2f} МБ, "
                            f"доступно памяти: {available_memory / 1_000_000:.2f} МБ, использовать батчи: {use_batches}")
        except Exception as e:
            if self.debug_mode:
                logger.warning(f"Ошибка при оценке памяти: {e}, используется батчевая загрузка")
            use_batches = True

        if not use_batches:
            all_times: List[datetime] = []
            all_values: List[float] = []
            for db_file in sensor["source_files"]:
                try:
                    with sqlite3.connect(db_file, timeout=10) as conn:
                        cursor = conn.cursor()
                        cursor.execute(f"PRAGMA table_info(data)")
                        columns = [row[1] for row in cursor.fetchall()]
                        if f"data_format_{sensor_index}" not in columns:
                            continue

                        query = f"SELECT [time@timestamp], data_format_{sensor_index} FROM data WHERE data_format_{sensor_index} IS NOT NULL"
                        if start_time or end_time:
                            conditions = []
                            if start_time:
                                conditions.append(f"[time@timestamp] >= {int(start_time.timestamp())}")
                            if end_time:
                                conditions.append(f"[time@timestamp] <= {int(end_time.timestamp())}")
                            query += " AND " + " AND ".join(conditions)
                        query += " ORDER BY [time@timestamp]"
                        cursor.execute(query)

                        for row in cursor:
                            try:
                                ts_float = float(row[0])
                                dt = datetime.fromtimestamp(ts_float, tz=tzutc())
                                value = float(row[1])
                                all_times.append(dt)
                                all_values.append(value)
                            except (ValueError, TypeError) as e:
                                if self.debug_mode:
                                    logger.warning(f"Ошибка преобразования данных в файле {db_file}: {e}")
                                continue
                except sqlite3.Error as e:
                    if self.debug_mode:
                        logger.error(f"Ошибка при загрузке данных из файла {db_file}: {e}")
                    continue
            return all_times, all_values, magic_numbers

        batch_size = self._calculate_batch_size(total_rows)
        def data_iterator():
            for db_file in sensor["source_files"]:
                try:
                    with sqlite3.connect(db_file, timeout=10) as conn:
                        cursor = conn.cursor()
                        cursor.execute(f"PRAGMA table_info(data)")
                        columns = [row[1] for row in cursor.fetchall()]
                        if f"data_format_{sensor_index}" not in columns:
                            continue

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
                            cursor.execute(query)

                            times: List[datetime] = []
                            values: List[float] = []
                            rows_fetched = 0
                            for row in cursor:
                                try:
                                    ts_float = float(row[0])
                                    dt = datetime.fromtimestamp(ts_float, tz=tzutc())
                                    value = float(row[1])
                                    times.append(dt)
                                    values.append(value)
                                    rows_fetched += 1
                                except (ValueError, TypeError) as e:
                                    if self.debug_mode:
                                        logger.warning(f"Ошибка преобразования данных в файле {db_file}: {e}")
                                    continue

                            if rows_fetched == 0:
                                break
                            if times:
                                yield times, values, magic_numbers
                            offset += batch_size
                except sqlite3.Error as e:
                    if self.debug_mode:
                        logger.error(f"Ошибка при загрузке данных из файла {db_file}: {e}")
                    continue
        return data_iterator()

    def get_sensor_time_period(self, sensor_index: int) -> Tuple[Optional[str], Optional[str], Optional[int]]:
        """Определение временного диапазона данных и количества записей для датчика через SQL."""
        matching_sensors = [sensor for sensor in self.sensor_info if sensor["index"] == sensor_index]
        if not matching_sensors:
            raise ValueError(f"Датчик с индексом {sensor_index} не найден.")

        sensor = matching_sensors[0]
        all_min_ts = []
        all_max_ts = []
        total_data_count = 0

        for db_file in sensor["source_files"]:
            try:
                with sqlite3.connect(db_file, timeout=10) as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='data';")
                    if not cursor.fetchone():
                        if self.debug_mode:
                            logger.warning(f"В файле {db_file} нет таблицы data, пропускаю.")
                        continue

                    cursor.execute(f"PRAGMA table_info(data)")
                    columns = [row[1] for row in cursor.fetchall()]
                    if f"data_format_{sensor_index}" not in columns:
                        if self.debug_mode:
                            logger.info(f"В файле {db_file} отсутствует колонка data_format_{sensor_index}, пропускаю.")
                        continue

                    query = f"SELECT MIN([time@timestamp]), MAX([time@timestamp]), COUNT(*) FROM data WHERE data_format_{sensor_index} IS NOT NULL"
                    cursor.execute(query)
                    min_ts, max_ts, count = cursor.fetchone()
                    if min_ts is not None and max_ts is not None:
                        all_min_ts.append(float(min_ts))
                        all_max_ts.append(float(max_ts))
                        total_data_count += count
            except sqlite3.Error as e:
                if self.debug_mode:
                    logger.error(f"Ошибка при получении временного периода из файла {db_file}: {e}")
                continue

        if not all_min_ts or not all_max_ts:
            return None, None, 0

        start_ts = min(all_min_ts)
        end_ts = max(all_max_ts)
        start_time = datetime.fromtimestamp(start_ts, tz=tzutc()).strftime('%Y-%m-%d %H:%M:%S')
        end_time = datetime.fromtimestamp(end_ts, tz=tzutc()).strftime('%Y-%m-%d %H:%M:%S')
        return start_time, end_time, total_data_count

    def create_magic_numbers_dict(self) -> Dict[str, Dict[str, Any]]:
        """Создание словаря с магическими числами для каждого датчика."""
        if self.sensor_magic_numbers:
            return self.sensor_magic_numbers

        sensors = self.get_sensor_info()
        if self.debug_mode:
            logger.info("Создание словаря магических чисел:")
        for sensor in sensors:
            sensor_name = sensor["sensor_name"]
            data_type = sensor["data_type"]
            if data_type.lower() in ["temperature", "temp"]:
                self.sensor_magic_numbers[sensor_name] = {
                    "type": "temperature",
                    "unit": "К",
                    "normal_range": [0, 350],
                    "threshold_jump": 30,
                    "working_levels": [20, 80, 300],
                    "tolerance": 10
                }
            else:
                self.sensor_magic_numbers[sensor_name] = {
                    "type": data_type.lower(),
                    "unit": "unknown",
                    "normal_range": [0, 100],
                    "threshold_jump": 10,
                    "working_levels": [],
                    "tolerance": 5
                }
            if self.debug_mode:
                logger.info(f"  {sensor_name}: {self.sensor_magic_numbers[sensor_name]}")
        return self.sensor_magic_numbers

    def save_metadata_to_json(self) -> None:
        """Сохранение метаданных (информация о датчиках и временной период) в JSON-файл."""
        output_data = {
            "sensors": self.get_sensor_info(),
            "time_period": self.get_time_period(),
            "magic_numbers": self.create_magic_numbers_dict()
        }
        json_path = self.output_dir / "metadata.json"
        try:
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=4, ensure_ascii=False)
            if self.debug_mode:
                logger.info(f"Метаданные сохранены в: {json_path}")
        except IOError as e:
            if self.debug_mode:
                logger.error(f"Ошибка при сохранении метаданных в JSON: {e}")
            raise

    def save_analysis_results(self, results: Dict[str, Any], filename: str) -> None:
        """Сохранение результатов анализа в JSON-файл."""
        json_path = self.output_dir / filename
        try:
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=4, ensure_ascii=False)
            if self.debug_mode:
                logger.info(f"Результаты анализа сохранены в: {json_path}")
        except IOError as e:
            if self.debug_mode:
                logger.error(f"Ошибка при сохранении результатов анализа: {e}")
            raise

    def plot_data(self, times: List[datetime], values: List[float], sensor_name: str, title: str, filename: str, color: str = 'g') -> None:
        """Отрисовка и сохранение графика с помощью PyQtGraph."""
        app = QtWidgets.QApplication.instance()
        if app is None:
            app = QtWidgets.QApplication([])

        times_numeric = np.array([t.timestamp() for t in times])
        values_numeric = np.array(values)

        win = pg.GraphicsLayoutWidget(show=True, title=title)
        win.resize(800, 400)
        plot_item = win.addPlot(title=title)
        curve = plot_item.plot(pen=pg.mkPen(color=color, width=2), name=sensor_name)

        curve.setData(times_numeric, values_numeric)
        plot_item.setLabel('left', 'Значение')
        plot_item.setLabel('bottom', 'Время')
        plot_item.showGrid(x=True, y=True)

        plot_path = self.output_dir / f"{filename}.png"
        try:
            exporter = pg.exporters.ImageExporter(plot_item)
            exporter.parameters()['width'] = 800
            exporter.export(str(plot_path))
            if self.debug_mode:
                logger.info(f"График сохранён: {plot_path}")
        except Exception as e:
            if self.debug_mode:
                logger.error(f"Ошибка при сохранении графика: {e}")
            raise

        if self.debug_mode:
            pg.exec()

    def plot_selected_sensor(self, sensor_name: str, start_time: Optional[str] = None, end_time: Optional[str] = None) -> None:
        """Отрисовка и сохранение графика для выбранного датчика за указанный период."""
        sensors = self.get_sensor_info()
        if not sensors:
            if self.debug_mode:
                logger.error("Нет информации о датчиках для отрисовки графика.")
            return

        sensor = next((s for s in sensors if s["sensor_name"] == sensor_name), None)
        if not sensor:
            if self.debug_mode:
                logger.error(f"Датчик с именем {sensor_name} не найден.")
            raise ValueError(f"Датчик с именем {sensor_name} не найден.")

        sensor_index = sensor["index"]
        if self.debug_mode:
            logger.info(f"Выбран датчик: {sensor_name} (Индекс: {sensor_index})")

        time_period = self.get_time_period()
        if not start_time:
            start_time = time_period["start_time"]
        if not end_time:
            end_time = time_period["end_time"]

        try:
            start_dt = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S').replace(tzinfo=tzutc()) if start_time else None
            end_dt = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S').replace(tzinfo=tzutc()) if end_time else None
        except ValueError as e:
            if self.debug_mode:
                logger.error(f"Неверный формат даты. Ожидается 'YYYY-MM-DD HH:MM:SS'. Ошибка: {e}")
            raise

        if start_dt and end_dt and start_dt > end_dt:
            if self.debug_mode:
                logger.error("Начальная дата должна быть раньше конечной.")
            raise ValueError("Начальная дата должна быть раньше конечной.")

        if self.debug_mode:
            logger.info(f"Период для графика: с {start_time or 'начало'} по {end_time or 'конец'}")

        all_times: List[datetime] = []
        all_values: List[float] = []
        data = self.get_data_stream(sensor_index, start_dt, end_dt)
        if isinstance(data, tuple):
            times, values, _ = data
            all_times.extend(times)
            all_values.extend(values)
        else:
            for times, values, _ in data:
                all_times.extend(times)
                all_values.extend(values)

        if not all_times:
            if self.debug_mode:
                logger.warning(f"Нет данных для датчика {sensor_name} в указанный период.")
            return

        start_str = start_time.replace(":", "-").replace(" ", "_") if start_time else "start"
        end_str = end_time.replace(":", "-").replace(" ", "_") if end_time else "end"
        filename = f"sensor_plot_{sensor_name}_{start_str}_{end_str}"

        self.plot_data(
            times=all_times,
            values=all_values,
            sensor_name=sensor_name,
            title=f"График данных для датчика {sensor_name} ({start_time or 'начало'} - {end_time or 'конец'})",
            filename=filename
        )

    def print_sensor_info(self, sensor_name: str) -> None:
        """Вывод и сохранение полной информации о выбранном датчике, включая количество записей."""
        sensors = self.get_sensor_info()
        if not sensors:
            if self.debug_mode:
                logger.error("Нет информации о датчиках.")
            return

        sensor = next((s for s in sensors if s["sensor_name"] == sensor_name), None)
        if not sensor:
            if self.debug_mode:
                logger.error(f"Датчик с именем {sensor_name} не найден.")
            raise ValueError(f"Датчик с именем {sensor_name} не найден.")

        sensor_index = sensor["index"]
        if self.debug_mode:
            logger.info(f"Информация о датчике: {sensor_name}")
            logger.info(f"  Индекс: {sensor['index']}")
            logger.info(f"  Тип данных: {sensor['data_type']}")
            logger.info(f"  Количество источников: {len(sensor['source_files'])}")
            for source_file in sensor['source_files']:
                logger.info(f"    - Источник: {source_file}")

        start_time, end_time, data_count = self.get_sensor_time_period(sensor_index)
        if start_time and end_time:
            data_period = f"с {start_time} по {end_time}"
        else:
            if self.debug_mode:
                logger.warning(f"Нет данных для датчика {sensor_name}.")
            data_period = "Нет данных"

        if self.debug_mode:
            logger.info(f"  Временной диапазон данных: {data_period}")
            logger.info(f"  Количество записей: {data_count}")

        magic_numbers = self.sensor_magic_numbers.get(sensor_name, {})
        if self.debug_mode:
            logger.info(f"  Магические числа: {magic_numbers if magic_numbers else 'Не определены'}")

        filename = f"sensor_info_{sensor_name}.txt"
        file_path = self.output_dir / filename
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(f"Информация о датчике: {sensor_name}\n")
                f.write(f"Индекс: {sensor['index']}\n")
                f.write(f"Тип данных: {sensor['data_type']}\n")
                f.write(f"Количество источников: {len(sensor['source_files'])}\n")
                for source_file in sensor['source_files']:
                    f.write(f"  - Источник: {source_file}\n")
                f.write(f"Временной диапазон данных: {data_period}\n")
                f.write(f"Количество записей: {data_count}\n")
                f.write(f"Магические числа: {magic_numbers if magic_numbers else 'Не определены'}\n")
            if self.debug_mode:
                logger.info(f"Информация о датчике сохранена в: {file_path}")
        except IOError as e:
            if self.debug_mode:
                logger.error(f"Ошибка при сохранении информации о датчике: {e}")
            raise

    def plot_random_sensor(self) -> None:
        """Отрисовка графика для случайного датчика."""
        sensors = self.get_sensor_info()
        if not sensors:
            if self.debug_mode:
                logger.error("Нет информации о датчиках для отрисовки графика.")
            return

        random_sensor = random.choice(sensors)
        sensor_name = random_sensor["sensor_name"]
        sensor_index = random_sensor["index"]
        if self.debug_mode:
            logger.info(f"Выбран случайный датчик: {sensor_name} (Индекс: {sensor_index})")

        all_times: List[datetime] = []
        all_values: List[float] = []
        data = self.get_data_stream(sensor_index)
        if isinstance(data, tuple):
            times, values, _ = data
            all_times.extend(times)
            all_values.extend(values)
        else:
            for times, values, _ in data:
                all_times.extend(times)
                all_values.extend(values)

        if not all_times:
            if self.debug_mode:
                logger.warning(f"Нет данных для датчика {sensor_name}.")
            return

        self.plot_data(
            times=all_times,
            values=all_values,
            sensor_name=sensor_name,
            title=f"График данных для датчика {sensor_name}",
            filename=f"random_sensor_plot_{sensor_name}"
        )

if __name__ == "__main__":
    data_manager = DataManager(
        folder_path=r"D:\Автоматизация\cMT-7232\datalog",
        output_dir="output",
        debug_mode=True
    )
    data_manager.save_metadata_to_json()
    data_manager.plot_random_sensor()
    data_manager.plot_selected_sensor(
        sensor_name="T23 (Тво4)",
        start_time="2025-05-28 00:00:00",
        end_time="2025-06-04 23:59:59"
    )
    data_manager.print_sensor_info(sensor_name="T23 (Тво4)")
    data_manager.get_sensor_info()
    data_manager.get_time_period()

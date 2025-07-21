import json
import random
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Tuple, Union, Dict

import numpy as np
import pyqtgraph as pg
from PyQt5.QtWidgets import QApplication

import os
import traceback

os.environ["QT_QPA_PLATFORM_PLUGIN_PATH"] = r"C:\Users\Иван Литвак\AppData\Local\Programs\Python\Python311\Lib\site-packages\PyQt5\Qt\plugins\platforms"
os.environ["QT_LOGGING_RULES"] = "qt5ct.debug=false"
os.environ["QT_PLUGIN_PATH"] = r"C:\Users\Иван Литвак\AppData\Local\Programs\Python\Python311\Lib\site-packages\PyQt5\Qt5\plugins"

from pyqtgraph.exporters import ImageExporter
from dateutil.tz import tzutc

import logging

class DataProcessor:
    def __init__(
        self,
        DataReader,
        folder_path: str,
        debug_mode: bool = False,
        output_dir: Optional[Union[str, Path]] = None,
        logger: logging.Logger = None
    ):
        """
        Инициализация DataProcessor.

        :param folder_path: Путь к папке с базами данных для DataReader.
        :param debug_mode: Включить отладочные логи.
        :param output_dir: Папка для сохранения метаданных и графиков (по умолчанию "Database").
        :param logger: Логгер, переданный из main.
        """
        self.reader = DataReader
        self.debug_mode = debug_mode
        self.output_dir = Path(output_dir) if output_dir else Path("Database")
        self.logger = logger or logging.getLogger(__name__)
        try:
            self.output_dir.mkdir(parents=True, exist_ok=True)
            self.logger.debug("Создана директория вывода: %s", self.output_dir)
            self._app = self._init_qt_app()
        except Exception as e:
            self.logger.error("Ошибка инициализации DataProcessor: %s", e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())
            raise

    def _init_qt_app(self) -> QApplication:
        """Создаёт QApplication, если он ещё не создан."""
        self.logger.debug("Инициализация QApplication")
        try:
            app = QApplication.instance()
            if app is None:
                app = QApplication([])
                self.logger.debug("QApplication создан")
            return app
        except Exception as e:
            self.logger.error("Ошибка создания QApplication: %s", e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())
            raise

    def save_metadata_to_json(self) -> None:
        """Сохраняет метаданные с информацией о сенсорах и периоде времени в JSON-файл."""
        self.logger.debug("Сохранение метаданных в JSON")
        output_data = {
            "sensors": self.reader.get_sensor_info(),
            "time_period": self.reader.get_time_period(),
        }
        json_path = self.output_dir / "metadata.json"
        try:
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=4, ensure_ascii=False)
            self.logger.debug("Метаданные сохранены в: %s", json_path)
        except IOError as e:
            self.logger.error("Ошибка сохранения метаданных: %s", e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())
            raise

    def _extract_times_values(
        self,
        data_stream: Union[Tuple[List[datetime], List[float]], List[Tuple[List[datetime], List[float]]]]
    ) -> Tuple[List[datetime], List[float]]:
        """Извлекает списки времени и значений из потока данных."""
        self.logger.debug("Извлечение времени и значений из потока данных")
        all_times, all_values = [], []
        try:
            if isinstance(data_stream, tuple):
                all_times, all_values = data_stream
            else:
                for times, values in data_stream:
                    all_times.extend(times)
                    all_values.extend(values)
            if not all_times or not all_values:
                self.logger.warning("Пустые данные после извлечения")
            self.logger.debug("Извлечено %d временных меток и значений", len(all_times))
            return all_times, all_values
        except Exception as e:
            self.logger.error("Ошибка извлечения данных: %s", e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())
            return [], []

    def plot_data(self, times, values, sensor_name, title, filename, color='g', grid=True) -> Path:
        """Строит и сохраняет график данных, возвращает путь к файлу."""
        self.logger.debug("Построение графика для датчика %s", sensor_name)
        if not times or not values:
            self.logger.error("Нет данных для отрисовки графика для %s", sensor_name)
            raise ValueError(f"Нет данных для построения графика для {sensor_name}")

        try:
            times_numeric = np.array([t.timestamp() for t in times])
            values_numeric = np.array(values)

            win = pg.GraphicsLayoutWidget(show=False, title=title)  # show=False для минимизации GUI
            win.resize(800, 400)
            plot_item = win.addPlot(title=title)
            curve = plot_item.plot(pen=pg.mkPen(color=color, width=2), name=sensor_name)
            curve.setData(times_numeric, values_numeric)
            plot_item.setLabel('left', 'Значение', units='ед.')
            plot_item.setLabel('bottom', 'Время', units='с')
            if grid:
                plot_item.showGrid(x=True, y=True)

            plot_path = self.output_dir / f"{filename}.png"
            exporter = ImageExporter(plot_item)
            exporter.parameters()['width'] = 800
            exporter.export(str(plot_path))
            self.logger.debug("График сохранён: %s", plot_path)
            win.close()  # Закрываем окно после сохранения
            return plot_path
        except Exception as e:
            self.logger.error("Ошибка сохранения графика для %s: %s", sensor_name, e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())
            raise

    def plot_selected_sensor(
        self,
        sensor_name: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
    ) -> Path:
        """Строит график для выбранного сенсора за указанный промежуток времени."""
        self.logger.debug("Построение графика для датчика %s с периода %s по %s", sensor_name, start_time, end_time)
        sensors = self.reader.get_sensor_info()
        sensor = next((s for s in sensors if s["sensor_name"] == sensor_name), None)
        if not sensor:
            self.logger.error("Датчик %s не найден", sensor_name)
            raise ValueError(f"Датчик {sensor_name} не найден.")

        try:
            start_dt = (
                datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S').replace(tzinfo=tzutc())
                if start_time else None
            )
            end_dt = (
                datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S').replace(tzinfo=tzutc())
                if end_time else None
            )
            if start_dt and end_dt and start_dt > end_dt:
                self.logger.error("Начальная дата %s позже конечной %s", start_dt, end_dt)
                raise ValueError("Начальная дата должна быть раньше конечной.")

            data = self.reader.get_data_stream(sensor["index"], start_dt, end_dt)
            all_times, all_values = self._extract_times_values(data)

            if not all_times or not all_values:
                self.logger.error("Нет данных для датчика %s за период %s - %s", sensor_name, start_time, end_time)
                raise ValueError(f"Нет данных для датчика {sensor_name} за указанный период.")

            start_str = start_time.replace(":", "-").replace(" ", "_") if start_time else "start"
            end_str = end_time.replace(":", "-").replace(" ", "_") if end_time else "end"
            filename = f"sensor_plot_{sensor_name}_{start_str}_{end_str}"
            return self.plot_data(all_times, all_values, sensor_name, f"График для {sensor_name}", filename)
        except Exception as e:
            self.logger.error("Ошибка построения графика для %s: %s", sensor_name, e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())
            raise

    def get_time_period(self) -> Dict[str, str]:
        """Возвращает доступный временной период данных."""
        self.logger.debug("Получение временного периода")
        try:
            period = self.reader.get_time_period()
            if not isinstance(period, dict) or "start_time" not in period or "end_time" not in period:
                self.logger.error("Некорректный формат временного периода")
                raise ValueError("Некорректный формат временного периода")
            return period
        except Exception as e:
            self.logger.error("Ошибка получения временного периода: %s", e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())
            raise

    def plot_random_sensor(self) -> Path:
        """Строит график случайно выбранного сенсора за весь период, возвращает путь к файлу."""
        self.logger.debug("Построение графика для случайного датчика")
        sensors = self.reader.get_sensor_info()
        if not sensors:
            self.logger.error("Нет доступных датчиков для отрисовки")
            raise ValueError("Нет доступных датчиков")

        try:
            sensor = random.choice(sensors)
            sensor_name = sensor["sensor_name"]
            self.logger.debug("Выбран случайный датчик: %s", sensor_name)
            data = self.reader.get_data_stream(sensor["index"])
            all_times, all_values = self._extract_times_values(data)

            if not all_times or not all_values:
                self.logger.error("Нет данных для датчика %s", sensor_name)
                raise ValueError(f"Нет данных для датчика {sensor_name}")

            filename = f"random_sensor_plot_{sensor_name}"
            return self.plot_data(all_times, all_values, sensor_name, f"График для {sensor_name}", filename)
        except Exception as e:
            self.logger.error("Ошибка построения случайного графика: %s", e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())
            raise
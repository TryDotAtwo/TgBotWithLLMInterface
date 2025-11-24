import json
import random
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple, Union, Dict
import numpy as np
import pyqtgraph as pg
from pyqtgraph import DateAxisItem
from PyQt6.QtWidgets import QApplication, QSizePolicy
import os
import traceback
import math
# os.environ["QT_QPA_PLATFORM_PLUGIN_PATH"] = r"C:\Users\Иван Литвак\AppData\Local\Programs\Python\Python311\Lib\site-packages\PyQt6\Qt6\plugins\platforms"
# os.environ["QT_LOGGING_RULES"] = "qt5ct.debug=false"
# os.environ["QT_PLUGIN_PATH"] = r"C:\Users\Иван Литвак\AppData\Local\Programs\Python\Python311\Lib\site-packages\PyQt6\Qt6\plugins"
from pyqtgraph.exporters import ImageExporter
from dateutil.tz import tzutc, tzlocal
import logging
from pyqtgraph.Qt import QtCore, QtGui  # Добавлено для RotatedDateAxisItem

class RotatedDateAxisItem(DateAxisItem):
    """Кастомный DateAxisItem с поворотом тиков на 90° и кастомной генерацией тиков."""
    def __init__(self, orientation, angle=-90, **kwargs):
        super().__init__(orientation, **kwargs)
        self.angle = angle

    def sizeHint(self, which=QSizePolicy.Policy.Preferred, detail=''):
        """Переопределение sizeHint для резервирования дополнительного пространства под повёрнутые метки."""
        s = super().sizeHint(which, detail)
        if which == QSizePolicy.Policy.Preferred or which == QSizePolicy.Policy.Minimum:
            extra = 0 # Дополнительное пространство для повёрнутого текста
            s.setHeight(s.height() + extra)
        return s

    def tickValues(self, minVal, maxVal, size):
        """Переопределение для генерации равномерных тиков, заполняющих шкалу."""
        if maxVal <= minVal:
            return [(0, [])]
        # В 3 раза больше тиков — ~60 для 1200px (size / 20)
        num_ticks = max(30, int(size / 20))
        spacing = (maxVal - minVal) / num_ticks
        positions = []
        # Первый тик >= minVal
        start = math.ceil(minVal / spacing) * spacing
        x = start
        while x <= maxVal:
            positions.append(x)
            x += spacing
        if len(positions) < 2:
            return [(0, [minVal, maxVal])]
        if hasattr(self, 'logger'):
            self.logger.debug("Generated %d ticks with spacing %.0f sec (size: %.0f px)", len(positions), spacing, size)
        return [(spacing, positions)]

    def drawPicture(self, p, axisSpec, tickSpecs, textSpecs):
        p.setRenderHint(p.RenderHint.Antialiasing, False)
        p.setRenderHint(p.RenderHint.TextAntialiasing, True)
        # --- Ось ---
        pen, p1, p2 = axisSpec
        p.setPen(pen)
        p.drawLine(p1, p2)
        # --- Тики ---
        for pen, p1, p2 in tickSpecs:
            p.setPen(pen)
            p.drawLine(p1, p2)
        # --- Метки ---
        if self.style['tickFont']:
            p.setFont(self.style['tickFont'])
        p.setPen(self.textPen())
        fm = p.fontMetrics()
        text_height = fm.height()
        extra_offset = 30 # Отступ от тика вниз
        for rect, flags, text in textSpecs:
            p.save()
            # 1. К центру тика (по X), к низу тика (по Y)
            tick_x = rect.center().x()
            tick_y = rect.bottom()
            p.translate(tick_x, tick_y)
            # 2. Поворот на -90° → текст "вниз"
            p.rotate(-90)
            # 3. СДВИГ ВНИЗ: в повёрнутой СК — это по X!
            p.translate(-(text_height + extra_offset), 0) # ← ВОТ ЭТО КЛЮЧ!
            # 4. Рисуем текст: AlignLeft | AlignTop (в повёрнутой СК)
            text_rect = QtCore.QRectF(0, 0, 300, text_height)
            p.drawText(text_rect,
                       QtCore.Qt.AlignmentFlag.AlignLeft | QtCore.Qt.AlignmentFlag.AlignTop,
                       text)
            p.restore()

    def boundingRect(self):
        """Увеличиваем bounding rect, чтобы текст не обрезался."""
        rect = super().boundingRect()
        rect.adjust(0, 0, 0, 0) # Увеличиваем пространство снизу
        return rect

class DataProcessor:
    def __init__(
        self,
        DataReader,
        folder_path: str,
        debug_mode: bool = False,
        output_dir: Optional[Union[str, Path]] = None,
        logger: logging.Logger = None,
        report_generator=None, # 👈 добавили сюда
        build_report_data=None
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
        self.report_generator = report_generator # 👈 сохраняем внутри
        self.build_report_data = build_report_data
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
            if len(all_times) == 0 or len(all_values) == 0:
                self.logger.warning("Пустые данные после извлечения")
            self.logger.debug("Извлечено %d временных меток и значений", len(all_times))
            return all_times, all_values
        except Exception as e:
            self.logger.error("Ошибка извлечения данных: %s", e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())
            return [], []

    def plot_data(self, times, values, sensor_name, title, filename, color='g', grid=True, max_points: int = None, y_label: Optional[str] = None, y_units: Optional[str] = None) -> Path:
        """Строит и сохраняет график данных, возвращает путь к файлу.
       
        :param y_label: Опциональный заголовок для оси Y (по умолчанию 'Значение').
        :param y_units: Опциональные единицы для оси Y (по умолчанию 'ед.').
        """
        self.logger.debug("Построение графика для датчика %s", sensor_name)
        if len(times) == 0 or len(values) == 0:
            self.logger.error("Нет данных для отрисовки графика для %s", sensor_name)
            raise ValueError(f"Нет данных для построения графика для {sensor_name}")
        try:
            # Timestamps в секундах (UTC)
            times_numeric = np.array([t.timestamp() for t in times])
            values_numeric = np.array(values)
         
            # Сортировка по времени
            sort_idx = np.argsort(times_numeric)
            times_numeric = times_numeric[sort_idx]
            values_numeric = values_numeric[sort_idx]
         
            # Downsampling отключён (max_points=None)
            if max_points and len(times_numeric) > max_points:
                step = len(times_numeric) // max_points
                times_numeric = times_numeric[::step]
                values_numeric = values_numeric[::step]
                self.logger.debug("Downsampled до %d точек", len(times_numeric))
         
            # Диагностика density
            duration_sec = max(times_numeric) - min(times_numeric)
            density = duration_sec / 1200 # Для width=1200px
            self.logger.debug("Диапазон времени: %s - %s (секунды: %.0f - %.0f, duration: %.0f сек, density: %.0f сек/пиксель)",
                              min(times), max(times), min(times_numeric), max(times_numeric), duration_sec, density)
            win = pg.GraphicsLayoutWidget(show=False, title=title)
            win.resize(1200, 1100) # Высота 1100 для места снизу
            win.setBackground('w')
            plot_item = win.addPlot(title=title)
            plot_item.getViewBox().setBackgroundColor('w')
         
            # Добавляем данные
            curve = plot_item.plot(pen=pg.mkPen(color=color, width=2), name=sensor_name)
            curve.setData(times_numeric, values_numeric)
         
            # Настройки осей
            if y_label:
                plot_item.setLabel('left', y_label, units=y_units or 'ед.')
            else:
                plot_item.setLabel('left', 'Значение', units='ед.')
            plot_item.setLabel('bottom', 'Время')
         
            # Увеличиваем bottom margin для опускания оси и места под labels
            plot_item.layout.setContentsMargins(0, 0, 0, 50)
         
            # Используем кастомный DateAxisItem с поворотом и кастомными тиками
            date_axis = RotatedDateAxisItem(orientation='bottom', angle=-90)
            date_axis.logger = self.logger # Для debug в tickValues
         
            # Кастомный формат — время в локальной TZ
            def custom_tick_strings(values, scale, spacing):
                """Кастомный формат: время HH:MM:%S в локальной TZ для всех тиков."""
                strings = []
                local_tz = tzlocal()
                for val in values:
                    if val is None:
                        strings.append('')
                    else:
                        try:
                            utc_dt = datetime.fromtimestamp(val, tz=timezone.utc)
                            local_dt = utc_dt.astimezone(local_tz)
                            strings.append(local_dt.strftime('%d.%m'))
                        except Exception:
                            strings.append('')
                return strings
         
            date_axis.tickStrings = custom_tick_strings # Применяем кастом
         
            plot_item.setAxisItems({'bottom': date_axis})
         
            if grid:
                plot_item.showGrid(x=True, y=True, alpha=0.3)
            # Принудительное обновление для layout и рендеринга
            date_axis.update()
            plot_item.update()
            win.update()
         
            # Resize трюк для force update
            win.resize(1200, 1101)
            win.resize(1200, 1100)
         
            # Принудительный рендеринг
            self._app.processEvents()
            plot_path = self.output_dir / f"{filename}.png"
            exporter = ImageExporter(plot_item) # Экспортируем plot_item с margin
            exporter.parameters()['width'] = 1200
            exporter.parameters()['height'] = 800 # Увеличиваем height для margin
            exporter.export(str(plot_path))
         
            self.logger.debug("График сохранён: %s (размер: %d точек, density: %.0f сек/пиксель, тики: равномерные ~%d с поворотом -90°, локальное время, Y-label: %s)",
                              plot_path, len(times_numeric), density, max(30, int(1200 / 20)), y_label or 'Значение')
         
            win.close()
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
            y_label: Optional[str] = None,
            y_units: Optional[str] = None,
        ) -> Path:
            """Строит график для выбранного сенсора за указанный промежуток времени.
           
            :param y_label: Опциональный заголовок для оси Y.
            :param y_units: Опциональные единицы для оси Y.
            """
            self.logger.debug("Построение графика для датчика %s с периода %s по %s", sensor_name, start_time, end_time)
            sensors = self.reader.get_sensor_info()
            sensor = sensors.get(sensor_name)
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
                data = self.reader.get_data_stream(sensor_name, start_time=start_dt, end_time=end_dt)
                all_times, all_values = self._extract_times_values(data)
                if len(all_times) == 0 or len(all_values) == 0:
                    self.logger.error("Нет данных для датчика %s за период %s - %s", sensor_name, start_time, end_time)
                    raise ValueError(f"Нет данных для датчика {sensor_name} за указанный период.")
                start_str = start_time.replace(":", "-").replace(" ", "_") if start_time else "start"
                end_str = end_time.replace(":", "-").replace(" ", "_") if end_time else "end"
                filename = f"sensor_plot_{sensor_name}_{start_str}_{end_str}"
                return self.plot_data(all_times, all_values, sensor_name, f"График для {sensor_name}", filename, y_label=y_label, y_units=y_units)
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



    def plot_random_sensor(self, y_label: Optional[str] = None, y_units: Optional[str] = None) -> Path:
        """Строит график случайно выбранного сенсора за весь период, возвращает путь к файлу.
   
        :param y_label: Опциональный заголовок для оси Y.
        :param y_units: Опциональные единицы для оси Y.
        """
        self.logger.debug("Построение графика для случайного датчика")
        sensors = self.reader.get_sensor_info()
        if not sensors:
            self.logger.error("Нет доступных датчиков для отрисовки")
            raise ValueError("Нет доступных датчиков")
        try:
            sensor = random.choice(list(sensors.values()))
            sensor_name = sensor["sensor_name"]
            self.logger.debug("Выбран случайный датчик: %s", sensor_name)

            # Получаем весь поток данных
            all_times, all_values = self.reader.get_data_stream(sensor_name)

            # === 🔥 Ограничение количества точек для предотвращения MemoryError ===
            MAX_POINTS = 20000
            if len(all_times) > MAX_POINTS:
                step = max(1, len(all_times) // MAX_POINTS)
                self.logger.debug(f"Downsample данных: исходно {len(all_times)}, шаг {step}")
                all_times = all_times[::step]
                all_values = all_values[::step]
            # =====================================================================

            if len(all_times) == 0 or len(all_values) == 0:
                self.logger.error("Нет данных для датчика %s", sensor_name)
                raise ValueError(f"Нет данных для датчика {sensor_name}")

            filename = f"random_sensor_plot_{sensor_name}"
            return self.plot_data(
                all_times,
                all_values,
                sensor_name,
                f"График для {sensor_name}",
                filename,
                y_label=y_label,
                y_units=y_units
            )
        except Exception as e:
            self.logger.error("Ошибка построения случайного графика: %s", e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())
            raise


    def generate_report(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        output_dir: str = "reports",
        logger: logging.Logger = None
    ) -> Tuple[List[Path], Path, Path]:
        """
        Генерирует отчёт с 5 фиксированными графиками (LS01, T01, P11, T06, P12).
        sensor_name НЕ передаётся — датчики жёстко заданы.
        Args:
            data_processor: DataProcessor
            start_time: datetime (UTC)
            end_time: datetime (UTC)
            output_dir: Папка для графиков и отчётов
            logger: Логгер
        Returns:
            (plot_paths, pdf_path, Path)
        """
        logger = logger or logging.getLogger(__name__)
        output_dir = Path(output_dir)
        plots_dir = output_dir / "plots"
        reports_dir = output_dir / "reports"
        plots_dir.mkdir(parents=True, exist_ok=True)
        reports_dir.mkdir(parents=True, exist_ok=True)
        # === ФИКСИРОВАННЫЕ 5 ДАТЧИКОВ ===
        FIXED_SENSORS = [
            ("LS01 (газгольдер)", "Объём", "Объём", "литры"),
            ("T01 (DT51)", "T01 (DT51)", "Температура", "K"),
            ("P11 (ВД22)", "P11", "Давление", "Torr"),
            ("T06 (T32)", "T06 (T32)", "Температура", "K"),
            ("P12 (ВД21)", "ВД21", "Давление", "Torr")
        ]
        plot_paths = []
        image_paths_dict = {}
        try:
            reader = self.reader
            sensor_info = reader.get_sensor_info()
            # === 1. Строим 5 графиков ===
            for idx, (sensor_name, description, y_label, y_units) in enumerate(FIXED_SENSORS, start=1):
                if sensor_name not in sensor_info:
                    logger.warning(f"Датчик {sensor_name} не найден — пропуск")
                    plot_paths.append(None)
                    continue
                sensor = sensor_info[sensor_name]
                logger.debug("Получение данных для: %s", sensor_name)
                data_stream = self.reader.get_data_stream(sensor_name, start_time=start_time, end_time=end_time)
                times, values = self._extract_times_values(data_stream)
                if not times:
                    logger.warning(f"Нет данных для {sensor_name}")
                    plot_paths.append(None)
                    continue
                # Имя файла
                start_str = start_time.strftime("%Y%m%d_%H%M") if start_time else "start"
                end_str = end_time.strftime("%Y%m%d_%H%M") if end_time else "end"
                filename = f"plot_{idx}_{sensor_name}_{start_str}_{end_str}"
                # Построение графика
                plot_path = self.plot_data(
                    times=times,
                    values=values,
                    sensor_name=sensor_name,
                    title=description,
                    filename=filename,
                    color='b',
                    y_label=y_label,
                    y_units=y_units
                )
                plot_paths.append(plot_path)
                image_paths_dict[f"image{idx}"] = str(plot_path)
                logger.info(f"График {idx} ({sensor_name}): {plot_path}")
            # === 2. Техносхема ===
            image_paths_dict["image6"] = r"C:\Users\Иван Литвак\source\repos\Автоматизация отчетов\Автоматизация отчетов\Техносхема.jpg"
            # === 3. Период для отчёта ===
            all_times = []
            for path, (name, _, _, _) in zip(plot_paths, FIXED_SENSORS):
                if path:
                    # Перечитываем данные для получения времени
                    sensor = sensor_info[name]
                    stream = self.reader.get_data_stream(name, start_time=start_time, end_time=end_time)
                    t, _ = self._extract_times_values(stream)
                    all_times.extend(t)
            if all_times:
                start_local = min(all_times).astimezone(tzlocal())
                end_local = max(all_times).astimezone(tzlocal())
            else:
                start_local = (start_time or datetime.now(timezone.utc)).astimezone(tzlocal())
                end_local = (end_time or datetime.now(timezone.utc)).astimezone(tzlocal())
            # === 4. minimal_data ===
            minimal_data = {
                "period": {
                    "start_date": start_local.strftime("%d.%m.%Y"),
                    "end_date": end_local.strftime("%d.%m.%Y")
                },
                "udsh_measurements": [
                    {"party": 1, "registered": 5300},
                    {"party": 2, "registered": 4700},
                    {"party": 3, "registered": 3000}
                ],
                "image_paths": image_paths_dict,
                "content": [
                    {"text": "<b>Вывод:</b> Все системы работают в штатном режиме.", "font_size": 10}
                ]
            }
            # === 5. Генерация отчёта ===
            full_data = self.build_report_data(minimal_data)
            start_short = start_local.strftime("%d%m%y")
            end_short = end_local.strftime("%d%m%y")
            report_name = f"Отчет_КЗ201_{start_short}-{end_short}"
            pdf_path = reports_dir / f"{report_name}.pdf"
            docx_path = reports_dir / f"{report_name}.docx"
            pdf_out, docx_out = self.report_generator(
                data=full_data,
                pdf_output=str(pdf_path),
                docx_output=str(docx_path)
            )
            logger.info(f"PDF: {pdf_out}")
            logger.info(f"DOCX: {docx_out}")
            # === 6. Дополнительный график SUM_BALLS ===
            sum_balls_sensor_name = "SUM_BALLS"
            if sum_balls_sensor_name in sensor_info:
                sensor = sensor_info[sum_balls_sensor_name]
                logger.debug("Получение данных для: %s", sum_balls_sensor_name)
                data_stream = self.reader.get_data_stream(sum_balls_sensor_name, start_time=start_time, end_time=end_time)
                times, values = self._extract_times_values(data_stream)
                if times:
                    # Имя файла
                    start_str = start_time.strftime("%Y%m%d_%H%M") if start_time else "start"
                    end_str = end_time.strftime("%Y%m%d_%H%M") if end_time else "end"
                    filename = f"plot_6_{sum_balls_sensor_name}_{start_str}_{end_str}"
                    # Построение графика
                    sum_balls_plot_path = self.plot_data(
                        times=times,
                        values=values,
                        sensor_name=sum_balls_sensor_name,
                        title="Счетчик шариков",
                        filename=filename,
                        color='b',
                        y_label="Количество шариков",
                        y_units="тыс. шт."
                    )
                    plot_paths.append(sum_balls_plot_path)
                    logger.info(f"Дополнительный график SUM_BALLS: {sum_balls_plot_path}")
                else:
                    logger.warning(f"Нет данных для {sum_balls_sensor_name}")
                    plot_paths.append(None)
            else:
                logger.warning(f"Датчик {sum_balls_sensor_name} не найден — пропуск")
                plot_paths.append(None)
            return plot_paths, Path(pdf_out), Path(docx_out)
        except Exception as e:
            logger.error(f"Ошибка в generate_report: {e}")
            logger.error(traceback.format_exc())
            raise

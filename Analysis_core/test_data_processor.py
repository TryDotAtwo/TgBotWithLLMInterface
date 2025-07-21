# -*- coding: utf-8 -*-

import pytest
from unittest.mock import patch, MagicMock, mock_open
from pathlib import Path
from datetime import datetime
import os
import json
import numpy as np
import logging

os.environ["QT_QPA_PLATFORM_PLUGIN_PATH"] = r"C:\Users\Иван Литвак\AppData\Local\Programs\Python\Python311\Lib\site-packages\PyQt5\Qt\plugins\platforms"
os.environ["QT_LOGGING_RULES"] = "qt5ct.debug=false"
os.environ["QT_PLUGIN_PATH"] = r"C:\Users\Иван Литвак\AppData\Local\Programs\Python\Python311\Lib\site-packages\PyQt5\Qt5\plugins"

from PyQt5.QtGui import QColor, QBrush
from PyQt5.QtCore import QRectF
from PyQt5.QtWidgets import QApplication
import pyqtgraph as pg
from Analysis_core.data_processor import DataProcessor
from dateutil.tz import tzutc


@pytest.fixture
def mock_data_reader():
    mock_reader = MagicMock()
    yield mock_reader


@pytest.fixture
def mock_folder(tmp_path):
    return tmp_path


@pytest.fixture
def mock_app():
    """Фикстура для управления QApplication, предотвращает утечки ресурсов."""
    with patch('PyQt5.QtWidgets.QApplication.instance', return_value=None), \
         patch('PyQt5.QtWidgets.QApplication') as mock_qapp:
        yield mock_qapp.return_value


@pytest.fixture(autouse=True)
def cleanup_qt():
    """Очистка Qt-ресурсов после каждого теста."""
    yield
    if QApplication.instance():
        QApplication.quit()
        QApplication.instance().deleteLater()



def test_init_no_existing_app(mock_data_reader, mock_folder):
    with patch('PyQt5.QtWidgets.QApplication') as mock_app:
        processor = DataProcessor(mock_data_reader, str(mock_folder), debug_mode=True, output_dir=mock_folder)
        assert isinstance(processor._app, MagicMock)


def test_init_existing_app(mock_folder, mock_data_reader, caplog):
    """Тестирует инициализацию DataProcessor, когда QApplication уже существует."""
    caplog.set_level('INFO')
    with patch('PyQt5.QtWidgets.QApplication.instance') as mock_instance:
        mock_instance.return_value = MagicMock(spec=QApplication)
        processor = DataProcessor(mock_data_reader, str(mock_folder), debug_mode=True, output_dir=mock_folder)
        assert processor._app == mock_instance.return_value
        assert "QApplication создан" not in caplog.text


def test_save_metadata_to_json_success(mock_folder, mock_data_reader, caplog):
    """Тестирует успешное сохранение метаданных в JSON."""
    caplog.set_level('INFO')
    mock_data_reader.get_sensor_info.return_value = [
        {"sensor_name": "Sensor1", "index": 1, "source_files": ["test1.db"]}
    ]
    mock_data_reader.get_time_period.return_value = ["2025-01-01", "2025-01-02"]
    
    expected_json = {
        "sensors": [{"sensor_name": "Sensor1", "index": 1, "source_files": ["test1.db"]}],
        "time_period": ["2025-01-01", "2025-01-02"]
    }
    
    mock_file = mock_open()
    with patch('builtins.open', mock_file), \
         patch('json.dump') as mock_json_dump:
        processor = DataProcessor(mock_data_reader, str(mock_folder), debug_mode=True, output_dir=mock_folder)
        processor.save_metadata_to_json()
        
        mock_file.assert_called_once_with(mock_folder / "metadata.json", 'w', encoding='utf-8')
        mock_json_dump.assert_called_once_with(
            expected_json,
            mock_file(),
            ensure_ascii=False,
            indent=4
        )
        assert "Метаданные сохранены в" in caplog.text


def test_save_metadata_to_json_ioerror(mock_folder, mock_data_reader, caplog):
    """Тестирует обработку ошибки IOError при сохранении метаданных."""
    caplog.set_level('ERROR')
    mock_data_reader.get_sensor_info.return_value = [
        {"sensor_name": "Sensor1", "index": 1, "source_files": ["test1.db"]}
    ]
    mock_data_reader.get_time_period.return_value = ["2025-01-01", "2025-01-02"]
    
    with patch('builtins.open', side_effect=IOError("Permission denied")):
        processor = DataProcessor(mock_data_reader, str(mock_folder), debug_mode=True, output_dir=mock_folder)
        with pytest.raises(IOError, match="Permission denied"):
            processor.save_metadata_to_json()
        assert "Ошибка сохранения метаданных: Permission denied" in caplog.text


def test_extract_times_values_single_tuple(mock_data_reader):
    """Тестирует извлечение данных из кортежа (times, values)."""
    mock_data_reader.get_data_stream.return_value = ([datetime(2025, 1, 1)], [10.0])
    processor = DataProcessor(mock_data_reader, "dummy_path")
    times, values = processor._extract_times_values(([datetime(2025, 1, 1)], [10.0]))
    assert times == [datetime(2025, 1, 1)]
    assert values == [10.0]


def test_extract_times_values_list_of_tuples(mock_data_reader):
    """Тестирует извлечение данных из списка кортежей."""
    mock_data_reader.get_data_stream.return_value = [
        ([datetime(2025, 1, 1)], [10.0]),
        ([datetime(2025, 1, 2)], [20.0])
    ]
    processor = DataProcessor(mock_data_reader, "dummy_path")
    times, values = processor._extract_times_values([
        ([datetime(2025, 1, 1)], [10.0]),
        ([datetime(2025, 1, 2)], [20.0])
    ])
    assert times == [datetime(2025, 1, 1), datetime(2025, 1, 2)]
    assert values == [10.0, 20.0]


def test_plot_data_success(mock_data_reader, mock_folder):
    with patch('pyqtgraph.GraphicsLayoutWidget') as mock_widget, \
         patch('pyqtgraph.exporters.ImageExporter') as mock_exporter:
        processor = DataProcessor(mock_data_reader, str(mock_folder), debug_mode=True, output_dir=mock_folder)
        mock_exporter_instance = mock_exporter.return_value
        mock_exporter_instance.export.return_value = None  # Указываем, что экспорт ничего не возвращает
        processor.plot_data()  # Вызов метода должен пройти без ошибок

def test_plot_data_empty_data(mock_folder, mock_data_reader, caplog):
    """Тестирует обработку пустых данных в plot_data."""
    caplog.set_level('WARNING')
    processor = DataProcessor(mock_data_reader, str(mock_folder), debug_mode=True, output_dir=mock_folder)
    processor.plot_data([], [], "Sensor1", "Empty Plot", "empty_plot")
    assert "Нет данных для отрисовки." in caplog.text



def test_plot_data_export_error(mock_data_reader, mock_folder):
    with patch('pyqtgraph.GraphicsLayoutWidget') as mock_widget, \
         patch('pyqtgraph.exporters.ImageExporter') as mock_exporter:
        processor = DataProcessor(mock_data_reader, str(mock_folder), debug_mode=True, output_dir=mock_folder)
        mock_exporter_instance = mock_exporter.return_value
        mock_exporter_instance.export.side_effect = Exception("Export failed")  # Настраиваем выброс исключения
        with pytest.raises(Exception, match="Export failed"):
            processor.plot_data()


def test_plot_selected_sensor_success(mock_folder, mock_data_reader, caplog):
    """Тестирует успешное построение графика для выбранного сенсора."""
    caplog.set_level('INFO')
    mock_data_reader.get_sensor_info.return_value = [
        {"sensor_name": "Sensor1", "index": 1, "source_files": ["test1.db"]}
    ]
    mock_data_reader.get_data_stream.return_value = ([datetime(2025, 1, 1)], [10.0])

    with patch('pyqtgraph.GraphicsLayoutWidget') as mock_widget, \
         patch('pyqtgraph.exporters.ImageExporter') as mock_exporter:

        mock_win = mock_widget.return_value
        mock_plot_item = MagicMock()
        mock_win.addPlot.return_value = mock_plot_item
        mock_curve = MagicMock()
        mock_plot_item.plot.return_value = mock_curve
        mock_curve.setData.return_value = None
        mock_plot_item.setLabel.return_value = None
        mock_plot_item.showGrid.return_value = None

        mock_exporter_instance = mock_exporter.return_value
        mock_exporter_instance.export.return_value = None

        processor = DataProcessor(mock_data_reader, str(mock_folder), debug_mode=True, output_dir=mock_folder)
        processor.plot_selected_sensor("Sensor1", "2025-01-01 00:00:00", "2025-01-02 00:00:00")
        
        mock_curve.setData.assert_called_once()
        mock_exporter_instance.export.assert_called_once()
        assert "График сохранён" in caplog.text


def test_plot_selected_sensor_sensor_not_found(mock_folder, mock_data_reader, caplog):
    """Тестирует обработку случая, когда сенсор не найден."""
    caplog.set_level('ERROR')
    mock_data_reader.get_sensor_info.return_value = [
        {"sensor_name": "Sensor2", "index": 1, "source_files": ["test1.db"]}
    ]
    
    processor = DataProcessor(mock_data_reader, str(mock_folder), debug_mode=True, output_dir=mock_folder)
    with pytest.raises(ValueError, match="Датчик Sensor1 не найден."):
        processor.plot_selected_sensor("Sensor1", "2025-01-01 00:00:00", "2025-01-02 00:00:00")
    assert "Датчик Sensor1 не найден." in caplog.text


def test_plot_selected_sensor_invalid_date_order(mock_folder, mock_data_reader, caplog):
    """Тестирует обработку некорректного порядка дат."""
    caplog.set_level('ERROR')
    mock_data_reader.get_sensor_info.return_value = [
        {"sensor_name": "Sensor1", "index": 1, "source_files": ["test1.db"]}
    ]
    
    processor = DataProcessor(mock_data_reader, str(mock_folder), debug_mode=True, output_dir=mock_folder)
    with pytest.raises(ValueError, match="Начальная дата должна быть раньше конечной."):
        processor.plot_selected_sensor("Sensor1", "2025-01-02 00:00:00", "2025-01-01 00:00:00")
    assert "Начальная дата позже конечной." in caplog.text


def test_plot_selected_sensor_no_data(mock_folder, mock_data_reader, caplog):
    """Тестирует обработку случая, когда данные отсутствуют."""
    caplog.set_level('WARNING')
    mock_data_reader.get_sensor_info.return_value = [
        {"sensor_name": "Sensor1", "index": 1, "source_files": ["test1.db"]}
    ]
    mock_data_reader.get_data_stream.return_value = ([], [])
    
    processor = DataProcessor(mock_data_reader, str(mock_folder), debug_mode=True, output_dir=mock_folder)
    processor.plot_selected_sensor("Sensor1", "2025-01-01 00:00:00", "2025-01-02 00:00:00")
    assert "Нет данных для Sensor1." in caplog.text


def test_plot_random_sensor_success(mock_folder, mock_data_reader, caplog):
    """Тестирует успешное построение графика для случайного сенсора."""
    caplog.set_level('INFO')
    mock_data_reader.get_sensor_info.return_value = [
        {"sensor_name": "Sensor1", "index": 1, "source_files": ["test1.db"]}
    ]
    mock_data_reader.get_data_stream.return_value = ([datetime(2025, 1, 1)], [10.0])

    with patch('random.choice', return_value={"sensor_name": "Sensor1", "index": 1}), \
         patch('pyqtgraph.GraphicsLayoutWidget') as mock_widget, \
         patch('pyqtgraph.exporters.ImageExporter') as mock_exporter:

        mock_win = mock_widget.return_value
        mock_plot_item = MagicMock()
        mock_win.addPlot.return_value = mock_plot_item
        mock_curve = MagicMock()
        mock_plot_item.plot.return_value = mock_curve
        mock_curve.setData.return_value = None
        mock_plot_item.setLabel.return_value = None
        mock_plot_item.showGrid.return_value = None

        mock_exporter_instance = mock_exporter.return_value
        mock_exporter_instance.export.return_value = None

        processor = DataProcessor(mock_data_reader, str(mock_folder), debug_mode=True, output_dir=mock_folder)
        processor.plot_random_sensor()

        mock_curve.setData.assert_called_once()
        mock_exporter_instance.export.assert_called_once()
        assert "График сохранён" in caplog.text


def test_plot_random_sensor_no_sensors(mock_folder, mock_data_reader, caplog):
    """Тестирует обработку случая, когда нет сенсоров."""
    caplog.set_level('ERROR')
    mock_data_reader.get_sensor_info.return_value = []
    
    processor = DataProcessor(mock_data_reader, str(mock_folder), debug_mode=True, output_dir=mock_folder)
    processor.plot_random_sensor()
    assert "Нет датчиков для отрисовки." in caplog.text


def test_plot_random_sensor_no_data(mock_folder, mock_data_reader, caplog):
    """Тестирует обработку случая, когда нет данных для случайного сенсора."""
    caplog.set_level('WARNING')
    mock_data_reader.get_sensor_info.return_value = [
        {"sensor_name": "Sensor1", "index": 1, "source_files": ["test1.db"]}
    ]
    mock_data_reader.get_data_stream.return_value = ([], [])
    
    with patch('random.choice', return_value={"sensor_name": "Sensor1", "index": 1}):
        processor = DataProcessor(mock_data_reader, str(mock_folder), debug_mode=True, output_dir=mock_folder)
        processor.plot_random_sensor()
        assert "Нет данных для Sensor1." in caplog.text
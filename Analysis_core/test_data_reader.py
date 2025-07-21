# -*- coding: utf-8 -*-
import pytest
import sqlite3
import pickle
import os
from pathlib import Path
from datetime import datetime
from unittest.mock import patch, MagicMock
import psutil

from Analysis_core.data_reader import DataReader

# Вспомогательный контекст-менеджер для sqlite3.connect
class DummyCM:
    def __init__(self, conn):
        self._conn = conn
    def __enter__(self):
        return self._conn
    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

@pytest.fixture
def mock_folder(tmp_path):
    # Создаём тестовую папку с одним .db файлом
    db_path = tmp_path / "test.db"
    db_path.write_text("")
    return tmp_path

@pytest.fixture
def data_reader(mock_folder):
    # Мокаем sqlite для WAL и загрузки файлов
    conn = MagicMock()
    conn.execute = MagicMock()
    conn.cursor = MagicMock(return_value=MagicMock())
    with patch("Analysis_core.data_reader.sqlite3.connect", return_value=DummyCM(conn)):
        return DataReader(str(mock_folder), debug_mode=True)

def test_data_reader_init_with_missing_folder():
    with pytest.raises(FileNotFoundError):
        DataReader("nonexistent_folder", debug_mode=True)

def test_data_reader_init(mock_folder):
    reader = DataReader(str(mock_folder), debug_mode=True)
    assert isinstance(reader, DataReader)
    assert reader.folder_path == Path(mock_folder)
    assert isinstance(reader.db_files, list)
    assert len(reader.db_files) == 1
    assert reader.db_files[0].name == "test.db"

def test_get_time_period_cache_load(tmp_path, mock_folder, monkeypatch):
    # Подготовим файл кэша в tmp_path/Cache
    cache_dir = tmp_path / "Cache"
    cache_dir.mkdir()
    fake_cache = {"start_time": "2024-01-01 00:00:00", "end_time": "2024-01-02 00:00:00"}
    cache_file = cache_dir / "time_period_cache.pkl"
    with open(cache_file, "wb") as f:
        pickle.dump(fake_cache, f)

    # Перейдём в tmp_path, чтобы DataReader использовал наш Cache
    monkeypatch.chdir(tmp_path)

    # Мокаем sqlite для WAL
    conn = MagicMock()
    conn.execute = MagicMock()
    conn.cursor = MagicMock(return_value=MagicMock())
    with patch("Analysis_core.data_reader.sqlite3.connect", return_value=DummyCM(conn)):
        reader = DataReader(str(mock_folder), debug_mode=True)
        assert reader.time_period == fake_cache

def test_get_time_period_calculation(data_reader):
    # Сбросим ранее сохраненный период
    data_reader.time_period = {"start_time": None, "end_time": None}
    # Мокаем курсор для возврата timestamp
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ("1710000000", "1710100000")

    mock_conn = MagicMock()
    mock_conn.__enter__.return_value = mock_conn
    mock_conn.__exit__.return_value = False
    mock_conn.cursor.return_value = mock_cursor

    with patch("Analysis_core.data_reader.sqlite3.connect", return_value=mock_conn):
        result = data_reader.get_time_period()
        assert result["start_time"].startswith("2024-")
        assert result["end_time"].startswith("2024-")

def test_get_time_period_no_data(data_reader):
    # Сбросим кеш, чтобы метод действительно выполнялся
    data_reader.time_period = {"start_time": None, "end_time": None}
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (None, None)

    mock_conn = MagicMock()
    mock_conn.__enter__.return_value = mock_conn
    mock_conn.__exit__.return_value = False
    mock_conn.cursor.return_value = mock_cursor

    with patch("Analysis_core.data_reader.sqlite3.connect", return_value=mock_conn):
        with pytest.raises(ValueError, match="Нет данных для временного периода"):
            data_reader.get_time_period()

def test_get_sensor_info(data_reader):
    # Очистим ранее полученную информацию
    data_reader.sensor_info = []
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("Sensor1", 1, "float"), ("Sensor2", 2, "int")]

    mock_conn = MagicMock()
    mock_conn.__enter__.return_value = mock_conn
    mock_conn.__exit__.return_value = False
    mock_conn.cursor.return_value = mock_cursor

    with patch("Analysis_core.data_reader.sqlite3.connect", return_value=mock_conn):
        sensors = data_reader.get_sensor_info()
        assert isinstance(sensors, list)
        assert len(sensors) == 2
        assert sensors[0]["sensor_name"] == "Sensor1"
        assert sensors[1]["index"] == 2

def test_get_sensor_info_deduplicate(data_reader):
    data_reader.sensor_info = []
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("Sensor1", 1, "float"), ("Sensor1", 1, "float")]

    mock_conn = MagicMock()
    mock_conn.__enter__.return_value = mock_conn
    mock_conn.__exit__.return_value = False
    mock_conn.cursor.return_value = mock_cursor

    with patch("Analysis_core.data_reader.sqlite3.connect", return_value=mock_conn):
        sensors = data_reader.get_sensor_info(deduplicate_by_index=True)
        assert len(sensors) == 1
        assert sensors[0]["sensor_name"] == "Sensor1"

def test_get_data_stream_empty(data_reader):
    with patch.object(data_reader, "get_sensor_info", return_value=[{"index": 5, "source_files": ["mock.db"]}]):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (0,)

        mock_conn = MagicMock()
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.__exit__.return_value = False
        mock_conn.cursor.return_value = mock_cursor

        with patch("Analysis_core.data_reader.sqlite3.connect", return_value=mock_conn), \
             patch("Analysis_core.data_reader.psutil.virtual_memory", return_value=MagicMock(available=1)):
            times, values = data_reader.get_data_stream(5)
            assert times == []
            assert values == []

def test_get_data_stream_single_batch(data_reader):
    with patch.object(data_reader, "get_sensor_info", return_value=[{"index": 1, "source_files": ["mock.db"]}]):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1,)
        mock_cursor.__iter__.return_value = iter([("1710000000", "42.0")])

        mock_conn = MagicMock()
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.__exit__.return_value = False
        mock_conn.cursor.return_value = mock_cursor

        with patch("Analysis_core.data_reader.sqlite3.connect", return_value=mock_conn), \
             patch("Analysis_core.data_reader.psutil.virtual_memory", return_value=MagicMock(available=1_000_000_000)):
            times, values = data_reader.get_data_stream(1)
            assert len(times) == 1
            assert values == [42.0]
            assert isinstance(times[0], datetime)

def test_get_data_stream_generator(data_reader):
    with patch.object(data_reader, "get_sensor_info", return_value=[{"index": 1, "source_files": ["mock.db"]}]), \
         patch("Analysis_core.data_reader.psutil.virtual_memory", return_value=MagicMock(available=1)), \
         patch.object(data_reader, "_calculate_batch_size", return_value=1):

        # Мокаем сначала для подсчёта total_rows, затем для итерации
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (10,)  # total_rows=10 -> use_batches True при available=1
        mock_cursor.__iter__.side_effect = [iter([("1710000000", "42.0")]), iter([])]

        mock_conn = MagicMock()
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.__exit__.return_value = False
        mock_conn.cursor.return_value = mock_cursor

        with patch("Analysis_core.data_reader.sqlite3.connect", return_value=mock_conn):
            stream = data_reader.get_data_stream(1)
            assert hasattr(stream, "__iter__")
            times, values = next(stream)
            assert values == [42.0]
            assert isinstance(times[0], datetime)

def test_get_data_stream_sensor_not_found(data_reader):
    with patch.object(data_reader, "get_sensor_info", return_value=[]):
        with pytest.raises(ValueError, match="Датчик с индексом 999 не найден"):
            data_reader.get_data_stream(999)
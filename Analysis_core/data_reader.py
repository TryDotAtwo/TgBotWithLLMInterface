# -*- coding: utf-8 -*-
import sqlite3
from typing import List, Dict, Any, Tuple, Iterator, Optional, Union
from pathlib import Path
from datetime import datetime
from dateutil.tz import tzutc
import logging
import psutil
import traceback
from collections import defaultdict, Counter
import time


import sqlite3
import shutil
from pathlib import Path
from collections import defaultdict
import hashlib
import sqlite3
import shutil
from pathlib import Path
from collections import defaultdict
import hashlib
import json
from datetime import datetime
import logging
import os



class DatabaseMerger:
    def __init__(self, folder_path: str, merged_db_path: str = "merged.db", logger=None):
        self.folder_path = Path(folder_path)
        self.merged_db_path = Path(merged_db_path)
        self.logger = logger or logging.getLogger(__name__)

        self.index_mapping = {}
        self.global_to_names = defaultdict(set)
        self.global_to_type = {}
        self.next_global_idx = 0

        # Кеш состояния исходных файлов (для инкрементального мержа)
        self.source_state = {}  # path → {"size": int, "mtime": float, "max_ts": float, "row_count": int}



    def merge_databases(self, force_rebuild: bool = False) -> Path:
        rebuild_needed = force_rebuild or not self.merged_db_path.exists()

        if not rebuild_needed:
            if self._is_merge_up_to_date():
                self.logger.info("Объединённая база актуальна → только инкремент")
            else:
                self.logger.info("Изменения обнаружены → инкрементальное обновление")

        # Загружаем состояние и маппинг (глобальные ID должны быть стабильны!)
        self._load_previous_state()
        self._load_global_mapping()

        # Защита от битой базы
        if self.merged_db_path.exists():
            try:
                test_conn = sqlite3.connect(f"file:{self.merged_db_path}?mode=ro", uri=True, timeout=5)
                test_conn.execute("SELECT 1 FROM sqlite_master LIMIT 1;")
                test_conn.close()
            except sqlite3.DatabaseError as e:
                if "malformed" in str(e).lower() or "damaged" in str(e).lower():
                    self.logger.warning("Обнаружена битая база merged.db — удаляем и пересоздаём")
                    for suf in ["", "-wal", "-shm"]:
                        p = self.merged_db_path.with_suffix(f".db{suf}")
                        if p.exists():
                            p.unlink(missing_ok=True)
                    # Принудительно пересоздаём как первый запуск
                    rebuild_needed = True

        db_files = sorted([
            p for p in self.folder_path.rglob("*.db")
            if p != self.merged_db_path and "merged" not in p.name
        ])
        if not db_files:
            raise FileNotFoundError("Нет .db файлов в папке")

        temp_db = self.merged_db_path.with_suffix(".tmp.db")
        for p in [temp_db, temp_db.with_suffix(".tmp.db-wal"), temp_db.with_suffix(".tmp.db-shm")]:
            p.unlink(missing_ok=True)

        # Собираем маппинг (используем сохранённый + добавляем новые)
        self._build_global_mapping(db_files)

        first_run = not self.merged_db_path.exists()
        dst_path = str(temp_db if first_run else self.merged_db_path)
        dst = sqlite3.connect(dst_path)
        dst.execute("PRAGMA journal_mode = WAL;")
        dst.execute("PRAGMA synchronous = NORMAL;")
        dst.execute("PRAGMA cache_size = -200000;")

        # Счётчик добавленных строк за весь процесс
        total_new_rows = 0
        previous_total = 0  # Для точного логирования по файлам

        try:
            if first_run:
                dst.execute('CREATE TABLE data (data_index INTEGER PRIMARY KEY, "time@timestamp" REAL);')
                dst.execute("""CREATE TABLE data_format (
                    comment TEXT,
                    data_format_index INTEGER PRIMARY KEY,
                    data_type TEXT
                );""")

                # Создаём все колонки сразу
                for gid in sorted(self.global_to_type.keys()):
                    col = f"data_format_{gid}"
                    dst.execute(f'ALTER TABLE data ADD COLUMN "{col}" REAL;')

                # Заполняем data_format
                cur = dst.cursor()
                for gid, names in self.global_to_names.items():
                    alias = " | ".join(sorted(names))[:500]
                    dtype = self.global_to_type.get(gid, "REAL")
                    cur.execute("INSERT INTO data_format VALUES (?, ?, ?)", (alias, gid, dtype))
                dst.commit()

            else:
                # === Инкремент: добавляем только новые колонки и обновляем алиасы ===
                cur = dst.cursor()
                cur.execute("PRAGMA table_info(data)")
                existing_cols = {row[1] for row in cur.fetchall()}

                cur.execute("SELECT data_format_index, comment FROM data_format")
                existing_format = {row[0]: row[1] for row in cur.fetchall()}

                new_cols = False
                new_aliases = False

                for gid in sorted(self.global_to_type.keys()):
                    col_name = f"data_format_{gid}"

                    # Добавляем колонку, если её нет
                    if col_name not in existing_cols:
                        self.logger.info("Добавляю новую колонку: %s", col_name)
                        dst.execute(f'ALTER TABLE data ADD COLUMN "{col_name}" REAL;')
                        new_cols = True

                    # Обновляем/добавляем запись в data_format
                    current_comment = existing_format.get(gid)
                    new_comment = " | ".join(sorted(self.global_to_names[gid]))[:500]
                    dtype = self.global_to_type.get(gid, "REAL")

                    if gid not in existing_format:
                        cur.execute(
                            "INSERT INTO data_format (comment, data_format_index, data_type) VALUES (?, ?, ?)",
                            (new_comment, gid, dtype)
                        )
                        new_aliases = True
                    elif new_comment != current_comment:
                        cur.execute(
                            "UPDATE data_format SET comment = ?, data_type = ? WHERE data_format_index = ?",
                            (new_comment, dtype, gid)
                        )
                        new_aliases = True

                if new_cols or new_aliases:
                    dst.commit()
                    self.logger.info("Добавлены новые колонки и/или обновлены алиасы в data_format")

            # === Копируем только новые строки ===
            for db_file in db_files:
                file_path = str(db_file)
                stat = db_file.stat()
                current_size = stat.st_size
                current_mtime = stat.st_mtime

                prev = self.source_state.get(file_path, {})
                if not force_rebuild and prev.get("size") == current_size and abs(prev.get("mtime", 0) - current_mtime) < 1:
                    self.logger.debug("Без изменений → %s", db_file.name)
                    continue

                self.logger.info("Обрабатываю (изменён/новый): %s", db_file.name)

                src = sqlite3.connect(f"file:{db_file}?mode=ro", uri=True, timeout=30.0)
                src_cur = src.cursor()

                # Максимальная метка времени в исходном файле
                src_cur.execute('SELECT MAX("time@timestamp") FROM data')
                current_max_ts = src_cur.fetchone()[0] or 0
                last_known_ts = prev.get("max_ts", 0)

                where_clause = 'WHERE "time@timestamp" > ?' if not first_run and last_known_ts else ""
                params = [last_known_ts] if where_clause else []

                src_cur.execute("PRAGMA table_info(data)")
                src_cols = [row[1] for row in src_cur.fetchall()]

                select_parts = ['"time@timestamp"']
                placeholders = ['"time@timestamp"']

                for col in src_cols:
                    if col in ("data_index", "time@timestamp") or not col.startswith("data_format_"):
                        continue
                    try:
                        old_idx = int(col.split("_")[-1])
                    except:
                        continue
                    key = (str(db_file.parent), old_idx)
                    if key not in self.index_mapping:
                        continue
                    gid = self.index_mapping[key]
                    new_col = f"data_format_{gid}"
                    select_parts.append(f'"{col}"')
                    placeholders.append(f'"{new_col}"')

                if len(select_parts) == 1:
                    src.close()
                    continue

                query = f"SELECT {', '.join(select_parts)} FROM data {where_clause}"
                src_cur.execute(query, params)

                batch_size = 10000
                batch = src_cur.fetchmany(batch_size)

                while batch:
                    dst.executemany(
                        f"INSERT INTO main.data ({', '.join(placeholders)}) VALUES ({', '.join(['?'] * len(placeholders))})",
                        batch
                    )
                    total_new_rows += len(batch)
                    batch = src_cur.fetchmany(batch_size)

                dst.commit()

                # Обновляем состояние
                row_count = src.execute("SELECT COUNT(*) FROM data").fetchone()[0]
                self.source_state[file_path] = {
                    "size": current_size,
                    "mtime": current_mtime,
                    "max_ts": float(current_max_ts),
                    "row_count": row_count
                }

                rows_this_file = total_new_rows - previous_total
                previous_total = total_new_rows
                self.logger.info("Добавлено %d новых строк из %s", rows_this_file, db_file.name)

                src.close()

            # Финализация
            if total_new_rows == 0 and not first_run:
                self.logger.info("Новых данных нет — merged.db не изменился")
            else:
                if first_run:
                    dst.execute('CREATE INDEX IF NOT EXISTS idx_time ON data ("time@timestamp");')

                if first_run:
                    # Безопасная замена временной базы
                    dst.close()
                    self._finalize_temp_db(temp_db)

            # Сохраняем всё
            self._save_current_state()
            self._save_merge_metadata(db_files)
            self._save_global_mapping()

        finally:
            if 'dst' in locals():
                dst.close()

        return self.merged_db_path

    # Вспомогательный метод для финализации (чтобы не дублировать код)
    def _finalize_temp_db(self, temp_db: Path):
        self.logger.info("Финализируем merged.db: сбрасываем WAL и заменяем файл")
        try:
            conn = sqlite3.connect(str(temp_db), timeout=30)
            conn.execute("PRAGMA wal_checkpoint(FULL);")
            conn.close()
            time.sleep(0.3)
        except Exception as e:
            self.logger.error("Ошибка wal_checkpoint: %s", e)

        for _ in range(6):
            try:
                for suf in ["", "-wal", "-shm"]:
                    p = self.merged_db_path.with_suffix(f".db{suf}")
                    if p.exists():
                        p.unlink()
                break
            except PermissionError:
                time.sleep(1)

        renamed = False
        try:
            temp_db.rename(self.merged_db_path)
            for suf in ["-wal", "-shm"]:
                src = temp_db.with_suffix(f".tmp.db{suf}")
                dst = self.merged_db_path.with_suffix(f".db{suf}")
                if src.exists():
                    src.rename(dst)
            renamed = True
            self.logger.info("merged.db заменён атомарно через rename")
        except Exception as e:
            self.logger.warning("rename не сработал (%s) — используем copy2", e)

        if not renamed:
            import shutil
            shutil.copy2(temp_db, self.merged_db_path)
            for suf in ["-wal", "-shm"]:
                src = temp_db.with_suffix(f".tmp.db{suf}")
                dst = self.merged_db_path.with_suffix(f".db{suf}")
                if src.exists():
                    shutil.copy2(src, dst)
            self.logger.info("merged.db заменён через copy2")

        for p in [temp_db, temp_db.with_suffix(".tmp.db-wal"), temp_db.with_suffix(".tmp.db-shm")]:
            try:
                p.unlink(missing_ok=True)
            except:
                pass

    def _build_global_mapping(self, db_files):
        for db_file in db_files:
            folder = str(db_file.parent)
            try:
                conn = sqlite3.connect(f"file:{db_file}?mode=ro", uri=True, timeout=10)
                has_format = conn.execute("SELECT name FROM sqlite_master WHERE name='data_format'").fetchone()
                if has_format:
                    for comment, idx, dtype in conn.execute("SELECT comment, data_format_index, data_type FROM data_format"):
                        try:
                            old_idx = int(idx)
                        except:
                            continue
                        key = (folder, old_idx)

                        # ЕСЛИ КЛЮЧ УЖЕ ЕСТЬ — используем старый gid
                        if key not in self.index_mapping:
                            # Только если нет — создаём новый
                            gid = self.next_global_idx
                            self.index_mapping[key] = gid
                            self.global_to_type[gid] = dtype or "REAL"
                            self.next_global_idx += 1
                        else:
                            gid = self.index_mapping[key]

                        name = (comment or "").strip() or f"sensor_{old_idx}"
                        self.global_to_names[gid].add(name)
                conn.close()
            except Exception as e:
                self.logger.debug("Не удалось прочитать data_format из %s: %s", db_file.name, e)

    def _load_previous_state(self):
        state_path = self.merged_db_path.with_suffix(".state.json")
        if state_path.exists():
            try:
                self.source_state = json.loads(state_path.read_text(encoding="utf-8"))
            except:
                self.source_state = {}

    def _save_current_state(self):
        state_path = self.merged_db_path.with_suffix(".state.json")
        state_path.write_text(json.dumps(self.source_state, indent=2, ensure_ascii=False), encoding="utf-8")

    def _get_folder_hash(self):
        h = hashlib.sha256()
        for f in sorted(self.folder_path.rglob("*.db")):
            st = f.stat()
            h.update(f"{f}:{st.st_size}:{int(st.st_mtime)}".encode())
        return h.hexdigest()

    def _save_merge_metadata(self, db_files):
        meta = {
            "merged_at": datetime.now().isoformat(),
            "source_hash": self._get_folder_hash(),
            "total_sources": len(db_files),
            "format": "incremental_data_format_N"
        }
        self.merged_db_path.with_suffix(".meta.json").write_text(
            json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8"
        )

    def _is_merge_up_to_date(self):
        meta_path = self.merged_db_path.with_suffix(".meta.json")
        if not meta_path.exists():
            return False
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            return meta.get("source_hash") == self._get_folder_hash()
        except:
            return False


    def _load_global_mapping(self):
        """Загружаем сохранённый маппинг (folder, old_idx) → global_id"""
        mapping_path = self.merged_db_path.with_suffix(".mapping.json")
        if mapping_path.exists():
            try:
                data = json.loads(mapping_path.read_text(encoding="utf-8"))
                self.index_mapping = {(k[0], k[1]): v for k, v in data.get("index_mapping", {}).items()}
                self.next_global_idx = data.get("next_global_idx", 0)
                self.logger.info(f"Загружено глобальное сопоставление: {len(self.index_mapping)} записей")
            except Exception as e:
                self.logger.warning(f"Не удалось загрузить mapping.json: {e}")

    def _save_global_mapping(self):
        """Сохраняем маппинг на диск"""
        mapping_path = self.merged_db_path.with_suffix(".mapping.json")
        data = {
            "next_global_idx": self.next_global_idx,
            "index_mapping": {str(k): v for k, v in self.index_mapping.items()}
        }
        mapping_path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")








class DataReader:
    """Класс для чтения данных из баз SQLite с использованием HistoryManager для кэширования."""

    def __init__(self, folder_path: str, history_manager=None, debug_mode: bool = False, logger: logging.Logger = None):
        """Инициализация DataReader."""
        self.folder_path: Path = Path(folder_path)
        self.debug_mode: bool = debug_mode
        self.logger = logger or logging.getLogger(__name__)
        self.history_manager = history_manager  # Добавляем HistoryManager
        self.db_files: List[Path] = []
        self.sensor_info: Dict[str, Dict[str, Any]] = {}  # Dict[name -> sensor]
        self.time_period: Dict[str, str] = {"start_time": None, "end_time": None}
        self.bytes_per_row = 32
        self.logger.debug("Инициализация DataReader с путем: %s", folder_path)
        self._initialize()

    def _initialize(self) -> None:
        """Инициализация настроек."""
        try:
            # === НОВАЯ ЛОГИКА: Автоматическое создание/обновление merged.db ===
            merged_db_path = self.folder_path / "merged.db"
            meta_path = merged_db_path.with_suffix(".meta.json")

            need_rebuild = True
            if merged_db_path.exists() and meta_path.exists():
                # Проверяем актуальность по хэшу
                merger_check = DatabaseMerger(self.folder_path, merged_db_path, logger=self.logger)
                if merger_check._is_merge_up_to_date():
                    self.logger.info("Обнаружена актуальная объединённая база: %s", merged_db_path)
                    need_rebuild = False
                else:
                    self.logger.info("Объединённая база устарела — будет пересоздана")
            else:
                self.logger.info("Объединённая база не найдена — будет создана при первом запуске")

            if need_rebuild:
                self.logger.info("Запуск объединения всех .db файлов в один merged.db...")
                merger = DatabaseMerger(self.folder_path, merged_db_path, logger=self.logger)
                merger.merge_databases(force_rebuild=True)
                self.logger.info("Объединение успешно завершено!")

            # === Теперь работаем ТОЛЬКО с merged.db ===
            if not merged_db_path.exists():
                raise FileNotFoundError("Не удалось создать merged.db")

            self.db_files = [merged_db_path]  # ← ВАЖНО: теперь только один файл!
            # ============================================================

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

    def get_sensor_info(self) -> Dict[str, Dict[str, Any]]:
        """Получение информации о датчиках — упрощённая версия для merged.db"""
        self.logger.debug("Получение информации о датчиках (режим merged.db)")

        # Кэширование через HistoryManager
        if self.history_manager:
            cached = self.history_manager.get_cache("sensor_info")
            if cached:
                self.sensor_info = cached
                self.logger.debug("sensor_info загружен из кэша HistoryManager: %d датчиков", len(cached))
                return cached

        if self.sensor_info:
            return self.sensor_info

        self.sensor_info = {}
        db_file = self.db_files[0]  # Теперь всегда только один файл — merged.db

        try:
            with sqlite3.connect(db_file, timeout=10) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT comment, data_format_index, data_type FROM data_format")
                for comment, idx_str, data_type in cursor.fetchall():
                    if not comment:
                        continue
                    idx = int(idx_str)
                    # Разбиваем алиасы по |
                    names = [n.strip() for n in comment.split("|")]
                    for name in names:
                        if name:  # на случай пустых
                            if name in self.sensor_info:
                                self.logger.debug("Дубликат имени '%s' — уже есть, пропускаем", name)
                                continue
                            self.sensor_info[name] = {
                                "sensor_name": name,
                                "index": idx,
                                "data_type": data_type,
                                "source_files": [str(db_file)],
                                "folder": str(db_file.parent),
                                "all_names": names  # опционально: все имена
                            }
        except sqlite3.Error as e:
            self.logger.error("Ошибка чтения data_format из %s: %s", db_file, e)
            raise

        if not self.sensor_info:
            raise ValueError("Не найдено ни одного датчика в объединённой базе")

        # Сохраняем в кэш
        if self.history_manager:
            self.history_manager.set_cache("sensor_info", self.sensor_info, ttl_seconds=86400)

        self.logger.debug("Загружено %d уникальных имён датчиков из merged.db", len(self.sensor_info))
        return self.sensor_info


    def get_data_stream(self, sensor_name: str, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None) -> Union[Iterator[Tuple[List[datetime], List[float]]], Tuple[List[datetime], List[float]]]:
        """Получение потоков данных для датчика по имени (оригинальный интерфейс для новых вызовов)."""
        self.logger.debug("Получение данных для датчика с именем '%s', start_time=%s, end_time=%s", sensor_name, start_time, end_time)
        sensor_info = self.get_sensor_info()
        if sensor_name not in sensor_info:
            self.logger.error("Датчик с именем '%s' не найден", sensor_name)
            raise ValueError(f"Датчик с именем '{sensor_name}' не найден.")
        sensor = sensor_info[sensor_name]
        source_files = sensor["source_files"]
        return self._get_data_stream_internal(sensor["index"], source_files, start_time, end_time)

    def _get_data_stream_internal(self, sensor_index: int, source_files: List[str], 
                                  start_time: Optional[datetime] = None, 
                                  end_time: Optional[datetime] = None
                                  ) -> Tuple[List[datetime], List[float]]:
        """Внутренняя реализация получения данных без генераторов и батчей."""
        all_times: List[datetime] = []
        all_values: List[float] = []

        for db_file_str in source_files:
            db_file = Path(db_file_str)
            try:
                with sqlite3.connect(db_file, timeout=10) as conn:
                    cursor = conn.cursor()
                    query = f"SELECT [time@timestamp], data_format_{sensor_index} FROM data WHERE data_format_{sensor_index} IS NOT NULL"
                    conditions = []
                    if start_time:
                        conditions.append(f"[time@timestamp] >= {int(start_time.timestamp())}")
                    if end_time:
                        conditions.append(f"[time@timestamp] <= {int(end_time.timestamp())}")
                    if conditions:
                        query += " AND " + " AND ".join(conditions)
                    query += " ORDER BY [time@timestamp]"

                    self.logger.debug("Выполняется запрос: %s", query)
                    cursor.execute(query)
                    rows = cursor.fetchall()
                    self.logger.debug("Получено строк из %s: %d", db_file, len(rows))

                    for row in rows:
                        all_times.append(datetime.fromtimestamp(float(row[0]), tz=tzutc()))
                        all_values.append(float(row[1]))

            except sqlite3.Error as e:
                self.logger.error("Ошибка получения данных из %s: %s", db_file, e)
                self.logger.error("Трассировка стека: %s", traceback.format_exc())

        self.logger.debug("Возвращено всего записей: %d", len(all_times))
        return all_times, all_values

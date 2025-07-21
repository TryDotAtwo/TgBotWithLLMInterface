# -*- coding: utf-8 -*-
import sqlite3
import logging
from datetime import datetime, timedelta
from threading import Lock
from typing import Dict, List, Any, Optional
import json
import traceback

class HistoryManager:
    """Управляет персистентной историей переписки пользователей и файловым кешем с таймаутом и безопасным многопоточным доступом."""

    def __init__(self, db_path: str, timeout_hours: int, max_history_size: int, logger: logging.Logger = None):
        self.db_path = db_path
        self.timeout = timedelta(hours=timeout_hours)
        self.max_history_size = max_history_size
        self._lock = Lock()
        self.logger = logger or logging.getLogger(__name__)
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("PRAGMA journal_mode=WAL;")
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS user_history (
                        user_id INTEGER,
                        timestamp TEXT,
                        message TEXT,
                        is_bot INTEGER,
                        user_info TEXT,
                        PRIMARY KEY (user_id, timestamp)
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS user_task_count (
                        user_id INTEGER PRIMARY KEY,
                        task_count INTEGER DEFAULT 0
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS cache (
                        key TEXT PRIMARY KEY,
                        value TEXT,
                        expire_at INTEGER
                    )
                    """
                )
                conn.commit()

            self.clear_all_cache()  # Очистка кеша при запуске
            self.clear_all_history()  # Очистка всей истории при запуске
            self.clean_old_histories(inactivity_hours=3)  # Очистка истории неактивных пользователей
            self.logger.debug("Кеш и история очищены при инициализации")
        except Exception as e:
            self.logger.error("Ошибка инициализации HistoryManager: %s", e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())

    def _get_connection(self) -> sqlite3.Connection:
        """Создаёт новое соединение для безопасного параллельного доступа."""
        self.logger.debug("Создание нового соединения с БД: %s", self.db_path)
        return sqlite3.connect(self.db_path, check_same_thread=False)

    def _clear_expired_cache(self) -> None:
        """Удаляет из кеша все устаревшие записи."""
        now = int(datetime.utcnow().timestamp())
        try:
            with self._lock, self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM cache WHERE expire_at IS NOT NULL AND expire_at < ?", (now,))
                conn.commit()
            self.logger.debug("Удалены устаревшие записи кеша")
        except Exception as e:
            self.logger.error("Ошибка очистки устаревшего кеша: %s", e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())

    def set_cache(self, key: str, value: Any, ttl_seconds: Optional[int] = None) -> None:
        """Сохраняет значение в кеш под заданным ключом с опциональным TTL."""
        expire_at = None
        if ttl_seconds is not None:
            expire_at = int(datetime.utcnow().timestamp()) + ttl_seconds
        data = json.dumps(value, ensure_ascii=False)
        try:
            with self._lock, self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT INTO cache(key, value, expire_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(key) DO UPDATE SET value=excluded.value, expire_at=excluded.expire_at;
                    """,
                    (key, data, expire_at)
                )
                conn.commit()
            self.logger.debug("Установлен кеш: key=%s, expire_at=%s", key, expire_at)
        except Exception as e:
            self.logger.error("Ошибка при записи в кеш: %s", e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())

    def get_cache(self, key: str) -> Optional[Any]:
        """Возвращает значение из кеша или None, если нет или истек."""
        now = int(datetime.utcnow().timestamp())
        try:
            with self._lock, self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT value, expire_at FROM cache WHERE key = ?", (key,))
                row = cursor.fetchone()
                if not row:
                    self.logger.debug("Кеш не найден для ключа: %s", key)
                    return None
                data, expire_at = row
                if expire_at is not None and now > expire_at:
                    cursor.execute("DELETE FROM cache WHERE key = ?", (key,))
                    conn.commit()
                    self.logger.debug("Кеш истек для ключа: %s", key)
                    return None
                result = json.loads(data)
                self.logger.debug("Получено значение из кеша для ключа: %s", key)
                return result
        except json.JSONDecodeError as e:
            self.logger.error("Ошибка декодирования JSON из кеша: %s", e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())
            return None
        except Exception as e:
            self.logger.error("Ошибка получения из кеша: %s", e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())
            return None

    def clear_cache(self, key: str) -> None:
        """Удаляет запись кеша по ключу."""
        try:
            with self._lock, self._get_connection() as conn:
                conn.execute("DELETE FROM cache WHERE key = ?", (key,))
                conn.commit()
            self.logger.debug("Кеш очищен для ключа: %s", key)
        except Exception as e:
            self.logger.error("Ошибка очистки кеша: %s", e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())

    def clear_all_cache(self) -> None:
        """Очищает весь кеш."""
        try:
            with self._lock, self._get_connection() as conn:
                conn.execute("DELETE FROM cache")
                conn.commit()
            self.logger.debug("Весь кеш очищен")
        except Exception as e:
            self.logger.error("Ошибка полной очистки кеша: %s", e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())

    def add_message(self, user_id: int, message: str, is_bot: bool, user_info: Dict[str, Any]) -> None:
        """Добавляет запись в историю пользователя."""
        timestamp = datetime.utcnow().isoformat()
        user_info_json = json.dumps(user_info, ensure_ascii=False)
        try:
            with self._lock, self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO user_history
                      (user_id, timestamp, message, is_bot, user_info)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (user_id, timestamp, message, int(is_bot), user_info_json)
                )
                conn.commit()
            self.logger.debug("Добавлено сообщение: user_id=%d, message=%s, is_bot=%s", user_id, message[:50], is_bot)
        except Exception as e:
            self.logger.error("Ошибка добавления сообщения: %s", e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())

    def get_history(self, user_id: int) -> List[Dict[str, Any]]:
        """Возвращает историю за период timeout и ограниченную по max_history_size."""
        now = datetime.utcnow()
        cutoff = (now - self.timeout).isoformat()
        try:
            with self._lock, self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM user_history WHERE user_id = ? AND timestamp < ?", (user_id, cutoff))
                conn.commit()
                cursor.execute(
                    "SELECT timestamp, message, is_bot, user_info"
                    " FROM user_history WHERE user_id = ? ORDER BY timestamp ASC",
                    (user_id,)
                )
                rows = cursor.fetchall()
            records: List[Dict[str, Any]] = []
            for ts, msg, is_bot, ui in rows:
                dt = datetime.fromisoformat(ts)
                if now - dt <= self.timeout:
                    info = {}
                    try:
                        info = json.loads(ui)
                    except json.JSONDecodeError as e:
                        self.logger.error("Ошибка декодирования user_info: %s", e)
                        self.logger.error("Трассировка стека: %s", traceback.format_exc())
                    records.append({
                        "timestamp": dt.isoformat(),
                        "message": msg,
                        "is_bot": bool(is_bot),
                        "user_info": info
                    })
            result = records[-self.max_history_size:]
            self.logger.debug("Получена история: user_id=%d, записей=%d", user_id, len(result))
            return result
        except Exception as e:
            self.logger.error("Ошибка получения истории: %s", e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())
            return []


    def get_all_users_history(self, search="", language="", date_from="", date_to="", message_type="") -> List[Dict[str, Any]]:
        now = datetime.utcnow()
        cutoff = (now - self.timeout).isoformat()
        query = """
            SELECT user_id, timestamp, message, is_bot, user_info
            FROM user_history
            WHERE timestamp >= ?
        """
        params = [cutoff]
    
        if search:
            query += " AND (user_id LIKE ? OR message LIKE ? OR user_info LIKE ?)"
            search_term = f"%{search}%"
            params.extend([search_term, search_term, search_term])
        if language:
            query += " AND user_info LIKE ?"
            params.append(f"%\"language_code\":\"{language}%\"")
        if date_from:
            query += " AND timestamp >= ?"
            params.append(datetime.strptime(date_from, "%Y-%m-%d").isoformat())
        if date_to:
            date_to_dt = datetime.strptime(date_to, "%Y-%m-%d") + timedelta(days=1)
            query += " AND timestamp < ?"
            params.append(date_to_dt.isoformat())
        if message_type:
            query += " AND is_bot = ?"
            params.append(1 if message_type == "bot" else 0)
    
        query += " ORDER BY user_id, timestamp ASC"
    
        try:
            with self._lock, self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM user_history WHERE timestamp < ?", (cutoff,))
                conn.commit()
                cursor.execute(query, params)
                rows = cursor.fetchall()
            records: List[Dict[str, Any]] = []
            user_records: Dict[int, List[Dict[str, Any]]] = {}
            for user_id, ts, msg, is_bot, ui in rows:
                dt = datetime.fromisoformat(ts)
                if now - dt <= self.timeout:
                    info = {}
                    try:
                        info = json.loads(ui)
                    except json.JSONDecodeError as e:
                        self.logger.error("Ошибка декодирования user_info: %s", e)
                    record = {
                        "user_id": user_id,
                        "timestamp": dt.isoformat(),
                        "message": msg,
                        "is_bot": bool(is_bot),
                        "user_info": info
                    }
                    if user_id not in user_records:
                        user_records[user_id] = []
                    user_records[user_id].append(record)
            for user_id in user_records:
                records.extend(user_records[user_id][-self.max_history_size:])
            self.logger.debug("Получена отфильтрованная история всех пользователей: записей=%d", len(records))
            return records
        except Exception as e:
            self.logger.error("Ошибка получения истории всех пользователей: %s", e)
            return []


    def get_task_count(self, user_id: int) -> int:
        """Возвращает текущее число задач пользователя."""
        try:
            with self._lock, self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT task_count FROM user_task_count WHERE user_id = ?", (user_id,))
                row = cursor.fetchone()
                count = row[0] if row else 0
                self.logger.debug("Получено количество задач: user_id=%d, count=%d", user_id, count)
                return count
        except Exception as e:
            self.logger.error("Ошибка получения количества задач: %s", e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())
            return 0

    def update_task_count(self, user_id: int, increment: bool) -> None:
        """Инкремент/декремент счётчика задач пользователя."""
        delta = 1 if increment else -1
        try:
            with self._lock, self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT INTO user_task_count(user_id, task_count)
                    VALUES (?, COALESCE((SELECT task_count FROM user_task_count WHERE user_id = ?), 0) + ?)
                    ON CONFLICT(user_id) DO UPDATE SET task_count = task_count + ?;
                    """,
                    (user_id, user_id, delta, delta)
                )
                conn.commit()
            self.logger.debug("Обновлено количество задач: user_id=%d, действие=%s", user_id, "увеличено" if increment else "уменьшено")
        except Exception as e:
            self.logger.error("Ошибка обновления счётчика задач: %s", e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())

    def clear_old_history(self):
        """Очищает старую историю по timeout."""
        cutoff = (datetime.utcnow() - self.timeout).isoformat()
        try:
            with self._lock, self._get_connection() as conn:
                conn.execute("DELETE FROM user_history WHERE timestamp < ?", (cutoff,))
                conn.commit()
            self.logger.debug("Старая история очищена до %s", cutoff)
        except Exception as e:
            self.logger.error("Ошибка очистки старой истории: %s", e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())

    def clear_all_history(self) -> None:
        """Полностью очищает всю историю всех пользователей."""
        try:
            with self._lock, self._get_connection() as conn:
                conn.execute("DELETE FROM user_history")
                conn.commit()
            self.logger.debug("Вся история пользователей очищена")
        except Exception as e:
            self.logger.error("Ошибка очистки всей истории: %s", e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())

    def clean_old_histories(self, inactivity_hours: int) -> None:
        """Очищает историю пользователей, неактивных более inactivity_hours часов."""
        cutoff = datetime.utcnow() - timedelta(hours=inactivity_hours)
        cutoff_iso = cutoff.isoformat()
        try:
            with self._lock, self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT user_id, MAX(timestamp) as last_msg_time
                    FROM user_history
                    GROUP BY user_id
                    HAVING last_msg_time < ?
                """, (cutoff_iso,))
                users_to_clear = [row[0] for row in cursor.fetchall()]
                for user_id in users_to_clear:
                    cursor.execute("DELETE FROM user_history WHERE user_id = ?", (user_id,))
                    self.logger.debug("История очищена для user_id=%d (неактивен > %d часов)", user_id, inactivity_hours)
                conn.commit()
        except Exception as e:
            self.logger.error("Ошибка очистки неактивных историй: %s", e)
            self.logger.error("Трассировка стека: %s", traceback.format_exc())
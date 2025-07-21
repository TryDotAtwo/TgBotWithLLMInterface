# -*- coding: utf-8 -*-
# dashboard.py

import logging
from pathlib import Path
from typing import Dict, List, Any
from datetime import datetime, timedelta
from quart import Quart, render_template, request, jsonify
import json
import html

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("dashboard.log", encoding="utf-8")],
)
logger = logging.getLogger(__name__)

CONFIG = {
    "template_dir": Path("templates"),
    "dashboard_template": "dashboard.html",
    "host": "127.0.0.1",
    "port": 8000,
    "page_size": 10,
}

DASHBOARD_HTML_CONTENT = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Telegram Bot Dashboard</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .container { max-width: 1200px; margin: auto; }
        .stats, .filters, .users { margin-bottom: 20px; }
        table { width: 100%; border-collapse: collapse; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f2f2f2; }
        .filters form { display: flex; gap: 10px; flex-wrap: wrap; }
        button { padding: 8px; cursor: pointer; }
    </style>
</head>
<body>
    <div class="container">
        <h1>Telegram Bot Dashboard</h1>
        <nav>
            <a href="/">Home</a> | <a href="/#users">Users</a> | <a href="/#messages">Messages</a>
        </nav>

        <div class="stats">
            <h2>Statistics</h2>
            <p>Total Users: {{ stats.total_users }}</p>
            <p>Active Users (24h): {{ stats.active_users }}</p>
            <p>Total Messages: {{ stats.total_messages }}</p>
        </div>

        <div class="filters">
            <h2>Filters</h2>
            <form id="filter-form">
                <input type="text" id="search" placeholder="Search (ID, Name, Message)">
                <select id="language">
                    <option value="">All</option>
                    <option value="ru">Russian</option>
                    <option value="en">English</option>
                </select>
                <input type="date" id="date-from" placeholder="Date From">
                <input type="date" id="date-to" placeholder="Date To">
                <select id="message-type">
                    <option value="">All</option>
                    <option value="user">User</option>
                    <option value="bot">Bot</option>
                </select>
                <button type="submit">Apply Filters</button>
            </form>
        </div>

        <div class="users">
            <h2>Users</h2>
            <table id="users-table">
                <thead>
                    <tr>
                        <th>ID</th>
                        <th>Name</th>
                        <th>Username</th>
                        <th>Language</th>
                        <th>Actions</th>
                    </tr>
                </thead>
                <tbody id="users-tbody"></tbody>
            </table>
            <div id="pagination"></div>
        </div>

        <div class="user-details">
            <h2>User Details</h2>
            <div id="user-details"></div>
        </div>
    </div>

    <script>
        async function loadUsers(page = 1) {
            const form = document.getElementById('filter-form');
            const search = document.getElementById('search').value;
            const language = document.getElementById('language').value;
            const dateFrom = document.getElementById('date-from').value;
            const dateTo = document.getElementById('date-to').value;
            const messageType = document.getElementById('message-type').value;

            const url = `/api/users?page=${page}&search=${encodeURIComponent(search)}&language=${encodeURIComponent(language)}&date-from=${encodeURIComponent(dateFrom)}&date-to=${encodeURIComponent(dateTo)}&message-type=${encodeURIComponent(messageType)}`;
            const response = await fetch(url);
            const data = await response.json();

            const tbody = document.getElementById('users-tbody');
            tbody.innerHTML = '';
            data.users.forEach(user => {
                const row = document.createElement('tr');
                row.innerHTML = `
                    <td>${user.user_id}</td>
                    <td>${user.user_info.first_name} ${user.user_info.last_name}</td>
                    <td>${user.user_info.username}</td>
                    <td>${user.user_info.language_code}</td>
                    <td><button onclick="loadUserDetails(${user.user_id})">Details</button></td>
                `;
                tbody.appendChild(row);
            });

            const pagination = document.getElementById('pagination');
            pagination.innerHTML = '';
            for (let i = 1; i <= data.total_pages; i++) {
                const button = document.createElement('button');
                button.textContent = i;
                button.onclick = () => loadUsers(i);
                if (i === page) button.style.fontWeight = 'bold';
                pagination.appendChild(button);
            }
        }

        async function loadUserDetails(userId) {
            const response = await fetch(`/api/user/${userId}`);
            const user = await response.json();
            const details = document.getElementById('user-details');
            details.innerHTML = `
                <h3>User ID: ${user.user_id}</h3>
                <p>Name: ${user.user_info.first_name} ${user.user_info.last_name}</p>
                <p>Username: ${user.user_info.username}</p>
                <p>Language: ${user.user_info.language_code}</p>
                <h4>Messages:</h4>
                <ul>
                    ${user.messages.map(msg => `
                        <li>
                            ${msg.timestamp} (${msg.is_bot ? 'Bot' : 'User'}): ${msg.message}
                        </li>
                    `).join('')}
                </ul>
            `;
        }

        document.getElementById('filter-form').addEventListener('submit', (e) => {
            e.preventDefault();
            loadUsers(1);
        });

        window.onload = () => loadUsers(1);
    </script>
</body>
</html>
"""

class Dashboard:
    """Модуль дашборда для мониторинга активности пользователей."""

    def __init__(self, bot, history_manager, config: Dict[str, Any] = CONFIG):
        self.bot = bot
        self.history_manager = history_manager
        self.config = config
        self.app = Quart(__name__, template_folder=str(self.config["template_dir"]))
        self._setup_template()
        self._setup_routes()
        logger.info("Дашборд инициализирован")

    def _setup_template(self) -> None:
        """Создаёт шаблон дашборда (файл HTML)."""
        self.config["template_dir"].mkdir(parents=True, exist_ok=True)
        template_path = self.config["template_dir"] / self.config["dashboard_template"]
        with open(template_path, "w", encoding="utf-8") as f:
            f.write(DASHBOARD_HTML_CONTENT)
        logger.info("Шаблон записан: %s", template_path)

    def _setup_routes(self) -> None:
        """Настраивает маршруты для Quart-приложения."""
        @self.app.route("/")
        async def dashboard_route():
            try:
                stats = self._get_stats()
                return await render_template("dashboard.html", stats=stats)
            except Exception as e:
                logger.error("Ошибка при загрузке дашборда: %s", str(e))
                return jsonify({"error": "Internal server error"}), 500

        @self.app.route("/api/users")
        async def get_users_route():
            try:
                page = int(request.args.get("page", 1))
                search = html.escape(request.args.get("search", ""))
                language = request.args.get("language", "")
                date_from = request.args.get("date-from", "")
                date_to = request.args.get("date-to", "")
                message_type = request.args.get("message-type", "")
                users, total_users = self._get_filtered_users(
                    page, search, language, date_from, date_to, message_type
                )
                total_pages = (total_users + self.config["page_size"] - 1) // self.config["page_size"]
                return jsonify({"users": users, "total_pages": total_pages})
            except Exception as e:
                logger.error("Ошибка при получении данных пользователей: %s", str(e))
                return jsonify({"error": "Internal server error"}), 500

        @self.app.route("/api/user/<int:user_id>")
        async def get_user_route(user_id):
            try:
                user = self._get_user(user_id)
                return jsonify(user)
            except Exception as e:
                logger.error("Ошибка при получении данных пользователя %s: %s", user_id, str(e))
                return jsonify({"error": "Internal server error"}), 500

    def _get_stats(self) -> Dict[str, int]:
        """Считает общую статистику для дашборда через HistoryManager."""
        try:
            all_history = self.history_manager.get_all_users_history()
            user_ids = {entry["user_id"] for entry in all_history}
            total_users = len(user_ids)
            active_users = len({
                entry["user_id"] for entry in all_history
                if datetime.fromisoformat(entry["timestamp"]) > datetime.utcnow() - timedelta(hours=24)
            })
            total_messages = len(all_history)
            stats = {
                "total_users": total_users,
                "active_users": active_users,
                "total_messages": total_messages,
            }
            logger.debug("Статистика: %s", stats)
            return stats
        except Exception as e:
            logger.error("Ошибка при получении статистики: %s", str(e))
            raise

    def _get_filtered_users(
        self, page: int, search: str, language: str, date_from: str, date_to: str, message_type: str
    ) -> tuple[List[Dict[str, Any]], int]:
        """Возвращает список пользователей с учётом фильтров и постраничной навигации."""
        offset = (page - 1) * self.config["page_size"]
        all_history = self.history_manager.get_all_users_history()
        filtered_users = []
        user_ids = set()

        for entry in all_history:
            user_id = entry["user_id"]
            if user_id in user_ids:
                continue

            user_info = entry.get("user_info", {})
            if not user_info:
                user_info = {"first_name": "Unknown", "last_name": "Unknown", "username": "Unknown", "language_code": "Unknown"}

            # Фильтры
            if search and not (
                str(user_id).lower().find(search.lower()) != -1 or
                user_info.get("first_name", "").lower().find(search.lower()) != -1 or
                user_info.get("last_name", "").lower().find(search.lower()) != -1 or
                user_info.get("username", "").lower().find(search.lower()) != -1 or
                entry["message"].lower().find(search.lower()) != -1
            ):
                continue

            if language and user_info.get("language_code", "").lower() != language.lower():
                continue

            timestamp = datetime.fromisoformat(entry["timestamp"])
            if date_from:
                try:
                    date_from_dt = datetime.strptime(date_from, "%Y-%m-%d")
                    if timestamp.date() < date_from_dt.date():
                        continue
                except ValueError:
                    logger.warning("Некорректный формат date_from: %s", date_from)

            if date_to:
                try:
                    date_to_dt = datetime.strptime(date_to, "%Y-%m-%d") + timedelta(days=1)
                    if timestamp.date() >= date_to_dt.date():
                        continue
                except ValueError:
                    logger.warning("Некорректный формат date_to: %s", date_to)

            if message_type and entry["is_bot"] != (message_type == "bot"):
                continue

            user_ids.add(user_id)
            filtered_users.append({
                "user_id": user_id,
                "user_info": user_info
            })

        total_users = len(user_ids)
        paginated_users = filtered_users[offset:offset + self.config["page_size"]]
        return paginated_users, total_users

    def _get_user(self, user_id: int) -> Dict[str, Any]:
        """Возвращает полную информацию о пользователе и его переписку."""
        try:
            history = self.history_manager.get_history(user_id)
            if not history:
                return {
                    "user_id": user_id,
                    "user_info": {"first_name": "Unknown", "last_name": "Unknown", "username": "Unknown", "language_code": "Unknown"},
                    "messages": []
                }

            user_info = history[0].get("user_info", {"first_name": "Unknown", "last_name": "Unknown", "username": "Unknown", "language_code": "Unknown"})
            messages = [
                {
                    "timestamp": entry["timestamp"],
                    "message": html.escape(entry["message"]),
                    "is_bot": entry["is_bot"]
                }
                for entry in history
            ]
            user = {
                "user_id": user_id,
                "user_info": user_info,
                "messages": messages
            }
            return user
        except Exception as e:
            logger.error("Ошибка при получении данных пользователя %s: %s", user_id, str(e))
            raise
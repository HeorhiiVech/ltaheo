# soloq_logic.py

import os
import requests
import time
from datetime import datetime, timedelta, timezone
from collections import defaultdict
import math
import sqlite3

# Импорты из вашего проекта
from database import get_db_connection, SOLOQ_GAMES_HEADER
from scrims_logic import log_message, get_champion_data, get_champion_icon_html

# --- Константы ---
# Загружаем ключ из переменных окружения
RIOT_API_KEY = os.getenv("RIOT_API_KEY")
if not RIOT_API_KEY:
    log_message("!!! ОШИБКА: RIOT_API_KEY не найден в переменных окружения (.env файле)")
    # Можно либо завершить работу, либо продолжить без возможности обновления
    # raise ValueError("RIOT_API_KEY не установлен")

# Ростер команды (как в вашем коде)
# Переименовал Centu для единообразия с другими файлами
TEAM_ROSTERS = {
    "Gamespace": {
        "Aytekn": {"game_name": ["AyteknnnN777"], "tag_line": ["777"], "role": "TOP"},
    }
}
# Регион по умолчанию для API (можно вынести в конфиг)
DEFAULT_REGION_ACCOUNT = "europe" # Для Account API (получение PUUID)
DEFAULT_REGION_MATCH = "europe"   # Для Match API (история, детали)

# URL Адреса Riot API
BASE_ACCOUNT_URL = f"https://{DEFAULT_REGION_ACCOUNT}.api.riotgames.com/riot/account/v1/accounts/by-riot-id"
BASE_MATCH_HISTORY_URL = f"https://{DEFAULT_REGION_MATCH}.api.riotgames.com/lol/match/v5/matches/by-puuid"
BASE_MATCH_DETAIL_URL = f"https://{DEFAULT_REGION_MATCH}.api.riotgames.com/lol/match/v5/matches"

# Задержка между запросами к Riot API (в секундах) - НАСТРОЙТЕ ПРИ НЕОБХОДИМОСТИ
RIOT_API_DELAY = 1.3 # ~1 запрос в 1.3 секунды, чтобы уложиться в стандартные лимиты (20/сек, 100/2мин)

# --- Вспомогательная функция для запросов к Riot API ---
def _riot_api_request(url):
    """Отправляет GET запрос к Riot API с обработкой ошибок и лимитов."""
    if not RIOT_API_KEY:
        log_message("Riot API request failed: API Key not configured.")
        return None

    headers = {"X-Riot-Token": RIOT_API_KEY}
    try:
        # log_message(f"Riot API Request: {url}") # Лог для отладки
        response = requests.get(url, headers=headers, timeout=15)

        # Обработка Rate Limit (429) - простая задержка, можно улучшить экспоненциальным отступлением
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", "5")) # По умолчанию ждем 5 секунд
            log_message(f"Rate limited (429). Retrying after {retry_after} seconds...")
            time.sleep(retry_after)
            response = requests.get(url, headers=headers, timeout=15) # Повторная попытка

        response.raise_for_status() # Вызовет исключение для других ошибок (4xx, 5xx)
        return response.json()

    except requests.exceptions.HTTPError as http_err:
        log_message(f"HTTP error occurred: {http_err} - URL: {url}")
        # Дополнительная информация при 403 (Forbidden) - часто из-за неверного ключа или его истечения
        if response is not None and response.status_code == 403:
             log_message("Received 403 Forbidden. Check if RIOT_API_KEY is valid and has not expired.")
        # Дополнительная информация при 404 (Not Found)
        elif response is not None and response.status_code == 404:
             log_message("Received 404 Not Found. Check the Riot ID, PUUID, or Match ID.")
    except requests.exceptions.RequestException as req_err:
        log_message(f"Request exception occurred: {req_err} - URL: {url}")
    except Exception as e:
        log_message(f"An unexpected error occurred during Riot API request: {e} - URL: {url}")

    return None # Возвращаем None при любой ошибке

# --- Функции для получения данных от Riot API ---
def get_puuid(game_name, tag_line):
    """Получает PUUID по Riot ID (game name + tag line)."""
    url = f"{BASE_ACCOUNT_URL}/{game_name}/{tag_line}"
    data = _riot_api_request(url)
    if data and "puuid" in data:
        return data["puuid"]
    else:
        log_message(f"Could not get PUUID for {game_name}#{tag_line}")
        return None


def get_soloq_activity_data(player_name, aggregation_type="Day"):
    log_message(f"Getting activity data for {player_name}. Aggregate by: {aggregation_type}")
    conn = get_db_connection()
    if not conn: return {}

    # Используем defaultdict для удобства
    activity_data = defaultdict(lambda: {'wins': 0, 'losses': 0, 'total': 0})
    try:
        cursor = conn.cursor()
        # Получаем Timestamp и Win для игрока
        cursor.execute("SELECT Timestamp, Win FROM soloq_games WHERE Player_Name = ?", (player_name,))
        rows = cursor.fetchall()

        for row in rows:
            ts = row['Timestamp']
            win = row['Win'] # 0 или 1
            # Проверка на None перед конвертацией
            if ts is None:
                log_message(f"Warning: Found null Timestamp for player {player_name}")
                continue
            try:
                dt_object = datetime.fromtimestamp(ts, timezone.utc)
            except (OSError, OverflowError, TypeError) as ts_err:
                 log_message(f"Warning: Invalid timestamp {ts} for player {player_name}: {ts_err}")
                 continue


            if aggregation_type == "Day":
                date_key = dt_object.strftime("%Y-%m-%d")
            elif aggregation_type == "Week":
                 # Начало недели (понедельник)
                 start_of_week = dt_object - timedelta(days=dt_object.weekday())
                 date_key = start_of_week.strftime("%Y-%m-%d")
            elif aggregation_type == "Month":
                 date_key = dt_object.strftime("%Y-%m-01") # Первый день месяца
            else: # По умолчанию - день
                 date_key = dt_object.strftime("%Y-%m-%d")

            activity_data[date_key]['total'] += 1
            # Убедимся, что win это 0 или 1
            if win == 1:
                activity_data[date_key]['wins'] += 1
            else: # Считаем все остальное (0 или None) как поражение
                activity_data[date_key]['losses'] += 1

    except sqlite3.Error as e:
        log_message(f"DB Error getting activity data for {player_name}: {e}")
        return {}
    finally:
        if conn: conn.close()

    # Вернем просто словарь, его удобнее обработать в JS
    log_message(f"Activity data processed for {player_name}. Found {len(activity_data)} points.")
    return dict(activity_data)

def get_match_ids(puuid, count=20, start_time=None):
    """Получает список ID матчей для PUUID."""
    # Уменьшил count до 20 для ускорения обновлений, можно увеличить до 100
    url = f"{BASE_MATCH_HISTORY_URL}/{puuid}/ids?start=0&count={count}"
    if start_time: # Опционально: фильтр по времени начала (Unix timestamp seconds)
        url += f"&startTime={start_time}"
    # Добавляем тип матча - только ranked solo/duo (420)
    url += "&queue=420"
    match_ids = _riot_api_request(url)
    # API возвращает список строк или None при ошибке
    return match_ids if isinstance(match_ids, list) else []

def get_match_details(match_id):
    """Получает детали конкретного матча по его ID."""
    url = f"{BASE_MATCH_DETAIL_URL}/{match_id}"
    return _riot_api_request(url)

# --- Логика сохранения данных в БД ---
def fetch_and_store_soloq_data(player_name):
    """
    Загружает историю матчей SoloQ для игрока из Riot API
    и сохраняет новые игры в базу данных SQLite.
    """
    if not RIOT_API_KEY:
        log_message(f"Skipping SoloQ update for {player_name}: RIOT_API_KEY not set.")
        return 0

    player_config = TEAM_ROSTERS["Gamespace"].get(player_name)
    if not player_config:
        log_message(f"Player {player_name} not found in TEAM_ROSTERS.")
        return 0

    log_message(f"Starting SoloQ update for {player_name}...")
    added_count_total = 0
    processed_accounts = 0

    # Получаем существующие Match ID из БД для этого игрока
    conn = get_db_connection()
    if not conn: return -1 # Ошибка подключения к БД
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT Match_ID FROM soloq_games WHERE Player_Name = ?", (player_name,))
        existing_match_ids = {row['Match_ID'] for row in cursor.fetchall()}
        log_message(f"Found {len(existing_match_ids)} existing SoloQ games for {player_name} in DB.")
    except sqlite3.Error as e:
        log_message(f"Error reading existing SoloQ games for {player_name}: {e}")
        existing_match_ids = set() # Продолжаем без проверки дубликатов в случае ошибки

    # Перебираем все Riot ID аккаунты игрока
    for game_name, tag_line in zip(player_config.get("game_name", []), player_config.get("tag_line", [])):
        processed_accounts += 1
        log_message(f"Processing account {processed_accounts}/{len(player_config.get('game_name', []))}: {game_name}#{tag_line}")

        puuid = get_puuid(game_name, tag_line)
        time.sleep(RIOT_API_DELAY) # Задержка после запроса PUUID

        if not puuid:
            continue # Переходим к следующему аккаунту, если PUUID не найден

        match_ids = get_match_ids(puuid, count=100) # Берем последние 30 игр (можно настроить)
        time.sleep(RIOT_API_DELAY) # Задержка после запроса истории

        if not match_ids:
            log_message(f"No recent SoloQ match IDs found for {game_name}#{tag_line} (PUUID: {puuid})")
            continue

        new_match_ids = [m_id for m_id in match_ids if m_id not in existing_match_ids]
        log_message(f"Found {len(new_match_ids)} new match(es) to process for {game_name}#{tag_line}.")

        added_count_for_account = 0
        sql_column_names_db = [hdr.replace(" ", "_").replace(".", "").replace("-", "_") for hdr in SOLOQ_GAMES_HEADER]
        quoted_column_names_db = [f'"{col}"' for col in sql_column_names_db]
        columns_string_db = ', '.join(quoted_column_names_db)
        sql_placeholders_db = ", ".join(["?"] * len(sql_column_names_db))
        insert_sql = f"INSERT OR IGNORE INTO soloq_games ({columns_string_db}) VALUES ({sql_placeholders_db})"

        for match_id in new_match_ids:
            match_details = get_match_details(match_id)
            time.sleep(RIOT_API_DELAY) # Задержка после запроса деталей матча

            if not match_details or "info" not in match_details:
                log_message(f"Failed to get details for Match ID: {match_id}")
                continue

            info = match_details["info"]
            participants = info.get("participants", [])
            game_creation_ts = info.get("gameCreation") // 1000 # Секунды Unix timestamp
            game_date_readable = datetime.fromtimestamp(game_creation_ts, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

            player_part_data = None
            for p_data in participants:
                if p_data.get("puuid") == puuid:
                    player_part_data = p_data
                    break

            if not player_part_data:
                log_message(f"Could not find participant data for PUUID {puuid} in Match ID: {match_id}")
                continue

            # Извлекаем нужные данные
            win = 1 if player_part_data.get("win", False) else 0
            champion = player_part_data.get("championName", "UnknownChamp")
            # Определяем роль (может быть teamPosition или individualPosition)
            role = player_part_data.get("teamPosition") or player_part_data.get("individualPosition") or "UNKNOWN"
            if role == "UTILITY": role = "SUPPORT" # Приводим к общему виду? Или оставляем UTILITY? Оставим UTILITY пока
            # Нормализуем роль из API к нашим значениям, если нужно
            role_map = {"TOP": "TOP", "JUNGLE": "JUNGLE", "MIDDLE": "MIDDLE", "BOTTOM": "BOTTOM", "UTILITY": "UTILITY", "SUPPORT": "UTILITY"}
            role_normalized = role_map.get(role.upper(), "UNKNOWN")

            kills = player_part_data.get("kills", 0)
            deaths = player_part_data.get("deaths", 0)
            assists = player_part_data.get("assists", 0)

            # Собираем данные для вставки
            row_dict = {
                "Match_ID": match_id,
                "Player_Name": player_name,
                "Riot_Name": game_name,
                "Riot_Tag": tag_line,
                "Timestamp": game_creation_ts,
                "Date_Readable": game_date_readable,
                "Win": win,
                "Champion": champion,
                "Role": role_normalized, # Сохраняем нормализованную роль
                "Kills": kills,
                "Deaths": deaths,
                "Assists": assists
            }
            data_tuple = tuple(row_dict.get(sql_col, None) for sql_col in sql_column_names_db)

            # Вставляем в БД
            try:
                cursor.execute(insert_sql, data_tuple)
                if cursor.rowcount > 0:
                    added_count_for_account += 1
                    existing_match_ids.add(match_id) # Добавляем в сет, чтобы не обработать снова
            except sqlite3.Error as e:
                 log_message(f"DB Insert Error SoloQ Match:{match_id} for Player:{player_name}: {e}")
                 log_message(f"Data attempted (SoloQ): {data_tuple}")

        # Коммит после обработки одного Riot ID аккаунта
        try:
            conn.commit()
            log_message(f"Added {added_count_for_account} new game(s) for {game_name}#{tag_line}.")
            added_count_total += added_count_for_account
        except sqlite3.Error as e:
            log_message(f"DB Commit Error after processing account {game_name}#{tag_line}: {e}")
            conn.rollback() # Откатываем изменения для этого аккаунта при ошибке коммита

    # Закрываем соединение после обработки всех аккаунтов игрока
    conn.close()
    log_message(f"SoloQ update finished for {player_name}. Total new games added: {added_count_total}")
    return added_count_total


# --- Логика агрегации данных из БД ---
def aggregate_soloq_data_from_db(player_name, time_filter="All Time", date_from_str=None, date_to_str=None):
    log_message(f"Aggregating SoloQ data for {player_name}. Time filter: {time_filter}, Dates: {date_from_str} - {date_to_str}") # Добавил даты в лог
    player_config = TEAM_ROSTERS["Gamespace"].get(player_name)
    if not player_config:
        log_message(f"Cannot aggregate: Player {player_name} not in roster.")
        return {}
    player_main_role = player_config.get("role")

    conn = get_db_connection()
    if not conn: return {}

    # --- Фильтр по времени ---
    sql = "SELECT Champion, Win, Kills, Deaths, Assists, Role FROM soloq_games WHERE Player_Name = ?"
    params = [player_name]
    date_filter_active = False

    # Приоритет у ручного выбора дат
    if date_from_str or date_to_str:
        try:
            if date_from_str:
                # Добавляем 00:00:00 к начальной дате
                dt_from = datetime.strptime(date_from_str + " 00:00:00", "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                ts_from = int(dt_from.timestamp())
                sql += " AND Timestamp >= ?"
                params.append(ts_from)
                # log_message(f"Applying date filter: Timestamp >= {dt_from}")
                date_filter_active = True
            if date_to_str:
                # Добавляем 23:59:59 к конечной дате
                dt_to = datetime.strptime(date_to_str + " 23:59:59", "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                ts_to = int(dt_to.timestamp())
                sql += " AND Timestamp <= ?"
                params.append(ts_to)
                # log_message(f"Applying date filter: Timestamp <= {dt_to}")
                date_filter_active = True
        except ValueError:
             log_message(f"Invalid date format received: from='{date_from_str}', to='{date_to_str}'. Ignoring date filter.")
             date_filter_active = False # Сбрасываем флаг при ошибке

    # Если ручные даты не применились, используем dropdown
    if not date_filter_active and time_filter != "All Time":
        now_utc_ts = int(datetime.now(timezone.utc).timestamp())
        delta_seconds = None
        if time_filter == "1 week": delta_seconds = timedelta(weeks=1).total_seconds()
        elif time_filter == "2 weeks": delta_seconds = timedelta(weeks=2).total_seconds()
        elif time_filter == "3 weeks": delta_seconds = timedelta(weeks=3).total_seconds()
        elif time_filter == "4 weeks": delta_seconds = timedelta(weeks=4).total_seconds()

        if delta_seconds:
            cutoff_ts = now_utc_ts - delta_seconds
            sql += " AND Timestamp >= ?"
            params.append(int(cutoff_ts))
            # log_message(f"Applying time filter: {time_filter} (Timestamp >= {datetime.fromtimestamp(cutoff_ts, timezone.utc)})") # Скрыл лог
        # else: log_message(f"Warning: Unknown time filter '{time_filter}'.")

    # --- Агрегация ---
    aggregated_data = defaultdict(lambda: {'games': 0, 'wins': 0, 'kills': 0, 'deaths': 0, 'assists': 0})
    try:
        cursor = conn.cursor()
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        # log_message(f"Fetched {len(rows)} SoloQ games for {player_name} from DB matching filters.") # Скрыл лог

        for row in rows:
            game = dict(row)
            if game.get("Role") == player_main_role: # Фильтр по основной роли
                champion = game.get("Champion")
                if champion:
                    stats = aggregated_data[champion]
                    stats['games'] += 1
                    stats['wins'] += game.get("Win", 0)
                    stats['kills'] += game.get("Kills", 0)
                    stats['deaths'] += game.get("Deaths", 0)
                    stats['assists'] += game.get("Assists", 0)

    except sqlite3.Error as e: log_message(f"DB Error aggregating SoloQ data for {player_name}: {e}"); return {}
    finally: conn.close()

    # --- Форматирование результата ---
    formatted_stats = []
    # Загружаем данные чемпионов один раз перед циклом
    champ_data_local = get_champion_data()
    for champ, data in aggregated_data.items():
        games = data['games']
        if games > 0:
            win_rate = round((data['wins'] / games) * 100, 1)
            deaths = max(1, data['deaths'])
            kda = round((data['kills'] + data['assists']) / deaths, 1)
            formatted_stats.append({ "Champion": champ, "Games": games, "WinRate": win_rate, "KDA": kda, "icon_html": get_champion_icon_html(champ, champ_data_local, width=30, height=30) })

    formatted_stats.sort(key=lambda x: x['Games'], reverse=True)
    # log_message(f"Aggregation complete for {player_name}. Found stats for {len(formatted_stats)} champions.") # Скрыл лог
    return formatted_stats


# --- Логика получения данных для графика ---
def get_soloq_timeline_data(player_name, aggregation_type="Day"):
    """Получает данные для графика игр по времени из БД."""
    log_message(f"Getting timeline data for {player_name}. Aggregate by: {aggregation_type}")
    conn = get_db_connection()
    if not conn: return []

    timeline_data = defaultdict(int)
    try:
        cursor = conn.cursor()
        # Получаем все временные метки для игрока
        cursor.execute("SELECT Timestamp FROM soloq_games WHERE Player_Name = ?", (player_name,))
        rows = cursor.fetchall()

        for row in rows:
            ts = row['Timestamp']
            dt_object = datetime.fromtimestamp(ts, timezone.utc)

            if aggregation_type == "Day":
                date_key = dt_object.strftime("%Y-%m-%d")
            elif aggregation_type == "Week":
                 # Начало недели (понедельник)
                 start_of_week = dt_object - timedelta(days=dt_object.weekday())
                 date_key = start_of_week.strftime("%Y-%m-%d")
            elif aggregation_type == "Month":
                 date_key = dt_object.strftime("%Y-%m-01") # Первый день месяца
            else: # По умолчанию - день
                 date_key = dt_object.strftime("%Y-%m-%d")

            timeline_data[date_key] += 1

    except sqlite3.Error as e:
        log_message(f"DB Error getting timeline data for {player_name}: {e}")
        return []
    finally:
        conn.close()

    # Преобразуем в список словарей и сортируем по дате
    formatted_timeline = [{"date": date_str, "count": count} for date_str, count in timeline_data.items()]
    formatted_timeline.sort(key=lambda x: x['date'])

    log_message(f"Timeline data processed for {player_name}. Found {len(formatted_timeline)} points.")
    return formatted_timeline

# --- Блок для тестирования (если нужно запустить отдельно) ---
if __name__ == '__main__':
    print("Testing soloq_logic...")
    # Загрузка .env файла, если запускаем отдельно
    from dotenv import load_dotenv
    load_dotenv()
    RIOT_API_KEY = os.getenv("RIOT_API_KEY")
    if not RIOT_API_KEY:
        print("!!! RIOT_API_KEY не найден в .env файле !!!")
    else:
        # Пример теста: обновить данные для одного игрока
        test_player = "Aytekn" # Укажите имя игрока для теста
        print(f"\n--- Testing fetch_and_store_soloq_data for {test_player} ---")
        added = fetch_and_store_soloq_data(test_player)
        print(f"Fetch test completed for {test_player}. Added games: {added}")

        # Пример теста: агрегировать данные для одного игрока за всё время
        print(f"\n--- Testing aggregate_soloq_data_from_db for {test_player} (All Time) ---")
        stats_all = aggregate_soloq_data_from_db(test_player, "All Time")
        print(f"Found stats for {len(stats_all)} champions.")
        print("Top 3 champs:", stats_all[:3])

         # Пример теста: агрегировать данные за 1 неделю
        print(f"\n--- Testing aggregate_soloq_data_from_db for {test_player} (1 week) ---")
        stats_1w = aggregate_soloq_data_from_db(test_player, "1 week")
        print(f"Found stats for {len(stats_1w)} champions.")
        print("Top 3 champs:", stats_1w[:3])

        # Пример теста: получить данные для графика по дням
        print(f"\n--- Testing get_soloq_timeline_data for {test_player} (Day) ---")
        timeline_day = get_soloq_timeline_data(test_player, "Day")
        print("First 5 timeline points (Day):", timeline_day[:5])
        print("Last 5 timeline points (Day):", timeline_day[-5:])

        print("\nSoloQ logic testing finished.")
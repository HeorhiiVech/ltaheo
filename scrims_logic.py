# scrims_logic.py (Обновленная HLL версия)

import requests
import json
import os
from datetime import datetime, timedelta, timezone
import time
from collections import defaultdict
import sqlite3
# Убедитесь, что database.py находится там, где его можно импортировать
# Возможно, потребуется from .database import ... если структура проекта изменилась
from database import get_db_connection, SCRIMS_HEADER
import math # Для округления

# --- КОНСТАНТЫ (HLL) ---
GRID_API_KEY = os.getenv("GRID_API_KEY")
GRID_BASE_URL = "https://api.grid.gg/"
TEAM_NAME = "Gamespace MC" # HLL Team Name
PLAYER_IDS = {"26433": "IceBreaker", "25262": "Pallet", "25266": "Tsiperakos", "20958": "Nikiyas", "21922": "CENTU"} # HLL Roster
ROSTER_RIOT_NAME_TO_GRID_ID = {"IceBreaker": "26433", "Pallet": "25262", "Tsiperakos": "25266", "Nikiyas": "20958", "CENTU": "21922"} # HLL Roster
PLAYER_ROLES_BY_ID = {"26433": "TOP", "25262": "JUNGLE", "25266": "MIDDLE", "20958": "BOTTOM", "21922": "UTILITY"} # HLL Roles
API_REQUEST_DELAY = 0.5 # HLL Delay
ROLE_ORDER_FOR_SHEET = ["TOP", "JUNGLE", "MIDDLE", "BOTTOM", "UTILITY"]
PLAYER_DISPLAY_ORDER = ["IceBreaker", "Pallet", "Tsiperakos", "Nikiyas", "CENTU"] # HLL Player Order

# --- Логирование ---
def log_message(message):
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"{timestamp} :: {message}")

# --- Функции для работы с GRID API (Без изменений от HLL версии) ---
def post_graphql_request(query_string, variables, endpoint, retries=3, initial_delay=1):
    """ Отправляет GraphQL POST запрос с обработкой ошибок и повторами """
    if not GRID_API_KEY:
        log_message("API Key Error: GRID_API_KEY not set.")
        return None
    headers = {"x-api-key": GRID_API_KEY, "Content-Type": "application/json"}
    payload = json.dumps({"query": query_string, "variables": variables})
    url = f"{GRID_BASE_URL}{endpoint}"
    last_exception = None

    for attempt in range(retries):
        try:
            response = requests.post(url, headers=headers, data=payload, timeout=20)
            response.raise_for_status()
            response_data = response.json()
            if "errors" in response_data and response_data["errors"]:
                error_msg = response_data["errors"][0].get("message", "Unknown GraphQL error")
                log_message(f"GraphQL Error in response: {json.dumps(response_data['errors'])}")
                if "UNAUTHENTICATED" in error_msg or "UNAUTHORIZED" in error_msg or "forbidden" in error_msg.lower():
                     log_message(f"GraphQL Auth/Permission Error: {error_msg}. Check API Key/Permissions.")
                     return None
                last_exception = Exception(f"GraphQL Error: {error_msg}")
                time.sleep(initial_delay * (2 ** attempt))
                continue
            return response_data.get("data")
        except requests.exceptions.HTTPError as http_err:
            log_message(f"HTTP error on attempt {attempt + 1}: {http_err}")
            last_exception = http_err
            if response is not None:
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", initial_delay * (2 ** attempt)))
                    log_message(f"Rate limited (429). Retrying after {retry_after} seconds.")
                    time.sleep(retry_after)
                    continue
                elif response.status_code in [401, 403]:
                    log_message(f"Authorization error ({response.status_code}). Check API Key/Permissions.")
                    return None
                elif response.status_code == 400:
                     try: error_details = response.json(); log_message(f"Bad Request (400) details: {json.dumps(error_details)}")
                     except json.JSONDecodeError: log_message(f"Bad Request (400), could not decode JSON: {response.text[:500]}")
                     break # Не повторяем 400 Bad Request
            if response is None or 500 <= response.status_code < 600: # Повторяем серверные ошибки
                 time.sleep(initial_delay * (2 ** attempt))
            else: break # Не повторяем другие клиентские ошибки
        except requests.exceptions.RequestException as req_err: log_message(f"Request exception on attempt {attempt + 1}: {req_err}"); last_exception = req_err; time.sleep(initial_delay * (2 ** attempt))
        except json.JSONDecodeError as json_err: log_message(f"JSON decode error attempt {attempt+1}: {json_err}. Response: {response.text[:200] if response else 'N/A'}"); last_exception = json_err; time.sleep(initial_delay * (2 ** attempt))
        except Exception as e: import traceback; log_message(f"Unexpected error in post_graphql attempt {attempt + 1}: {e}\n{traceback.format_exc()}"); last_exception = e; time.sleep(initial_delay * (2 ** attempt))

    log_message(f"GraphQL request failed after {retries} attempts. Last error: {last_exception}")
    return None

def get_rest_request(endpoint, retries=5, initial_delay=2, expected_type='json'):
    """ Отправляет REST GET запрос с обработкой ошибок и повторами """
    if not GRID_API_KEY:
        log_message("API Key Error: GRID_API_KEY not set.")
        return None
    headers = {"x-api-key": GRID_API_KEY}
    if expected_type == 'json': headers['Accept'] = 'application/json'

    url = f"{GRID_BASE_URL}{endpoint}"
    last_exception = None

    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=15) # Таймаут 15 секунд
            if response.status_code == 200:
                if expected_type == 'json':
                    try: return response.json()
                    except json.JSONDecodeError as json_err: log_message(f"JSON decode error (200 OK): {json_err}. Response: {response.text[:200]}"); last_exception = json_err; break # Не повторяем ошибку декодирования
                else: return response.content # Возвращаем байты для .jsonl и др.
            elif response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", initial_delay * (2 ** attempt)))
                log_message(f"Rate limited (429). Retrying after {retry_after} seconds.")
                time.sleep(retry_after); last_exception = requests.exceptions.HTTPError(f"429 Too Many Requests"); continue
            elif response.status_code == 404: log_message(f"Resource not found (404) at {endpoint}"); last_exception = requests.exceptions.HTTPError(f"404 Not Found"); return None # Не найдено - не повторяем
            elif response.status_code in [401, 403]: error_msg = f"Auth error ({response.status_code}) for {endpoint}. Check API Key."; log_message(error_msg); last_exception = requests.exceptions.HTTPError(f"{response.status_code} Unauthorized/Forbidden"); return None # Ошибка доступа - не повторяем
            else: response.raise_for_status() # Вызовет HTTPError для других кодов 4xx/5xx
        except requests.exceptions.HTTPError as http_err: log_message(f"HTTP error attempt {attempt + 1}: {http_err}"); last_exception = http_err; time.sleep(initial_delay * (2 ** attempt)) # Повторяем серверные ошибки
        except requests.exceptions.RequestException as req_err: log_message(f"Request exception attempt {attempt + 1}: {req_err}"); last_exception = req_err; time.sleep(initial_delay * (2 ** attempt)) # Повторяем ошибки сети
        except Exception as e: log_message(f"Unexpected error attempt {attempt + 1}: {e}"); last_exception = e; time.sleep(initial_delay * (2 ** attempt)) # Повторяем другие ошибки

    log_message(f"REST GET failed after {retries} attempts for {endpoint}. Last error: {last_exception}")
    return None

def get_all_series(days_ago=30):
    """ Получает список ID и дат начала LoL скримов за последние N дней """
    query_string = """
        query ($filter: SeriesFilter, $first: Int, $after: Cursor, $orderBy: SeriesOrderBy, $orderDirection: OrderDirection) {
          allSeries( filter: $filter, first: $first, after: $after, orderBy: $orderBy, orderDirection: $orderDirection ) {
            totalCount, pageInfo { hasNextPage, endCursor }, edges { node { id, startTimeScheduled } } } }
    """
    start_thresh = (datetime.now(timezone.utc) - timedelta(days=days_ago)).strftime("%Y-%m-%dT%H:%M:%SZ")
    variables_template = { "filter": {"titleId": 3, "types": ["SCRIM"], "startTimeScheduled": {"gte": start_thresh}}, "first": 50, "orderBy": "StartTimeScheduled", "orderDirection": "DESC" }
    all_nodes = []; cursor = None; page_num = 1; max_pages = 20

    log_message(f"Fetching series from the last {days_ago} days...")
    while page_num <= max_pages:
        current_variables = variables_template.copy()
        if cursor: current_variables["after"] = cursor
        else: current_variables.pop("after", None)

        response_data = post_graphql_request(query_string=query_string, variables=current_variables, endpoint="central-data/graphql")
        if not response_data: log_message(f"Failed to fetch series page {page_num}. Stopping."); break

        series_data = response_data.get("allSeries", {}); edges = series_data.get("edges", [])
        nodes = [edge["node"] for edge in edges if "node" in edge]; all_nodes.extend(nodes)
        page_info = series_data.get("pageInfo", {}); has_next_page = page_info.get("hasNextPage", False); cursor = page_info.get("endCursor")
        if not has_next_page or not cursor: break
        page_num += 1; time.sleep(API_REQUEST_DELAY)

    log_message(f"Finished fetching series. Total series found: {len(all_nodes)}")
    return all_nodes

def get_series_state(series_id):
    """ Получает список игр (id, sequenceNumber) для заданной серии """
    query_template = """ query GetSeriesGames($seriesId: ID!) { seriesState ( id: $seriesId ) { id, games { id, sequenceNumber } } } """
    variables = {"seriesId": series_id}
    response_data = post_graphql_request(query_string=query_template, variables=variables, endpoint="live-data-feed/series-state/graphql")

    if response_data and response_data.get("seriesState") and "games" in response_data["seriesState"]:
        games = response_data["seriesState"]["games"]
        if games is None: log_message(f"Series {series_id} found, but games list is null."); return []
        return games
    elif response_data and not response_data.get("seriesState"): log_message(f"No seriesState found for series {series_id}."); return []
    else: log_message(f"Failed to get games for series {series_id}."); return []

def download_riot_summary_data(series_id, sequence_number):
    """ Скачивает Riot Summary JSON для конкретной игры """
    endpoint = f"file-download/end-state/riot/series/{series_id}/games/{sequence_number}/summary"
    summary_data = get_rest_request(endpoint, expected_type='json')
    return summary_data

# --- НОВОЕ: Скачивание LiveStats (из UOL) ---
def download_riot_livestats_data(series_id, sequence_number):
    """ Скачивает Riot LiveStats (.jsonl) для конкретной игры LoL """
    endpoint = f"file-download/events/riot/series/{series_id}/games/{sequence_number}"
    log_message(f"Attempting to download LiveStats for s:{series_id} g:{sequence_number} from {endpoint}")

    # Ожидаем сырой контент (байты)
    livestats_content_bytes = get_rest_request(endpoint, expected_type='content', retries=2, initial_delay=5)

    if livestats_content_bytes:
        log_message(f"Successfully downloaded LiveStats content for s:{series_id} g:{sequence_number} ({len(livestats_content_bytes)} bytes)")
        try:
            # Пытаемся декодировать как UTF-8
            return livestats_content_bytes.decode('utf-8')
        except UnicodeDecodeError:
            log_message(f"Warning: Could not decode LiveStats as UTF-8 for s:{series_id} g:{sequence_number}. Trying latin-1.")
            try:
                # Попытка с другой кодировкой
                return livestats_content_bytes.decode('latin-1')
            except Exception as e_dec:
                 log_message(f"Error decoding livestats content with latin-1 for s:{series_id} g:{sequence_number}: {e_dec}. Returning None.")
                 return None
        except Exception as e:
            log_message(f"Error decoding livestats content for s:{series_id} g:{sequence_number}: {e}")
            return None
    else:
        log_message(f"Failed to download LiveStats for s:{series_id} g:{sequence_number}")
        return None

# --- Вспомогательные функции парсинга ---
def normalize_player_name(riot_id_game_name):
    """ Удаляет известные командные префиксы из игрового имени Riot ID """
    if isinstance(riot_id_game_name, str):
        known_prefixes = ["GSMC "] # Используем префикс HLL
        for prefix in known_prefixes:
            if riot_id_game_name.startswith(prefix):
                return riot_id_game_name[len(prefix):].strip()
    return riot_id_game_name

def extract_team_tag(riot_id_game_name):
    """Пытается извлечь потенциальный тег команды."""
    if isinstance(riot_id_game_name, str) and ' ' in riot_id_game_name:
        parts = riot_id_game_name.split(' ', 1); tag = parts[0]
        if 2 <= len(tag) <= 5 and tag.isupper() and tag.isalnum():
             common_roles = {"MID", "TOP", "BOT", "JGL", "JUG", "JG", "JUN", "ADC", "SUP", "SPT"}
             if tag.upper() not in common_roles: return tag
    return None

# --- Функция обновления и сохранения данных скримов в SQLite (Без изменений от HLL) ---
def fetch_and_store_scrims():
    """
    Получает последние скримы с GRID API, парсит их
    и сохраняет в базу данных SQLite.
    Возвращает количество добавленных игр.
    """
    log_message("Starting scrims update process...")
    series_list = get_all_series(days_ago=30)
    if not series_list: log_message("No recent series found."); return 0

    conn = get_db_connection()
    if not conn: log_message("DB Connection failed for scrim update."); return -1 # Возвращаем -1 при ошибке БД
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT Game_ID FROM scrims")
        existing_game_ids = {row['Game_ID'] for row in cursor.fetchall()}
        log_message(f"Found {len(existing_game_ids)} existing game IDs in DB.")
    except sqlite3.Error as e:
        log_message(f"Error reading existing game IDs: {e}. Proceeding without duplicate check.")
        existing_game_ids = set()

    added_count = 0; processed_series_count = 0; total_series = len(series_list)

    sql_column_names = [hdr.replace(" ", "_").replace(".", "").replace("-", "_") for hdr in SCRIMS_HEADER]
    quoted_column_names = [f'"{col}"' for col in sql_column_names]
    columns_string = ', '.join(quoted_column_names)
    sql_placeholders = ", ".join(["?"] * len(sql_column_names))
    insert_sql = f"INSERT OR IGNORE INTO scrims ({columns_string}) VALUES ({sql_placeholders})"

    for series_summary in series_list:
        processed_series_count += 1
        if processed_series_count % 10 == 0:
            log_message(f"Processing series {processed_series_count}/{total_series}...")

        series_id = series_summary.get("id")
        if not series_id: continue

        games_in_series = get_series_state(series_id)
        if not games_in_series: time.sleep(API_REQUEST_DELAY / 2); continue

        for game_info in games_in_series:
            game_id = game_info.get("id"); sequence_number = game_info.get("sequenceNumber")
            if not game_id or sequence_number is None: continue
            if game_id in existing_game_ids: continue

            summary_data = download_riot_summary_data(series_id, sequence_number)
            if not summary_data: time.sleep(API_REQUEST_DELAY); continue

            try:
                participants = summary_data.get("participants", []); teams_data = summary_data.get("teams", [])
                if not participants or len(participants) != 10 or not teams_data or len(teams_data) != 2: continue

                our_side = None; our_team_id = None
                for idx, p in enumerate(participants):
                    normalized_name = normalize_player_name(p.get("riotIdGameName"))
                    if normalized_name in ROSTER_RIOT_NAME_TO_GRID_ID: # Используем HLL ростер
                        current_side='blue' if idx<5 else 'red'; current_team_id=100 if idx<5 else 200
                        if our_side is None: our_side=current_side; our_team_id=current_team_id
                        elif our_side!=current_side: log_message(f"Warn: Players on both sides! G:{game_id}"); break
                if our_side is None: continue

                opponent_team_name = "Opponent"; opponent_tags = defaultdict(int)
                opponent_indices = range(5, 10) if our_side=='blue' else range(0, 5)
                for idx in opponent_indices:
                    if idx<len(participants): tag=extract_team_tag(participants[idx].get("riotIdGameName")); tag and opponent_tags.update({tag: opponent_tags[tag] + 1})
                if opponent_tags: sorted_tags=sorted(opponent_tags.items(), key=lambda item: item[1], reverse=True); opponent_team_name=sorted_tags[0][0] if sorted_tags[0][1]>=3 else "Opponent"
                blue_team_name = TEAM_NAME if our_side == 'blue' else opponent_team_name; red_team_name = TEAM_NAME if our_side == 'red' else opponent_team_name

                result = "Unknown"
                for team_summary in teams_data:
                    if team_summary.get("teamId") == our_team_id: win_status = team_summary.get("win"); result = "Win" if win_status is True else "Loss" if win_status is False else "Unknown"; break

                blue_bans = ["N/A"]*5; red_bans = ["N/A"]*5
                for team in teams_data:
                    target_bans = blue_bans if team.get("teamId")==100 else red_bans; bans_list = sorted(team.get("bans",[]), key=lambda x: x.get('pickTurn',99))
                    for i, ban in enumerate(bans_list[:5]): target_bans[i] = str(c_id) if (c_id := ban.get("championId", -1)) != -1 else "N/A"

                game_creation_timestamp = summary_data.get("gameCreation")
                date_str = "N/A"
                if game_creation_timestamp:
                    try: dt_obj=datetime.fromtimestamp(game_creation_timestamp/1000, timezone.utc); date_str=dt_obj.strftime("%Y-%m-%d %H:%M:%S");
                    except Exception: pass

                game_duration_sec = summary_data.get("gameDuration", 0)
                duration_str = "N/A"
                if game_duration_sec > 0:
                    try: minutes, seconds = divmod(int(game_duration_sec), 60); duration_str = f"{minutes}:{seconds:02d}";
                    except Exception: pass

                game_version = summary_data.get("gameVersion", "N/A"); patch_str = "N/A"
                if game_version!="N/A": parts=game_version.split('.'); patch_str=f"{parts[0]}.{parts[1]}" if len(parts)>=2 else game_version

                row_dict = {sql_col: "N/A" for sql_col in sql_column_names}
                row_dict["Date"] = date_str; row_dict["Patch"] = patch_str
                row_dict["Blue_Team_Name"] = blue_team_name; row_dict["Red_Team_Name"] = red_team_name
                row_dict["Duration"] = duration_str; row_dict["Result"] = result
                row_dict["Game_ID"] = game_id
                for i in range(5): row_dict[f"Blue_Ban_{i+1}_ID"] = blue_bans[i]; row_dict[f"Red_Ban_{i+1}_ID"] = red_bans[i]

                role_to_abbr = {"TOP": "TOP", "JUNGLE": "JGL", "MIDDLE": "MID", "BOTTOM": "BOT", "UTILITY": "SUP"}
                for idx, p in enumerate(participants):
                    if not all(k in p for k in ['riotIdGameName', 'championName', 'kills', 'deaths', 'assists', 'totalDamageDealtToChampions', 'totalMinionsKilled', 'neutralMinionsKilled']):
                        log_message(f"Warn G:{game_id}: Incomplete participant data index {idx}"); continue

                    role_name = ROLE_ORDER_FOR_SHEET[idx % 5]; side_prefix = "Blue" if idx < 5 else "Red"
                    role_abbr = role_to_abbr.get(role_name); player_col_prefix = f"{side_prefix}_{role_abbr}"
                    if not role_abbr: continue

                    player_name = normalize_player_name(p.get("riotIdGameName")) or "Unknown"
                    row_dict[f"{player_col_prefix}_Player"] = player_name
                    row_dict[f"{player_col_prefix}_Champ"] = p.get("championName", "N/A")
                    row_dict[f"{player_col_prefix}_K"] = p.get('kills',0)
                    row_dict[f"{player_col_prefix}_D"] = p.get('deaths',0)
                    row_dict[f"{player_col_prefix}_A"] = p.get('assists',0)
                    row_dict[f"{player_col_prefix}_Dmg"] = p.get('totalDamageDealtToChampions',0)
                    row_dict[f"{player_col_prefix}_CS"] = p.get('totalMinionsKilled',0)+p.get('neutralMinionsKilled',0)

                data_tuple = tuple(row_dict.get(sql_col, "N/A") for sql_col in sql_column_names)
                try:
                    cursor.execute(insert_sql, data_tuple)
                    if cursor.rowcount > 0: added_count += 1; existing_game_ids.add(game_id)
                except sqlite3.Error as e: log_message(f"DB Insert Error G:{game_id}: {e}")

            except Exception as e:
                log_message(f"Parse/Process fail G:{game_id}: {e}"); import traceback; log_message(traceback.format_exc()); continue
            finally: time.sleep(API_REQUEST_DELAY / 4)
        try: conn.commit() # Commit after each series
        except sqlite3.Error as e: log_message(f"DB Commit Error after S:{series_id}: {e}")
        time.sleep(API_REQUEST_DELAY / 2)

    try: conn.commit() # Final commit
    except sqlite3.Error as e: log_message(f"DB Final Commit Error: {e}")
    finally: conn.close()

    log_message(f"Scrims update finished. Added {added_count} new game(s).")
    return added_count

# --- Функции для работы с Data Dragon ---
_champion_data_cache = {}
_latest_patch_cache = None
_patch_cache_time = None

def get_latest_patch_version(cache_duration=3600):
    """Получает последнюю версию патча LoL, кэширует результат."""
    global _latest_patch_cache, _patch_cache_time
    now = time.time()
    if _latest_patch_cache and _patch_cache_time and (now - _patch_cache_time < cache_duration):
        return _latest_patch_cache
    try:
        response = requests.get("https://ddragon.leagueoflegends.com/api/versions.json", timeout=10)
        response.raise_for_status()
        versions = response.json()
        if versions:
            _latest_patch_cache = versions[0]
            _patch_cache_time = now
            return _latest_patch_cache
        else: return "14.7.1" # Fallback
    except Exception as e: log_message(f"Error getting latest patch: {e}"); return "14.7.1"

# ОБНОВЛЕННАЯ normalize_champion_name_for_ddragon (с UOL)
def normalize_champion_name_for_ddragon(champ):
    """Нормализует имя чемпиона для URL Data Dragon."""
    if not champ or champ == "N/A": return None
    # Словарь исключений и специфического регистра
    overrides = {
        "Nunu & Willump": "Nunu", "Wukong": "MonkeyKing", "Renata Glasc": "Renata",
        "K'Sante": "KSante", "LeBlanc": "Leblanc", "Miss Fortune": "MissFortune",
        "Jarvan IV": "JarvanIV", "Twisted Fate": "TwistedFate", "Dr. Mundo": "DrMundo",
        "Xin Zhao": "XinZhao", "Bel'Veth": "Belveth", "Kai'Sa": "Kaisa",
        "Cho'Gath": "Chogath", "Kha'Zix": "Khazix", "Vel'Koz": "Velkoz",
        "Rek'Sai": "RekSai", "Aurelion Sol": "AurelionSol", # Добавлено из UOL
        "Fiddlesticks": "Fiddlesticks" # Добавлено из UOL
    }
    if champ in overrides: return overrides[champ]
    # Общая очистка (убираем пробелы, апострофы, точки)
    name_clean = ''.join(c for c in champ if c.isalnum())
    # Некоторые стандартные случаи ddragon после очистки (в нижнем регистре для сравнения)
    ddragon_exceptions = {
        "monkeyking": "MonkeyKing", "ksante": "KSante", "leblanc": "Leblanc",
        "missfortune": "MissFortune", "jarvaniv": "JarvanIV", "twistedfate": "TwistedFate",
        "drmundo": "DrMundo", "xinzhao": "XinZhao", "belveth": "Belveth", "kaisa": "Kaisa",
        "chogath": "Chogath", "khazix": "Khazix", "velkoz": "Velkoz", "reksai": "RekSai",
         "aurelionsol": "AurelionSol" # Добавлено из UOL
         }
    name_clean_lower = name_clean.lower()
    if name_clean_lower in ddragon_exceptions: return ddragon_exceptions[name_clean_lower]
    # Если не в исключениях, просто возвращаем очищенное имя
    # Data Dragon обычно чувствителен к регистру, но очищенное имя часто работает
    return name_clean

def get_champion_data(cache_duration=86400):
    """Загружает данные чемпионов с Data Dragon, кэширует результат."""
    global _champion_data_cache
    now = time.time()
    cache_key = 'champion_data'
    if cache_key in _champion_data_cache and (now - _champion_data_cache[cache_key]['timestamp'] < cache_duration):
        return _champion_data_cache[cache_key]['data']

    patch_version = get_latest_patch_version()
    url = f"https://ddragon.leagueoflegends.com/cdn/{patch_version}/data/en_US/champion.json"
    log_message(f"Fetching champion data from ddragon (Patch: {patch_version})...")
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()['data']
        champion_id_map = {} # 'ID': 'Name'
        champion_name_map = {} # 'Name': 'DDragonName'
        for champ_ddragon_name, champ_info in data.items():
             champ_id = champ_info['key']
             champ_name = champ_info['name']
             champion_id_map[str(champ_id)] = champ_name
             normalized_ddragon_name = normalize_champion_name_for_ddragon(champ_name)
             # Используем нормализованное имя, если оно не None, иначе исходное из ddragon
             champion_name_map[champ_name] = normalized_ddragon_name if normalized_ddragon_name else champ_ddragon_name

        result_data = {'id_map': champion_id_map, 'name_map': champion_name_map}
        _champion_data_cache[cache_key] = {'data': result_data, 'timestamp': now}
        log_message("Champion data fetched and cached.")
        return result_data
    except Exception as e:
        log_message(f"Failed to fetch or process champion data: {e}")
        return {'id_map': {}, 'name_map': {}}

# ОБНОВЛЕННАЯ get_champion_icon_html (из UOL)
def get_champion_icon_html(champion_name_or_id, champion_data, width=25, height=25):
    """Генерирует HTML img тэг (или fallback span '?') для иконки чемпиона."""
    func_input = champion_name_or_id # Сохраняем исходное значение для логов/title

    if not champion_name_or_id or not champion_data:
        return f'<span title="Icon error: Input missing for {func_input}">?</span>' # Заглушка

    champ_name = None
    ddragon_name = None
    input_is_string = isinstance(champion_name_or_id, str)
    input_as_string = str(champion_name_or_id) # Преобразуем в строку для поиска в словарях

    id_map = champion_data.get('id_map', {})
    name_map = champion_data.get('name_map', {})

    # 1. Определение имени чемпиона (champ_name)
    if input_as_string in id_map: # Если передан ID
        champ_name = id_map[input_as_string]
    elif input_is_string: # Если передана строка (может быть имя)
        champ_name = champion_name_or_id

    # 2. Определение имени для Data Dragon (ddragon_name)
    if champ_name:
        # Сначала ищем точное совпадение имени в name_map
        if champ_name in name_map:
            ddragon_name = name_map[champ_name]
        else:
            # Если точного совпадения нет, пробуем нормализовать имя и поискать снова
            normalized_name_from_champ = normalize_champion_name_for_ddragon(champ_name)
            if normalized_name_from_champ and normalized_name_from_champ in name_map.values(): # Проверяем, есть ли такое значение в name_map
                 ddragon_name = normalized_name_from_champ
            elif normalized_name_from_champ: # Если нет в values, используем само нормализованное имя
                 ddragon_name = normalized_name_from_champ
                 # log_message(f"[Icon Debug] Used normalized name '{ddragon_name}' for '{champ_name}' as direct map failed.")
    # Если имя определить не удалось, но на входе была строка, пробуем нормализовать входную строку
    elif input_is_string:
         normalized_input = normalize_champion_name_for_ddragon(champion_name_or_id)
         if normalized_input:
              ddragon_name = normalized_input
              # log_message(f"[Icon Debug] Used normalized input '{ddragon_name}' for '{func_input}'.")

    # 3. Проверка валидности ddragon_name и генерация HTML
    is_ddragon_name_valid = False
    if ddragon_name:
        ddragon_name_lower = ddragon_name.lower()
        # Проверяем на невалидные значения
        if ddragon_name_lower not in ["n/a", "-1", "unknown", "none", "null", ""]:
            is_ddragon_name_valid = True

    if is_ddragon_name_valid:
        patch = get_latest_patch_version()
        icon_url = f"https://ddragon.leagueoflegends.com/cdn/{patch}/img/champion/{ddragon_name}.png"
        display_name_title = champ_name if champ_name else ddragon_name # Для title используем лучшее доступное имя
        return (f'<img src="{icon_url}" width="{width}" height="{height}" '
                f'alt="{display_name_title}" title="{display_name_title}" '
                f'style="vertical-align: middle; margin: 1px;">')
    else:
        # Если не смогли получить валидное имя для ddragon, возвращаем "?"
        display_name_fallback = champ_name if champ_name else func_input
        # log_message(f"[Icon] Failed to find valid ddragon name for '{display_name_fallback}'. Returning '?'.")
        return f'<span title="Icon error: {display_name_fallback}">?</span>'

# --- Основная функция агрегации данных (Без изменений от HLL) ---
def aggregate_scrim_data(time_filter="All Time", side_filter="all"):
    """
    Загружает данные из SQLite, фильтрует по времени и стороне (для статы игроков),
    и агрегирует статистику.
    """
    log_message(f"Aggregating scrim data. Time: {time_filter}, Side: {side_filter}")
    conn = get_db_connection()
    if not conn: return {}, [], {}, {} # Добавлены пустые словари/списки

    where_clause = ""
    params = []
    if time_filter != "All Time":
        now_utc = datetime.now(timezone.utc)
        delta = None
        if time_filter == "3 Days": delta = timedelta(days=3)
        elif time_filter == "1 Week": delta = timedelta(weeks=1)
        elif time_filter == "2 Weeks": delta = timedelta(weeks=2)
        elif time_filter == "4 Weeks": delta = timedelta(weeks=4)
        elif time_filter == "2 Months": delta = timedelta(days=60)
        if delta:
            cutoff_date = (now_utc - delta).strftime("%Y-%m-%d %H:%M:%S")
            where_clause = "WHERE \"Date\" >= ?"
            params.append(cutoff_date)
            log_message(f"Applying time filter: Date >= {cutoff_date}")
        else: log_message(f"Warning: Unknown time filter '{time_filter}'.")

    all_scrim_data = []
    try:
        cursor = conn.cursor()
        select_all_sql = f"SELECT * FROM scrims {where_clause} ORDER BY \"Date\" DESC"
        cursor.execute(select_all_sql, params)
        all_scrim_data = cursor.fetchall()
        log_message(f"Fetched {len(all_scrim_data)} rows from DB based on time filter.")
    except Exception as e:
        log_message(f"Error fetching data for aggregation: {e}")
        if conn: conn.close(); return {}, [], {}, {} # Добавлены пустые словари/списки
    finally:
        if conn: conn.close()

    if not all_scrim_data:
        log_message("No data found for the selected time filter."); return {}, [], {}, {}

    champion_data = get_champion_data()

    overall_stats = { "total_games": 0, "blue_wins": 0, "blue_losses": 0, "red_wins": 0, "red_losses": 0 }
    history_list = []
    player_stats_agg = defaultdict(lambda: defaultdict(lambda: {'games': 0, 'wins': 0, 'k': 0, 'd': 0, 'a': 0, 'dmg': 0, 'cs': 0}))
    role_to_abbr = {"TOP": "TOP", "JUNGLE": "JGL", "MIDDLE": "MID", "BOTTOM": "BOT", "UTILITY": "SUP"}
    valid_side_filters = ["blue", "red"]
    filter_side_norm = side_filter.lower() if side_filter.lower() in valid_side_filters else 'all'

    for row in all_scrim_data:
        try:
            game = dict(row)
            overall_stats["total_games"] += 1
            result = game.get("Result", "Unknown")
            is_our_blue = game.get("Blue_Team_Name") == TEAM_NAME
            is_our_red = game.get("Red_Team_Name") == TEAM_NAME

            if is_our_blue:
                if result == "Win": overall_stats["blue_wins"] += 1
                elif result == "Loss": overall_stats["blue_losses"] += 1
            if is_our_red:
                if result == "Win": overall_stats["red_wins"] += 1
                elif result == "Loss": overall_stats["red_losses"] += 1

            our_side_prefix = None; game_side_matches_filter = False
            if is_our_blue: our_side_prefix = "Blue"; game_side_matches_filter = (filter_side_norm == 'all' or filter_side_norm == 'blue')
            elif is_our_red: our_side_prefix = "Red"; game_side_matches_filter = (filter_side_norm == 'all' or filter_side_norm == 'red')

            if our_side_prefix and game_side_matches_filter:
                is_win = (result == "Win")
                for role in ROLE_ORDER_FOR_SHEET:
                    role_abbr = role_to_abbr.get(role); player_col_prefix = f"{our_side_prefix}_{role_abbr}"
                    if not role_abbr: continue
                    player_name = game.get(f"{player_col_prefix}_Player"); champion_name = game.get(f"{player_col_prefix}_Champ")
                    if player_name in PLAYER_DISPLAY_ORDER and champion_name and champion_name != "N/A":
                         try: k = int(game.get(f"{player_col_prefix}_K", 0) or 0)
                         except (ValueError, TypeError): k = 0
                         try: d = int(game.get(f"{player_col_prefix}_D", 0) or 0)
                         except (ValueError, TypeError): d = 0
                         try: a = int(game.get(f"{player_col_prefix}_A", 0) or 0)
                         except (ValueError, TypeError): a = 0
                         try: dmg = int(game.get(f"{player_col_prefix}_Dmg", 0) or 0)
                         except (ValueError, TypeError): dmg = 0
                         try: cs = int(game.get(f"{player_col_prefix}_CS", 0) or 0)
                         except (ValueError, TypeError): cs = 0
                         stats = player_stats_agg[player_name][champion_name]
                         stats['games'] += 1; stats['wins'] += is_win; stats['k'] += k; stats['d'] += d;
                         stats['a'] += a; stats['dmg'] += dmg; stats['cs'] += cs

            hist_entry = {"Date": game.get("Date", "N/A"),"Patch": game.get("Patch", "N/A"), "Blue_Team_Name": game.get("Blue_Team_Name", "N/A"), "Red_Team_Name": game.get("Red_Team_Name", "N/A"), "Result": result, "Duration": game.get("Duration", "N/A"), "Game_ID": game.get("Game_ID", "N/A")}
            bb_icons = [get_champion_icon_html(game.get(f"Blue_Ban_{i}_ID"), champion_data) for i in range(1, 6)]
            rb_icons = [get_champion_icon_html(game.get(f"Red_Ban_{i}_ID"), champion_data) for i in range(1, 6)]
            hist_entry["B_Bans_HTML"] = " ".join(filter(None, bb_icons))
            hist_entry["R_Bans_HTML"] = " ".join(filter(None, rb_icons))
            bp_icons = [get_champion_icon_html(game.get(f"Blue_{role_to_abbr[role]}_Champ"), champion_data) for role in ROLE_ORDER_FOR_SHEET if role in role_to_abbr]
            rp_icons = [get_champion_icon_html(game.get(f"Red_{role_to_abbr[role]}_Champ"), champion_data) for role in ROLE_ORDER_FOR_SHEET if role in role_to_abbr]
            hist_entry["B_Picks_HTML"] = " ".join(filter(None, bp_icons))
            hist_entry["R_Picks_HTML"] = " ".join(filter(None, rp_icons))
            history_list.append(hist_entry)
        except Exception as row_err:
            log_message(f"Error processing row: {dict(row) if row else 'N/A'}. Error: {row_err}"); continue

    final_player_stats = defaultdict(dict)
    for player in PLAYER_DISPLAY_ORDER:
        if player in player_stats_agg:
            champ_dict = player_stats_agg[player]
            sorted_champs = sorted(champ_dict.items(), key=lambda item: item[1]['games'], reverse=True)
            for champ, stats in sorted_champs:
                games = stats['games']
                if games > 0:
                    stats['win_rate'] = round((stats['wins'] / games) * 100, 1)
                    stats['kda'] = round((stats['k'] + stats['a']) / max(1, stats['d']), 1)
                    stats['avg_dmg'] = math.ceil(stats['dmg'] / games)
                    stats['avg_cs'] = round(stats['cs'] / games, 1)
                    stats['icon_html'] = get_champion_icon_html(champ, champion_data, width=30, height=30)
                    final_player_stats[player][champ] = stats

    log_message("Aggregation complete.")
    return overall_stats, history_list, dict(final_player_stats), champion_data # Возвращаем dict

# --- Блок для тестирования ---
if __name__ == '__main__':
    print("Testing scrims_logic...")
    from dotenv import load_dotenv
    load_dotenv()
    GRID_API_KEY = os.getenv("GRID_API_KEY")
    if not GRID_API_KEY: print("FATAL: GRID_API_KEY not found.")
    else:
        print("Testing data aggregation...")
        test_overall, test_hist, test_player, test_champ_data = aggregate_scrim_data(time_filter="All Time", side_filter='all')
        print("\n--- Overall Stats ---"); print(test_overall)
        print("\n--- Sample History (First 2) ---"); print(test_hist[:2])
        print("\n--- Sample Player Stats (First Player) ---")
        first_player = PLAYER_DISPLAY_ORDER[0] if PLAYER_DISPLAY_ORDER else None
        if first_player: print(f"Stats for {first_player}: {dict(test_player.get(first_player, {}))}")
        else: print("No players in display order.")
        print("\nTest complete.")
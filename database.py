# lol_app_LTA_2/database.py
# lol_app_LTA/database.py

import sqlite3
import os
import sys
from datetime import datetime, timezone

_basedir = os.path.abspath(os.path.dirname(__file__))
DATABASE_PATH = os.path.join(_basedir, 'scrims_data.db')

# --- Заголовки таблиц ---
SCRIMS_HEADER = [
    "Date", "Patch", "Blue Team Name", "Red Team Name", "Duration", "Result",
    "Blue Ban 1 ID", "Blue Ban 2 ID", "Blue Ban 3 ID", "Blue Ban 4 ID", "Blue Ban 5 ID",
    "Red Ban 1 ID", "Red Ban 2 ID", "Red Ban 3 ID", "Red Ban 4 ID", "Red Ban 5 ID",
    "Blue_TOP_Player", "Blue_TOP_Champ", "Blue_TOP_K", "Blue_TOP_D", "Blue_TOP_A", "Blue_TOP_Dmg", "Blue_TOP_CS",
    "Blue_JGL_Player", "Blue_JGL_Champ", "Blue_JGL_K", "Blue_JGL_D", "Blue_JGL_A", "Blue_JGL_Dmg", "Blue_JGL_CS",
    "Blue_MID_Player", "Blue_MID_Champ", "Blue_MID_K", "Blue_MID_D", "Blue_MID_A", "Blue_MID_Dmg", "Blue_MID_CS",
    "Blue_BOT_Player", "Blue_BOT_Champ", "Blue_BOT_K", "Blue_BOT_D", "Blue_BOT_A", "Blue_BOT_Dmg", "Blue_BOT_CS",
    "Blue_SUP_Player", "Blue_SUP_Champ", "Blue_SUP_K", "Blue_SUP_D", "Blue_SUP_A", "Blue_SUP_Dmg", "Blue_SUP_CS",
    "Red_TOP_Player", "Red_TOP_Champ", "Red_TOP_K", "Red_TOP_D", "Red_TOP_A", "Red_TOP_Dmg", "Red_TOP_CS",
    "Red_JGL_Player", "Red_JGL_Champ", "Red_JGL_K", "Red_JGL_D", "Red_JGL_A", "Red_JGL_Dmg", "Red_JGL_CS",
    "Red_MID_Player", "Red_MID_Champ", "Red_MID_K", "Red_MID_D", "Red_MID_A", "Red_MID_Dmg", "Red_MID_CS",
    "Red_BOT_Player", "Red_BOT_Champ", "Red_BOT_K", "Red_BOT_D", "Red_BOT_A", "Red_BOT_Dmg", "Red_BOT_CS",
    "Red_SUP_Player", "Red_SUP_Champ", "Red_SUP_K", "Red_SUP_D", "Red_SUP_A", "Red_SUP_Dmg", "Red_SUP_CS",
    "Game ID"
]

TOURNAMENT_GAMES_HEADER_BASE = [
    "Tournament Name", "Stage Name", "Date", "Patch", "Blue Team Name", "Red Team Name",
    "Duration", "Winner Side", "Blue Ban 1 ID", "Blue Ban 2 ID", "Blue Ban 3 ID", "Blue Ban 4 ID", "Blue Ban 5 ID",
    "Red Ban 1 ID", "Red Ban 2 ID", "Red Ban 3 ID", "Red Ban 4 ID", "Red Ban 5 ID",
    "Blue_TOP_Champ", "Blue_JGL_Champ", "Blue_MID_Champ", "Blue_BOT_Champ", "Blue_SUP_Champ",
    "Red_TOP_Champ", "Red_JGL_Champ", "Red_MID_Champ", "Red_BOT_Champ", "Red_SUP_Champ",
    "Blue_TOP_PUUID", "Blue_JGL_PUUID", "Blue_MID_PUUID", "Blue_BOT_PUUID", "Blue_SUP_PUUID",
    "Red_TOP_PUUID", "Red_JGL_PUUID", "Red_MID_PUUID", "Red_BOT_PUUID", "Red_SUP_PUUID",
    "Blue_TOP_PartID", "Blue_JGL_PartID", "Blue_MID_PartID", "Blue_BOT_PartID", "Blue_SUP_PartID",
    "Red_TOP_PartID", "Red_JGL_PartID", "Red_MID_PartID", "Red_BOT_PartID", "Red_SUP_PartID",
    "Game ID", "Series ID", "Sequence Number"
]
draft_action_columns = []
for i in range(1, 21):
    draft_action_columns.extend([ f"Draft_Action_{i}_Type", f"Draft_Action_{i}_TeamID", f"Draft_Action_{i}_ChampName", f"Draft_Action_{i}_ChampID", f"Draft_Action_{i}_ActionID" ])
TOURNAMENT_GAMES_HEADER = TOURNAMENT_GAMES_HEADER_BASE + draft_action_columns

SOLOQ_GAMES_HEADER = [
    "Match_ID", "Player_Name", "Riot_Name", "Riot_Tag", "Timestamp", "Date_Readable",
    "Win", "Champion", "Role", "Kills", "Deaths", "Assists"
]

manual_draft_action_headers = [f"action_{i}_champion" for i in range(1, 21)]
MANUAL_DRAFTS_HEADER = [
    "id", "filter_team_name", "game_index", "blue_team_editable_name",
    "red_team_editable_name",
] + manual_draft_action_headers + ["last_updated"]

def get_db_connection():
    """Создает и возвращает соединение с базой данных SQLite."""
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_PATH, timeout=10.0)
        conn.row_factory = sqlite3.Row
    except sqlite3.Error as e:
        print(f"Ошибка подключения к SQLite: {e}")
    return conn

def create_table_from_header(cursor, table_name, header_list, primary_key_column="Game ID"):
    """Вспомогательная функция для создания таблицы по списку заголовков."""
    columns_sql = []
    header_copy = list(header_list)
    pk_col_name_sql = primary_key_column.replace(" ", "_").replace(".", "").replace("-", "_")

    pk_col_type = "INTEGER" if primary_key_column == "id" else "TEXT"
    pk_sql = f'"{pk_col_name_sql}" {pk_col_type} PRIMARY KEY'
    if pk_col_type == "INTEGER":
        pk_sql += " AUTOINCREMENT"
    columns_sql.append(pk_sql)

    try: header_copy.remove(primary_key_column)
    except ValueError:
         try: header_copy.remove(pk_col_name_sql)
         except ValueError: print(f"Warning: Primary key '{primary_key_column}' or '{pk_col_name_sql}' not found in header for table '{table_name}'.")

    for header_name in header_copy:
        col_name_sql = header_name.replace(" ", "_").replace(".", "").replace("-", "_")
        col_type = "TEXT"

        is_scrims_numeric = table_name == "scrims" and \
                           any(x in header_name for x in ["_K", "_D", "_A", "_Dmg", "_CS"])
        is_soloq_numeric = table_name == "soloq_games" and \
                           header_name in ["Win", "Timestamp", "Kills", "Deaths", "Assists"]
        is_tourn_numeric = table_name == "tournament_games" and \
                           (header_name == "Sequence Number" or "PartID" in header_name)
        is_manual_numeric = table_name == "manual_drafts" and \
                            header_name == "game_index"

        if is_scrims_numeric or is_soloq_numeric or is_tourn_numeric or is_manual_numeric:
            col_type = "INTEGER DEFAULT 0"
            if header_name == "game_index" and table_name == "manual_drafts":
                 col_type = "INTEGER DEFAULT 1"
        
        if table_name == "manual_drafts" and col_name_sql in ["filter_team_name", "game_index"]:
            col_type += " NOT NULL"

        columns_sql.append(f'"{col_name_sql}" {col_type}')

    create_table_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(columns_sql)});'
    try:
        cursor.execute(create_table_sql)
        print(f"Таблица '{table_name}' успешно проверена/создана.")
        return True
    except sqlite3.Error as e:
        print(f"Ошибка при создании таблицы '{table_name}': {e}")
        return False

def init_db():
    """Инициализирует базу данных: создает таблицы, если они не существуют."""
    conn = get_db_connection()
    if conn is None:
        print("Не удалось подключиться к БД для инициализации.")
        return

    cursor = conn.cursor()
    try:
        # --- Существующие таблицы ---
        print("Проверка/создание таблицы scrims...")
        create_table_from_header(cursor, "scrims", SCRIMS_HEADER, primary_key_column="Game ID")
        
        print("Проверка/создание таблицы tournament_games...")
        create_table_from_header(cursor, "tournament_games", TOURNAMENT_GAMES_HEADER, primary_key_column="Game ID")
        
        print("Проверка/создание таблицы soloq_games...")
        create_table_from_header(cursor, "soloq_games", SOLOQ_GAMES_HEADER, primary_key_column="Match_ID")

        print("Проверка/создание таблицы manual_drafts...")
        if create_table_from_header(cursor, "manual_drafts", MANUAL_DRAFTS_HEADER, primary_key_column="id"):
             try:
                 cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_manual_drafts_team_game ON manual_drafts (filter_team_name, game_index);')
                 print("Уникальный индекс для 'manual_drafts' (team, game_index) успешно проверен/создан.")
             except sqlite3.Error as e:
                 print(f"Ошибка при создании уникального индекса для 'manual_drafts': {e}")
        
        print("Проверка/создание таблицы schedule_entries...")
        create_schedule_sql = "CREATE TABLE IF NOT EXISTS schedule_entries (id INTEGER PRIMARY KEY AUTOINCREMENT, entry_date TEXT NOT NULL, entry_type TEXT NOT NULL, details_time TEXT, details_opponent TEXT, details_notes TEXT, color TEXT);"
        cursor.execute(create_schedule_sql)
        print("Таблица 'schedule_entries' успешно проверена/создана.")
        
        print("Проверка/создание таблицы schedule_notes...")
        create_notes_sql = "CREATE TABLE IF NOT EXISTS schedule_notes (month_id TEXT PRIMARY KEY, notes_content TEXT DEFAULT '');"
        cursor.execute(create_notes_sql)
        print("Таблица 'schedule_notes' (по месяцам) успешно проверена/создана.")

        print("Проверка/создание таблицы jungle_pathing...")
        create_pathing_sql = """
        CREATE TABLE IF NOT EXISTS jungle_pathing (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id TEXT NOT NULL,
            player_puuid TEXT NOT NULL,
            path_sequence TEXT NOT NULL,
            last_updated TEXT NOT NULL
        );"""
        create_pathing_index_sql = "CREATE INDEX IF NOT EXISTS idx_jungle_pathing_game_id ON jungle_pathing (game_id);"
        create_pathing_unique_sql = "CREATE UNIQUE INDEX IF NOT EXISTS idx_jungle_pathing_game_player ON jungle_pathing (game_id, player_puuid);"
        try:
            cursor.execute(create_pathing_sql)
            cursor.execute(create_pathing_index_sql)
            cursor.execute(create_pathing_unique_sql)
            print("Таблица 'jungle_pathing' и индексы успешно проверены/созданы.")
        except sqlite3.Error as e:
             print(f"Ошибка при создании таблицы/индексов 'jungle_pathing': {e}")

        print("Проверка/создание таблицы player_positions_snapshots...")
        create_positions_sql = """
        CREATE TABLE IF NOT EXISTS player_positions_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id TEXT NOT NULL,
            timestamp_seconds INTEGER NOT NULL,
            positions_json TEXT NOT NULL,
            last_updated TEXT NOT NULL
        );"""
        create_positions_index_sql = "CREATE INDEX IF NOT EXISTS idx_player_positions_game_id ON player_positions_snapshots (game_id);"
        create_positions_unique_sql = "CREATE UNIQUE INDEX IF NOT EXISTS idx_player_positions_game_timestamp ON player_positions_snapshots (game_id, timestamp_seconds);"
        try:
            cursor.execute(create_positions_sql)
            cursor.execute(create_positions_index_sql)
            cursor.execute(create_positions_unique_sql)
            print("Таблица 'player_positions_snapshots' и индексы успешно проверены/созданы.")
        except sqlite3.Error as e:
            print(f"Ошибка при создании таблицы/индексов 'player_positions_snapshots': {e}")

        print("Проверка/создание таблицы first_wards_data...")
        create_first_wards_sql = """
        CREATE TABLE IF NOT EXISTS first_wards_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id TEXT NOT NULL,
            player_puuid TEXT NOT NULL,
            participant_id INTEGER,
            player_name TEXT,
            champion_name TEXT,
            ward_type TEXT,
            timestamp_seconds REAL NOT NULL,
            pos_x INTEGER,
            pos_z INTEGER,
            last_updated TEXT NOT NULL
        );"""
        create_first_wards_unique_sql = "CREATE UNIQUE INDEX IF NOT EXISTS idx_first_wards_game_player ON first_wards_data (game_id, player_puuid);"
        create_first_wards_game_id_index_sql = "CREATE INDEX IF NOT EXISTS idx_first_wards_data_game_id ON first_wards_data (game_id);"
        try:
            cursor.execute(create_first_wards_sql)
            cursor.execute(create_first_wards_unique_sql)
            cursor.execute(create_first_wards_game_id_index_sql)
            print("Таблица 'first_wards_data' и индексы успешно проверены/созданы (с колонкой player_name).")
        except sqlite3.Error as e:
            print(f"Ошибка при создании таблицы/индексов 'first_wards_data': {e}")
        
        print("Проверка/создание таблицы all_wards_data...")
        create_all_wards_sql = """
        CREATE TABLE IF NOT EXISTS all_wards_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id TEXT NOT NULL,
            player_puuid TEXT NOT NULL,
            participant_id INTEGER,
            player_name TEXT,
            champion_name TEXT,
            ward_type TEXT,
            timestamp_seconds REAL NOT NULL,
            pos_x INTEGER,
            pos_z INTEGER,
            last_updated TEXT NOT NULL
        );
        """
        create_all_wards_game_id_index_sql = "CREATE INDEX IF NOT EXISTS idx_all_wards_data_game_id ON all_wards_data (game_id);"
        create_all_wards_puuid_index_sql = "CREATE INDEX IF NOT EXISTS idx_all_wards_data_player_puuid ON all_wards_data (player_puuid);"
        create_all_wards_timestamp_index_sql = "CREATE INDEX IF NOT EXISTS idx_all_wards_data_timestamp ON all_wards_data (timestamp_seconds);"
        try:
            cursor.execute(create_all_wards_sql)
            cursor.execute(create_all_wards_game_id_index_sql)
            cursor.execute(create_all_wards_puuid_index_sql)
            cursor.execute(create_all_wards_timestamp_index_sql)
            print("Таблица 'all_wards_data' и индексы успешно проверены/созданы.")
        except sqlite3.Error as e:
            print(f"Ошибка при создании таблицы/индексов 'all_wards_data': {e}")

        print("Проверка/создание таблицы player_positions_timeline...")
        create_positions_timeline_sql = """
        CREATE TABLE IF NOT EXISTS player_positions_timeline (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id TEXT NOT NULL,
            timestamp_ms INTEGER NOT NULL,
            participant_id INTEGER NOT NULL,
            player_puuid TEXT,
            pos_x INTEGER,
            pos_z INTEGER,
            last_updated TEXT NOT NULL
        );
        """
        create_timeline_game_id_index_sql = "CREATE INDEX IF NOT EXISTS idx_timeline_game_id ON player_positions_timeline (game_id);"
        create_timeline_timestamp_index_sql = "CREATE INDEX IF NOT EXISTS idx_timeline_timestamp ON player_positions_timeline (timestamp_ms);"
        create_timeline_game_puuid_index_sql = "CREATE INDEX IF NOT EXISTS idx_timeline_game_puuid ON player_positions_timeline (game_id, player_puuid);"
        try:
            cursor.execute(create_positions_timeline_sql)
            cursor.execute(create_timeline_game_id_index_sql)
            cursor.execute(create_timeline_timestamp_index_sql)
            cursor.execute(create_timeline_game_puuid_index_sql)
            print("Таблица 'player_positions_timeline' и индексы успешно проверены/созданы.")
        except sqlite3.Error as e:
            print(f"Ошибка при создании таблицы/индексов 'player_positions_timeline': {e}")
            
        # <<< НОВАЯ ТАБЛИЦА ДЛЯ ОБЪЕКТОВ >>>
        print("Проверка/создание таблицы objective_events...")
        create_objectives_sql = """
        CREATE TABLE IF NOT EXISTS objective_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id TEXT NOT NULL,
            timestamp_ms INTEGER NOT NULL,
            objective_type TEXT NOT NULL,     -- 'DRAGON', 'BARON', 'HERALD', 'TOWER', 'VOIDGRUB'
            objective_subtype TEXT,           -- 'FIRE', 'WATER', 'OUTER', 'INNER' etc.
            team_id INTEGER,                  -- 100 for Blue, 200 for Red
            killer_participant_id INTEGER,
            lane TEXT                         -- 'TOP_LANE', 'MID_LANE', 'BOT_LANE' for towers
        );
        """
        # Индексы для ускорения выборок
        create_objectives_game_id_index = "CREATE INDEX IF NOT EXISTS idx_objectives_game_id ON objective_events (game_id);"
        create_objectives_type_index = "CREATE INDEX IF NOT EXISTS idx_objectives_type ON objective_events (objective_type);"
        try:
            cursor.execute(create_objectives_sql)
            cursor.execute(create_objectives_game_id_index)
            cursor.execute(create_objectives_type_index)
            print("Таблица 'objective_events' и индексы успешно проверены/созданы.")
        except sqlite3.Error as e:
            print(f"Ошибка при создании таблицы/индексов 'objective_events': {e}")

        conn.commit()
    except sqlite3.Error as e:
        print(f"Ошибка при инициализации БД: {e}")
        conn.rollback()
    finally:
        conn.close()


if __name__ == '__main__':
    print(f"!!! ВНИМАНИЕ: Обновлена схема таблицы 'tournament_games' (добавлены PUUID/PartID).")
    print(f"!!! ВНИМАНИЕ: Добавлены новые таблицы 'jungle_pathing', 'player_positions_snapshots'.")
    print(f"!!! ВНИМАНИЕ: В таблицу 'first_wards_data' добавлена колонка 'player_name'.")
    print(f"!!! ВНИМАНИЕ: Добавлена новая таблица 'all_wards_data'.")
    print(f"!!! ВНИМАНИЕ: Добавлена новая таблица 'player_positions_timeline' для Proximity.")
    print(f"!!! Если приложение после обновления выдает ошибки БД,")
    print(f"!!! попробуйте удалить старый файл БД: {DATABASE_PATH}")
    print(f"!!! и перезапустить приложение для создания новой БД.")
    init_db()
    print("Инициализация базы данных завершена.")
# start_positions_logic.py

import sqlite3
import json
from collections import defaultdict
# <<< ИЗМЕНЕНИЯ: Добавлены импорты для генерации иконок
from scrims_logic import log_message, get_champion_data, get_champion_icon_html
from database import get_db_connection
from tournament_logic import TEAM_TAG_TO_FULL_NAME, UNKNOWN_BLUE_TAG, UNKNOWN_RED_TAG

def get_start_positions_data(selected_team_full_name, selected_champion, games_filter):
    """
    Извлекает данные о стартовых позициях и таймлайны для выбранной команды и фильтров.
    """
    conn = get_db_connection()
    if not conn:
        return [], {"error": "Database connection failed"}, []

    # <<< ИЗМЕНЕНИЕ: Загружаем данные чемпионов один раз
    champion_data = get_champion_data()

    all_teams_display = []
    stats = {"error": None, "message": None, "games_data": []}
    available_champions = ["All"]

    try:
        cursor = conn.cursor()
        
        # 1. Получаем список всех команд
        cursor.execute(f"""
            SELECT DISTINCT Blue_Team_Name as team_tag FROM tournament_games
            WHERE Blue_Team_Name NOT IN ('{UNKNOWN_BLUE_TAG}', 'Blue Team')
            UNION
            SELECT DISTINCT Red_Team_Name as team_tag FROM tournament_games
            WHERE Red_Team_Name NOT IN ('{UNKNOWN_RED_TAG}', 'Red Team')
        """)
        all_teams_tags = {row['team_tag'] for row in cursor.fetchall() if row['team_tag']}
        all_teams_display = sorted(list(set([TEAM_TAG_TO_FULL_NAME.get(tag, tag) for tag in all_teams_tags])))

        if not selected_team_full_name:
            stats["message"] = "Please select a team to view starting positions."
            return all_teams_display, stats, available_champions

        # 2. Определяем тег команды
        selected_team_tag = None
        for tag, full_name in TEAM_TAG_TO_FULL_NAME.items():
            if full_name == selected_team_full_name:
                selected_team_tag = tag
                break
        if not selected_team_tag and selected_team_full_name in all_teams_tags:
            selected_team_tag = selected_team_full_name
        
        if not selected_team_tag:
            stats["error"] = f"Team tag not found for '{selected_team_full_name}'."
            return all_teams_display, stats, available_champions

        # 3. Получаем список доступных чемпионов для фильтра
        champs_query = """
            SELECT DISTINCT champ FROM (
                SELECT Blue_TOP_Champ as champ FROM tournament_games WHERE Blue_Team_Name = :tag UNION ALL
                SELECT Blue_JGL_Champ as champ FROM tournament_games WHERE Blue_Team_Name = :tag UNION ALL
                SELECT Blue_MID_Champ as champ FROM tournament_games WHERE Blue_Team_Name = :tag UNION ALL
                SELECT Blue_BOT_Champ as champ FROM tournament_games WHERE Blue_Team_Name = :tag UNION ALL
                SELECT Blue_SUP_Champ as champ FROM tournament_games WHERE Blue_Team_Name = :tag UNION ALL
                SELECT Red_TOP_Champ as champ FROM tournament_games WHERE Red_Team_Name = :tag UNION ALL
                SELECT Red_JGL_Champ as champ FROM tournament_games WHERE Red_Team_Name = :tag UNION ALL
                SELECT Red_MID_Champ as champ FROM tournament_games WHERE Red_Team_Name = :tag UNION ALL
                SELECT Red_BOT_Champ as champ FROM tournament_games WHERE Red_Team_Name = :tag UNION ALL
                SELECT Red_SUP_Champ as champ FROM tournament_games WHERE Red_Team_Name = :tag
            ) WHERE champ IS NOT NULL AND champ != 'N/A' ORDER BY champ ASC
        """
        cursor.execute(champs_query, {'tag': selected_team_tag})
        available_champions.extend([row['champ'] for row in cursor.fetchall()])

        # 4. Получаем последние игры
        query_games = "SELECT * FROM tournament_games WHERE (Blue_Team_Name = ? OR Red_Team_Name = ?)"
        params_games = [selected_team_tag, selected_team_tag]
        
        if selected_champion and selected_champion != "All":
            champion_filter_sql = """
             AND (? IN (Blue_TOP_Champ, Blue_JGL_Champ, Blue_MID_Champ, Blue_BOT_Champ, Blue_SUP_Champ,
                        Red_TOP_Champ, Red_JGL_Champ, Red_MID_Champ, Red_BOT_Champ, Red_SUP_Champ))
            """
            query_games += champion_filter_sql
            params_games.append(selected_champion)

        query_games += ' ORDER BY "Date" DESC'
        if games_filter != 'All' and games_filter.isdigit():
            query_games += f" LIMIT {int(games_filter)}"
        
        cursor.execute(query_games, params_games)
        game_rows = [dict(row) for row in cursor.fetchall()]

        if not game_rows:
            stats["message"] = "No games found for the selected filters."
            return all_teams_display, stats, available_champions
            
        game_ids_to_query = [game["Game_ID"] for game in game_rows]
        
        # 5. Извлекаем данные о позициях для этих игр до 01:40 (100000 мс)
        positions_data = defaultdict(lambda: defaultdict(list))
        if game_ids_to_query:
            placeholders = ','.join(['?'] * len(game_ids_to_query))
            pos_query = f"""
                SELECT game_id, timestamp_ms, player_puuid, pos_x, pos_z
                FROM player_positions_timeline
                WHERE game_id IN ({placeholders}) AND timestamp_ms <= 100000
                ORDER BY timestamp_ms
            """
            cursor.execute(pos_query, game_ids_to_query)
            for row in cursor.fetchall():
                positions_data[row['game_id']][row['timestamp_ms']].append(dict(row))

        # 6. Собираем полные данные по каждой игре
        for game in game_rows:
            game_id = game["Game_ID"]
            is_our_team_blue = game["Blue_Team_Name"] == selected_team_tag
            
            # <<< ИЗМЕНЕНИЕ: Собираем инфо об игроках и СРАЗУ ГЕНЕРИРУЕМ HTML ИКОНОК
            players_info = {}
            player_icons = {} # Новый словарь для хранения HTML иконок
            for side_prefix, team_id in [("Blue", 100), ("Red", 200)]:
                for role_abbr in ["TOP", "JGL", "MID", "BOT", "SUP"]:
                    puuid = game.get(f"{side_prefix}_{role_abbr}_PUUID")
                    champ = game.get(f"{side_prefix}_{role_abbr}_Champ")
                    if puuid and champ:
                        players_info[puuid] = {
                            "championName": champ,
                            "teamId": team_id
                        }
                        if champ not in player_icons:
                            player_icons[champ] = get_champion_icon_html(champ, champion_data)

            game_timeline = []
            if game_id in positions_data:
                for ts, positions in sorted(positions_data[game_id].items()):
                    frame = {"timestamp": ts, "positions": []}
                    for pos in positions:
                        player_info = players_info.get(pos["player_puuid"])
                        if player_info:
                            frame["positions"].append({
                                "championName": player_info["championName"],
                                "teamId": player_info["teamId"],
                                "x": pos["pos_x"],
                                "z": pos["pos_z"]
                            })
                    if frame["positions"]:
                       game_timeline.append(frame)

            if game_timeline:
                stats["games_data"].append({
                    "game_id": game_id,
                    "blue_team": TEAM_TAG_TO_FULL_NAME.get(game["Blue_Team_Name"], game["Blue_Team_Name"]),
                    "red_team": TEAM_TAG_TO_FULL_NAME.get(game["Red_Team_Name"], game["Red_Team_Name"]),
                    "winner": game["Winner_Side"],
                    "is_win": (is_our_team_blue and game["Winner_Side"] == "Blue") or \
                              (not is_our_team_blue and game["Winner_Side"] == "Red"),
                    "timeline": json.dumps(game_timeline),
                    # <<< ИЗМЕНЕНИЕ: Передаем готовый словарь с HTML иконок в шаблон
                    "player_icons": json.dumps(player_icons) 
                })

    except sqlite3.Error as e:
        log_message(f"DB Error in get_start_positions_data: {e}")
        stats["error"] = "A database error occurred."
    except Exception as e:
        import traceback
        log_message(f"!!! CRITICAL Error in get_start_positions_data: {e}\n{traceback.format_exc()}")
        stats["error"] = "A critical error occurred."
    finally:
        if conn: conn.close()
    
    if not stats["games_data"] and not stats["error"]:
        stats["message"] = "No position data found for the selected filters."

    return all_teams_display, stats, available_champions
# lol_app_LTA_1.4v/jng_clear_logic.py

import sqlite3
import json
from collections import defaultdict
import math

# Импорты из существующих модулей вашего проекта
from database import get_db_connection
from scrims_logic import log_message, get_champion_icon_html, get_champion_data
from tournament_logic import TEAM_TAG_TO_FULL_NAME, UNKNOWN_BLUE_TAG, UNKNOWN_RED_TAG

def get_jng_clear_data(selected_team_full_name, selected_champion):
    """
    Извлекает и агрегирует данные о зачистке леса для страницы JNG Clear.
    Версия 2.3: Финальное исправление ошибки инициализации.
    """
    conn = get_db_connection()
    if not conn:
        return [], {"error": "Database connection failed"}, []

    champion_data = get_champion_data()
    all_teams_display = []
    # ИСПРАВЛЕННАЯ СТРУКТУРА: Простой словарь, который инициализируется корректно.
    stats = {
        "error": None, "message": None,
        "blue_side": {
            "total_games": 0,
            "champions": defaultdict(lambda: {'games': 0, 'wins': 0}),
            "clears": [defaultdict(list) for _ in range(7)],
            "deltas": [[] for _ in range(6)],
            "overall_camp_times": [[] for _ in range(7)]
        },
        "red_side": {
            "total_games": 0,
            "champions": defaultdict(lambda: {'games': 0, 'wins': 0}),
            "clears": [defaultdict(list) for _ in range(7)],
            "deltas": [[] for _ in range(6)],
            "overall_camp_times": [[] for _ in range(7)]
        }
    }
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
            stats["message"] = "Please select a team to view jungle clear patterns."
            return all_teams_display, stats, available_champions

        # 2. Определяем тег команды
        selected_team_tag = None
        for tag, full_name in TEAM_TAG_TO_FULL_NAME.items():
            if full_name == selected_team_full_name: selected_team_tag = tag; break
        if not selected_team_tag and selected_team_full_name in all_teams_tags:
            selected_team_tag = selected_team_full_name
        if not selected_team_tag:
            stats["error"] = f"Team tag not found for '{selected_team_full_name}'."; return all_teams_display, stats, available_champions

        # 3. Получаем чемпионов-лесников для фильтра
        champs_query = """
            SELECT DISTINCT champ FROM (SELECT Blue_JGL_Champ as champ FROM tournament_games WHERE Blue_Team_Name = :tag UNION ALL SELECT Red_JGL_Champ as champ FROM tournament_games WHERE Red_Team_Name = :tag) 
            WHERE champ IS NOT NULL AND champ != 'N/A' ORDER BY champ ASC
        """
        cursor.execute(champs_query, {'tag': selected_team_tag})
        available_champions.extend([row['champ'] for row in cursor.fetchall()])

        # 4. Получаем игры и данные о путях
        query_games = "SELECT * FROM tournament_games WHERE Blue_Team_Name = ? OR Red_Team_Name = ?"
        cursor.execute(query_games, [selected_team_tag, selected_team_tag])
        game_rows = [dict(row) for row in cursor.fetchall()]
        if not game_rows:
            stats["message"] = "No games found for the selected team."; return all_teams_display, stats, available_champions

        game_ids = [game["Game_ID"] for game in game_rows]
        paths_query = f"SELECT game_id, player_puuid, path_sequence FROM jungle_pathing WHERE game_id IN ({','.join(['?']*len(game_ids))})"
        cursor.execute(paths_query, game_ids)
        paths_data = {(row['game_id'], row['player_puuid']): json.loads(row['path_sequence']) for row in cursor.fetchall()}

        # 5. Обрабатываем каждую игру
        for game in game_rows:
            is_blue = game["Blue_Team_Name"] == selected_team_tag
            side_key, prefix = ("blue_side", "Blue") if is_blue else ("red_side", "Red")
            
            jungler_champ = game.get(f"{prefix}_JGL_Champ")
            if not jungler_champ or jungler_champ == "N/A": continue
            if selected_champion != "All" and jungler_champ != selected_champion: continue

            jungler_puuid = game.get(f"{prefix}_JGL_PUUID")
            is_win = game.get("Winner_Side") == prefix

            stats[side_key]["total_games"] += 1
            stats[side_key]["champions"][jungler_champ]['games'] += 1
            if is_win: stats[side_key]["champions"][jungler_champ]['wins'] += 1
            
            path_sequence = paths_data.get((game['Game_ID'], jungler_puuid))
            if path_sequence:
                camp_clears = [a for a in path_sequence if isinstance(a, dict) and 'action' in a and 'time' in a and not a['action'].startswith('Gank') and a['action'] != 'Recall']
                
                for i in range(min(7, len(camp_clears))):
                    camp_name, clear_time = camp_clears[i]['action'], camp_clears[i]['time']
                    stats[side_key]["clears"][i][camp_name].append(clear_time)
                    stats[side_key]["overall_camp_times"][i].append(clear_time)
                
                for i in range(min(6, len(camp_clears) - 1)):
                    delta = camp_clears[i+1]['time'] - camp_clears[i]['time']
                    if delta > 0: stats[side_key]["deltas"][i].append(delta)

    except Exception as e:
        import traceback
        log_message(f"CRITICAL Error in get_jng_clear_data: {e}\n{traceback.format_exc()}")
        stats["error"] = "A critical error occurred."
    finally:
        if conn: conn.close()

    # 6. Финальная обработка и форматирование данных
    def format_side_stats(side_data):
        if side_data["total_games"] == 0: return None
        
        formatted_champions = []
        sorted_champs = sorted(side_data["champions"].items(), key=lambda item: item[1]['games'], reverse=True)
        for champ, data in sorted_champs:
            formatted_champions.append({
                "name": champ, "icon": get_champion_icon_html(champ, champion_data, 32, 32),
                "games": data['games'], "winrate": round((data['wins'] / data['games']) * 100) if data['games'] > 0 else 0
            })
        
        overall_timers = []
        for times in side_data["overall_camp_times"]:
            if times:
                avg_time = sum(times) / len(times)
                mins, secs = divmod(int(avg_time), 60)
                overall_timers.append(f"{mins}:{secs:02d}")
            else: overall_timers.append(None)

        overall_deltas = [f"+{round(sum(d)/len(d))}s" if d else None for d in side_data["deltas"]]
        
        clear_details = []
        all_camp_names = sorted(list(set(camp for slot in side_data["clears"] for camp in slot.keys())))
        
        for i in range(7):
            camp_slot_data = side_data["clears"][i]
            total_in_slot = sum(len(times) for times in camp_slot_data.values())
            slot_stats = {}
            if total_in_slot > 0:
                for camp_name in all_camp_names:
                    if camp_name in camp_slot_data:
                        times = camp_slot_data[camp_name]
                        count = len(times)
                        avg_time = sum(times) / count
                        mins, secs = divmod(int(avg_time), 60)
                        slot_stats[camp_name] = {
                            "count": count,
                            "percentage": round((count / total_in_slot) * 100),
                            "avg_time": f"{mins}:{secs:02d}"
                        }
            clear_details.append(slot_stats)

        return {
            "champions": formatted_champions, "overall_timers": overall_timers, "overall_deltas": overall_deltas,
            "clear_details": clear_details, "all_camp_names": all_camp_names
        }

    stats["blue_side"] = format_side_stats(stats["blue_side"])
    stats["red_side"] = format_side_stats(stats["red_side"])
    
    return all_teams_display, stats, available_champions
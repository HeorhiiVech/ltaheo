# lol_app_LTA_1.5v/objects_logic.py

import sqlite3
from collections import defaultdict
import statistics
import traceback

from database import get_db_connection
from scrims_logic import log_message
from tournament_logic import TEAM_TAG_TO_FULL_NAME, UNKNOWN_BLUE_TAG, UNKNOWN_RED_TAG

def get_objects_data(selected_team_full_name):
    """
    Извлекает и агрегирует данные по всем игровым объектам для выбранной команды.
    Версия 2.1: Исправлена логика для Atakhan.
    """
    conn = get_db_connection()
    if not conn:
        return [], {"error": "Database connection failed"}

    all_teams_display = []
    stats = {
        "error": None, "message": None,
        "selected_team_name": selected_team_full_name,
        "overall": {}, "blue_side": {}, "red_side": {}
    }

    try:
        cursor = conn.cursor()

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
            stats["message"] = "Please select a team to view object statistics."
            return all_teams_display, stats

        selected_team_tag = None
        for tag, full_name in TEAM_TAG_TO_FULL_NAME.items():
            if full_name == selected_team_full_name: selected_team_tag = tag; break
        if not selected_team_tag and selected_team_full_name in all_teams_tags:
            selected_team_tag = selected_team_full_name
        if not selected_team_tag:
            stats["error"] = f"Team tag not found for '{selected_team_full_name}'."; return all_teams_display, stats

        cursor.execute("SELECT * FROM tournament_games WHERE Blue_Team_Name = ? OR Red_Team_Name = ?", (selected_team_tag, selected_team_tag))
        games = [dict(row) for row in cursor.fetchall()]
        
        if not games:
            stats["message"] = "No games found for the selected team."
            return all_teams_display, stats
            
        game_ids = [game["Game_ID"] for game in games]
        placeholders = ','.join(['?'] * len(game_ids))
        cursor.execute(f"SELECT * FROM objective_events WHERE game_id IN ({placeholders}) ORDER BY timestamp_ms ASC", game_ids)
        events = [dict(row) for row in cursor.fetchall()]

        _process_side_data(games, events, selected_team_tag, "overall", stats["overall"])
        _process_side_data(games, events, selected_team_tag, "blue", stats["blue_side"])
        _process_side_data(games, events, selected_team_tag, "red", stats["red_side"])

    except Exception as e:
        log_message(f"CRITICAL Error in get_objects_data: {e}\n{traceback.format_exc()}")
        stats["error"] = "A critical error occurred."
    finally:
        if conn: conn.close()

    return all_teams_display, stats

def _process_side_data(all_games, all_events, team_tag, side_filter, output_stats):
    games_on_side = []
    if side_filter == "overall": games_on_side = all_games
    elif side_filter == "blue": games_on_side = [g for g in all_games if g['Blue_Team_Name'] == team_tag]
    elif side_filter == "red": games_on_side = [g for g in all_games if g['Red_Team_Name'] == team_tag]
    
    total_games = len(games_on_side)
    if total_games == 0:
        output_stats['message'] = f"No games played on {side_filter} side."
        return
        
    game_ids_on_side = {g['Game_ID'] for g in games_on_side}
    events_on_side = [e for e in all_events if e['game_id'] in game_ids_on_side]

    output_stats['total_games'] = total_games
    output_stats['drakes'] = _calculate_drake_stats(games_on_side, events_on_side, team_tag)
    output_stats['voidgrubs'] = _calculate_voidgrub_stats(games_on_side, events_on_side, team_tag)
    # --- ИЗМЕНЕНИЕ: Считаем статистику по objective_type 'ATAKHAN' ---
    output_stats['atakhan'] = _calculate_generic_objective_stats(games_on_side, events_on_side, team_tag, 'ATAKHAN')
    output_stats['heralds'] = _calculate_generic_objective_stats(games_on_side, events_on_side, team_tag, 'HERALD')
    output_stats['barons'] = _calculate_generic_objective_stats(games_on_side, events_on_side, team_tag, 'BARON')
    output_stats['first_tower'] = _calculate_ft_stats(games_on_side, events_on_side, team_tag)

def _ms_to_min_sec(ms):
    if not isinstance(ms, (int, float)) or ms <= 0: return "N/A"
    seconds = int(ms / 1000)
    minutes = seconds // 60
    seconds %= 60
    return f"{minutes}:{seconds:02d}"

def _calculate_drake_stats(games, events, team_tag):
    total_games = len(games)
    if total_games == 0: return {}

    drake_spawn_takes = defaultdict(lambda: {'us': 0, 'them': 0})
    games_with_soul = 0
    games_with_soul_and_win = 0
    first_drake_by_us_times = []
    drakes_by_us_before_7 = 0
    drakes_by_us_before_15 = 0
    total_drakes_by_us = 0

    for game in games:
        game_id = game['Game_ID']
        is_blue = game['Blue_Team_Name'] == team_tag
        our_team_id = 100 if is_blue else 200
        
        # --- ИЗМЕНЕНИЕ: Исключаем ELDER и ATAKHAN из подсчета обычных драконов ---
        drakes_in_game = sorted([
            e for e in events 
            if e['game_id'] == game_id 
            and e['objective_type'] == 'DRAGON' 
            and e['objective_subtype'] not in ['ELDER', 'ATAKHAN']
        ], key=lambda x: x['timestamp_ms'])
        
        our_drake_count = 0
        their_drake_count = 0
        soul_achieved = False

        for i, drake in enumerate(drakes_in_game):
            spawn_num = i + 1
            if drake['team_id'] == our_team_id:
                drake_spawn_takes[spawn_num]['us'] += 1
                our_drake_count += 1
                total_drakes_by_us += 1
                if spawn_num == 1:
                    first_drake_by_us_times.append(drake['timestamp_ms'])
            else:
                drake_spawn_takes[spawn_num]['them'] += 1
                their_drake_count += 1
            
            if not soul_achieved and (our_drake_count >= 4 or their_drake_count >= 4):
                if our_drake_count >= 4:
                    games_with_soul += 1
                    is_win = game['Winner_Side'] == ('Blue' if is_blue else 'Red')
                    if is_win:
                        games_with_soul_and_win += 1
                soul_achieved = True
        
        drakes_by_us_in_game = [d for d in drakes_in_game if d['team_id'] == our_team_id]
        drakes_by_us_before_7 += len([d for d in drakes_by_us_in_game if d['timestamp_ms'] < 7 * 60 * 1000])
        drakes_by_us_before_15 += len([d for d in drakes_by_us_in_game if d['timestamp_ms'] < 15 * 60 * 1000])

    result = { "take_rate": {} }
    for i in range(1, 5):
        us = drake_spawn_takes[i]['us']
        them = drake_spawn_takes[i]['them']
        total = us + them
        result["take_rate"][i] = round((us / total) * 100) if total > 0 else 0

    result["avg_drakes_at_7min"] = round(drakes_by_us_before_7 / total_games, 2)
    result["avg_drakes_at_15min"] = round(drakes_by_us_before_15 / total_games, 2)
    result["avg_drakes_per_game"] = round(total_drakes_by_us / total_games, 2)
    result["soul_percent"] = round((games_with_soul / total_games) * 100) if total_games > 0 else 0
    result["soul_wr_percent"] = round((games_with_soul_and_win / games_with_soul) * 100) if games_with_soul > 0 else 0
    
    result["avg_first_drake_timer"] = _ms_to_min_sec(statistics.mean(first_drake_by_us_times)) if first_drake_by_us_times else "N/A"
    result["min_first_drake_timer"] = _ms_to_min_sec(min(first_drake_by_us_times)) if first_drake_by_us_times else "N/A"
    result["max_first_drake_timer"] = _ms_to_min_sec(max(first_drake_by_us_times)) if first_drake_by_us_times else "N/A"
    result["all_first_drake_timers"] = sorted([_ms_to_min_sec(t) for t in first_drake_by_us_times])

    return result

def _calculate_voidgrub_stats(games, events, team_tag):
    """
    Рассчитывает всю статистику по Личинкам Бездны.
    Версия 2.2: Винрейт в таблице теперь возвращается как число для корректного окрашивания.
    """
    grubs_by_game = defaultdict(lambda: {'our_team_count': 0, 'win': False})
    first_grub_times = []
    total_games = len(games)
    our_team_wins = len([g for g in games if g['Winner_Side'] == ('Blue' if g['Blue_Team_Name'] == team_tag else 'Red')])
    base_winrate = (our_team_wins / total_games) * 100 if total_games > 0 else 0

    for game in games:
        game_id = game['Game_ID']
        is_blue = game['Blue_Team_Name'] == team_tag
        our_team_id = 100 if is_blue else 200
        
        grubs_in_game = sorted([e for e in events if e['game_id'] == game_id and e['objective_type'] == 'VOIDGRUB'], key=lambda x: x['timestamp_ms'])
        
        our_grubs_in_game = [g for g in grubs_in_game if g['team_id'] == our_team_id]
        if our_grubs_in_game:
            first_grub_times.append(our_grubs_in_game[0]['timestamp_ms'])

        grubs_by_game[game_id]['our_team_count'] = len(our_grubs_in_game)
        grubs_by_game[game_id]['win'] = game['Winner_Side'] == ('Blue' if is_blue else 'Red')

    games_with_grubs_dist = defaultdict(lambda: {'count': 0, 'wins': 0})
    for data in grubs_by_game.values():
        count_key = data['our_team_count'] if data['our_team_count'] < 3 else 3 # 0, 1, 2, 3+
        games_with_grubs_dist[count_key]['count'] += 1
        if data['win']:
            games_with_grubs_dist[count_key]['wins'] += 1
    
    result = {"wr_by_grubs": [], "first_grub_stats": {}}
    for i in range(4):
        data = games_with_grubs_dist.get(i, {'count': 0, 'wins': 0})
        count = data['count']
        wr = (data['wins'] / count * 100) if count > 0 else 0
        result["wr_by_grubs"].append({
            "grubs_count": i,
            "games": count,
            # --- ИЗМЕНЕНИЕ: Возвращаем число вместо строки ---
            "winrate": wr,
            "diff_wr": f"{'+' if wr > base_winrate else ''}{(wr - base_winrate):.2f}%"
        })

    total_grubs_taken = sum(g['our_team_count'] for g in grubs_by_game.values())
    games_with_one_plus_grubs = len([g for g in grubs_by_game.values() if g['our_team_count'] >= 1])
    games_with_three_plus_grubs = len([g for g in grubs_by_game.values() if g['our_team_count'] >= 3])

    result["first_grub_stats"] = {
        "avg_taken": f"{(total_grubs_taken / total_games):.2f}" if total_games > 0 else "0.00",
        "one_plus_rate": round(games_with_one_plus_grubs / total_games * 100) if total_games > 0 else 0,
        "three_plus_rate": round(games_with_three_plus_grubs / total_games * 100) if total_games > 0 else 0,
        "avg_time": _ms_to_min_sec(statistics.mean(first_grub_times)) if first_grub_times else "N/A",
        "min_time": _ms_to_min_sec(min(first_grub_times)) if first_grub_times else "N/A",
        "max_time": _ms_to_min_sec(max(first_grub_times)) if first_grub_times else "N/A",
        "start_times": sorted([_ms_to_min_sec(t) for t in first_grub_times])
    }
    return result

def _calculate_generic_objective_stats(games, events, team_tag, obj_type, obj_subtype=None):
    # Эта функция остается без изменений
    obj_times = []
    games_with_obj = 0
    games_with_obj_win = 0
    
    for game in games:
        game_id = game['Game_ID']
        is_blue = game['Blue_Team_Name'] == team_tag
        our_team_id = 100 if is_blue else 200
        
        obj_events = [e for e in events if e['game_id'] == game_id and e['objective_type'] == obj_type]
        if obj_subtype:
            obj_events = [e for e in obj_events if e['objective_subtype'] == obj_subtype]

        our_team_obj_events = sorted([e for e in obj_events if e['team_id'] == our_team_id], key=lambda x: x['timestamp_ms'])
        
        if our_team_obj_events:
            games_with_obj += 1
            obj_times.append(our_team_obj_events[0]['timestamp_ms'])
            if game['Winner_Side'] == ('Blue' if is_blue else 'Red'):
                games_with_obj_win += 1

    total_games = len(games)
    return {
        "percent": round((games_with_obj / total_games) * 100) if total_games > 0 else 0,
        "total": games_with_obj,
        "winrate": round((games_with_obj_win / games_with_obj) * 100) if games_with_obj > 0 else 0,
        "avg_time": _ms_to_min_sec(statistics.mean(obj_times)) if obj_times else "N/A",
        "min_time": _ms_to_min_sec(min(obj_times)) if obj_times else "N/A",
        "max_time": _ms_to_min_sec(max(obj_times)) if obj_times else "N/A",
        "start_times": sorted([_ms_to_min_sec(t) for t in obj_times])
    }

def _calculate_ft_stats(games, events, team_tag):
    """
    Рассчитывает всю статистику по башням.
    Версия 2.2: FT% по линиям теперь показывает распределение, где была взята FT.
    """
    total_games = len(games)
    if total_games == 0: return {}
    
    # --- Этап 1: Сбор всех релевантных событий по каждой игре ---
    
    # Собираем сами события, когда НАША команда забирает FT, для анализа их местоположения
    our_ft_events = [] 
    
    # Таймеры для FTL (Первая Башня на Линии - для расчета таймера)
    our_ftl_times = {'TOP': [], 'MID': [], 'BOT': []}
    
    # Таймеры для сравнения T1 Destroyed vs T1 Lost
    our_first_t1_times = {'TOP': [], 'MID': [], 'BOT': []}
    enemy_first_t1_times = {'TOP': [], 'MID': [], 'BOT': []}

    for game in games:
        game_id = game['Game_ID']
        is_blue = game['Blue_Team_Name'] == team_tag
        our_team_id = 100 if is_blue else 200
        enemy_team_id = 200 if is_blue else 100

        all_towers_in_game = sorted([e for e in events if e['game_id'] == game_id and e['objective_type'] == 'TOWER'], key=lambda x: x['timestamp_ms'])
        
        # 1. Находим самую первую башню в игре
        first_tower_event = next((t for t in all_towers_in_game), None)
        if first_tower_event and first_tower_event['team_id'] == our_team_id:
            our_ft_events.append(first_tower_event) # Сохраняем событие
        
        # 2. Собираем данные для таймеров по линиям
        for lane_key, lane_db in [('TOP', 'TOP_LANE'), ('MID', 'MID_LANE'), ('BOT', 'BOT_LANE')]:
            towers_on_lane = [t for t in all_towers_in_game if t['lane'] == lane_db]
            
            # Собираем таймеры для "Avg FT [LANE] Timer"
            first_tower_on_lane = next((t for t in towers_on_lane), None)
            if first_tower_on_lane and first_tower_on_lane['team_id'] == our_team_id:
                our_ftl_times[lane_key].append(first_tower_on_lane['timestamp_ms'])
                
            # Собираем таймеры для "T1 Destroyed"
            our_first_t1 = next((t for t in towers_on_lane if t['objective_subtype'] == 'OUTER' and t['team_id'] == our_team_id), None)
            if our_first_t1:
                our_first_t1_times[lane_key].append(our_first_t1['timestamp_ms'])

            # Собираем таймеры для "T1 Lost"
            enemy_first_t1 = next((t for t in towers_on_lane if t['objective_subtype'] == 'OUTER' and t['team_id'] == enemy_team_id), None)
            if enemy_first_t1:
                enemy_first_t1_times[lane_key].append(enemy_first_t1['timestamp_ms'])

    # --- Этап 2: Расчет итоговой статистики ---
    result = {"by_lane": {}}
    
    our_ft_count = len(our_ft_events)
    our_ft_total_times = [e['timestamp_ms'] for e in our_ft_events]

    # Общая статистика по FT (не меняется)
    result['avg_ft_percent'] = round((our_ft_count / total_games) * 100) if total_games > 0 else 0
    result['avg_ft_timer'] = _ms_to_min_sec(statistics.mean(our_ft_total_times)) if our_ft_total_times else "N/A"
    
    # Считаем, на каких линиях были взяты FT
    ft_location_counts = {'TOP': 0, 'MID': 0, 'BOT': 0}
    for ft_event in our_ft_events:
        if ft_event['lane'] == 'TOP_LANE': ft_location_counts['TOP'] += 1
        elif ft_event['lane'] == 'MID_LANE': ft_location_counts['MID'] += 1
        elif ft_event['lane'] == 'BOT_LANE': ft_location_counts['BOT'] += 1
        
    # Статистика по линиям
    for lane in ['TOP', 'MID', 'BOT']:
        # --- НОВАЯ ЛОГИКА для FT % по линии ---
        # Считаем процент от общего числа взятых FT, а не от числа игр
        avg_ftl_percent = round((ft_location_counts[lane] / our_ft_count) * 100) if our_ft_count > 0 else 0
        
        # Логика таймеров остается прежней
        avg_ftl_timer = _ms_to_min_sec(statistics.mean(our_ftl_times[lane])) if our_ftl_times[lane] else "N/A"
        
        our_t1_avg_ms = statistics.mean(our_first_t1_times[lane]) if our_first_t1_times[lane] else 0
        enemy_t1_avg_ms = statistics.mean(enemy_first_t1_times[lane]) if enemy_first_t1_times[lane] else 0
        
        time_diff_ms = our_t1_avg_ms - enemy_t1_avg_ms if our_t1_avg_ms and enemy_t1_avg_ms else 0
        time_diff_str = "0:00"
        if time_diff_ms != 0:
            prefix = "+" if time_diff_ms > 0 else "-"
            time_diff_str = f"{prefix}{_ms_to_min_sec(abs(time_diff_ms))}"
            
        result["by_lane"][lane] = {
            "ft_percent": avg_ftl_percent,
            "ft_timer": avg_ftl_timer,
            "t1_our_avg": _ms_to_min_sec(our_t1_avg_ms),
            "t1_enemy_avg": _ms_to_min_sec(enemy_t1_avg_ms),
            "t1_diff": time_diff_str
        }
        
    return result
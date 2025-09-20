# swap_logic.py
import sqlite3
from collections import defaultdict
import traceback

try:
    from shapely.geometry import Point, Polygon
    SHAPELY_AVAILABLE = True
except ImportError:
    SHAPELY_AVAILABLE = False
    Point, Polygon = None, None

from database import get_db_connection
from scrims_logic import log_message
from tournament_logic import TEAM_TAG_TO_FULL_NAME, UNKNOWN_BLUE_TAG, UNKNOWN_RED_TAG, rift_zones, rift_zone_polygons_list

ZONE_POLYGONS = {}
if SHAPELY_AVAILABLE and rift_zones and rift_zone_polygons_list:
    try:
        ZONE_POLYGONS = dict(zip(rift_zones, [Polygon(p) for p in rift_zone_polygons_list]))
    except Exception as e:
        log_message(f"[Swap Logic] Ошибка при обработке полигонов: {e}")
        ZONE_POLYGONS = {}
else:
    log_message(f"[Swap Logic] Shapely не доступен или списки зон пусты. Определение зон отключено.")

def _get_simplified_zone(x, y, zone_name):
    """
    Определяет упрощенное название зоны на основе полного названия,
    согласно строгим финальным категориям.
    """
    # Линии
    if "Top Lane" in zone_name: return "TOP"
    if "Bot Lane" in zone_name: return "BOT"
    if "Mid Lane" in zone_name: return "MID"
    
    # Река
    if "Baron Pit" in zone_name or "Top River" in zone_name: return "TOP River"
    if "Dragon Pit" in zone_name or "Bot River" in zone_name: return "BOT River"
    if "River" in zone_name:
        return "TOP River" if y > 7400 else "BOT River"

    # Джунгли
    blue_top_jng_camps = ["Gromp", "Blue Buff", "Wolves"]
    blue_bot_jng_camps = ["Krugs", "Red Buff", "Raptors"]
    red_top_jng_camps = blue_bot_jng_camps
    red_bot_jng_camps = blue_top_jng_camps

    if "Blue Side" in zone_name and any(camp in zone_name for camp in blue_top_jng_camps): return "TOP JNG"
    if "Blue Side" in zone_name and any(camp in zone_name for camp in blue_bot_jng_camps): return "BOT JNG"
    if "Red Side" in zone_name and any(camp in zone_name for camp in red_top_jng_camps): return "TOP JNG"
    if "Red Side" in zone_name and any(camp in zone_name for camp in red_bot_jng_camps): return "BOT JNG"
    
    # Если зона общая (например, "Jungle"), определяем по координате y (в игре это z)
    if "Jungle" in zone_name:
        return "TOP JNG" if y > 7400 else "BOT JNG"

    # Все остальные зоны (Base, Other и т.д.) игнорируются
    return None

def _get_zone_name_and_simplify(x, y):
    """Находит полное название зоны и сразу его упрощает."""
    if not SHAPELY_AVAILABLE or not ZONE_POLYGONS:
        return None
    
    point = Point(x, y)
    for zone_name, polygon in ZONE_POLYGONS.items():
        if point.within(polygon):
            return _get_simplified_zone(x, y, zone_name)
    # Если точка не попала ни в один полигон, она также игнорируется
    return None

def get_swap_data(selected_team_full_name, selected_champion, games_filter):
    conn = get_db_connection()
    if not conn:
        return [], {"error": "Database connection failed"}, []

    all_teams_display = []
    stats = {"error": None, "message": None, "data": {}}
    available_champions = ["All"]
    
    time_intervals = {
        "03:00-04:00": (180000, 240000),
        "04:00-05:00": (240000, 300000),
        "05:00-06:00": (300000, 360000),
        "06:00-07:00": (360000, 420000),
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
            stats["message"] = "Please select a team to view swap patterns."
            return all_teams_display, stats, available_champions

        selected_team_tag = None
        for tag, full_name in TEAM_TAG_TO_FULL_NAME.items():
            if full_name == selected_team_full_name: selected_team_tag = tag; break
        if not selected_team_tag and selected_team_full_name in all_teams_tags:
            selected_team_tag = selected_team_full_name
        
        if not selected_team_tag:
            stats["error"] = f"Team tag not found for '{selected_team_full_name}'."
            return all_teams_display, stats, available_champions
        
        champs_query = """
            SELECT DISTINCT champ FROM (
                SELECT Blue_TOP_Champ as champ FROM tournament_games WHERE Blue_Team_Name = :tag UNION ALL
                SELECT Blue_BOT_Champ as champ FROM tournament_games WHERE Blue_Team_Name = :tag UNION ALL
                SELECT Blue_SUP_Champ as champ FROM tournament_games WHERE Blue_Team_Name = :tag UNION ALL
                SELECT Red_TOP_Champ as champ FROM tournament_games WHERE Red_Team_Name = :tag UNION ALL
                SELECT Red_BOT_Champ as champ FROM tournament_games WHERE Red_Team_Name = :tag UNION ALL
                SELECT Red_SUP_Champ as champ FROM tournament_games WHERE Red_Team_Name = :tag
            ) WHERE champ IS NOT NULL AND champ != 'N/A' ORDER BY champ ASC
        """
        cursor.execute(champs_query, {'tag': selected_team_tag})
        available_champions.extend([row['champ'] for row in cursor.fetchall()])

        query_games = "SELECT * FROM tournament_games WHERE (Blue_Team_Name = ? OR Red_Team_Name = ?)"
        params_games = [selected_team_tag, selected_team_tag]
        
        if selected_champion and selected_champion != "All":
            query_games += " AND (? IN (Blue_TOP_Champ, Blue_BOT_Champ, Blue_SUP_Champ, Red_TOP_Champ, Red_BOT_Champ, Red_SUP_Champ))"
            params_games.append(selected_champion)

        query_games += ' ORDER BY "Date" DESC'
        if games_filter != 'All' and games_filter.isdigit():
            query_games += f" LIMIT {int(games_filter)}"
        
        cursor.execute(query_games, params_games)
        game_rows = [dict(row) for row in cursor.fetchall()]

        if not game_rows:
            stats["message"] = "No games found for the selected filters."
            return all_teams_display, stats, available_champions

        game_ids_to_query = []
        puuid_to_role_map = {}
        roles_to_query = ["TOP", "BOT", "SUP"]
        role_abbr_map = {"TOP": "TOP", "BOT": "BOT", "SUP": "SUP"}

        for game in game_rows:
            game_ids_to_query.append(game["Game_ID"])
            is_blue = game.get("Blue_Team_Name") == selected_team_tag
            prefix = "Blue" if is_blue else "Red"
            for role in roles_to_query:
                puuid = game.get(f"{prefix}_{role_abbr_map[role]}_PUUID")
                if puuid:
                    puuid_to_role_map[puuid] = role

        all_positions = []
        if game_ids_to_query:
            placeholders = ','.join(['?'] * len(game_ids_to_query))
            pos_query = f"""
                SELECT timestamp_ms, player_puuid, pos_x, pos_z
                FROM player_positions_timeline
                WHERE game_id IN ({placeholders}) AND timestamp_ms BETWEEN 180000 AND 420000
            """
            cursor.execute(pos_query, game_ids_to_query)
            all_positions = cursor.fetchall()
        
        if not all_positions:
            stats["message"] = "No position data found in the 3-7 minute range for the selected games."
            return all_teams_display, stats, available_champions

        tick_counts = {interval: {role: defaultdict(int) for role in roles_to_query} for interval in time_intervals}
        
        for pos in all_positions:
            puuid = pos['player_puuid']
            role = puuid_to_role_map.get(puuid)
            if not role: continue

            ts = pos['timestamp_ms']
            for interval_name, (start_ms, end_ms) in time_intervals.items():
                if start_ms <= ts < end_ms:
                    zone = _get_zone_name_and_simplify(pos['pos_x'], pos['pos_z'])
                    if zone: # Только если зона попала в одну из 7 категорий
                        tick_counts[interval_name][role][zone] += 1
                    break
        
        final_data = {interval: {role: [] for role in roles_to_query} for interval in time_intervals}
        for interval, roles_data in tick_counts.items():
            for role, zones_data in roles_data.items():
                total_ticks = sum(zones_data.values())
                if total_ticks == 0: continue
                
                zone_percentages = []
                for zone, count in zones_data.items():
                    zone_percentages.append({
                        "zone": zone,
                        "percentage": round((count / total_ticks) * 100, 2)
                    })
                
                final_data[interval][role] = sorted(zone_percentages, key=lambda x: x['percentage'], reverse=True)
        
        stats['data'] = final_data

    except sqlite3.Error as e:
        log_message(f"[Swap Logic] DB Error: {e}")
        stats["error"] = "A database error occurred."
    except Exception as e:
        log_message(f"[Swap Logic] CRITICAL Error: {e}\n{traceback.format_exc()}")
        stats["error"] = "A critical error occurred."
    finally:
        if conn: conn.close()

    return all_teams_display, stats, available_champions
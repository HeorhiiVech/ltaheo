# lol_app_LTA_2/app.py
# lol_app_LTA/app.py
from dotenv import load_dotenv
import os
import sys

_basedir = os.path.abspath(os.path.dirname(__file__))
dotenv_path = os.path.join(_basedir, '.env')

try:
    from scrims_logic import log_message
except ImportError:
    import logging
    log_message = logging.info
    logging.basicConfig(level=logging.INFO)

if os.path.exists(dotenv_path):
    log_message(f"Loading .env file from: {dotenv_path}")
    load_dotenv(dotenv_path=dotenv_path)
else:
    log_message(f"WARNING: .env file not found at expected path: {dotenv_path}. API keys might not be loaded.")

from flask import Flask, render_template, request, redirect, url_for, flash
from datetime import datetime, date
from database import get_db_connection, init_db
import json
import sqlite3

from scrims_logic import get_champion_icon_html, get_champion_data, get_latest_patch_version
from tournament_logic import (
    fetch_and_store_tournament_data,
    TARGET_TOURNAMENT_NAME_FOR_DB,
    aggregate_tournament_data,
    TEAM_TAG_TO_FULL_NAME,
    ICON_SIZE_DRAFTS,
    get_all_wards_data,
    get_proximity_data
)
from soloq_logic import (
    TEAM_ROSTERS,
    aggregate_soloq_data_from_db,
    fetch_and_store_soloq_data,
    get_soloq_activity_data
)
from start_positions_logic import get_start_positions_data
from jng_clear_logic import get_jng_clear_data
from objects_logic import get_objects_data
# <<< НОВЫЙ ИМПОРТ ДЛЯ SWAP
from swap_logic import get_swap_data


app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "a_default_secret_key_change_me")
app.jinja_env.globals.update(min=min, max=max)

with app.app_context(): init_db()

@app.context_processor
def inject_now():
    return {'now': datetime.utcnow()}

@app.context_processor
def inject_utility_processor():
    champ_data = get_champion_data()
    team_tag_map = TEAM_TAG_TO_FULL_NAME
    return dict(
        get_champion_icon_html=get_champion_icon_html,
        champion_data=champ_data,
        ICON_SIZE_DRAFTS=ICON_SIZE_DRAFTS,
        team_tag_map=team_tag_map,
        date=date,
        request=request,
        get_latest_patch_version=get_latest_patch_version
    )

@app.route('/')
def index():
    return redirect(url_for('tournament'))

@app.route('/tournament')
def tournament():
    selected_team_full_name = request.args.get('team')
    selected_side_filter = request.args.get('side_filter', 'all')
    side_filters = ["all", "blue", "red"]
    if selected_side_filter not in side_filters:
        selected_side_filter = 'all'

    all_teams_display, team_or_overall_stats, grouped_matches, all_game_details = [], {}, {}, []
    try:
        all_teams_display, team_or_overall_stats, grouped_matches, all_game_details_list = aggregate_tournament_data(
            selected_team_full_name=selected_team_full_name if selected_team_full_name else None,
            side_filter=selected_side_filter
        )
        all_game_details = all_game_details_list
    except Exception as e:
        log_message(f"Error in /tournament data aggregation: {e}")
        import traceback
        log_message(traceback.format_exc())
        flash(f"Error loading tournament data: {e}", "error")
        all_teams_display, team_or_overall_stats, grouped_matches, all_game_details = [], {"error": "Failed to load tournament data."}, {}, []

    return render_template('tournament.html', all_teams=all_teams_display, selected_team=selected_team_full_name, stats=team_or_overall_stats, side_filters=side_filters, selected_side_filter=selected_side_filter, matches=grouped_matches, all_game_details=all_game_details)

@app.route('/update_hll', methods=['POST'])
def update_hll_route():
    log_message("Updating HLL tournament data...")
    tournament_name_for_flash = TARGET_TOURNAMENT_NAME_FOR_DB
    try:
        added_games = fetch_and_store_tournament_data()
    except Exception as e:
        log_message(f"Error during HLL tournament update: {e}")
        flash(f"Error updating {tournament_name_for_flash}: {e}", "error")
        added_games = -1

    if added_games > 0: flash(f"Added/Updated {added_games} game(s) for {tournament_name_for_flash}!", "success")
    elif added_games == 0: flash(f"No new games found or updated for {tournament_name_for_flash}.", "info")
    return redirect(request.referrer or url_for('tournament'))

@app.route('/jng_clear')
def jng_clear():
    selected_team = request.args.get('team')
    selected_champion = request.args.get('champion', 'All')

    all_teams, stats, available_champions = [], {}, []
    try:
        all_teams, stats, available_champions = get_jng_clear_data(
            selected_team_full_name=selected_team,
            selected_champion=selected_champion
        )
    except Exception as e:
        log_message(f"Error in /jng_clear data aggregation: {e}")
        import traceback
        log_message(traceback.format_exc())
        flash(f"Error loading jungle clear data: {e}", "error")
        stats = {"error": "Failed to load jungle clear data."}

    return render_template(
        'jng_clear.html',
        all_teams=all_teams,
        selected_team=selected_team,
        available_champions=available_champions,
        selected_champion=selected_champion,
        stats=stats
    )

@app.route('/objects')
def objects():
    selected_team = request.args.get('team')
    all_teams, stats = [], {}
    try:
        all_teams, stats = get_objects_data(selected_team_full_name=selected_team)
    except Exception as e:
        log_message(f"Error in /objects data aggregation: {e}")
        import traceback
        log_message(traceback.format_exc())
        flash(f"Error loading object data: {e}", "error")
        stats = {"error": "Failed to load object data."}
    
    return render_template(
        'objects.html',
        all_teams=all_teams,
        stats=stats
    )

@app.route('/wards')
def wards():
    selected_team = request.args.get('team')
    selected_role = request.args.get('role', 'All')
    games_filter = request.args.get('games_filter', '20')
    selected_champion = request.args.get('champion', 'All')

    roles = ["All", "TOP", "JGL", "MID", "BOT", "SUP"]
    games_filters = ["5", "10", "20", "30", "50", "All"]
    
    all_teams, wards_by_interval, stats_or_error, available_champions = [], {}, {}, []
    try:
        all_teams, wards_by_interval, stats_or_error, available_champions = get_all_wards_data(
            selected_team_full_name=selected_team,
            selected_role=selected_role,
            games_filter=games_filter,
            selected_champion=selected_champion
        )
    except Exception as e:
        log_message(f"Error in /wards data aggregation: {e}")
        import traceback
        log_message(traceback.format_exc())
        flash(f"Error loading ward data: {e}", "error")
        stats_or_error = {"error": "Failed to load ward data."}

    return render_template(
        'wards.html',
        all_teams=all_teams,
        selected_team=selected_team,
        roles=roles,
        selected_role=selected_role,
        games_filters=games_filters,
        selected_games_filter=games_filter,
        wards_by_interval=wards_by_interval,
        stats=stats_or_error,
        available_champions=available_champions,
        selected_champion=selected_champion
    )

@app.route('/proximity')
def proximity():
    selected_team = request.args.get('team')
    selected_role = request.args.get('role', 'JUNGLE') 
    games_filter = request.args.get('games_filter', '20')

    proximity_roles = ["JUNGLE", "SUPPORT"]
    games_filters = ["5", "10", "20", "30", "50", "All"]

    all_teams, proximity_stats, players_in_role = [], {}, []
    try:
        all_teams, proximity_stats, players_in_role = get_proximity_data(
            selected_team_full_name=selected_team,
            selected_role=selected_role,
            games_filter=games_filter
        )
    except Exception as e:
        log_message(f"Error in /proximity data aggregation: {e}")
        import traceback
        log_message(traceback.format_exc())
        flash(f"Error loading proximity data: {e}", "error")
        proximity_stats = {"error": "Failed to load proximity data."}

    return render_template(
        'proximity.html',
        all_teams=all_teams,
        selected_team=selected_team,
        proximity_roles=proximity_roles,
        selected_role=selected_role,
        games_filters=games_filters,
        selected_games_filter=games_filter,
        stats=proximity_stats,
        players_in_role=players_in_role
    )

@app.route('/start_positions')
def start_positions():
    selected_team = request.args.get('team')
    selected_champion = request.args.get('champion', 'All')
    games_filter = request.args.get('games_filter', '10')

    games_filters = ["5", "10", "15", "20", "All"]

    all_teams, stats, available_champions = [], {}, []
    try:
        all_teams, stats, available_champions = get_start_positions_data(
            selected_team_full_name=selected_team,
            selected_champion=selected_champion,
            games_filter=games_filter
        )
    except Exception as e:
        log_message(f"Error in /start_positions data aggregation: {e}")
        import traceback
        log_message(traceback.format_exc())
        flash(f"Error loading start position data: {e}", "error")
        stats = {"error": "Failed to load start position data."}

    return render_template(
        'start_positions.html',
        all_teams=all_teams,
        selected_team=selected_team,
        games_filters=games_filters,
        selected_games_filter=games_filter,
        available_champions=available_champions,
        selected_champion=selected_champion,
        stats=stats
    )

@app.route('/soloq')
def soloq():
    selected_time_filter = request.args.get('time_filter', 'All Time')
    date_from_str = request.args.get('date_from')
    date_to_str = request.args.get('date_to')

    current_filter_label = selected_time_filter
    if date_from_str and date_to_str: current_filter_label = f"{date_from_str} to {date_to_str}"
    elif date_from_str: current_filter_label = f"From {date_from_str}"
    elif date_to_str: current_filter_label = f"Until {date_to_str}"

    time_filters_soloq = ["All Time", "1 week", "2 weeks", "3 weeks", "4 weeks"]
    player_stats_all = {}
    players = []
    target_team_roster_key = 'Gamespace'

    if target_team_roster_key not in TEAM_ROSTERS:
        flash(f"Team '{target_team_roster_key}' not found in SoloQ rosters configuration.", "error")
    else:
        players = list(TEAM_ROSTERS[target_team_roster_key].keys())
        for player in players:
            try:
                player_stats_all[player] = aggregate_soloq_data_from_db(
                    player, selected_time_filter, date_from_str, date_to_str
                )
            except Exception as e:
                log_message(f"Error aggregating SoloQ data for {player}: {e}")
                flash(f"Could not load SoloQ stats for {player}: {e}", "warning")
                player_stats_all[player] = []

    selected_player_viz = request.args.get('viz_player', players[0] if players else None)
    selected_agg_type = request.args.get('agg_type', 'Day')
    activity_data = {}
    if selected_player_viz:
        try:
            activity_data = get_soloq_activity_data(selected_player_viz, selected_agg_type)
        except Exception as e:
            log_message(f"Error getting SoloQ activity for {selected_player_viz}: {e}")
            flash(f"Could not load activity data for {selected_player_viz}: {e}", "warning")

    return render_template(
        'soloq.html',
        players=players,
        player_stats_all=player_stats_all,
        time_filters=time_filters_soloq,
        selected_time_filter=selected_time_filter,
        current_filter_label=current_filter_label,
        selected_player_viz=selected_player_viz,
        selected_agg_type=selected_agg_type,
        activity_data_json=json.dumps(activity_data)
    )

@app.route('/update_soloq', methods=['POST'])
def update_soloq_route():
    log_message("Получен запрос на обновление данных SoloQ...")
    api_key = os.getenv("RIOT_API_KEY")
    if not api_key:
        log_message("Update SoloQ failed: RIOT_API_KEY is not set in environment.")
        flash("Error: Riot API Key is not configured.", "error")
        return redirect(url_for('soloq'))

    target_team_roster_key = 'Gamespace'
    if target_team_roster_key not in TEAM_ROSTERS:
        flash(f"Team '{target_team_roster_key}' not found in SoloQ rosters.", "error")
        return redirect(url_for('soloq'))

    players = list(TEAM_ROSTERS[target_team_roster_key].keys())
    total_added_count = 0
    update_errors = 0
    for player in players:
        try:
            added_count = fetch_and_store_soloq_data(player)
            if added_count == -1: update_errors += 1
            elif added_count > 0: total_added_count += added_count
        except Exception as e:
            update_errors += 1
            log_message(f"Error during SoloQ update for player {player}: {e}")
            import traceback
            log_message(traceback.format_exc())
            flash(f"Failed to update SoloQ data for {player}: {e}", "error")

    if update_errors == 0:
        if total_added_count > 0: flash(f"Successfully added {total_added_count} new SoloQ game(s)!", "success")
        else: flash("No new SoloQ games found for any player.", "info")
    else:
        flash(f"SoloQ update completed with {update_errors} error(s). Check logs for details.", "warning")

    return redirect(request.referrer or url_for('soloq'))

# <<< НОВЫЙ МАРШРУТ ДЛЯ SWAP ---
@app.route('/swap')
def swap():
    selected_team = request.args.get('team')
    selected_champion = request.args.get('champion', 'All')
    games_filter = request.args.get('games_filter', '10')
    games_filters = ["5", "10", "20", "All"]

    all_teams, stats, available_champions = [], {}, []
    try:
        all_teams, stats, available_champions = get_swap_data(
            selected_team_full_name=selected_team,
            selected_champion=selected_champion,
            games_filter=games_filter
        )
    except Exception as e:
        log_message(f"Error in /swap data aggregation: {e}")
        import traceback
        log_message(traceback.format_exc())
        flash(f"Error loading swap data: {e}", "error")
        stats = {"error": "Failed to load swap data."}

    return render_template(
        'swap.html',
        all_teams=all_teams,
        selected_team=selected_team,
        available_champions=available_champions,
        selected_champion=selected_champion,
        games_filters=games_filters,
        selected_games_filter=games_filter,
        stats=stats
    )
# --- КОНЕЦ НОВОГО МАРШРУТА ---

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5002, debug=True)
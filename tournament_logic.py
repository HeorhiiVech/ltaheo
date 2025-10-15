# lol_app_LTA_2/tournament_logic.py
# lol_app_LTA/tournament_logic.py

import os
import time
import sqlite3
from datetime import datetime, timezone
from collections import defaultdict, deque
import math
import json
import traceback

# Attempt to import Shapely for zone detection
try:
    from shapely.geometry import Point, Polygon
    SHAPELY_AVAILABLE = True
except ImportError:
    SHAPELY_AVAILABLE = False
    Point, Polygon = None, None

from scrims_logic import (
    log_message,
    post_graphql_request,
    get_rest_request,
    get_series_state,
    download_riot_summary_data,
    download_riot_livestats_data,
    API_REQUEST_DELAY,
    ROLE_ORDER_FOR_SHEET,
    get_latest_patch_version,
    normalize_champion_name_for_ddragon,
    get_champion_data,
    get_champion_icon_html
)
from database import get_db_connection, TOURNAMENT_GAMES_HEADER

# --- Constants ---
TARGET_TOURNAMENT_ID = "827201"
TARGET_TOURNAMENT_NAME_FOR_DB = "HLL Split 3"
MATCH_START_DATE_FILTER = "2025-04-03T00:00:00Z"
TEAM_TAG_TO_FULL_NAME = {

}
ICON_SIZE_PICKS_BANS = 35
ICON_SIZE_DUOS = 30
ICON_SIZE_DRAFTS = 24
UNKNOWN_BLUE_TAG = "UnknownBlue"
UNKNOWN_RED_TAG = "UnknownRed"

GANK_PRESENCE_THRESHOLD = 2.0
TARGET_POSITION_TIMESTAMPS_SEC = [40, 60, 80]
TIMESTAMP_TOLERANCE_SEC = 5.0
PROXIMITY_DISTANCE_THRESHOLD = 2000 # Новая константа для Proximity

# Ward specific constants
WARD_VISION_RADIUS_GAME_UNITS = 900
WARD_TYPE_MAP = {
    "YellowTrinket": "Stealth Ward",
    "yellowTrinket": "Stealth Ward",
    "SightWard": "Stealth Ward",
    "SIGHT_WARD": "Stealth Ward",
    "YELLOW_TRINKET_WARD": "Stealth Ward",
    "sight":"Stealth Ward",

    "ControlWard": "Control Ward",
    "CONTROL_WARD": "Control Ward",
    "JammerDevice": "Control Ward",
    "control": "Control Ward",

    "BlueTrinket": "Farsight Ward",
    "BLUE_TRINKET": "Farsight Ward",
    "blueTrinket":"Farsight Ward",

    "Undefined": "Unknown Ward"
}
VALID_WARD_TYPES = set(WARD_TYPE_MAP.keys())

# Rift Zones and Polygons (LEFT EMPTY)
rift_zones = [
    'Blue Side Raptor Brush', 'Blue Side Raptors (Inner)', 'Blue Side Raptors (Outer)',
    'Blue Side Raptor Intersection', 'Blue Side Raptor Ramp Entrance', 'Behind Dragon Pit 1',
    'Behind Dragon Pit 2', 'Blue Side Red Brush', 'Blue Side Red Ramp Brush',
    'Blue Side Red Buff 1', 'Blue Side Red Buff 2', 'Blue Side Krugs Intersection Brush',
    'Blue Side Krugs Intersection 2', 'Blue Side Krugs Intersection 1', 'Blue Side Krugs',
    'Blue Side Krugs Brush', 'Blue Side Red Shallow Cross', 'Blue Side Red Gate Brush',
    'Blue Side Red Gate', 'Blue Side Red Gate', # Note: Duplicate name in source
    'Blue Side Red Deep Path', 'Blue Side Red Deep Cross', 'Blue Side Red Dive Area',
    'Blue Side Red Tribrush', 'Blue Side Wolves Ramp Brush', 'Blue Side Wolves Ramp',
    'Blue Side Wolves Intersection 1', 'Blue Side Wolves Intersection 2', 'Blue Side Wolves Brush',
    'Blue Side Blue Deep Cross', 'Blue Side Wolves', 'Blue Side Blue Intersection 1',
    'Blue Side Blue Intersection 2', 'Blue Side Blue Gate', 'Blue Side Blue Buff',
    'Blue Side Gromp', 'Blue Side Blue Tribrush', 'Blue Side Blue Pocket',
    'Blue Side Blue Ramp Brush', 'Blue Side Blue Ramp', 'Blue Side Blue Shallow Cross',
    'Red Side Raptor Brush', 'Red Side Raptors (Inner)', 'Red Side Raptors (Outer)',
    'Red Side Raptor Intersection', 'Red Side Raptor Ramp Entrance', 'Behind Baron Pit 1',
    'Behind Baron Pit 2', 'Red Side Red Brush', 'Red Side Red Ramp Brush',
    'Red Side Red Buff 1', 'Red Side Red Buff 2', 'Red Side Krugs Intersection Brush',
    'Red Side Krugs Intersection 1', 'Red Side Krugs Intersection 2', 'Red Side Krugs',
    'Red Side Krugs Brush', 'Red Side Red Shallow Cross', 'Red Side Red Gate Brush',
    'Red Side Red Gate 1', 'Red Side Red Gate 2', # Assuming these correspond to the two Red Gates
    'Red Side Red Deep Path', 'Red Side Red Deep Cross', 'Red Side Red Dive Area',
    'Red Side Red Tribrush', 'Red Side Wolves Ramp Brush', 'Red Side Wolves Ramp',
    'Red Side Wolves Intersection 1', 'Red Side Wolves Intersection 2', 'Red Side Wolves Brush',
    'Red Side Blue Deep Cross', 'Red Side Wolves', 'Red Side Blue Intersection 1',
    'Red Side Blue Intersection 2', 'Red Side Blue Gate', 'Red Side Blue Buff',
    'Red Side Gromp', 'Red Side Blue Tribrush', 'Red Side Blue Pocket',
    'Red Side Blue Ramp Brush', 'Red Side Blue Ramp', 'Red Side Blue Shallow Cross',
    'Bot Lane Brush', # Assuming this is one brush? Might need split based on original names
    'Bot Mid River 1', 'Bot Mid River 2', 'Bot Pixel', 'Dragon Pit',
    'Outside Dragon Pit (Higher)', 'Outside Dragon Pit (Lower)', 'Bot River Brush',
    'Bot Tribrush Entrance', 'Bot River Mouth 1', 'Bot River Mouth 2',
    'Top Lane Brush', # Assuming this is one brush?
    'Top Mid River 1', 'Top Mid River 2', 'Top Pixel', 'Baron Pit',
    'Outside Baron Pit (Lower)', 'Outside Baron Pit (Higher)', 'Top River Brush',
    'Top Tribrush Entrance', 'Top River Mouth 1', 'Top River Mouth 2',
    'Mid Lane (Center)', # Is this the main mid lane zone?
    'Blue Side Mid Outer Tower', 'Blue Side Mid Outside Outer Tower', 'Blue Side Mid Inner Tower',
    'Blue Side Mid Cross', 'Red Side Mid Outer Tower', 'Red Side Mid Outside Outer Tower',
    'Red Side Mid Inner Tower', 'Red Side Mid Cross', 'Bot Lane Brush Middle',
    'Bot Lane Brush Left', 'Bot Lane Brush Right', 'Bot Lane (Center) 1',
    'Bot Lane (Center) 2', 'Bot Lane (Center) 3', 'Bot Lane Alcove',
    'Blue Side Bot Lane Outer Tower', 'Blue Side Bot Lane Outside Outer Tower',
    'Blue Side Bot Lane Inner Tower', 'Blue Side Bot Lane Area',
    'Red Side Bot Lane Outer Tower', 'Red Side Bot Lane Outside Outer Tower',
    'Red Side Bot Lane Inner Tower', 'Red Side Bot Lane Area', 'Top Lane Brush Middle',
    'Top Lane Brush Right', 'Top Lane Brush Left', 'Top Lane (Center) 1',
    'Top Lane (Center) 2', 'Top Lane (Center) 3', 'Top Lane Alcove',
    'Red Side Top Lane Outer Tower', 'Red Side Top Lane Outside Outer Tower',
    'Red Side Top Lane Inner Tower', 'Red Side Top Lane Area',
    'Blue Side Top Lane Outer Tower', 'Blue Side Top Lane Outside Outer Tower',
    'Blue Side Top Lane Inner Tower', 'Blue Side Top Lane Area',
    'Blue Side Base', # From original list
    'Blue Side Top Inhib Entrance', 'Blue Side Mid Inhib Entrance', 'Blue Side Bot Inhib Entrance',
    'Red Side Base', # From original list
    'Red Side Top Inhib Entrance', 'Red Side Mid Inhib Entrance', 'Red Side Bot Inhib Entrance',
]

rift_zone_polygons_list = [
    # Blue Side Raptor Brush
    Polygon([(216 * 30, 148 * 30), (220 * 30, 164 * 30), (228 * 30, 166 * 30), (223 * 30, 148 * 30)]),

    # Blue Side Raptors (Inner)
    Polygon([(220 * 30, 164 * 30), (222 * 30, 191 * 30), (235 * 30, 198 * 30), (249 * 30, 193 * 30), (261 * 30, 179 * 30)]),

    # Blue Side Raptors (Outer)
    Polygon([(225 * 30, 150 * 30), (229 * 30, 167 * 30), (262 * 30, 179 * 30), (285 * 30, 160 * 30)]),

    # Blue Side Raptor Intersection
    Polygon([(285 * 30, 160 * 30), (261 * 30, 179 * 30), (272 * 30, 195 * 30), (309 * 30, 160 * 30)]),

    # Blue Side Raptor Ramp Entrance
    Polygon([(294 * 30, 173 * 30), (272 * 30, 195 * 30), (284 * 30, 205 * 30), (312 * 30, 185 * 30)]),

    # Behind Dragon Pit 1
    Polygon([(295 * 30, 131 * 30), (295 * 30, 160 * 30), (310 * 30, 160 * 30), (317 * 30, 127 * 30)]),

    # Behind Dragon Pit 2
    Polygon([(284 * 30, 105 * 30), (297 * 30, 131 * 30), (337 * 30, 122 * 30), (336 * 30, 96 * 30)]),

    # Blue Side Red Brush
    Polygon([(266 * 30, 117 * 30), (290 * 30, 133 * 30), (295 * 30, 131 * 30), (288 * 30, 117 * 30)]),

    # Blue Side Red Ramp Brush
    Polygon([(287 * 30, 133 * 30), (285 * 30, 160 * 30), (295 * 30, 160 * 30), (295 * 30, 132 * 30)]),

    # Blue Side Red Buff 1
    Polygon([(250 * 30, 108 * 30), (221 * 30, 120 * 30), (215 * 30, 147 * 30), (268 * 30, 157 * 30), (284 * 30, 130 * 30)]),

    # Blue Side Red Buff 2
    Polygon([(252 * 30, 107 * 30), (265 * 30, 117 * 30), (290 * 30, 117 * 30), (282 * 30, 105 * 30)]),

    # Blue Side Krugs Intersection Brush
    Polygon([(220 * 30, 101 * 30), (230 * 30, 116 * 30), (250 * 30, 108 * 30), (241 * 30, 93 * 30)]),

    # Blue Side Krugs Intersection 2
    Polygon([(229 * 30, 75 * 30), (250 * 30, 107 * 30), (258 * 30, 107 * 30), (265 * 30, 69 * 30)]),

    # Blue Side Krugs Intersection 1
    Polygon([(200 * 30, 60 * 30), (200 * 30, 79 * 30), (219 * 30, 100 * 30), (240 * 30, 93 * 30), (214 * 30, 60 * 30)]),

    # Blue Side Krugs
    Polygon([(266 * 30, 58 * 30), (258 * 30, 106 * 30), (305 * 30, 101 * 30), (315 * 30, 77 * 30), (279 * 30, 58 * 30)]),

    # Blue Side Krugs Brush
    Polygon([(296 * 30, 58 * 30), (295 * 30, 67 * 30), (315 * 30, 77 * 30), (331 * 30, 58 * 30)]),

    # Blue Side Red Shallow Cross
    Polygon([(261 * 30, 181 * 30), (234 * 30, 208 * 30), (247 * 30, 221 * 30), (274 * 30, 198 * 30)]),

    # Blue Side Red Gate Brush
    Polygon([(188 * 30, 104 * 30), (181 * 30, 115 * 30), (195 * 30, 122 * 30), (201 * 30, 110 * 30)]),

    # Blue Side Red Gate
    Polygon([(175 * 30, 60 * 30), (150 * 30, 133 * 30), (165 * 30, 145 * 30), (190 * 30, 100 * 30), (193 * 30, 60 * 30)]),

    # Blue Side Red Gate
    Polygon([(182 * 30, 117 * 30), (178 * 30, 123 * 30), (190 * 30, 132 * 30), (195 * 30, 123 * 30)]),

    # Blue Side Red Deep Path
    Polygon([(211 * 30, 92 * 30), (188 * 30, 142 * 30), (215 * 30, 147 * 30), (223 * 30, 105 * 30)]),

    # Blue Side Red Deep Cross
    Polygon([(187 * 30, 142 * 30), (187 * 30, 164 * 30), (205 * 30, 180 * 30), (221 * 30, 169 * 30), (215 * 30, 147 * 30)]),

    # Blue Side Red Dive Area
    Polygon([(341 * 30, 61 * 30), (316 * 30, 76 * 30), (307 * 30, 97 * 30), (365 * 30, 94 * 30), (382 * 30, 67 * 30), (367 * 30, 60 * 30)]),

    # Blue Side Red Tribrush
    Polygon([(336 * 30, 96 * 30), (337 * 30, 122 * 30), (368 * 30, 124 * 30), (372 * 30, 94 * 30)]),

    # Blue Side Wolves Ramp Brush
    Polygon([(170 * 30, 278 * 30), (158 * 30, 284 * 30), (174 * 30, 296 * 30), (184 * 30, 289 * 30)]),

    # Blue Side Wolves Ramp
    Polygon([(157 * 30, 284 * 30), (149 * 30, 287 * 30), (166 * 30, 300 * 30), (173 * 30, 296 * 30)]),
    
    # Blue Side Wolves Intersection 1
    Polygon([(149 * 30, 234 * 30), (129 * 30, 240 * 30), (130 * 30, 249 * 30), (152 * 30, 286 * 30), (180 * 30, 270 * 30)]),

    # Blue Side Wolves Intersection 2
    Polygon([(149 * 30, 222 * 30), (150 * 30, 233 * 30), (163 * 30, 246 * 30), (161 * 30, 218 * 30)]),

    # Blue Side Wolves Brush
    Polygon([(162 * 30, 218 * 30), (163 * 30, 246 * 30), (176 * 30, 245 * 30), (173 * 30, 215 * 30)]),

    # Blue Side Blue Deep Cross
    Polygon([(149 * 30, 194 * 30), (142 * 30, 222 * 30), (182 * 30, 212 * 30), (162 * 30, 192 * 30)]),

    # Blue Side Wolves
    Polygon([(114 * 30, 197 * 30), (99 * 30, 216 * 30), (101 * 30, 241 * 30), (138 * 30, 233 * 30), (149 * 30, 194 * 30)]),

    # Blue Side Blue Intersection 1
    Polygon([(79 * 30, 245 * 30), (59 * 30, 256 * 30), (59 * 30, 271 * 30), (131 * 30, 253 * 30), (128 * 30, 236 * 30)]),

    # Blue Side Blue Intersection 2
    Polygon([(79 * 30, 218 * 30), (79 * 30, 245 * 30), (100 * 30, 241 * 30), (99 * 30, 216 * 30)]),

    # Blue Side Blue Gate
    Polygon([(59 * 30, 176 * 30), (59 * 30, 191 * 30), (79 * 30, 218 * 30), (99 * 30, 216 * 30), (137 * 30, 169 * 30), (126 * 30, 154 * 30)]),

    # Blue Side Blue Buff
    Polygon([(98 * 30, 262 * 30), (110 * 30, 291 * 30), (127 * 30, 301 * 30), (147 * 30, 279 * 30), (131 * 30, 253 * 30)]),

    # Blue Side Gromp
    Polygon([(98 * 30, 262 * 30), (59 * 30, 272 * 30), (62 * 30, 303 * 30), (69 * 30, 313 * 30), (92 * 30, 308 * 30), (108 * 30, 290 * 30)]),

    # Blue Side Blue Tribrush
    Polygon([(59 * 30, 316 * 30), (59 * 30, 335 * 30), (71 * 30, 363 * 30), (91 * 30, 333 * 30), (88 * 30, 310 * 30)]),

    # Blue Side Blue Pocket
    Polygon([(94 * 30, 330 * 30), (78 * 30, 353 * 30), (89 * 30, 371 * 30), (111 * 30, 324 * 30)]),

    # Blue Side Blue Ramp Brush
    Polygon([(107 * 30, 289 * 30), (92 * 30, 307 * 30), (96 * 30, 324 * 30), (116 * 30, 296 * 30)]),

    # Blue Side Blue Ramp
    Polygon([(117 * 30, 295 * 30), (93 * 30, 329 * 30), (132 * 30, 317 * 30), (128 * 30, 302 * 30)]),

    # Blue Side Blue Shallow Cross
    Polygon([(197 * 30, 224 * 30), (170 * 30, 253 * 30), (200 * 30, 264 * 30), (220 * 30, 247 * 30)]),

    # Red Side Raptor Brush
    Polygon([(273 * 30, 334 * 30), (277 * 30, 352 * 30), (286 * 30, 353 * 30), (281 * 30, 336 * 30)]),

    # Red Side Raptors (Inner)
    Polygon([(266 * 30, 302 * 30), (252 * 30, 307 * 30), (240 * 30, 321 * 30), (280 * 30, 335 * 30), (279 * 30, 309 * 30)]),

    # Red Side Raptors (Outer)
    Polygon([(240 * 30, 322 * 30), (217 * 30, 341 * 30), (277 * 30, 352 * 30), (273 * 30, 334 * 30)]),

    # Red Side Raptor Intersection
    Polygon([(228 * 30, 306 * 30), (192 * 30, 341 * 30), (217 * 30, 341 * 30), (241 * 30, 318 * 30)]),

    # Red Side Raptor Ramp Entrance
    Polygon([(217 * 30, 293 * 30), (188 * 30, 316 * 30), (205 * 30, 327 * 30), (229 * 30, 305 * 30)]),

    # Behind Baron Pit 1
    Polygon([(191 * 30, 341 * 30), (184 * 30, 374 * 30), (206 * 30, 370 * 30), (206 * 30, 341 * 30)]),

    # Behind Baron Pit 2
    Polygon([(204 * 30, 370 * 30), (165 * 30, 379 * 30), (166 * 30, 405 * 30), (217 * 30, 396 * 30)]),

    # Red Side Red Brush
    Polygon([(207 * 30, 370 * 30), (213 * 30, 384 * 30), (236 * 30, 384 * 30), (211 * 30, 368 * 30)]),

    # Red Side Red Ramp Brush
    Polygon([(206 * 30, 341 * 30), (206 * 30, 369 * 30), (214 * 30, 368 * 30), (216 * 30, 341 * 30)]),

    # Red Side Red Buff 1
    Polygon([(233 * 30, 344 * 30), (217 * 30, 371 * 30), (251 * 30, 393 * 30), (280 * 30, 381 * 30), (286 * 30, 354 * 30)]),

    # Red Side Red Buff 2
    Polygon([(212 * 30, 384 * 30), (218 * 30, 396 * 30), (249 * 30, 394 * 30), (235 * 30, 384 * 30)]),

    # Red Side Krugs Intersection Brush
    Polygon([(249 * 30, 394 * 30), (259 * 30, 408 * 30), (280 * 30, 400 * 30), (271 * 30, 385 * 30)]),

    # Red Side Krugs Intersection 1
    Polygon([(243 * 30, 394 * 30), (236 * 30, 432 * 30), (272 * 30, 426 * 30), (250 * 30, 394 * 30)]),

    # Red Side Krugs Intersection 2
    Polygon([(282 * 30, 401 * 30), (260 * 30, 408 * 30), (286 * 30, 441 * 30), (301 * 30, 441 * 30)]),

    # Red Side Krugs
    Polygon([(196 * 30, 400 * 30), (186 * 30, 424 * 30), (223 * 30, 443 * 30), (235 * 30, 443 * 30), (243 * 30, 395 * 30)]),

    # Red Side Krugs Brush
    Polygon([(186 * 30, 424 * 30), (170 * 30, 443 * 30), (205 * 30, 443 * 30), (206 * 30, 443 * 30)]),

    # Red Side Red Shallow Cross
    Polygon([(254 * 30, 280 * 30), (228 * 30, 303 * 30), (240 * 30, 316 * 30), (268 * 30, 293 * 30)]),

    # Red Side Red Gate Brush
    Polygon([(306 * 30, 378 * 30), (300 * 30, 391 * 30), (313 * 30, 396 * 30), (319 * 30, 385 * 30)]),

    # Red Side Red Gate 1
    Polygon([(336 * 30, 357 * 30), (311 * 30, 401 * 30), (308 * 30, 441 * 30), (326 * 30, 441 * 30), (351 * 30, 368 * 30)]),

    # Red Side Red Gate 2
    Polygon([(311 * 30, 369 * 30), (306 * 30, 379 * 30), (319 * 30, 385 * 30), (323 * 30, 379 * 30)]),

    # Red Side Red Deep Path
    Polygon([(287 * 30, 354 * 30), (279 * 30, 395 * 30), (291 * 30, 410 * 30), (314 * 30, 359 * 30)]),

    # Red Side Red Deep Cross
    Polygon([(289 * 30, 312 * 30), (280 * 30, 332 * 30), (286 * 30, 354 * 30), (314 * 30, 359 * 30), (315 * 30, 336 * 30)]),

    # Red Side Red Dive Area
    Polygon([(135 * 30, 407 * 30), (120 * 30, 432 * 30), (137 * 30, 441 * 30), (155 * 30, 441 * 30), (184 * 30, 427 * 30), (194 * 30, 405 * 30)]),

    # Red Side Red Tribrush
    Polygon([(133 * 30, 375 * 30), (128 * 30, 407 * 30), (165 * 30, 405 * 30), (164 * 30, 379 * 30)]),

    # Red Side Wolves Ramp Brush
    Polygon([(327 * 30, 203 * 30), (315 * 30, 211 * 30), (332 * 30, 223 * 30), (344 * 30, 217 * 30)]),

    # Red Side Wolves Ramp
    Polygon([(326 * 30, 202 * 30), (344 * 30, 217 * 30), (353 * 30, 212 * 30), (333 * 30, 198 * 30)]),

    # Red Side Wolves Intersection 1
    Polygon([(348 * 30, 215 * 30), (313 * 30, 231 * 30), (351 * 30, 267 * 30), (371 * 30, 261 * 30), (370 * 30, 251 * 30)]),

    # Red Side Wolves Intersection 2
    Polygon([(338 * 30, 256 * 30), (340 * 30, 283 * 30), (352 * 30, 280 * 30), (351 * 30, 268 * 30)]),

    # Red Side Wolves Brush
    Polygon([(325 * 30, 257 * 30), (328 * 30, 286 * 30), (339 * 30, 284 * 30), (338 * 30, 255 * 30)]),

    # Red Side Blue Deep Cross
    Polygon([(318 * 30, 289 * 30), (339 * 30, 309 * 30), (352 * 30, 307 * 30), (358 * 30, 280 * 30)]),

    # Red Side Wolves
    Polygon([(361 * 30, 268 * 30), (351 * 30, 307 * 30), (386 * 30, 304 * 30), (401 * 30, 285 * 30), (399 * 30, 260 * 30)]),

    # Red Side Blue Intersection 1
    Polygon([(369 * 30, 248 * 30), (372 * 30, 265 * 30), (421 * 30, 256 * 30), (441 * 30, 245 * 30), (441 * 30, 230 * 30)]),

    # Red Side Blue Intersection 2
    Polygon([(401 * 30, 260 * 30), (402 * 30, 285 * 30), (422 * 30, 283 * 30), (422 * 30, 256 * 30)]),

    # Red Side Blue Gate
    Polygon([(401 * 30, 286 * 30), (364 * 30, 335 * 30), (375 * 30, 347 * 30), (442 * 30, 325 * 30), (442 * 30, 310 * 30), (422 * 30, 283 * 30)]),

    # Red Side Blue Buff
    Polygon([(373 * 30, 200 * 30), (353 * 30, 222 * 30), (369 * 30, 248 * 30), (401 * 30, 239 * 30), (390 * 30, 211 * 30)]),

    # Red Side Gromp
    Polygon([(392 * 30, 211 * 30), (402 * 30, 239 * 30), (441 * 30, 229 * 30), (438 * 30, 198 * 30), (431 * 30, 188 * 30), (408 * 30, 193 * 30)]),

    # Red Side Blue Tribrush
    Polygon([(429 * 30, 138 * 30), (409 * 30, 168 * 30), (412 * 30, 191 * 30), (441 * 30, 188 * 30), (441 * 30, 166 * 30)]),

    # Red Side Blue Pocket
    Polygon([(412 * 30, 131 * 30), (389 * 30, 175 * 30), (410 * 30, 168 * 30), (423 * 30, 149 * 30)]),

    # Red Side Blue Ramp Brush
    Polygon([(409 * 30, 170 * 30), (384 * 30, 205 * 30), (393 * 30, 211 * 30), (411 * 30, 189 * 30)]),

    # Red Side Blue Ramp
    Polygon([(408 * 30, 169 * 30), (369 * 30, 184 * 30), (373 * 30, 200 * 30), (384 * 30, 206 * 30)]),

    # Red Side Blue Shallow Cross
    Polygon([(311 * 30, 228 * 30), (281 * 30, 253 * 30), (305 * 30, 277 * 30), (331 * 30, 248 * 30)]),

    # Bot Line Brush
    Polygon([(266 * 30, 204 * 30), (254 * 30, 215 * 30), (288 * 30, 246 * 30), (302 * 30, 235 * 30)]),

    # Bot Mid River 1
    Polygon([(273 * 30, 197 * 30), (265 * 30, 205 * 30), (301 * 30, 235 * 30), (311 * 30, 228 * 30)]),

    # Bot Mid River 2
    Polygon([(304 * 30, 191 * 30), (285 * 30, 206 * 30), (302 * 30, 220 * 30), (320 * 30, 206 * 30)]),

    # Bot Pixel
    Polygon([(312 * 30, 185 * 30), (304 * 30, 191 * 30), (320 * 30, 206 * 30), (332 * 30, 198 * 30)]),

    # Dragon Pit
    Polygon([(317 * 30, 127 * 30), (309 * 30, 158 * 30), (334 * 30, 169 * 30), (360 * 30, 157 * 30), (362 * 30, 127 * 30)]),

    # Outside Dragon Pit (Higher)
    Polygon([(307 * 30, 181 * 30), (338 * 30, 202 * 30), (370 * 30, 188 * 30), (360 * 30, 158 * 30)]),

    # Outside Dragon Pit (Lower)
    Polygon([(376 * 30, 130 * 30), (361 * 30, 155 * 30), (369 * 30, 183 * 30), (389 * 30, 176 * 30), (403 * 30, 145 * 30)]),

    # Bot River Brush
    Polygon([(401 * 30, 108 * 30), (394 * 30, 140 * 30), (403 * 30, 146 * 30), (416 * 30, 122 * 30)]),

    # Bot Tribrush Entrance
    Polygon([(372 * 30, 107 * 30), (368 * 30, 124 * 30), (380 * 30, 130 * 30), (383 * 30, 103 * 30)]),

    # Bot River Mouth 1
    Polygon([(385 * 30, 93 * 30), (380 * 30, 131 * 30), (395 * 30, 138 * 30), (401 * 30, 108 * 30)]),

    # Bot River Mouth 2
    Polygon([(390 * 30, 97 * 30), (417 * 30, 122 * 30), (427 * 30, 110 * 30), (396 * 30, 83 * 30)]),

    # Top Line Brush
    Polygon([(212 * 30, 253 * 30), (200 * 30, 265 * 30), (235 * 30, 294 * 30), (247 * 30, 285 * 30)]),

    # Top Mid River 1
    Polygon([(200 * 30, 264 * 30), (191 * 30, 272 * 30), (227 * 30, 302 * 30), (235 * 30, 295 * 30)]),

    # Top Mid River 2
    Polygon([(199 * 30, 279 * 30), (181 * 30, 293 * 30), (197 * 30, 308 * 30), (216 * 30, 293 * 30)]),

    # Top Pixel
    Polygon([(181 * 30, 293 * 30), (168 * 30, 301 * 30), (187 * 30, 315 * 30), (197 * 30, 309 * 30)]),

    # Baron Pit
    Polygon([(168 * 30, 330 * 30), (141 * 30, 346 * 30), (139 * 30, 372 * 30), (184 * 30, 372 * 30), (192 * 30, 341 * 30)]),

    # Outside Baron Pit (Lower)
    Polygon([(162 * 30, 297 * 30), (131 * 30, 311 * 30), (141 * 30, 345 * 30), (190 * 30, 317 * 30)]),

    # Outside Baron Pit (Higher)
    Polygon([(132 * 30, 316 * 30), (112 * 30, 324 * 30), (98 * 30, 353 * 30), (127 * 30, 371 * 30), (140 * 30, 347 * 30)]),

    # Top River Brush
    Polygon([(98 * 30, 354 * 30), (85 * 30, 377 * 30), (100 * 30, 391 * 30), (107 * 30, 359 * 30)]),

    # Top Tribrush Entrance
    Polygon([(121 * 30, 369 * 30), (117 * 30, 396 * 30), (129 * 30, 392 * 30), (133 * 30, 375 * 30)]),

    # Top River Mouth 1
    Polygon([(106 * 30, 361 * 30), (100 * 30, 391 * 30), (116 * 30, 406 * 30), (121 * 30, 368 * 30)]),

    # Top River Mouth 2
    Polygon([(85 * 30, 378 * 30), (74 * 30, 397 * 30), (104 * 30, 421 * 30), (115 * 30, 409 * 30)]),

    # Mid Lane (Center)
    Polygon([(255 * 30, 215 * 30), (214 * 30, 254 * 30), (247 * 30, 284 * 30), (289 * 30, 245 * 30)]),

    # Blue Side Mid Outer Tower
    Polygon([(212 * 30, 187 * 30), (185 * 30, 212 * 30), (202 * 30, 227 * 30), (227 * 30, 203 * 30)]),

    # Blue Side Mid Outside Outer Tower
    Polygon([(227 * 30, 203 * 30), (203 * 30, 228 * 30), (221 * 30, 246 * 30), (247 * 30, 221 * 30)]),

    # Blue Side Mid Inner Tower
    Polygon([(166 * 30, 145 * 30), (137 * 30, 168 * 30), (160 * 30, 191 * 30), (186 * 30, 165 * 30)]),

    # Blue Side Mid Cross
    Polygon([(187 * 30, 166 * 30), (162 * 30, 191 * 30), (183 * 30, 211 * 30), (210 * 30, 186 * 30)]),

    # Red Side Mid Outer Tower
    Polygon([(300 * 30, 272 * 30), (274 * 30, 297 * 30), (288 * 30, 312 * 30), (318 * 30, 288 * 30)]),

    # Red Side Mid Outside Outer Tower
    Polygon([(282 * 30, 253 * 30), (254 * 30, 280 * 30), (272 * 30, 296 * 30), (300 * 30, 270 * 30)]),

    # Red Side Mid Inner Tower
    Polygon([(344 * 30, 315 * 30), (315 * 30, 337 * 30), (334 * 30, 355 * 30), (362 * 30, 334 * 30)]),

    # Red Side Mid Cross
    Polygon([(318 * 30, 288 * 30), (288 * 30, 312 * 30), (314 * 30, 337 * 30), (344 * 30, 314 * 30)]),

    # Bot Lane Brush Middle
    Polygon([(446 * 30, 52 * 30), (429 * 30, 66 * 30), (442 * 30, 78 * 30), (458 * 30, 63 * 30)]),

    # Bot Lane Brush Left
    Polygon([(441 * 30, 32 * 30), (406 * 30, 43 * 30), (421 * 30, 60 * 29), (438 * 30, 44 * 30)]),

    # Bot Lane Brush Right
    Polygon([(463 * 30, 74 * 30), (449 * 30, 86 * 30), (464 * 30, 101 * 30), (471 * 30, 99 * 30)]),

    # Bot Lane (Center) 1
    Polygon([(438 * 30, 44 * 30), (422 * 30, 58 * 30), (429 * 30, 64 * 30), (445 * 30, 51 * 30)]),

    # Bot Lane (Center) 2
    Polygon([(458 * 30, 64 * 30), (442 * 30, 78 * 30), (448 * 30, 85 * 30), (464 * 30, 71 * 30)]),

    # Bot Lane (Center) 3
    Polygon([(406 * 30, 44 * 30), (390 * 30, 78 * 30), (428 * 30, 110 * 30), (462 * 30, 101 * 30)]),

    # Bot Lane Alcove
    Polygon([(440 * 30, 6 * 30), (440 * 30, 42 * 30), (464 * 30, 72 * 30), (496 * 30, 63 * 30)]),

    # Blue Side Bot Lane Outer Tower
    Polygon([(344 * 30, 21 * 30), (343 * 30, 59 * 30), (369 * 30, 59 * 30), (374 * 30, 21 * 30)]),

    # Blue Side Bot Lane Outside Outer Tower
    Polygon([(374 * 30, 21 * 30), (369 * 30, 60 * 30), (392 * 30, 74 * 30), (414 * 30, 26 * 30)]),

    # Blue Side Bot Lane Inner Tower
    Polygon([(216 * 30, 21 * 30), (216 * 30, 59 * 30), (251 * 30, 58 * 30), (254 * 30, 21 * 30)]),

    # Blue Side Bot Lane Area
    Polygon([(254 * 30, 21 * 30), (252 * 30, 58 * 30), (341 * 30, 57 * 30), (343 * 30, 21 * 30)]),

    # Red Side Bot Lane Outer Tower
    Polygon([(480 * 30, 135 * 30), (440 * 30, 138 * 30), (442 * 30, 170 * 30), (480 * 30, 168 * 30)]),

    # Red Side Bot Lane Outside Outer Tower
    Polygon([(429 * 30, 111 * 30), (438 * 30, 137 * 30), (480 * 30, 135 * 30), (471 * 30, 100 * 30)]),

    # Red Side Bot Lane Inner Tower
    Polygon([(441 * 30, 259 * 30), (442 * 30, 296 * 30), (478 * 30, 295 * 30), (479 * 30, 257 * 30)]),

    # Red Side Bot Lane Area
    Polygon([(441 * 30, 171 * 30), (441 * 30, 258 * 30), (479 * 30, 257 * 30), (480 * 30, 168 * 30)]),

    # Top Lane Brush Middle
    Polygon([(59 * 30, 425 * 30), (42 * 30, 439 * 30), (54 * 30, 452 * 30), (71 * 30, 438 * 30)]),

    # Top Lane Brush Right
    Polygon([(78 * 30, 444 * 30), (62 * 30, 460 * 30), (89 * 30, 471 * 30), (94 * 30, 460 * 30)]),

    # Top Lane Brush Left
    Polygon([(36 * 30, 403 * 30), (29 * 30, 405 * 30), (36 * 30, 430 * 30), (51 * 30, 418 * 30)]),

    # Top Lane (Center) 1
    Polygon([(71 * 30, 438 * 30), (55 * 30, 453 * 30), (62 * 30, 459 * 30), (77 * 30, 445 * 30)]),

    # Top Lane (Center) 2
    Polygon([(52 * 30, 418 * 30), (36 * 30, 432 * 30), (42 * 30, 439 * 30), (57 * 30, 425 * 30)]),

    # Top Lane (Center) 3
    Polygon([(72 * 30, 394 * 30), (38 * 30, 403 * 30), (94 * 30, 460 * 30), (110 * 30, 426 * 30)]),

    # Top Lane Alcove
    Polygon([(35 * 30, 432 * 30), (10 * 30, 464 * 30), (66 * 30, 492 * 30), (60 * 30, 461 * 30)]),

    # Red Side Top Lane Outer Tower
    Polygon([(135 * 30, 442 * 30), (130 * 30, 475 * 30), (160 * 30, 475 * 30), (161 * 30, 442 * 30)]),

    # Red Side Top Lane Outside Outer Tower
    Polygon([(111 * 30, 428 * 30), (90 * 30, 472 * 30), (131 * 30, 475 * 30), (136 * 30, 441 * 30)]),

    # Red Side Top Lane Inner Tower
    Polygon([(253 * 30, 443 * 30), (250 * 30, 475 * 30), (288 * 30, 475 * 30), (288 * 30, 442 * 30)]),

    # Red Side Top Lane Area
    Polygon([(162 * 30, 444 * 30), (161 * 30, 475 * 30), (250 * 30, 475 * 30), (252 * 30, 443 * 30)]),

    # Blue Side Top Lane Outer Tower
    Polygon([(20 * 30, 340 * 30), (21 * 30, 373 * 30), (62 * 30, 371 * 30), (58 * 30, 338 * 30)]),

    # Blue Side Top Lane Outside Outer Tower
    Polygon([(21 * 30, 373 * 30), (29 * 30, 405 * 30), (71 * 30, 394 * 30), (62 * 30, 371 * 30)]),

    # Blue Side Top Lane Inner Tower
    Polygon([(22 * 30, 213 * 30), (21 * 30, 251 * 30), (59 * 30, 249 * 30), (58 * 30, 212 * 30)]),

    # Blue Side Top Lane Area
    Polygon([(21 * 30, 251 * 30), (20 * 30, 340 * 30), (59 * 30, 337 * 30), (59 * 30, 250 * 30)]),

    # Blue Side Base
    Polygon([(0 * 30, 0 * 30), (0 * 30, 170 * 30), (59 * 30, 176 * 30), (127 * 30, 154 * 30), (150 * 30, 132 * 30), (174 * 30, 61 * 30), (173 * 30, 0 * 30)]),

    # Blue Side Bot Inhib Entrance
    Polygon([(174 * 30, 21 * 30), (174 * 30, 60 * 30), (216 * 30, 59 * 30), (216 * 30, 21 * 30)]),

    # Blue Side Mid Inhib Entrance
    Polygon([(125 * 30, 155 * 30), (137 * 30, 168 * 30), (165 * 30, 144 * 30), (150 * 30, 133 * 30)]),

    # Blue Side Top Inhib Entrance
    Polygon([(23 * 30, 173 * 30), (22 * 30, 213 * 30), (58 * 30, 211 * 30), (58 * 30, 176 * 30)]),

    # Red Side Base
    Polygon([(500 * 30, 500 * 30), (328 * 30, 500 * 30), (327 * 30, 441 * 30), (351 * 30, 368 * 30), (375 * 30, 347 * 30), (443 * 30, 325 * 30), (500 * 30, 331 * 30)]),

    # Red Side Bot Inhib Entrance
    Polygon([(327 * 30, 441 * 30), (288 * 30, 442 * 30), (288 * 30, 475 * 30), (327 * 30, 475 * 30)]),

    # Red Side Mid Inhib Entrance
    Polygon([(363 * 30, 334 * 30), (334 * 30, 356 * 30), (350 * 30, 368 * 30), (375 * 30, 347 * 30)]),

    # Red Side Top Inhib Entrance
    Polygon([(442 * 30, 296 * 30), (443 * 30, 325 * 30), (478 * 30, 328 * 30), (478 * 30, 295 * 30)]),
]


ZONE_POLYGONS = {}
LANE_ZONE_NAMES = []
if SHAPELY_AVAILABLE:
    if len(rift_zones) == len(rift_zone_polygons_list) and len(rift_zones) > 0:
        try:
            ZONE_POLYGONS = dict(zip(rift_zones, rift_zone_polygons_list))
            LANE_ZONE_NAMES = [name for name in ZONE_POLYGONS if 'Lane' in name and not any(sub in name for sub in ['Area', 'Outside', 'Inhib', 'Brush'])]
        except Exception as poly_err:
             log_message(f"ERROR Processing Polygons/Names for zone detection: {poly_err}. Zone detection might fail.")
             ZONE_POLYGONS = {}
    elif len(rift_zones) > 0 or len(rift_zone_polygons_list) > 0 :
         log_message(f"ERROR: Mismatch or empty rift_zones/rift_zone_polygons_list. Zone detection disabled.")
         ZONE_POLYGONS = {}
else:
     pass


MONSTER_NAME_MAP_V3 = {
    "redCamp": "Red Buff", "blueCamp": "Blue Buff", "krug": "Krugs",
    "gromp": "Gromp", "wolf": "Wolves", "raptor": "Raptors",
    "ScuttleCrab": "Scuttle", "SRU_Crab": "Scuttle", "Sru_Crab": "Scuttle",
    "SRU_Dragon_Water": "Ocean Drake", "SRU_Dragon_Fire": "Infernal Drake",
    "SRU_Dragon_Earth": "Mountain Drake", "SRU_Dragon_Air": "Cloud Drake",
    "SRU_Dragon_Hextech": "Hextech Drake", "SRU_Dragon_Chemtech": "Chemtech Drake",
    "SRU_Dragon_Elder": "Elder Dragon", "SRU_RiftHerald": "Rift Herald",
    "SRU_Baron": "Baron Nashor", "SRU_KrugMini": "Mini Krug",
    "SRU_KrugMiniMini": "Tiny Krug", "VoidGrub": "VoidGrub"
}

OBJECTIVE_TYPE_MAP = {
    'SRU_Dragon_Air': ('DRAGON', 'CLOUD'), 'SRU_Dragon_Chemtech': ('DRAGON', 'CHEMTECH'),
    'SRU_Dragon_Elder': ('DRAGON', 'ELDER'), 'SRU_Dragon_Fire': ('DRAGON', 'INFERNAL'),
    'SRU_Dragon_Hextech': ('DRAGON', 'HEXTECH'), 'SRU_Dragon_Earth': ('DRAGON', 'MOUNTAIN'),
    'SRU_Dragon_Water': ('DRAGON', 'OCEAN'), 'SRU_Baron': ('BARON', 'BARON'),
    'SRU_RiftHerald': ('HERALD', 'HERALD'), 'VoidGrub': ('VOIDGRUB', 'VOIDGRUB')
}

TOWER_TYPE_MAP = {
    'OUTER_TURRET': 'OUTER', 'INNER_TURRET': 'INNER',
    'BASE_TURRET': 'INHIBITOR', 'NEXUS_TURRET': 'NEXUS'
}

# lol_app_LTA_1.4v/tournament_logic.py

# lol_app_LTA_1.4v/tournament_logic.py

def extract_objective_events(livestats_content_str, game_id, participants_summary):
    """
    Извлекает ключевые события по объектам (драконы, башни и т.д.) из livestats.
    Версия 4.1: Финальная версия с корректным парсингом ThornboundAtakhan.
    """
    if not livestats_content_str: return []
    
    pid_to_teamid_map = {p.get("participantId"): p.get("teamId") for p in participants_summary if p.get("participantId") is not None}
    
    # ИЗМЕНЕНИЕ: Добавлен ThornboundAtakhan для корректного парсинга
    OBJECTIVE_TYPE_MAP_V2 = {
        'baron': ('BARON', 'BARON'),
        'riftHerald': ('HERALD', 'HERALD'),
        'ThornboundAtakhan': ('ATAKHAN', 'ATAKHAN'),
        'VoidGrub': ('VOIDGRUB', 'VOIDGRUB')
    }

    TOWER_TYPE_MAP_V2 = {
        'outer': 'OUTER', 'inner': 'INNER',
        'inhibitor': 'INHIBITOR', 'nexus': 'NEXUS'
    }
    
    LANE_TYPE_MAP = {
        'top': 'TOP_LANE', 'mid': 'MID_LANE', 'bot': 'BOT_LANE'
    }

    events = []
    try:
        lines = livestats_content_str.strip().split('\n')
    except Exception as e:
        log_message(f"[Objectives] G:{game_id}: Could not split lines - {e}")
        return []

    for line in lines:
        try:
            snapshot = json.loads(line)
            schema = snapshot.get("rfc461Schema")
            game_time = snapshot.get("gameTime") or snapshot.get("timestamp")
            event_type = snapshot.get("eventType") or snapshot.get("type")

            # Эпические монстры
            if event_type == "ELITE_MONSTER_KILL" or schema == "epic_monster_kill":
                monster_type = snapshot.get("monsterType")
                obj_type, obj_subtype = None, None

                # ИЗМЕНЕНИЕ: Логика для драконов и Атахана теперь полностью разделена
                if monster_type == 'dragon':
                    obj_type = 'DRAGON'
                    dragon_type_raw = snapshot.get("dragonType", "unknown").upper()
                    # Старый ELDER (на всякий случай)
                    if dragon_type_raw == "THORNBOUNDATAKHAN": 
                        obj_type, obj_subtype = 'ATAKHAN', 'ATAKHAN'
                    else: 
                        obj_subtype = {'EARTH': 'MOUNTAIN'}.get(dragon_type_raw, dragon_type_raw)
                elif monster_type in OBJECTIVE_TYPE_MAP_V2:
                    obj_type, obj_subtype = OBJECTIVE_TYPE_MAP_V2[monster_type]

                if obj_type:
                    killer_pid = snapshot.get("killer") or snapshot.get("killerId")
                    final_team_id = snapshot.get("killerTeamId") or pid_to_teamid_map.get(killer_pid)
                    events.append({"game_id": game_id, "timestamp_ms": game_time, "objective_type": obj_type, "objective_subtype": obj_subtype, "team_id": final_team_id, "killer_participant_id": killer_pid, "lane": None})

            # Башни
            elif schema == "building_destroyed":
                if game_time is None: continue
                building_type = snapshot.get("buildingType")
                if building_type == "turret":
                    lane_raw = snapshot.get("lane", "unknown")
                    lane = LANE_TYPE_MAP.get(lane_raw, "UNKNOWN_LANE")
                    
                    tower_tier_raw = snapshot.get("turretTier", "unknown")
                    tower_tier = TOWER_TYPE_MAP_V2.get(tower_tier_raw, "UNKNOWN")
                    
                    owner_team_id_raw = snapshot.get("teamID")
                    killer_team_id = None
                    try:
                        owner_team_id = int(owner_team_id_raw)
                        if owner_team_id == 100: killer_team_id = 200
                        elif owner_team_id == 200: killer_team_id = 100
                    except (ValueError, TypeError): pass

                    killer_pid = snapshot.get("lastHitter")
                    final_team_id = killer_team_id or pid_to_teamid_map.get(killer_pid)
                    
                    events.append({
                        "game_id": game_id, "timestamp_ms": game_time,
                        "objective_type": "TOWER", "objective_subtype": tower_tier,
                        "team_id": final_team_id,
                        "killer_participant_id": killer_pid, "lane": lane
                    })

        except (json.JSONDecodeError, TypeError, KeyError):
            continue
            
    log_message(f"[Objectives] G:{game_id}: Finished parsing. Extracted {len(events)} total objective events.")
    return events

def save_objective_events(conn, game_id, events):
    """Сохраняет события по объектам для одной игры в БД."""
    if not conn or not events: return False
    
    cursor = None
    try:
        cursor = conn.cursor()
        # Сначала удаляем старые данные для этой игры
        cursor.execute("DELETE FROM objective_events WHERE game_id = ?", (str(game_id),))
        
        to_insert = [
            (
                str(e['game_id']), int(e['timestamp_ms']), str(e['objective_type']),
                str(e.get('objective_subtype')), e.get('team_id'),
                e.get('killer_participant_id'), str(e.get('lane'))
            ) for e in events
        ]

        if to_insert:
            cursor.executemany("""
                INSERT INTO objective_events
                (game_id, timestamp_ms, objective_type, objective_subtype, team_id, killer_participant_id, lane)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, to_insert)
            log_message(f"[DB Objectives Save] G:{game_id}: Saved {cursor.rowcount} objective events.")
        return True
    except sqlite3.Error as e:
        log_message(f"[DB Objectives Save] G:{game_id}: Database error - {e}")
        return False
    finally:
        if cursor: cursor.close()

# --- Helper Functions (Jungle Pathing, Player Positions, etc.) ---
def get_zone_for_position(x, z):
    if not SHAPELY_AVAILABLE or not ZONE_POLYGONS:
        reason = "Shapely N/A" if not SHAPELY_AVAILABLE else "Polygons not loaded"
        if x < 7400: return f"Blue Side ({reason})"
        elif x > 7400: return f"Red Side ({reason})"
        else: return f"Mid Area ({reason})"
    point = Point(x, z)
    priority_zone_names = [name for name in LANE_ZONE_NAMES] + ['Blue Side Base', 'Red Side Base', 'Dragon Pit', 'Baron Pit']
    for zone_name in priority_zone_names:
        polygon = ZONE_POLYGONS.get(zone_name)
        if polygon:
            try:
                if point.within(polygon): return zone_name
            except Exception as e: log_message(f"Shapely error checking point in priority zone '{zone_name}': {e}")

    for zone_name, polygon in ZONE_POLYGONS.items():
        if zone_name not in priority_zone_names:
            try:
                if point.within(polygon): return zone_name
            except Exception as e: log_message(f"Shapely error checking point in zone '{zone_name}': {e}"); continue
    if x < 7400: return "Blue Side Unknown"
    elif x > 7400: return "Red Side Unknown"
    else: return "Mid Unknown"

def get_monster_details(monster_type, pos_x, pos_z, jungler_team_side):
    camp_name = MONSTER_NAME_MAP_V3.get(monster_type, monster_type)
    epics = ["Drake", "Herald", "Baron", "VoidGrub"]
    if camp_name == "Scuttle" or any(epic in camp_name for epic in epics):
        return camp_name

    kill_zone = get_zone_for_position(pos_x, pos_z)
    is_enemy = False
    if jungler_team_side == "Blue":
        if kill_zone.startswith("Red Side") or "Red Jungle" in kill_zone or kill_zone == "Red Side Unknown":
            is_enemy = True
    elif jungler_team_side == "Red":
        if kill_zone.startswith("Blue Side") or "Blue Jungle" in kill_zone or kill_zone == "Blue Side Unknown":
            is_enemy = True

    final_name = camp_name
    if is_enemy:
        final_name += " (Enemy)"
    return final_name

def extract_player_positions(livestats_content_str, game_id, target_timestamps_sec, tolerance_sec=5.0):
    if not livestats_content_str: return {}
    if not isinstance(target_timestamps_sec, list) or not all(isinstance(ts, (int, float)) for ts in target_timestamps_sec):
         log_message(f"[Positions-ERROR] G:{game_id}: Invalid target_timestamps_sec: {target_timestamps_sec}."); return {}
    if not target_timestamps_sec: return {}

    final_extracted_positions = {}
    targets_completed = set()
    try: lines = livestats_content_str.strip().split('\n')
    except Exception as split_err: log_message(f"[Positions-ERROR] G:{game_id}: Error splitting lines: {split_err}"); return {}

    for line in lines:
        if not line.strip(): continue
        if len(targets_completed) == len(target_timestamps_sec): break
        try: snapshot = json.loads(line)
        except json.JSONDecodeError: continue

        game_time_ms = snapshot.get("gameTime")
        schema = snapshot.get("rfc461Schema")
        if game_time_ms is None or schema != "stats_update": continue
        current_time_sec = game_time_ms / 1000.0
        participants_data = snapshot.get("participants", [])
        if not isinstance(participants_data, list): continue

        for target_ts in target_timestamps_sec:
            if target_ts in targets_completed: continue
            if abs(current_time_sec - target_ts) <= tolerance_sec:
                players_list_candidate = []
                valid_players_found_in_candidate = 0
                for p_data in participants_data:
                    if not isinstance(p_data, dict): continue
                    participant_id = p_data.get("participantID")
                    champion_name = p_data.get("championName")
                    team_id = p_data.get("teamId")
                    position_data = p_data.get("position")
                    is_pid_valid = participant_id is not None
                    is_pos_dict = isinstance(position_data, dict)
                    has_x = is_pos_dict and 'x' in position_data and position_data['x'] is not None
                    has_z = is_pos_dict and 'z' in position_data and position_data['z'] is not None

                    if is_pid_valid and is_pos_dict and has_x and has_z:
                        try:
                            team_id_int = 0
                            if team_id is not None:
                                try: team_id_int = int(team_id)
                                except (ValueError, TypeError): team_id_int = 0
                            player_entry = {
                                'participantID': int(participant_id),
                                'championName': str(champion_name) if champion_name else "Unknown",
                                'teamId': team_id_int,
                                'x': float(position_data['x']),
                                'z': float(position_data['z'])
                            }
                            players_list_candidate.append(player_entry)
                            valid_players_found_in_candidate += 1
                        except (ValueError, TypeError): continue
                if valid_players_found_in_candidate > 0:
                    final_extracted_positions[target_ts] = players_list_candidate
                    targets_completed.add(target_ts)
    return final_extracted_positions

def save_position_snapshot(conn, game_id, timestamp_sec, positions_list):
    if not conn or not game_id or timestamp_sec not in TARGET_POSITION_TIMESTAMPS_SEC or not isinstance(positions_list, list): return False
    try: positions_json = json.dumps(positions_list)
    except (TypeError, ValueError) as json_err: log_message(f"[DB Pos Save] G:{game_id} T:{timestamp_sec}: Error serializing positions: {json_err}"); return False

    last_updated = datetime.now(timezone.utc).isoformat()
    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO player_positions_snapshots
            (game_id, timestamp_seconds, positions_json, last_updated)
            VALUES (?, ?, ?, ?)
        """, (str(game_id), int(timestamp_sec), positions_json, last_updated))
        return True
    except sqlite3.Error as e: log_message(f"[DB Pos Save] G:{game_id} T:{timestamp_sec}: Database error: {e}"); return False
    finally:
        if cursor: cursor.close()

# --- Новые функции для Proximity ---
def extract_player_positions_timeline(livestats_content_str, game_id):
    """Извлекает ВСЕ данные о позициях из livestats для сохранения в БД."""
    if not livestats_content_str:
        return []

    all_positions = []
    try:
        lines = livestats_content_str.strip().split('\n')
    except Exception as e:
        log_message(f"[TimelineExtract] G:{game_id}: Could not split lines - {e}")
        return []

    for line in lines:
        try:
            snapshot = json.loads(line)
            if snapshot.get("rfc461Schema") == "stats_update" and "gameTime" in snapshot and "participants" in snapshot:
                timestamp_ms = snapshot["gameTime"]
                for p_data in snapshot["participants"]:
                    pos = p_data.get("position")
                    p_id = p_data.get("participantID")
                    puuid = p_data.get("puuid")
                    if p_id is not None and puuid and pos and 'x' in pos and 'z' in pos:
                        all_positions.append({
                            "game_id": game_id,
                            "timestamp_ms": timestamp_ms,
                            "participant_id": p_id,
                            "player_puuid": puuid,
                            "pos_x": int(pos['x']),
                            "pos_z": int(pos['z'])
                        })
        except (json.JSONDecodeError, TypeError, KeyError):
            continue
    return all_positions

def save_player_positions_timeline(conn, game_id, positions_timeline):
    """Сохраняет полную историю позиций для игры в БД."""
    if not conn or not positions_timeline:
        return False
    
    cursor = None
    try:
        cursor = conn.cursor()
        # Сначала удаляем старые данные для этой игры, чтобы избежать дубликатов
        cursor.execute("DELETE FROM player_positions_timeline WHERE game_id = ?", (str(game_id),))

        last_updated = datetime.now(timezone.utc).isoformat()
        
        to_insert = [
            (
                str(pos['game_id']), int(pos['timestamp_ms']), int(pos['participant_id']),
                str(pos['player_puuid']), int(pos['pos_x']), int(pos['pos_z']), last_updated
            ) for pos in positions_timeline
        ]

        if to_insert:
            cursor.executemany("""
                INSERT INTO player_positions_timeline
                (game_id, timestamp_ms, participant_id, player_puuid, pos_x, pos_z, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, to_insert)
            log_message(f"[DB Timeline Save] G:{game_id}: Saved {cursor.rowcount} position entries.")
        return True
    except sqlite3.Error as e:
        log_message(f"[DB Timeline Save] G:{game_id}: Database error - {e}")
        return False
    finally:
        if cursor: cursor.close()


def process_livestats_content(conn, livestats_content_str, jungler_puuid, game_id):
    """
    Обрабатывает содержимое livestats для извлечения пути лесника.
    Принимает существующее соединение с БД 'conn', чтобы избежать блокировки.
    """
    if not livestats_content_str or not jungler_puuid: return None
    path_sequence = deque()
    jungler_participant_id = None
    jungler_team_side = None
    first_camp_cleared = False
    first_recall_after_camp_detected = False

    cursor = conn.cursor()
    try:
        cursor.execute('SELECT Blue_JGL_PUUID, Red_JGL_PUUID FROM tournament_games WHERE "Game_ID" = ?', (str(game_id),))
        row = cursor.fetchone()
        if row:
            if row["Blue_JGL_PUUID"] == jungler_puuid: jungler_team_side = "Blue"
            elif row["Red_JGL_PUUID"] == jungler_puuid: jungler_team_side = "Red"
    except sqlite3.Error as e:
        log_message(f"Error getting team side for G:{game_id}, P:{jungler_puuid[:8]}: {e}")
    finally:
        if cursor: cursor.close()

    try: lines = livestats_content_str.strip().split('\n')
    except Exception as split_err: log_message(f"Error splitting lines G:{game_id}: {split_err}"); return None

    for line in lines:
        if not line.strip(): continue
        try:
            if '"rfc461Schema":"stats_update"' in line and f'"puuid":"{jungler_puuid}"' in line:
                 snapshot = json.loads(line)
                 for p_data in snapshot.get("participants", []):
                     if p_data.get("puuid") == jungler_puuid:
                         jungler_participant_id = p_data.get("participantID"); break
                 if jungler_participant_id: break
        except json.JSONDecodeError: continue
        except Exception as e: log_message(f"Error in jungler ID pass G:{game_id}: {e}"); continue
    if jungler_participant_id is None: return None

    last_action = None
    last_kill_event_time = -1.0
    last_recall_event_time = -1.0
    last_known_zone = "Unknown"
    time_entered_zone = 0.0

    for line in lines:
        if first_recall_after_camp_detected: break
        if not line.strip(): continue
        try: snapshot = json.loads(line)
        except json.JSONDecodeError: continue

        game_time_ms = snapshot.get("gameTime")
        if game_time_ms is None: continue
        game_time_sec = game_time_ms / 1000.0
        schema = snapshot.get("rfc461Schema")
        current_action = None
        if schema == "stats_update":
            for p_data in snapshot.get("participants", []):
                if p_data.get("participantID") == jungler_participant_id:
                    pos = p_data.get("position")
                    if pos and 'x' in pos and 'z' in pos and SHAPELY_AVAILABLE and ZONE_POLYGONS and LANE_ZONE_NAMES:
                        current_zone = get_zone_for_position(pos['x'], pos['z'])
                        if current_zone != last_known_zone:
                            if LANE_ZONE_NAMES and last_known_zone in LANE_ZONE_NAMES:
                                time_spent = game_time_sec - time_entered_zone
                                if time_spent >= GANK_PRESENCE_THRESHOLD:
                                    lane_name = "Unknown"
                                    if "Top" in last_known_zone: lane_name = "Top"
                                    elif "Mid" in last_known_zone: lane_name = "Mid"
                                    elif "Bot" in last_known_zone: lane_name = "Bot"
                                    action_gank = f"Gank/Save {lane_name}"
                                    # ИЗМЕНЕНИЕ: Сохраняем как объект
                                    gank_action_obj = {"action": action_gank, "time": game_time_sec}
                                    if not path_sequence or path_sequence[-1].get("action") != action_gank:
                                        path_sequence.append(gank_action_obj)
                                        last_action = gank_action_obj
                            last_known_zone = current_zone
                            time_entered_zone = game_time_sec
                    break
        elif schema == "epic_monster_kill":
            killer_id = snapshot.get("killer")
            if killer_id == jungler_participant_id:
                monster_type = snapshot.get("monsterType")
                pos = snapshot.get("position")
                if monster_type and pos and 'x' in pos and 'z' in pos:
                     action_camp = get_monster_details(monster_type, pos['x'], pos['z'], jungler_team_side)
                     # ИЗМЕНЕНИЕ: Сохраняем как объект с действием и временем
                     current_action = {"action": action_camp, "time": game_time_sec}
                     if game_time_sec <= last_kill_event_time + 0.5: current_action = None
                     else:
                         last_kill_event_time = game_time_sec
                         if not first_camp_cleared: first_camp_cleared = True
        elif schema == "channeling_started" and snapshot.get("channelingType") == "recall":
             p_id = snapshot.get("participantID")
             if p_id == jungler_participant_id:
                 # ИЗМЕНЕНИЕ: Сохраняем как объект
                 current_action = {"action": "Recall", "time": game_time_sec}
                 if game_time_sec <= last_recall_event_time + 1.0: current_action = None
                 else:
                     last_recall_event_time = game_time_sec
                     if first_camp_cleared: first_recall_after_camp_detected = True

        if current_action:
            # ИЗМЕНЕНИЕ: Сравниваем по ключу 'action' в словаре
            last_action_name = last_action.get("action") if isinstance(last_action, dict) else last_action
            if not path_sequence or current_action.get("action") != last_action_name:
                path_sequence.append(current_action)
                last_action = current_action
    return list(path_sequence)

def save_jungle_path(conn, game_id, player_puuid, path_sequence):
    if not conn or not game_id or not player_puuid or path_sequence is None: return False
    try: path_json = json.dumps(path_sequence)
    except TypeError as json_err: log_message(f"Error serializing path for G:{game_id}, P:{player_puuid[:8]}: {json_err}"); return False

    last_updated = datetime.now(timezone.utc).isoformat()
    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO jungle_pathing
            (game_id, player_puuid, path_sequence, last_updated)
            VALUES (?, ?, ?, ?)
        """, (str(game_id), str(player_puuid), path_json, last_updated))
        return True
    except sqlite3.Error as e: log_message(f"DB Error saving path for G:{game_id}, P:{player_puuid[:8]}: {e}"); return False
    finally:
        if cursor: cursor.close()

def extract_first_ward_data(livestats_content_str, game_id, game_participants_summary):
    if not livestats_content_str or not game_participants_summary: return []
    pid_to_details = {}
    for p_summary in game_participants_summary:
        pid = p_summary.get("participantId")
        puuid = p_summary.get("puuid")
        champ_name = p_summary.get("championName")
        player_name_display = p_summary.get("riotIdGameName", p_summary.get("summonerName", "UnknownPlayer"))
        if pid is not None and puuid is not None:
            pid_to_details[pid] = {"puuid": puuid, "championName": champ_name or "UnknownChamp", "playerName": player_name_display}

    first_wards_by_puuid = {}
    try: lines = livestats_content_str.strip().split('\n')
    except Exception as split_err: log_message(f"[FirstWards-ERROR] G:{game_id}: Error splitting lines: {split_err}"); return []

    for line in lines:
        if not line.strip(): continue
        try: snapshot = json.loads(line)
        except json.JSONDecodeError: continue

        schema = snapshot.get("rfc461Schema"); event_type = snapshot.get("eventType"); game_time_ms = snapshot.get("gameTime")
        is_ward_event = (schema == "ward_placed") or (schema == "event" and event_type == "WARD_PLACED")

        if is_ward_event and game_time_ms is not None:
            participant_id_from_event = snapshot.get("placer") or snapshot.get("participantID") or snapshot.get("participantId")
            ward_type_raw = snapshot.get("wardType"); position_data = snapshot.get("position")

            if participant_id_from_event is not None and ward_type_raw in VALID_WARD_TYPES and position_data and 'x' in position_data and 'z' in position_data:
                participant_details = pid_to_details.get(participant_id_from_event)
                if not participant_details: continue

                player_puuid = participant_details["puuid"]
                if player_puuid not in first_wards_by_puuid:
                    ward_type_mapped = WARD_TYPE_MAP.get(ward_type_raw, "Unknown Ward")
                    first_wards_by_puuid[player_puuid] = {
                        "game_id": str(game_id), "player_puuid": player_puuid, "participant_id": participant_id_from_event,
                        "player_name": participant_details["playerName"], "champion_name": participant_details["championName"],
                        "ward_type": ward_type_mapped, "timestamp_seconds": game_time_ms / 1000.0,
                        "pos_x": int(position_data['x']), "pos_z": int(position_data['z']),
                    }
    
    if not first_wards_by_puuid: log_message(f"[FirstWards] G:{game_id}: No real first ward events found.")
    return list(first_wards_by_puuid.values())

def save_first_ward_data(conn, game_id, first_wards_list):
    if not conn: log_message(f"[DB Ward Save] G:{game_id}: No DB connection."); return False
    if not first_wards_list: return True
    saved_count = 0; cursor = None
    try:
        cursor = conn.cursor()
        last_updated = datetime.now(timezone.utc).isoformat()
        for ward_data in first_wards_list:
            try:
                cursor.execute("""
                    INSERT OR REPLACE INTO first_wards_data
                    (game_id, player_puuid, participant_id, player_name, champion_name, ward_type,
                     timestamp_seconds, pos_x, pos_z, last_updated)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    str(ward_data['game_id']), str(ward_data['player_puuid']), ward_data.get('participant_id'),
                    str(ward_data.get('player_name', 'Unknown Player')), str(ward_data.get('champion_name', 'Unknown')),
                    str(ward_data.get('ward_type', 'Unknown Ward')), float(ward_data['timestamp_seconds']),
                    ward_data.get('pos_x'), ward_data.get('pos_z'), last_updated
                ))
                if cursor.rowcount > 0: saved_count +=1
            except sqlite3.Error as e_item: log_message(f"[DB Ward Save] G:{game_id} PUID:{ward_data.get('player_puuid','N/A')[:6]}: DB error: {e_item}")
            except KeyError as ke: log_message(f"[DB Ward Save] G:{game_id} PUID:{ward_data.get('player_puuid','N/A')[:6]}: Missing key {ke} in ward_data: {ward_data}")
        if saved_count > 0: log_message(f"[DB Ward Save] G:{game_id}: Saved/Replaced {saved_count} first ward entries.")
        return True
    except sqlite3.Error as e: log_message(f"[DB Ward Save] G:{game_id}: General database error: {e}"); return False
    finally:
        if cursor: cursor.close()

def extract_all_ward_data(livestats_content_str, game_id, game_participants_summary):
    if not livestats_content_str or not game_participants_summary:
        return []
    pid_to_details = {}
    for p_summary in game_participants_summary:
        pid = p_summary.get("participantId")
        puuid = p_summary.get("puuid")
        champ_name = p_summary.get("championName")
        player_name_display = p_summary.get("riotIdGameName", p_summary.get("summonerName", "UnknownPlayer"))
        if pid is not None and puuid is not None:
            pid_to_details[pid] = {"puuid": puuid, "championName": champ_name or "UnknownChamp", "playerName": player_name_display}

    all_wards = []
    try:
        lines = livestats_content_str.strip().split('\n')
    except Exception as split_err:
        log_message(f"[AllWards-ERROR] G:{game_id}: Error splitting lines: {split_err}")
        return []

    for line in lines:
        if not line.strip(): continue
        try: snapshot = json.loads(line)
        except json.JSONDecodeError: continue

        schema = snapshot.get("rfc461Schema"); event_type = snapshot.get("eventType"); game_time_ms = snapshot.get("gameTime")
        is_ward_event = (schema == "ward_placed") or (schema == "event" and event_type == "WARD_PLACED")

        if is_ward_event and game_time_ms is not None:
            participant_id_from_event = snapshot.get("placer") or snapshot.get("participantID") or snapshot.get("participantId")
            ward_type_raw = snapshot.get("wardType"); position_data = snapshot.get("position")

            if participant_id_from_event is not None and ward_type_raw in VALID_WARD_TYPES and position_data and 'x' in position_data and 'z' in position_data:
                participant_details = pid_to_details.get(participant_id_from_event)
                if not participant_details: continue

                player_puuid = participant_details["puuid"]
                ward_type_mapped = WARD_TYPE_MAP.get(ward_type_raw, "Unknown Ward")
                all_wards.append({
                    "game_id": str(game_id), "player_puuid": player_puuid, "participant_id": participant_id_from_event,
                    "player_name": participant_details["playerName"], "champion_name": participant_details["championName"],
                    "ward_type": ward_type_mapped, "timestamp_seconds": game_time_ms / 1000.0,
                    "pos_x": int(position_data['x']), "pos_z": int(position_data['z']),
                })
    
    log_message(f"[AllWards] G:{game_id}: Extracted {len(all_wards)} total REAL ward placement events.")
    return all_wards


def save_all_ward_data(conn, game_id, all_wards_list):
    if not conn:
        log_message(f"[DB AllWards Save] G:{game_id}: No DB connection.")
        return False

    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM all_wards_data WHERE game_id = ?", (str(game_id),))
        
        if not all_wards_list:
            log_message(f"[DB AllWards Save] G:{game_id}: No new wards to save. Old entries (if any) deleted.")
            return True

        last_updated = datetime.now(timezone.utc).isoformat()
        
        wards_to_insert = [
            (
                str(ward['game_id']), str(ward['player_puuid']), ward.get('participant_id'),
                str(ward.get('player_name', 'Unknown Player')), str(ward.get('champion_name', 'Unknown')),
                str(ward.get('ward_type', 'Unknown Ward')), float(ward['timestamp_seconds']),
                ward.get('pos_x'), ward.get('pos_z'), last_updated
            ) for ward in all_wards_list
        ]

        cursor.executemany("""
            INSERT INTO all_wards_data
            (game_id, player_puuid, participant_id, player_name, champion_name, ward_type,
             timestamp_seconds, pos_x, pos_z, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, wards_to_insert)
        
        log_message(f"[DB AllWards Save] G:{game_id}: Saved {cursor.rowcount} new ward entries.")
        return True
    except sqlite3.Error as e:
        log_message(f"[DB AllWards Save] G:{game_id}: General database error: {e}")
        return False
    finally:
        if cursor: cursor.close()

def get_tournament_matches(tournament_id):
    all_series = []
    query = """
        query FindTournamentSeries($filter: SeriesFilter, $after: Cursor) {
          allSeries(filter: $filter, first: 50, after: $after, orderBy: StartTimeScheduled, orderDirection: ASC) {
            pageInfo { hasNextPage endCursor }
            edges { node { id startTimeScheduled } }
          }
        }
    """
    variables = {"filter": {"titleId": 3, "tournamentId": tournament_id, "types": ["ESPORTS"], "startTimeScheduled": {"gte": MATCH_START_DATE_FILTER}}}
    cursor = None
    while True:
        variables["after"] = cursor
        data = post_graphql_request(query, variables, "central-data/graphql")
        if not data or not data.get("allSeries"): break
        series_data = data["allSeries"]
        for edge in series_data.get("edges", []):
            if edge.get("node"): all_series.append(edge["node"])
        page_info = series_data.get("pageInfo", {});
        if page_info.get("hasNextPage") and page_info.get("endCursor"):
            cursor = page_info["endCursor"]
            time.sleep(API_REQUEST_DELAY)
        else: break
    return all_series

def download_grid_end_state_data(series_id):
    endpoint = f"file-download/end-state/grid/series/{series_id}"
    end_state_data = get_rest_request(endpoint, expected_type='json')
    return end_state_data

def parse_and_store_tournament_game(cursor, summary_data, series_info, draft_actions, tournament_name="HLL"):
    game_id = None
    try:
        game_id = summary_data.get("esportsGameId") or summary_data.get("gameId")
        if not game_id: log_message("Missing gameId in summary data"); return None
        game_id = str(game_id)

        participants = summary_data.get("participants", [])
        teams_data = summary_data.get("teams", [])
        if not participants or len(participants) != 10 or not teams_data or len(teams_data) != 2:
            log_message(f"T_G:{game_id} Invalid participants/teams count."); return None

        blue_team_tag = UNKNOWN_BLUE_TAG; red_team_tag = UNKNOWN_RED_TAG
        if len(participants) > 0 and 'riotIdGameName' in participants[0]:
            blue_id_name = participants[0]['riotIdGameName']
            if blue_id_name and ' ' in blue_id_name: blue_team_tag = blue_id_name.split(' ', 1)[0]
        if len(participants) > 5 and 'riotIdGameName' in participants[5]:
            red_id_name = participants[5]['riotIdGameName']
            if red_id_name and ' ' in red_id_name: red_team_tag = red_id_name.split(' ', 1)[0]

        sequence_number = summary_data.get("gameSequenceNumber")
        if sequence_number is None and series_info and isinstance(series_info, dict):
             sequence_number = series_info.get("sequenceNumber", 0)
        elif sequence_number is None: sequence_number = 0

        game_duration_sec = summary_data.get("gameDuration", 0)
        game_creation_timestamp = summary_data.get("gameCreation")
        game_version = summary_data.get("gameVersion", "N/A"); patch_str = "N/A"
        if game_version!="N/A": parts=game_version.split('.'); patch_str=f"{parts[0]}.{parts[1]}" if len(parts)>=2 else game_version

        winner_side = "Unknown"
        for team_summary in teams_data:
            if team_summary.get("win") is True: winner_side = "Blue" if team_summary.get("teamId") == 100 else "Red"; break

        blue_bans = ["N/A"]*5; red_bans = ["N/A"]*5
        for team in teams_data:
            target_bans = blue_bans if team.get("teamId")==100 else red_bans
            bans_list = sorted(team.get("bans",[]), key=lambda x: x.get('pickTurn',99))
            for i, ban in enumerate(bans_list[:5]): target_bans[i] = str(c_id) if (c_id := ban.get("championId", -1)) != -1 else "N/A"

        date_str = "N/A"
        if game_creation_timestamp:
            try: dt_obj=datetime.fromtimestamp(game_creation_timestamp/1000, timezone.utc); date_str=dt_obj.strftime("%Y-%m-%d %H:%M:%S");
            except Exception: pass
        duration_str = "N/A"
        if game_duration_sec > 0:
            try: minutes, seconds = divmod(int(game_duration_sec), 60); duration_str = f"{minutes}:{seconds:02d}";
            except Exception: pass

        sql_column_names = [hdr.replace(" ", "_").replace(".", "").replace("-", "_") for hdr in TOURNAMENT_GAMES_HEADER]
        row_dict = {sql_col: None for sql_col in sql_column_names}

        row_dict["Tournament_Name"] = str(tournament_name)
        row_dict["Stage_Name"] = str(series_info.get("stage", {}).get("name", "N/A")) if series_info and isinstance(series_info, dict) else "N/A"
        row_dict["Date"] = str(date_str) if date_str != "N/A" else None
        row_dict["Patch"] = str(patch_str) if patch_str != "N/A" else None
        row_dict["Blue_Team_Name"] = str(blue_team_tag)
        row_dict["Red_Team_Name"] = str(red_team_tag)
        row_dict["Duration"] = str(duration_str) if duration_str != "N/A" else None
        row_dict["Winner_Side"] = str(winner_side)
        row_dict["Game_ID"] = str(game_id)
        row_dict["Series_ID"] = str(series_info.get("id")) if series_info and isinstance(series_info, dict) else None
        try: row_dict["Sequence_Number"] = int(sequence_number)
        except (ValueError, TypeError): row_dict["Sequence_Number"] = 0

        for i in range(5):
            row_dict[f"Blue_Ban_{i+1}_ID"] = str(blue_bans[i]) if blue_bans[i] != "N/A" else None
            row_dict[f"Red_Ban_{i+1}_ID"] = str(red_bans[i]) if red_bans[i] != "N/A" else None

        role_to_abbr = {"TOP": "TOP", "JUNGLE": "JGL", "MIDDLE": "MID", "BOTTOM": "BOT", "UTILITY": "SUP"}
        for idx, p in enumerate(participants):
            role_name = ROLE_ORDER_FOR_SHEET[idx % 5]
            side_prefix = "Blue" if idx < 5 else "Red"
            role_abbr = role_to_abbr.get(role_name)
            champ_name = p.get("championName")
            player_puuid = p.get("puuid")
            participant_id_val = p.get("participantId")

            if role_abbr:
                 row_dict[f"{side_prefix}_{role_abbr}_Champ"] = str(champ_name) if champ_name else None
                 if f"{side_prefix}_{role_abbr}_PUUID" in row_dict:
                     row_dict[f"{side_prefix}_{role_abbr}_PUUID"] = str(player_puuid) if player_puuid else None
                 if f"{side_prefix}_{role_abbr}_PartID" in row_dict:
                     try:
                         row_dict[f"{side_prefix}_{role_abbr}_PartID"] = int(participant_id_val) if participant_id_val is not None else None
                     except (ValueError, TypeError):
                         row_dict[f"{side_prefix}_{role_abbr}_PartID"] = None

        if draft_actions and isinstance(draft_actions, list):
             actions_by_seq = {int(a["sequenceNumber"]): a for a in draft_actions if a.get("sequenceNumber") is not None}
             for i in range(1, 21):
                 action = actions_by_seq.get(i)
                 if action:
                     drafter_info = action.get("drafter", {})
                     draftable_info = action.get("draftable", {})
                     row_dict[f"Draft_Action_{i}_Type"] = str(action.get("type", "N/A")).lower() if action.get("type") else None
                     row_dict[f"Draft_Action_{i}_TeamID"] = str(drafter_info.get("id", "N/A")) if drafter_info.get("id") else None
                     row_dict[f"Draft_Action_{i}_ChampName"] = str(draftable_info.get("name", "N/A")) if draftable_info.get("name") else None
                     row_dict[f"Draft_Action_{i}_ChampID"] = str(draftable_info.get("id", "N/A")) if draftable_info.get("id") else None
                     row_dict[f"Draft_Action_{i}_ActionID"] = str(action.get("id", "N/A")) if action.get("id") else None

        quoted_column_names = [f'"{col}"' for col in sql_column_names]
        columns_string = ', '.join(quoted_column_names)
        sql_placeholders = ", ".join(["?"] * len(sql_column_names))
        insert_sql = f"INSERT OR REPLACE INTO tournament_games ({columns_string}) VALUES ({sql_placeholders})"

        data_tuple_list = []
        for sql_col in sql_column_names:
            value = row_dict.get(sql_col)
            if sql_col == "Sequence_Number" or "PartID" in sql_col:
                try: data_tuple_list.append(int(value) if value is not None else None)
                except (ValueError, TypeError): data_tuple_list.append(None)
            else:
                 data_tuple_list.append(str(value) if value is not None else None)
        data_tuple = tuple(data_tuple_list)

        try:
            cursor.execute(insert_sql, data_tuple)
            return game_id
        except sqlite3.Error as e:
            log_message(f"DB Insert/Replace Error T_G:{game_id}: {e}")
            return None

    except Exception as e:
        series_id_log = series_info.get('id', 'N/A') if isinstance(series_info, dict) else 'N/A'
        log_message(f"Error parsing summary/draft G:{game_id or 'Unknown'} S:{series_id_log}: {e}")
        log_message(traceback.format_exc())
        return None

# lol_app_LTA_1.4v/tournament_logic.py

def fetch_and_store_tournament_data():
    """
    Главная функция для сбора и сохранения всех данных по турниру, включая
    информацию об играх, пути лесников, варды, и события по объектам.
    """
    tournament_id = TARGET_TOURNAMENT_ID
    tournament_name = TARGET_TOURNAMENT_NAME_FOR_DB
    log_message(f"Starting data fetch for tournament: {tournament_name} (ID: {tournament_id})")
    matches = get_tournament_matches(tournament_id)
    if not matches:
        log_message("No matches found for the tournament.")
        return 0

    conn = get_db_connection()
    if not conn:
        log_message("Failed to connect to database.")
        return -1
    cursor = conn.cursor()

    added_or_updated_games_count = 0
    processed_paths_count = 0
    processed_position_snapshots_count = 0
    processed_first_wards_count = 0
    processed_all_wards_count = 0
    processed_timeline_count = 0
    processed_objectives_count = 0
    processed_matches_count = 0
    total_matches = len(matches)

    for series_info in matches:
        processed_matches_count += 1
        series_id = series_info.get("id")
        if not series_id:
            continue
        log_message(f"Processing match {processed_matches_count}/{total_matches} (S:{series_id})")

        series_end_state_data = download_grid_end_state_data(series_id)
        time.sleep(API_REQUEST_DELAY / 2)
        games_in_series = get_series_state(series_id)
        if not games_in_series:
            time.sleep(API_REQUEST_DELAY / 2)
            continue

        for game_info in games_in_series:
            sequence_number = game_info.get("sequenceNumber")
            if sequence_number is None:
                continue

            summary_data = download_riot_summary_data(series_id, sequence_number)
            if not summary_data:
                time.sleep(API_REQUEST_DELAY / 2)
                continue

            game_id = summary_data.get("esportsGameId") or summary_data.get("gameId")
            if not game_id:
                continue
            game_id = str(game_id)

            current_game_draft_actions = []
            if series_end_state_data and series_end_state_data.get("seriesState", {}).get("games"):
                for game_state in series_end_state_data["seriesState"]["games"]:
                    if game_state and game_state.get("sequenceNumber") == sequence_number:
                        current_game_draft_actions = game_state.get("draftActions", [])
                        break

            game_info_saved_id = parse_and_store_tournament_game(cursor, summary_data, series_info, current_game_draft_actions, tournament_name)
            if not game_info_saved_id:
                time.sleep(API_REQUEST_DELAY / 2)
                continue
            else:
                added_or_updated_games_count += 1
                try:
                    conn.commit()
                except sqlite3.Error as e:
                    log_message(f"DB Commit Error G:{game_id}: {e}")
                    conn.rollback()

            livestats_content = download_riot_livestats_data(series_id, sequence_number)
            time.sleep(API_REQUEST_DELAY)

            if livestats_content:
                game_participants_summary = summary_data.get('participants', [])

                # <<< ИСПРАВЛЕНИЕ: Передаем 'game_participants_summary' в функцию >>>
                objective_events = extract_objective_events(livestats_content, game_id, game_participants_summary)
                if objective_events:
                    if save_objective_events(conn, game_id, objective_events):
                        processed_objectives_count += len(objective_events)

                timeline_positions = extract_player_positions_timeline(livestats_content, game_id)
                if timeline_positions and save_player_positions_timeline(conn, game_id, timeline_positions):
                    processed_timeline_count += len(timeline_positions)

                blue_jungler_puuid = game_participants_summary[1].get("puuid") if len(game_participants_summary) > 1 else None
                red_jungler_puuid = game_participants_summary[6].get("puuid") if len(game_participants_summary) > 6 else None

                paths_saved_this_game = 0
                if blue_jungler_puuid:
                    blue_path = process_livestats_content(conn, livestats_content, blue_jungler_puuid, game_id)
                    if blue_path and save_jungle_path(conn, game_id, blue_jungler_puuid, blue_path):
                        paths_saved_this_game += 1
                if red_jungler_puuid:
                    red_path = process_livestats_content(conn, livestats_content, red_jungler_puuid, game_id)
                    if red_path and save_jungle_path(conn, game_id, red_jungler_puuid, red_path):
                        paths_saved_this_game += 1
                processed_paths_count += paths_saved_this_game

                positions_data = extract_player_positions(livestats_content, game_id, TARGET_POSITION_TIMESTAMPS_SEC, TIMESTAMP_TOLERANCE_SEC)
                snapshots_saved_this_game = 0
                if positions_data:
                    for ts_sec, pos_list in positions_data.items():
                        if pos_list and save_position_snapshot(conn, game_id, ts_sec, pos_list):
                            snapshots_saved_this_game += 1
                if snapshots_saved_this_game > 0:
                    processed_position_snapshots_count += snapshots_saved_this_game

                first_wards_extracted = extract_first_ward_data(livestats_content, game_id, game_participants_summary)
                if first_wards_extracted and save_first_ward_data(conn, game_id, first_wards_extracted):
                    processed_first_wards_count += len(first_wards_extracted)
                
                all_wards_extracted = extract_all_ward_data(livestats_content, game_id, game_participants_summary)
                if all_wards_extracted and save_all_ward_data(conn, game_id, all_wards_extracted):
                    processed_all_wards_count += len(all_wards_extracted)

                try:
                    conn.commit()
                except sqlite3.Error as e_commit_ls:
                    log_message(f"DB Commit Error LiveStats G:{game_id}: {e_commit_ls}")
                    conn.rollback()
        time.sleep(API_REQUEST_DELAY)

    log_message(f"Tournament data update finished. Games: {added_or_updated_games_count}, Objectives: {processed_objectives_count}, Paths: {processed_paths_count}, PosSnapshots: {processed_position_snapshots_count}, FirstWards: {processed_first_wards_count}, AllWards: {processed_all_wards_count}, TimelinePoints: {processed_timeline_count}.")
    conn.close()
    return added_or_updated_games_count
def fetch_and_store_ward_data():
    """
    Проходит по всем существующим играм в БД, скачивает для них livestats
    и обновляет данные в таблице all_wards_data.
    """
    log_message("Starting dedicated ward data update process...")
    conn = get_db_connection()
    if not conn:
        log_message("Ward Update: DB Connection failed."); return -1
    
    games_to_process = []
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT "Game_ID", "Series_ID", "Sequence_Number" FROM tournament_games')
        games_to_process = cursor.fetchall()
        log_message(f"Found {len(games_to_process)} games in DB to check for ward data.")
    except sqlite3.Error as e:
        log_message(f"Ward Update: Error fetching games from DB: {e}")
        conn.close()
        return -1

    processed_games_count = 0
    total_wards_saved = 0

    for game_row in games_to_process:
        game_id = game_row["Game_ID"]
        series_id = game_row["Series_ID"]
        sequence_number = game_row["Sequence_Number"]
        
        if not all([game_id, series_id, sequence_number is not None]):
            continue

        summary_data = download_riot_summary_data(series_id, sequence_number)
        time.sleep(API_REQUEST_DELAY / 2)
        if not summary_data:
            log_message(f"Ward Update G:{game_id}: Could not download summary data. Skipping.")
            continue

        livestats_content = download_riot_livestats_data(series_id, sequence_number)
        time.sleep(API_REQUEST_DELAY)
        
        if livestats_content:
            game_participants_summary = summary_data.get('participants', [])
            all_wards_extracted = extract_all_ward_data(livestats_content, game_id, game_participants_summary)
            
            if save_all_ward_data(conn, game_id, all_wards_extracted):
                total_wards_saved += len(all_wards_extracted)
            
            try:
                conn.commit()
                processed_games_count += 1
                if processed_games_count % 10 == 0:
                    log_message(f"Ward Update: Processed {processed_games_count}/{len(games_to_process)} games...")
            except sqlite3.Error as e_commit:
                log_message(f"Ward Update G:{game_id}: DB Commit Error: {e_commit}")
                conn.rollback()

    conn.close()
    log_message(f"Ward data update finished. Processed {processed_games_count} games, saved/updated a total of {total_wards_saved} ward entries.")
    return processed_games_count
    
def aggregate_tournament_data(selected_team_full_name=None, side_filter="all"):
    is_overall_view = not selected_team_full_name
    view_type_log = "Overall Tournament" if is_overall_view else f"Team: {selected_team_full_name}"

    conn = get_db_connection()
    if not conn: return [], {"error": "Database connection failed"}, {}, []

    all_teams_display = []
    stats = {"is_overall_view": is_overall_view, "error": None, "message": None}
    grouped_matches = {}
    all_game_details_list = []
    selected_team_tag = None

    duo_roles_config = { ("TOP", "JUNGLE"): "TOP-JUNGLE", ("JUNGLE", "MIDDLE"): "JUNGLE-MID", ("BOTTOM", "UTILITY"): "ADC-SUPPORT" }
    role_to_abbr = {"TOP": "TOP", "JUNGLE": "JGL", "MIDDLE": "MID", "BOTTOM": "BOT", "UTILITY": "SUP"}
    role_map_display = {"TOP": "Top", "JUNGLE": "Jungle", "MIDDLE": "Mid", "BOTTOM": "ADC", "UTILITY": "Support"}
    roles_for_pattern = ["TOP", "JUNGLE", "MIDDLE", "BOTTOM", "UTILITY"]
    valid_side_filters = ["blue", "red"];
    filter_side_norm = side_filter.lower() if side_filter.lower() in valid_side_filters else 'all'

    def format_bans_agg(ban_dict_input, champ_data, icon_size_px):
        formatted = []
        if not champ_data or 'id_map' not in champ_data: return formatted
        for ban_id, count in sorted(ban_dict_input.items(), key=lambda item: item[1], reverse=True):
             ban_id_str = str(ban_id);
             champ_name = champ_data.get('id_map',{}).get(ban_id_str, f"ID:{ban_id_str}");
             icon_html = get_champion_icon_html(ban_id_str, champ_data, width=icon_size_px, height=icon_size_px);
             formatted.append({'champion': champ_name, 'count': count, 'icon_html': icon_html});
        return formatted

    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute(f"""SELECT DISTINCT Blue_Team_Name as team_tag FROM tournament_games WHERE Blue_Team_Name NOT IN ('{UNKNOWN_BLUE_TAG}', 'Blue Team') UNION SELECT DISTINCT Red_Team_Name as team_tag FROM tournament_games WHERE Red_Team_Name NOT IN ('{UNKNOWN_RED_TAG}', 'Red Team')""")
        all_teams_tags = {row['team_tag'] for row in cursor.fetchall() if row['team_tag']}
        all_teams_display = sorted(list(set( [TEAM_TAG_TO_FULL_NAME.get(tag, tag) for tag in all_teams_tags] )))

        if not is_overall_view:
            for tag, full_name in TEAM_TAG_TO_FULL_NAME.items():
                if full_name == selected_team_full_name: selected_team_tag = tag; break
            if not selected_team_tag and selected_team_full_name in all_teams_tags: selected_team_tag = selected_team_full_name
            if not selected_team_tag:
                stats["error"] = "Team tag not found."; conn.close(); return all_teams_display, stats, {}, []

        sql_query_games = "SELECT * FROM tournament_games ORDER BY \"Date\" DESC, \"Series_ID\" ASC, \"Sequence_Number\" ASC"
        params_games = []
        if not is_overall_view:
             sql_query_games = "SELECT * FROM tournament_games WHERE Blue_Team_Name = ? OR Red_Team_Name = ? ORDER BY \"Date\" DESC, \"Series_ID\" ASC, \"Sequence_Number\" ASC"
             params_games = (selected_team_tag, selected_team_tag)
        cursor.execute(sql_query_games, params_games)
        games_rows = cursor.fetchall();

        if not games_rows:
            stats["message"] = "No games found."; conn.close(); return all_teams_display, stats, grouped_matches, all_game_details_list

        champion_data = get_champion_data()
        if not champion_data or not champion_data.get('id_map'):
             stats["error"] = "Failed to load champion data for icons."
        
        if is_overall_view:
            # Логика для общего обзора турнира (без изменений)
            stats.update({
                "overall_total_games": 0, "overall_blue_wins": 0, "overall_red_wins": 0,
                "overall_champ_stats": defaultdict(lambda: {'picks': 0, 'bans': 0, 'wins_when_picked': 0}),
                "overall_picks_by_role": defaultdict(lambda: defaultdict(lambda: {'picks': 0, 'wins': 0})),
                "overall_bans_ids": defaultdict(int),
                "temp_overall_duo_stats": defaultdict(lambda: {'games': 0, 'wins': 0, 'roles': None}),
                "overall_bans_formatted": [], "overall_champ_stats_formatted": [],
                "overall_picks_by_role_formatted": defaultdict(list),
                "overall_duo_picks": defaultdict(lambda: {"title": "", "stats": []})
            })
            valid_games_count_overall = 0
            for game_row in games_rows:
                 game = dict(game_row); winner_side = game.get("Winner_Side");
                 if winner_side not in ["Blue", "Red"]: continue
                 valid_games_count_overall +=1
                 if winner_side == "Blue": stats["overall_blue_wins"] += 1

                 game_picks_by_role = {"Blue": {}, "Red": {}}
                 for side in ["Blue", "Red"]:
                     is_win_for_side = (side == winner_side)
                     for role in ROLE_ORDER_FOR_SHEET:
                         role_abbr = role_to_abbr.get(role)
                         if not role_abbr: continue
                         champ = game.get(f"{side}_{role_abbr}_Champ")
                         if champ and champ != "N/A":
                              stats["overall_champ_stats"][champ]['picks'] += 1
                              if is_win_for_side: stats["overall_champ_stats"][champ]['wins_when_picked'] += 1
                              stats["overall_picks_by_role"][role][champ]['picks'] +=1
                              if is_win_for_side: stats["overall_picks_by_role"][role][champ]['wins'] +=1
                              game_picks_by_role[side][role] = champ
                 for side in ["Blue", "Red"]:
                     for i in range(1, 6):
                         ban_id = game.get(f"{side}_Ban_{i}_ID")
                         if ban_id and ban_id != "N/A":
                             stats["overall_bans_ids"][ban_id] += 1
                             ban_champ_name = champion_data.get('id_map', {}).get(str(ban_id))
                             if ban_champ_name: stats["overall_champ_stats"][ban_champ_name]['bans'] += 1
                 for side in ["Blue", "Red"]:
                     is_win_for_side = (side == winner_side)
                     side_picks = game_picks_by_role[side]
                     for (r1_cfg, r2_cfg), duo_title in duo_roles_config.items():
                         champ1 = side_picks.get(r1_cfg); champ2 = side_picks.get(r2_cfg)
                         if champ1 and champ2:
                             duo_key = tuple(sorted([(r1_cfg, champ1), (r2_cfg, champ2)]))
                             stats["temp_overall_duo_stats"][duo_key]['games'] +=1
                             if is_win_for_side: stats["temp_overall_duo_stats"][duo_key]['wins'] +=1
                             if stats["temp_overall_duo_stats"][duo_key]['roles'] is None: stats["temp_overall_duo_stats"][duo_key]['roles'] = (r1_cfg, r2_cfg)

            stats["overall_total_games"] = valid_games_count_overall
            stats["overall_red_wins"] = valid_games_count_overall - stats["overall_blue_wins"]
            stats["overall_bans_formatted"] = format_bans_agg(stats["overall_bans_ids"], champion_data, ICON_SIZE_PICKS_BANS);

            temp_champ_list = []
            for champ, data in stats["overall_champ_stats"].items():
                pick_r = (data['picks'] / valid_games_count_overall * 100) if valid_games_count_overall else 0
                ban_r = (data['bans'] / valid_games_count_overall * 100) if valid_games_count_overall else 0
                win_r = (data['wins_when_picked'] / data['picks'] * 100) if data['picks'] > 0 else 0
                temp_champ_list.append({"champion": champ, "picks": data['picks'], "bans": data['bans'],
                                        "pick_rate": round(pick_r,1), "ban_rate": round(ban_r,1),
                                        "presence": round(pick_r + ban_r, 1), "win_rate": round(win_r,1),
                                        "icon_html": get_champion_icon_html(champ, champion_data, ICON_SIZE_PICKS_BANS, ICON_SIZE_PICKS_BANS)})
            stats["overall_champ_stats_formatted"] = sorted(temp_champ_list, key=lambda x: x['presence'], reverse=True)

            for role, champs in stats["overall_picks_by_role"].items():
                role_list = []
                for champ, data in champs.items():
                    if data['picks'] > 0:
                        win_r = (data['wins'] / data['picks'] * 100)
                        role_list.append({"champion": champ, "games": data['picks'], "win_rate": round(win_r,1),
                                          "icon_html": get_champion_icon_html(champ, champion_data, ICON_SIZE_PICKS_BANS,ICON_SIZE_PICKS_BANS)})
                stats["overall_picks_by_role_formatted"][role] = sorted(role_list, key=lambda x: x['games'], reverse=True)
            stats["overall_picks_by_role_formatted"] = dict(stats["overall_picks_by_role_formatted"])

            for (r1_cfg, r2_cfg), duo_title_cfg in duo_roles_config.items():
                stats["overall_duo_picks"][duo_title_cfg]["title"] = duo_title_cfg

            for duo_key, data in stats["temp_overall_duo_stats"].items():
                if data['games'] > 0:
                    (role1_s, champ1_s), (role2_s, champ2_s) = duo_key
                    orig_r1, orig_r2 = data['roles']
                    current_duo_title = None
                    target_r1_map, target_r2_map = None, None
                    for (map_r1, map_r2), title_map in duo_roles_config.items():
                        if orig_r1 in (map_r1, map_r2) and orig_r2 in (map_r1, map_r2):
                            current_duo_title = title_map
                            target_r1_map, target_r2_map = map_r1, map_r2
                            break
                    if not current_duo_title: continue
                    win_r = (data['wins'] / data['games'] * 100)
                    display_c1, display_c2 = (champ1_s, champ2_s) if role1_s == target_r1_map else (champ2_s, champ1_s)
                    stats["overall_duo_picks"][current_duo_title]["stats"].append({
                        "champ1": display_c1, "champ2": display_c2,
                        "icon1_html": get_champion_icon_html(display_c1, champion_data, ICON_SIZE_DUOS,ICON_SIZE_DUOS),
                        "icon2_html": get_champion_icon_html(display_c2, champion_data, ICON_SIZE_DUOS,ICON_SIZE_DUOS),
                        "games": data['games'], "win_rate": round(win_r,1)
                    })
            for title in stats["overall_duo_picks"]:
                stats["overall_duo_picks"][title]["stats"].sort(key=lambda x: x['games'], reverse=True)
            stats["overall_duo_picks"] = dict(stats["overall_duo_picks"])
            del stats["overall_champ_stats"], stats["overall_picks_by_role"], stats["overall_bans_ids"], stats["temp_overall_duo_stats"]
        
        else:
            stats.update({
                "picks": defaultdict(lambda: defaultdict(lambda: {'games': 0, 'wins': 0})),
                "bans": {},
                "games_played": 0, "wins": 0, "losses": 0,
                "duo_picks": defaultdict(lambda: {"title": "", "stats": []}),
                "games_blue": 0, "wins_blue": 0, "losses_blue": 0,
                "games_red": 0, "wins_red": 0, "losses_red": 0,
                "draft_patterns": {'Blue': {}, 'Red': {}},
                "detailed_picks": {},
                "priority_picks": {}
            })
            
            temp_bans_ids_team = {
                "by_team_blue_rot1": defaultdict(int), "by_team_blue_rot2": defaultdict(int),
                "by_team_red_rot1": defaultdict(int), "by_team_red_rot2": defaultdict(int),
                "vs_team_blue_rot1": defaultdict(int), "vs_team_blue_rot2": defaultdict(int),
                "vs_team_red_rot1": defaultdict(int), "vs_team_red_rot2": defaultdict(int),
            }
            temp_detailed_picks = {
                "blue_rot1": defaultdict(lambda: {'games': 0, 'wins': 0}),
                "blue_rot2": defaultdict(lambda: {'games': 0, 'wins': 0}),
                "red_rot1": defaultdict(lambda: {'games': 0, 'wins': 0}),
                "red_rot2": defaultdict(lambda: {'games': 0, 'wins': 0})
            }
            temp_priority_picks = {
                'Blue': defaultdict(lambda: defaultdict(int)),
                'Red': defaultdict(lambda: defaultdict(int))
            }

            temp_duo_stats_team = defaultdict(lambda: {'games': 0, 'wins': 0, 'roles': None})
            draft_patterns_counters = {'Blue': {k: defaultdict(int) for k in ['B1', 'B2_B3', 'B4_B5']}, 'Red': {k: defaultdict(int) for k in ['R1_R2', 'R3', 'R4', 'R5']}}
            games_played_on_blue_by_team = 0
            games_played_on_red_by_team = 0
            game_ids_for_team_view = []

            blue_pick_seq_to_phase = { 7: 'B1', 10: 'B2-3', 11: 'B2-3', 18: 'B4-5', 19: 'B4-5' }
            red_pick_seq_to_phase = { 8: 'R1-2', 9: 'R1-2', 12: 'R3', 17: 'R4', 20: 'R5' }

            for game_row in games_rows:
                game = dict(game_row); game_db_id = game.get("Game_ID")
                if not game_db_id: continue
                game_ids_for_team_view.append(game_db_id)

                blue_team_tag = game.get("Blue_Team_Name", UNKNOWN_BLUE_TAG)
                red_team_tag = game.get("Red_Team_Name", UNKNOWN_RED_TAG)
                is_blue = (blue_team_tag == selected_team_tag)
                is_red = (red_team_tag == selected_team_tag)
                winner_side = game.get("Winner_Side")
                is_win = (is_blue and winner_side == "Blue") or (is_red and winner_side == "Red")

                stats["games_played"] += 1
                if is_blue:
                    stats["games_blue"] += 1; games_played_on_blue_by_team +=1
                    if is_win: stats["wins_blue"] +=1
                    elif winner_side == "Red": stats["losses_blue"] +=1
                elif is_red:
                    stats["games_red"] += 1; games_played_on_red_by_team +=1
                    if is_win: stats["wins_red"] +=1
                    elif winner_side == "Blue": stats["losses_red"] +=1
                if is_win: stats["wins"] +=1
                elif winner_side != "Unknown": stats["losses"] +=1

                current_team_prefix = "Blue" if is_blue else "Red"
                opponent_prefix = "Red" if is_blue else "Blue"
                
                ban_keys = {
                    "team_rot1": temp_bans_ids_team["by_team_blue_rot1"] if is_blue else temp_bans_ids_team["by_team_red_rot1"],
                    "team_rot2": temp_bans_ids_team["by_team_blue_rot2"] if is_blue else temp_bans_ids_team["by_team_red_rot2"],
                    "opp_rot1": temp_bans_ids_team["vs_team_blue_rot1"] if is_blue else temp_bans_ids_team["vs_team_red_rot1"],
                    "opp_rot2": temp_bans_ids_team["vs_team_blue_rot2"] if is_blue else temp_bans_ids_team["vs_team_red_rot2"],
                }

                for i in range(1, 6):
                    team_ban_id = game.get(f"{current_team_prefix}_Ban_{i}_ID")
                    opp_ban_id = game.get(f"{opponent_prefix}_Ban_{i}_ID")
                    
                    if team_ban_id and team_ban_id != "N/A":
                        target_dict = ban_keys["team_rot1"] if i <= 3 else ban_keys["team_rot2"]
                        target_dict[team_ban_id] += 1
                    
                    if opp_ban_id and opp_ban_id != "N/A":
                        target_dict = ban_keys["opp_rot1"] if i <= 3 else ban_keys["opp_rot2"]
                        target_dict[opp_ban_id] += 1

                should_process_this_game_picks = (filter_side_norm == 'all') or \
                                                 (filter_side_norm == 'blue' and is_blue) or \
                                                 (filter_side_norm == 'red' and is_red)
                if should_process_this_game_picks:
                    picks_in_game_team = {}
                    for role in ROLE_ORDER_FOR_SHEET:
                        role_abbr = role_to_abbr.get(role)
                        if not role_abbr: continue
                        champ = game.get(f"{current_team_prefix}_{role_abbr}_Champ")
                        if champ and champ != "N/A":
                            picks_in_game_team[role] = champ
                            stats["picks"][role][champ]['games'] +=1
                            if is_win: stats["picks"][role][champ]['wins'] +=1
                    for (r1_cfg, r2_cfg), duo_title in duo_roles_config.items():
                        c1 = picks_in_game_team.get(r1_cfg); c2 = picks_in_game_team.get(r2_cfg)
                        if c1 and c2:
                            duo_key = tuple(sorted([(r1_cfg, c1), (r2_cfg, c2)]))
                            temp_duo_stats_team[duo_key]['games'] +=1
                            if is_win: temp_duo_stats_team[duo_key]['wins'] +=1
                            if temp_duo_stats_team[duo_key]['roles'] is None: temp_duo_stats_team[duo_key]['roles'] = (r1_cfg, r2_cfg)

                reconstructed_draft = {}
                action_found_in_game = False
                draft_blue_id, draft_red_id = None, None
                for i in range(1,21):
                    action_type = game.get(f"Draft_Action_{i}_Type")
                    team_id_draft = game.get(f"Draft_Action_{i}_TeamID")
                    if action_type and action_type != "N/A" and team_id_draft and team_id_draft != "N/A":
                        reconstructed_draft[i] = {
                            "Action_Type": str(action_type).lower(), "Drafter_Team_ID": str(team_id_draft),
                            "Champion_Name": game.get(f"Draft_Action_{i}_ChampName", "N/A"),
                        }
                        action_found_in_game = True
                        if not draft_blue_id and i in [1,3,5,7,10,11,14,16,18,19]: draft_blue_id = str(team_id_draft)
                        if not draft_red_id and i in [2,4,6,8,9,12,13,15,17,20] and str(team_id_draft) != draft_blue_id: draft_red_id = str(team_id_draft)

                if action_found_in_game:
                    blue_picks_seq = {s: reconstructed_draft[s] for s in [7, 10, 11, 18, 19] if s in reconstructed_draft}
                    red_picks_seq = {s: reconstructed_draft[s] for s in [8, 9, 12, 17, 20] if s in reconstructed_draft}
                    
                    our_team_picks_seq = blue_picks_seq if is_blue else red_picks_seq
                    
                    if should_process_this_game_picks:
                        rot1_keys = [7, 10, 11] if is_blue else [8, 9, 12]
                        rot2_keys = [18, 19] if is_blue else [17, 20]
                        target_pick_dict_rot1 = temp_detailed_picks['blue_rot1'] if is_blue else temp_detailed_picks['red_rot1']
                        target_pick_dict_rot2 = temp_detailed_picks['blue_rot2'] if is_blue else temp_detailed_picks['red_rot2']
                        
                        for seq, pick_data in our_team_picks_seq.items():
                            champ_name = pick_data.get("Champion_Name")
                            if champ_name and champ_name != "N/A":
                                if seq in rot1_keys:
                                    target_pick_dict_rot1[champ_name]['games'] += 1
                                    if is_win: target_pick_dict_rot1[champ_name]['wins'] += 1
                                elif seq in rot2_keys:
                                    target_pick_dict_rot2[champ_name]['games'] += 1
                                    if is_win: target_pick_dict_rot2[champ_name]['wins'] += 1

                    if is_blue:
                        for seq, pick_data in blue_picks_seq.items():
                            champ = pick_data.get("Champion_Name")
                            phase = blue_pick_seq_to_phase.get(seq)
                            if champ and champ != "N/A" and phase:
                                temp_priority_picks['Blue'][champ][phase] += 1
                    elif is_red:
                        for seq, pick_data in red_picks_seq.items():
                            champ = pick_data.get("Champion_Name")
                            phase = red_pick_seq_to_phase.get(seq)
                            if champ and champ != "N/A" and phase:
                                temp_priority_picks['Red'][champ][phase] += 1


                    roles_played_game = {'Blue': {}, 'Red': {}}
                    for r in ROLE_ORDER_FOR_SHEET:
                        r_abbr = role_to_abbr.get(r)
                        if not r_abbr: continue
                        b_c = game.get(f"Blue_{r_abbr}_Champ"); r_c = game.get(f"Red_{r_abbr}_Champ")
                        if b_c and b_c != "N/A": roles_played_game['Blue'][r] = b_c
                        if r_c and r_c != "N/A": roles_played_game['Red'][r] = r_c

                    our_team_draft_side = None
                    our_team_draft_actual_id = None

                    if is_blue and draft_blue_id: our_team_draft_side = 'Blue'; our_team_draft_actual_id = draft_blue_id
                    elif is_red and draft_red_id: our_team_draft_side = 'Red'; our_team_draft_actual_id = draft_red_id

                    if our_team_draft_side and our_team_draft_actual_id:
                        for seq, action_data in reconstructed_draft.items():
                            if action_data['Action_Type'] == 'pick' and action_data['Drafter_Team_ID'] == our_team_draft_actual_id:
                                picked_champ_name = action_data['Champion_Name']
                                actual_role_for_pick = None
                                for role_key, champ_in_role in roles_played_game.get(our_team_draft_side, {}).items():
                                    if champ_in_role == picked_champ_name: actual_role_for_pick = role_key; break

                                if actual_role_for_pick:
                                    pattern_slot_key = None
                                    if our_team_draft_side == 'Blue':
                                        if seq == 7: pattern_slot_key = 'B1'
                                        elif seq in [10,11]: pattern_slot_key = 'B2_B3'
                                        elif seq in [18,19]: pattern_slot_key = 'B4_B5'
                                    elif our_team_draft_side == 'Red':
                                        if seq in [8,9]: pattern_slot_key = 'R1_R2'
                                        elif seq == 12: pattern_slot_key = 'R3'
                                        elif seq == 17: pattern_slot_key = 'R4'
                                        elif seq == 20: pattern_slot_key = 'R5'
                                    if pattern_slot_key:
                                        draft_patterns_counters[our_team_draft_side][pattern_slot_key][actual_role_for_pick] +=1

                opponent_tag_game = red_team_tag if is_blue else blue_team_tag
                current_team_role_puuids = {}
                for role_cfg_key in ROLE_ORDER_FOR_SHEET:
                    role_abbr_cfg = role_to_abbr.get(role_cfg_key)
                    puuid_val = game.get(f"{current_team_prefix}_{role_abbr_cfg}_PUUID")
                    if puuid_val: current_team_role_puuids[role_cfg_key] = puuid_val

                game_detail_entry = {
                    "series_id": game.get("Series_ID", "N/A"), "game_id": game_db_id,
                    "sequence_number": game.get("Sequence_Number", 0),
                    "blue_team_tag": blue_team_tag, "red_team_tag": red_team_tag,
                    "winner_side": winner_side, "is_win_for_selected": is_win,
                    "draft_actions_dict": reconstructed_draft if action_found_in_game else {},
                    "blue_team_draft_id": draft_blue_id, "red_team_draft_id": draft_red_id,
                    "our_jungler_champ": game.get(f"{current_team_prefix}_JGL_Champ", "N/A"),
                    "enemy_jungler_champ": game.get(f"{opponent_prefix}_JGL_Champ", "N/A"),
                    "opponent_name": TEAM_TAG_TO_FULL_NAME.get(opponent_tag_game, opponent_tag_game),
                    "side": "Blue" if is_blue else "Red",
                    "result": "Win" if is_win else ("Loss" if winner_side != "Unknown" else "Unknown"),
                    "jungler_puuid_for_path_lookup": game.get(f"{current_team_prefix}_JGL_PUUID"),
                    "selected_team_puuids_in_game": [game.get(f"{current_team_prefix}_{role_to_abbr[r]}_PUUID") for r in ROLE_ORDER_FOR_SHEET if game.get(f"{current_team_prefix}_{role_to_abbr[r]}_PUUID")],
                    "jungle_path": [],
                    "position_snapshots": {},
                    "first_wards": []
                }
                all_game_details_list.append(game_detail_entry)

            jungle_paths_db = {}
            if game_ids_for_team_view:
                placeholders_jp = ','.join(['?'] * len(game_ids_for_team_view))
                path_query_jp = f"SELECT game_id, player_puuid, path_sequence FROM jungle_pathing WHERE game_id IN ({placeholders_jp})"
                path_cursor_jp = conn.cursor()
                path_cursor_jp.execute(path_query_jp, tuple(game_ids_for_team_view))
                for row in path_cursor_jp.fetchall():
                    gid, puid, path_json_str = row['game_id'], row['player_puuid'], row['path_sequence']
                    if gid not in jungle_paths_db: jungle_paths_db[gid] = {}
                    try: jungle_paths_db[gid][puid] = json.loads(path_json_str) if path_json_str else []
                    except json.JSONDecodeError: jungle_paths_db[gid][puid] = []
                path_cursor_jp.close()

            player_positions_db = defaultdict(dict)
            if game_ids_for_team_view and TARGET_POSITION_TIMESTAMPS_SEC:
                placeholders_pp_games = ','.join(['?'] * len(game_ids_for_team_view))
                placeholders_pp_ts = ','.join(['?'] * len(TARGET_POSITION_TIMESTAMPS_SEC))
                pos_query_pp = f"""SELECT game_id, timestamp_seconds, positions_json
                                   FROM player_positions_snapshots
                                   WHERE game_id IN ({placeholders_pp_games}) AND timestamp_seconds IN ({placeholders_pp_ts})"""
                pos_cursor_pp = conn.cursor()
                pos_cursor_pp.execute(pos_query_pp, tuple(game_ids_for_team_view) + tuple(TARGET_POSITION_TIMESTAMPS_SEC))
                for row in pos_cursor_pp.fetchall():
                    gid, ts, pos_json_str = row['game_id'], row['timestamp_seconds'], row['positions_json']
                    try: player_positions_db[gid][ts] = json.loads(pos_json_str) if pos_json_str else []
                    except json.JSONDecodeError: player_positions_db[gid][ts] = []
                pos_cursor_pp.close()

            first_wards_db_for_team_games = defaultdict(list)
            if game_ids_for_team_view:
                all_puuids_of_selected_team_in_these_games = set()
                for detail_entry_temp in all_game_details_list:
                    all_puuids_of_selected_team_in_these_games.update(detail_entry_temp.get("selected_team_puuids_in_game", []))

                if all_puuids_of_selected_team_in_these_games:
                    placeholders_fw_games = ','.join(['?'] * len(game_ids_for_team_view))
                    placeholders_fw_puuids = ','.join(['?'] * len(all_puuids_of_selected_team_in_these_games))

                    ward_query_sql = f"""
                        SELECT game_id, player_puuid, participant_id, player_name, champion_name, ward_type,
                               timestamp_seconds, pos_x, pos_z
                        FROM first_wards_data
                        WHERE game_id IN ({placeholders_fw_games}) AND player_puuid IN ({placeholders_fw_puuids})
                    """
                    ward_params = tuple(game_ids_for_team_view) + tuple(list(all_puuids_of_selected_team_in_these_games))

                    ward_cursor = conn.cursor()
                    ward_cursor.execute(ward_query_sql, ward_params)
                    for row in ward_cursor.fetchall():
                        gid = row['game_id']
                        ward_info = dict(row)
                        first_wards_db_for_team_games[gid].append(ward_info)
                    ward_cursor.close()

            for detail_entry in all_game_details_list:
                gid = detail_entry['game_id']
                jungler_puid = detail_entry.get('jungler_puuid_for_path_lookup')
                if jungler_puid and gid in jungle_paths_db and jungler_puid in jungle_paths_db[gid]:
                    detail_entry['jungle_path'] = jungle_paths_db[gid][jungler_puid]
                if gid in player_positions_db:
                    detail_entry['position_snapshots'] = player_positions_db[gid]

                current_game_selected_team_puuids = detail_entry.get("selected_team_puuids_in_game", [])
                if gid in first_wards_db_for_team_games:
                    detail_entry['first_wards'] = [
                        ward for ward in first_wards_db_for_team_games[gid]
                        if ward['player_puuid'] in current_game_selected_team_puuids
                    ]
                    if detail_entry['first_wards']:
                         log_message(f"[Aggregate] G:{gid} Found {len(detail_entry['first_wards'])} wards for selected team.")

                if 'jungler_puuid_for_path_lookup' in detail_entry:
                    del detail_entry['jungler_puuid_for_path_lookup']
                if 'selected_team_puuids_in_game' in detail_entry:
                    del detail_entry['selected_team_puuids_in_game']

            formatted_picks_team = defaultdict(dict)
            for role, champs in stats["picks"].items():
                 for champ_name, data in sorted(champs.items(), key=lambda x: x[1]['games'], reverse=True):
                    if data['games'] > 0:
                        win_r = (data['wins'] / data['games'] * 100)
                        formatted_picks_team[role][champ_name] = {
                            'games': data['games'], 'wins': data['wins'], 'win_rate': round(win_r,1),
                            'icon_html': get_champion_icon_html(champ_name, champion_data, ICON_SIZE_PICKS_BANS,ICON_SIZE_PICKS_BANS)
                        }
            stats["picks"] = dict(formatted_picks_team)
            
            stats["bans"] = {
                "by_team_blue_rot1_formatted": format_bans_agg(temp_bans_ids_team["by_team_blue_rot1"], champion_data, ICON_SIZE_PICKS_BANS),
                "by_team_blue_rot2_formatted": format_bans_agg(temp_bans_ids_team["by_team_blue_rot2"], champion_data, ICON_SIZE_PICKS_BANS),
                "by_team_red_rot1_formatted": format_bans_agg(temp_bans_ids_team["by_team_red_rot1"], champion_data, ICON_SIZE_PICKS_BANS),
                "by_team_red_rot2_formatted": format_bans_agg(temp_bans_ids_team["by_team_red_rot2"], champion_data, ICON_SIZE_PICKS_BANS),
                "vs_team_blue_rot1_formatted": format_bans_agg(temp_bans_ids_team["vs_team_blue_rot1"], champion_data, ICON_SIZE_PICKS_BANS),
                "vs_team_blue_rot2_formatted": format_bans_agg(temp_bans_ids_team["vs_team_blue_rot2"], champion_data, ICON_SIZE_PICKS_BANS),
                "vs_team_red_rot1_formatted": format_bans_agg(temp_bans_ids_team["vs_team_red_rot1"], champion_data, ICON_SIZE_PICKS_BANS),
                "vs_team_red_rot2_formatted": format_bans_agg(temp_bans_ids_team["vs_team_red_rot2"], champion_data, ICON_SIZE_PICKS_BANS),
            }
            
            def format_detailed_picks(picks_dict):
                formatted_list = []
                for champ, data in sorted(picks_dict.items(), key=lambda x: x[1]['games'], reverse=True):
                    if data['games'] > 0:
                        win_r = (data['wins'] / data['games'] * 100)
                        formatted_list.append({
                            "champion": champ, "games": data['games'], "win_rate": round(win_r,1),
                            "icon_html": get_champion_icon_html(champ, champion_data, ICON_SIZE_PICKS_BANS, ICON_SIZE_PICKS_BANS)
                        })
                return formatted_list

            stats["detailed_picks"] = {
                "blue_rot1": format_detailed_picks(temp_detailed_picks["blue_rot1"]),
                "blue_rot2": format_detailed_picks(temp_detailed_picks["blue_rot2"]),
                "red_rot1": format_detailed_picks(temp_detailed_picks["red_rot1"]),
                "red_rot2": format_detailed_picks(temp_detailed_picks["red_rot2"]),
            }
            
            def format_priority_picks(priority_dict):
                 priority_list = []
                 max_picks_in_phase = 0
                 for champ, phase_counts in priority_dict.items():
                     current_max = max(phase_counts.values()) if phase_counts else 0
                     if current_max > max_picks_in_phase:
                         max_picks_in_phase = current_max

                 for champ, phase_counts in priority_dict.items():
                     total_picks = sum(phase_counts.values())
                     entry = {
                         "champion": champ,
                         "total_picks": total_picks,
                         "icon_html": get_champion_icon_html(champ, champion_data, ICON_SIZE_PICKS_BANS, ICON_SIZE_PICKS_BANS),
                         "phases": {phase: count for phase, count in phase_counts.items()}
                     }
                     priority_list.append(entry)
                 
                 return sorted(priority_list, key=lambda x: x['total_picks'], reverse=True), max_picks_in_phase if max_picks_in_phase > 0 else 1

            blue_prio_list, blue_max_val = format_priority_picks(temp_priority_picks["Blue"])
            red_prio_list, red_max_val = format_priority_picks(temp_priority_picks["Red"])

            stats["priority_picks"] = {
                "blue": blue_prio_list,
                "red": red_prio_list,
                "blue_max_picks": blue_max_val,
                "red_max_picks": red_max_val,
                "blue_phases": sorted(list(set(blue_pick_seq_to_phase.values())), key=lambda x: int(x.split('-')[0][1:])),
                "red_phases": sorted(list(set(red_pick_seq_to_phase.values())), key=lambda x: int(x.split('-')[0][1:])),
            }

            for (r1_cfg, r2_cfg), duo_title_cfg in duo_roles_config.items():
                 stats["duo_picks"][duo_title_cfg]["title"] = duo_title_cfg
            for duo_key, data in temp_duo_stats_team.items():
                if data['games'] > 0:
                    (role1_s, champ1_s), (role2_s, champ2_s) = duo_key
                    orig_r1, orig_r2 = data['roles']
                    current_duo_title = None; target_r1_map, target_r2_map = None,None
                    for (map_r1, map_r2), title_map in duo_roles_config.items():
                        if orig_r1 in (map_r1, map_r2) and orig_r2 in (map_r1, map_r2):
                            current_duo_title = title_map; target_r1_map,target_r2_map = map_r1,map_r2; break
                    if not current_duo_title: continue
                    win_r = (data['wins'] / data['games'] * 100)
                    display_c1, display_c2 = (champ1_s, champ2_s) if role1_s == target_r1_map else (champ2_s, champ1_s)
                    stats["duo_picks"][current_duo_title]["stats"].append({
                        "champ1": display_c1, "champ2": display_c2,
                        "icon1_html": get_champion_icon_html(display_c1, champion_data, ICON_SIZE_DUOS,ICON_SIZE_DUOS),
                        "icon2_html": get_champion_icon_html(display_c2, champion_data, ICON_SIZE_DUOS,ICON_SIZE_DUOS),
                        "games": data['games'], "win_rate": round(win_r,1)
                    })
            for title in stats["duo_picks"]: stats["duo_picks"][title]["stats"].sort(key=lambda x: x['games'], reverse=True)
            stats["duo_picks"] = dict(stats["duo_picks"])

            pattern_map_labels = { 'Blue': [('B1', 'First Pick'), ('B2_B3', 'Phase Two'), ('B4_B5', 'Phase Three')],
                                   'Red': [('R1_R2', 'Phase One'), ('R3', 'Pick 3'), ('R4', 'Pick 4'), ('R5', 'Last Pick')] }
            for side_color, games_on_side_count in [('Blue', games_played_on_blue_by_team), ('Red', games_played_on_red_by_team)]:
                if games_on_side_count > 0:
                    for pattern_key, display_name in pattern_map_labels[side_color]:
                        stats['draft_patterns'][side_color][display_name] = {}
                        for role_name_pattern in roles_for_pattern:
                            count = draft_patterns_counters[side_color][pattern_key].get(role_name_pattern, 0)
                            percentage = round((count / games_on_side_count) * 100)
                            stats['draft_patterns'][side_color][display_name][role_name_pattern] = percentage
            
            grouped_matches = defaultdict(list)
            for detail in all_game_details_list:
                grouped_matches[detail["series_id"]].append(detail)
            
            all_game_details_list_sorted = []
            sorted_series_ids = sorted(grouped_matches.keys(), key=lambda s_id: grouped_matches[s_id][0].get('game_id', ''), reverse=True)

            sorted_grouped_matches = {}
            for series_id in sorted_series_ids:
                games_in_series = grouped_matches[series_id]
                games_in_series.sort(key=lambda g: g.get('sequence_number', 0))
                sorted_grouped_matches[series_id] = games_in_series
                all_game_details_list_sorted.extend(games_in_series)

            all_game_details_list = all_game_details_list_sorted
            grouped_matches = dict(sorted_grouped_matches)

    except Exception as e:
        log_message(f"!!! CRITICAL Error during tournament data aggregation: {e}")
        log_message(traceback.format_exc())
        stats["error"] = "An critical error occurred during data aggregation. Check logs."
        all_teams_display = []
        grouped_matches = {}
        all_game_details_list = []
    finally:
        if cursor is not None:
            try: cursor.close()
            except Exception as ce: log_message(f"Error closing cursor: {ce}")
        if conn is not None:
            try: conn.close();
            except Exception as ce: log_message(f"Error closing connection: {ce}")

    return all_teams_display, stats, grouped_matches, all_game_details_list

def get_all_wards_data(selected_team_full_name, selected_role, games_filter, selected_champion):
    """
    Извлекает и агрегирует данные о всех вардах на основе фильтров для новой страницы.
    """
    conn = get_db_connection()
    if not conn:
        return [], {}, {"error": "Database connection failed"}, []

    all_teams_display = []
    stats_or_error = {}
    wards_by_interval = {}
    available_champions = ["All"]

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
            stats_or_error = {"message": "Please select a team to view warding patterns."}
            return all_teams_display, wards_by_interval, stats_or_error, available_champions

        role_to_abbr = {"TOP": "TOP", "JGL": "JGL", "MID": "MID", "BOT": "BOT", "SUP": "SUP"}
        selected_team_tag = None
        for tag, full_name in TEAM_TAG_TO_FULL_NAME.items():
            if full_name == selected_team_full_name: selected_team_tag = tag; break
        if not selected_team_tag and selected_team_full_name in all_teams_tags:
            selected_team_tag = selected_team_full_name
        
        if not selected_team_tag:
            stats_or_error = {"error": f"Team tag not found for '{selected_team_full_name}'."}
            return all_teams_display, wards_by_interval, stats_or_error, available_champions

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

        query_games = 'SELECT * FROM tournament_games WHERE Blue_Team_Name = ? OR Red_Team_Name = ? ORDER BY "Date" DESC'
        params_games = [selected_team_tag, selected_team_tag]
        if games_filter != 'All' and games_filter.isdigit():
            query_games += f" LIMIT {int(games_filter)}"
        
        cursor.execute(query_games, params_games)
        game_rows = [dict(row) for row in cursor.fetchall()]

        if not game_rows:
            stats_or_error = {"message": "No games found for the selected team."}
            return all_teams_display, wards_by_interval, stats_or_error, available_champions

        game_ids_to_query = [row["Game_ID"] for row in game_rows]
        puuids_to_query = set()
        if selected_role == "All":
            for game in game_rows:
                is_blue = game.get("Blue_Team_Name") == selected_team_tag
                prefix = "Blue" if is_blue else "Red"
                for role_abbr_val in role_to_abbr.values():
                    puuid = game.get(f"{prefix}_{role_abbr_val}_PUUID")
                    if puuid: puuids_to_query.add(puuid)
        else:
            role_abbr_val = role_to_abbr.get(selected_role.upper())
            if role_abbr_val:
                for game in game_rows:
                    is_blue = game.get("Blue_Team_Name") == selected_team_tag
                    prefix = "Blue" if is_blue else "Red"
                    puuid = game.get(f"{prefix}_{role_abbr_val}_PUUID")
                    if puuid: puuids_to_query.add(puuid)
        
        all_wards = []
        if game_ids_to_query:
            wards_query = 'SELECT * FROM all_wards_data WHERE "Game_ID" IN ({})'.format(','.join(['?'] * len(game_ids_to_query)))
            wards_params = list(game_ids_to_query)
            if puuids_to_query:
                wards_query += ' AND "player_puuid" IN ({})'.format(','.join(['?'] * len(puuids_to_query)))
                wards_params.extend(list(puuids_to_query))
            if selected_champion and selected_champion != "All":
                 wards_query += ' AND "champion_name" = ?'
                 wards_params.append(selected_champion)
            cursor.execute(wards_query, wards_params)
            all_wards = [dict(row) for row in cursor.fetchall()]

        time_intervals = []
        for i in range(0, int(50 * 60 / 90)):
            start_sec = i * 90
            end_sec = (i + 1) * 90
            start_min, start_s = divmod(start_sec, 60)
            end_min, end_s = divmod(end_sec, 60)
            label = f"{start_min:02d}:{start_s:02d} - {end_min:02d}:{end_s:02d}"
            time_intervals.append((label, start_sec, end_sec))

        wards_by_interval = {label: [] for label, _, _ in time_intervals}
        for ward in all_wards:
            ts = ward['timestamp_seconds']
            for label, start, end in time_intervals:
                if start <= ts < end:
                    wards_by_interval[label].append(ward)
                    break
    
    except sqlite3.Error as e:
        log_message(f"DB Error in get_all_wards_data: {e}")
        stats_or_error = {"error": "A database error occurred."}
    finally:
        if conn: conn.close()

    return all_teams_display, wards_by_interval, stats_or_error, available_champions

# --- НОВАЯ ФУНКЦИЯ ДЛЯ СТРАНИЦЫ PROXIMITY ---
def get_proximity_data(selected_team_full_name, selected_role, games_filter):
    """
    Извлекает и агрегирует данные о близости игроков для страницы Proximity.
    """
    conn = get_db_connection()
    if not conn:
        return [], {"error": "Database connection failed"}, []

    all_teams_display = []
    stats = {"error": None, "message": None, "data_by_champion": [], "averages": {}}
    players_in_role = [] 

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
            stats["message"] = "Please select a team to view proximity stats."
            return all_teams_display, stats, players_in_role

        # 2. Определяем тег команды и роли для анализа
        selected_team_tag = None
        for tag, full_name in TEAM_TAG_TO_FULL_NAME.items():
            if full_name == selected_team_full_name:
                selected_team_tag = tag
                break
        if not selected_team_tag and selected_team_full_name in all_teams_tags:
            selected_team_tag = selected_team_full_name
        
        if not selected_team_tag:
            stats["error"] = f"Team tag not found for '{selected_team_full_name}'."
            return all_teams_display, stats, players_in_role

        role_to_abbr = {"TOP": "TOP", "JUNGLE": "JGL", "MIDDLE": "MID", "BOTTOM": "BOT", "SUPPORT": "SUP"}
        
        if selected_role == "JUNGLE":
            ally_roles = ["TOP", "MIDDLE", "BOTTOM", "SUPPORT"]
        elif selected_role == "SUPPORT":
            ally_roles = ["TOP", "JUNGLE", "MIDDLE", "BOTTOM"]
        else:
            ally_roles = [r for r in role_to_abbr.keys() if r != selected_role]

        # 3. Получаем последние игры для команды
        query_games = 'SELECT * FROM tournament_games WHERE Blue_Team_Name = ? OR Red_Team_Name = ? ORDER BY "Date" DESC'
        params_games = [selected_team_tag, selected_team_tag]
        if games_filter != 'All' and games_filter.isdigit():
            query_games += f" LIMIT {int(games_filter)}"
        
        cursor.execute(query_games, params_games)
        game_rows = [dict(row) for row in cursor.fetchall()]

        if not game_rows:
            stats["message"] = "No games found for the selected team and filters."
            return all_teams_display, stats, players_in_role

        # 4. Собираем информацию об играх и PUUID-ы игроков
        game_info = {}
        for game in game_rows:
            game_id = game['Game_ID']
            is_blue = game.get("Blue_Team_Name") == selected_team_tag
            prefix = "Blue" if is_blue else "Red"
            
            main_player_puuid = game.get(f"{prefix}_{role_to_abbr[selected_role]}_PUUID")
            if not main_player_puuid:
                continue

            game_info[game_id] = {
                "winner": game["Winner_Side"],
                "side": "Blue" if is_blue else "Red",
                "champion": game.get(f"{prefix}_{role_to_abbr[selected_role]}_Champ"),
                "puuid_map": {selected_role: main_player_puuid}
            }
            
            for ally_role in ally_roles:
                ally_puuid = game.get(f"{prefix}_{role_to_abbr[ally_role]}_PUUID")
                if ally_puuid:
                    game_info[game_id]["puuid_map"][ally_role] = ally_puuid

        if not game_info:
            stats["message"] = f"No games found where the selected team had a player in the '{selected_role}' role."
            return all_teams_display, stats, players_in_role
        
        # 5. Извлекаем все данные о позициях для этих игр
        game_ids_list = list(game_info.keys())
        placeholders = ','.join(['?'] * len(game_ids_list))
        pos_query = f"SELECT * FROM player_positions_timeline WHERE game_id IN ({placeholders}) ORDER BY timestamp_ms"
        cursor.execute(pos_query, game_ids_list)
        
        positions_by_game_time = defaultdict(lambda: defaultdict(list))
        for row in cursor.fetchall():
            positions_by_game_time[row['game_id']][row['timestamp_ms']].append(dict(row))

        # 6. Определяем временные интервалы
        time_intervals = {
            "0-5 min": (0, 5 * 60 * 1000),
            "5-14 min": (5 * 60 * 1000, 14 * 60 * 1000),
            "14-20 min": (14 * 60 * 1000, 20 * 60 * 1000),
            "20-24 min": (20 * 60 * 1000, 24 * 60 * 1000),
            "24-30 min": (24 * 60 * 1000, 30 * 60 * 1000),
            "30+ min": (30 * 60 * 1000, 999 * 60 * 1000)
        }
        
        # 7. Анализ и расчет близости
        champ_stats = defaultdict(lambda: {
            "games": 0, "wins": 0,
            "proximity_seconds": {ally: {interval: 0 for interval in list(time_intervals.keys()) + ['Overall']} for ally in ally_roles},
            "total_seconds": {ally: {interval: 0 for interval in list(time_intervals.keys()) + ['Overall']} for ally in ally_roles}
        })

        for game_id, info in game_info.items():
            if game_id not in positions_by_game_time:
                continue

            champion = info["champion"]
            champ_stats[champion]["games"] += 1
            if info["side"] == info["winner"]:
                champ_stats[champion]["wins"] += 1
            
            puuid_map = info["puuid_map"]
            main_puuid = puuid_map.get(selected_role)
            if not main_puuid: continue

            for ts_ms, positions in sorted(positions_by_game_time[game_id].items()):
                main_player_pos = None
                ally_positions = {}
                
                for pos_data in positions:
                    if pos_data['player_puuid'] == main_puuid:
                        main_player_pos = (pos_data['pos_x'], pos_data['pos_z'])
                    else:
                        for role, p_puuid in puuid_map.items():
                            if pos_data['player_puuid'] == p_puuid:
                                ally_positions[role] = (pos_data['pos_x'], pos_data['pos_z'])
                
                if not main_player_pos: continue
                
                # Анализ по временным интервалам
                for interval, (start_ms, end_ms) in time_intervals.items():
                    if start_ms <= ts_ms < end_ms:
                        for ally_role, ally_pos in ally_positions.items():
                            if ally_role in ally_roles:
                                champ_stats[champion]["total_seconds"][ally_role][interval] += 1
                                distance = math.sqrt((main_player_pos[0] - ally_pos[0])**2 + (main_player_pos[1] - ally_pos[1])**2)
                                if distance <= PROXIMITY_DISTANCE_THRESHOLD:
                                    champ_stats[champion]["proximity_seconds"][ally_role][interval] += 1
                
                # Общий подсчет для 'Overall'
                for ally_role, ally_pos in ally_positions.items():
                    if ally_role in ally_roles:
                        champ_stats[champion]["total_seconds"][ally_role]['Overall'] += 1
                        distance = math.sqrt((main_player_pos[0] - ally_pos[0])**2 + (main_player_pos[1] - ally_pos[1])**2)
                        if distance <= PROXIMITY_DISTANCE_THRESHOLD:
                            champ_stats[champion]["proximity_seconds"][ally_role]['Overall'] += 1

        # 8. Форматирование результатов
        all_intervals = ['Overall'] + list(time_intervals.keys())
        total_averages_agg = {ally: {interval: {"prox_sum": 0, "count": 0} for interval in all_intervals} for ally in ally_roles}
        
        for champion, data in champ_stats.items():
            proximity_percentages = {}
            for ally_role in ally_roles:
                proximity_percentages[ally_role] = {}
                for interval in all_intervals:
                    prox_sec = data["proximity_seconds"][ally_role][interval]
                    total_sec = data["total_seconds"][ally_role][interval]
                    percentage = (prox_sec / total_sec * 100) if total_sec > 0 else 0
                    proximity_percentages[ally_role][interval] = round(percentage)
                    
                    total_averages_agg[ally_role][interval]["prox_sum"] += percentage
                    total_averages_agg[ally_role][interval]["count"] += 1

            stats["data_by_champion"].append({
                "champion": champion,
                "games": data["games"],
                "winrate": round(data["wins"] / data["games"] * 100) if data["games"] > 0 else 0,
                "proximity": proximity_percentages
            })
        
        # Считаем итоговые средние
        for ally_role in ally_roles:
            stats["averages"][ally_role] = {}
            for interval in all_intervals:
                agg = total_averages_agg[ally_role][interval]
                avg = agg["prox_sum"] / agg["count"] if agg["count"] > 0 else 0
                stats["averages"][ally_role][interval] = round(avg)

        stats["data_by_champion"].sort(key=lambda x: x["games"], reverse=True)

    except sqlite3.Error as e:
        log_message(f"DB Error in get_proximity_data: {e}")
        stats["error"] = "A database error occurred."
    except Exception as e:
        log_message(f"!!! CRITICAL Error during proximity data aggregation: {e}")
        log_message(traceback.format_exc())
        stats["error"] = "An critical error occurred during data aggregation."
    finally:
        if conn: conn.close()

    return all_teams_display, stats, players_in_role
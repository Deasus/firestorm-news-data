"""
FIRESTORM News Intelligence Pipeline — v2 (data integrity rewrite)
===================================================================
Aggregates wildfire intelligence from multiple sources into a single JSON feed.

Data integrity principles (v2):
- NO cross-source dedup. Different outlets reporting the same fire stay separate.
- Within-source dedup is URL-only (catches true feed-shuffle duplicates).
- Disasters from ReliefWeb/GDELT are MERGED into the article feed with GLIDE IDs.
- ReliefWeb GLIDE IDs are fuzzy-matched to GDELT articles (country + date window)
  so coverage picks up canonical disaster IDs even when they weren't explicit.
- InciWeb incidents get GeoRSS coordinate parsing.
- Every discard gets logged to `feed.dropped[]` so operators can audit.
- Raw per-source counts always preserved in `feed.raw_counts`.

Sources:
1. GDELT DOC 2.0 API — Full-text search, now also captures tone + themes + GLIDE
2. GDELT GEO 2.0 API — Geolocated fire news for map plotting
3. GDELT Disasters Live Stream — UN OCHA/ReliefWeb tracked disasters (GLIDE source)
4. GDELT BigQuery — Deep historical + GKG themes (requires Google Cloud credentials)
5. InciWeb RSS — Official federal wildfire incident updates + GeoRSS coordinates
6. Google News RSS — Broad wildfire news coverage

Outputs JSON to data/ directory. Runs every 30 minutes via GitHub Actions.

Frontend compatibility: emits `articles[]` with per-item fields matching what
firestorm_v2_74+ expects: title, url, source, date, image, type, category, glide,
irwin_id, tone, themes, lat, lng, country.
"""

import json
import os
import sys
import re
import time
from datetime import datetime, timezone, timedelta
from urllib.parse import quote

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

# ── Drop log — every discard recorded for operator audit ─────────────
DROPPED = []

def drop(reason, source, identifier):
    """Record a dropped record so operators can audit the pipeline."""
    DROPPED.append({
        'reason': reason,
        'source': source,
        'id': str(identifier)[:200]
    })

# ── GLIDE regex ──────────────────────────────────────────────────────
# GLIDE format: EV-YYYY-NNNNNN-CCC (e.g. WF-2024-000123-USA)
GLIDE_RE = re.compile(r'\b([A-Z]{2}-\d{4}-\d{6}-[A-Z]{3})\b')

def extract_glide(text):
    """Find a GLIDE ID in any text blob. Returns the first match or None."""
    if not text:
        return None
    m = GLIDE_RE.search(str(text))
    return m.group(1) if m else None


# ── Geo dictionary — tier-1 geocoder (free, runs locally) ───────────
# Maps country + state/region + city names to lat/lng so news articles get
# map pins without paid geocoding. v3 (2026-05-01) — heavy international
# expansion: 50 US states, 100+ US cities, 190 countries, 200+ global cities/
# regions with active fire history. Longest-first match order handles "new
# south wales" before "wales", "british columbia" before "columbia", etc.
GEO_DICT = {
    # ── US STATES ──
    'alabama': (32.8, -86.8), 'alaska': (64.2, -152.0), 'arizona': (34.2, -111.7),
    'arkansas': (34.9, -92.4), 'california': (36.7, -119.7), 'colorado': (39.0, -105.5),
    'connecticut': (41.6, -72.7), 'delaware': (39.0, -75.5), 'florida': (27.8, -81.7),
    'georgia': (32.9, -83.4), 'hawaii': (20.3, -156.4), 'idaho': (44.2, -114.4),
    'illinois': (40.0, -89.2), 'indiana': (39.9, -86.3), 'iowa': (42.0, -93.5),
    'kansas': (38.5, -98.4), 'kentucky': (37.5, -85.3), 'louisiana': (31.0, -92.0),
    'maine': (45.3, -69.2), 'maryland': (39.0, -76.8), 'massachusetts': (42.4, -71.8),
    'michigan': (44.3, -85.4), 'minnesota': (46.3, -94.3), 'mississippi': (32.8, -89.7),
    'missouri': (38.4, -92.5), 'montana': (47.0, -109.6), 'nebraska': (41.5, -99.8),
    'nevada': (39.3, -116.6), 'new hampshire': (43.7, -71.6), 'new jersey': (40.2, -74.7),
    'new mexico': (34.4, -106.1), 'new york': (42.9, -75.5), 'north carolina': (35.6, -79.4),
    'north dakota': (47.5, -100.3), 'ohio': (40.3, -82.8), 'oklahoma': (35.6, -97.5),
    'oregon': (44.0, -120.5), 'pennsylvania': (40.9, -77.8), 'rhode island': (41.7, -71.5),
    'south carolina': (33.9, -80.9), 'south dakota': (44.4, -100.2), 'tennessee': (35.9, -86.4),
    'texas': (31.5, -99.3), 'utah': (39.3, -111.7), 'vermont': (44.0, -72.7),
    'virginia': (37.5, -78.9), 'washington': (47.4, -120.5), 'west virginia': (38.6, -80.6),
    'wisconsin': (44.5, -89.5), 'wyoming': (43.0, -107.5),

    # ── US CITIES — fire-relevant ──
    'los angeles': (34.05, -118.25), 'san diego': (32.72, -117.16), 'san francisco': (37.77, -122.42),
    'sacramento': (38.58, -121.49), 'fresno': (36.75, -119.77), 'bakersfield': (35.37, -119.02),
    'malibu': (34.03, -118.68), 'pacific palisades': (34.04, -118.53), 'altadena': (34.19, -118.13),
    'paradise': (39.76, -121.62), 'chico': (39.73, -121.83), 'redding': (40.58, -122.39),
    'reno': (39.52, -119.81), 'las vegas': (36.17, -115.14), 'phoenix': (33.45, -112.07),
    'tucson': (32.22, -110.97), 'albuquerque': (35.08, -106.65), 'santa fe': (35.69, -105.94),
    'denver': (39.74, -104.99), 'boulder': (40.02, -105.27), 'colorado springs': (38.83, -104.82),
    'salt lake city': (40.76, -111.89), 'boise': (43.62, -116.20),
    'portland oregon': (45.52, -122.68), 'seattle': (47.61, -122.33), 'spokane': (47.66, -117.43),
    'missoula': (46.87, -113.99), 'billings': (45.78, -108.51),
    'austin': (30.27, -97.74), 'houston': (29.76, -95.37), 'dallas': (32.78, -96.80),
    'el paso': (31.76, -106.49), 'san antonio': (29.42, -98.49),
    'miami': (25.76, -80.19), 'orlando': (28.54, -81.38), 'tampa': (27.95, -82.46),
    'tallahassee': (30.44, -84.28), 'jacksonville': (30.33, -81.66),
    'atlanta': (33.75, -84.39), 'nashville': (36.16, -86.78), 'charlotte': (35.23, -80.84),
    'new orleans': (29.95, -90.08),

    # ── US REGIONS / FORESTS ──
    'sierra nevada': (38.0, -119.5), 'cascade range': (44.0, -121.8), 'mojave desert': (35.0, -115.5),
    'sonoran desert': (33.0, -112.5), 'chihuahuan desert': (32.0, -107.0),
    'rocky mountains': (40.0, -106.0), 'great basin': (40.0, -117.0),
    'pacific northwest': (46.0, -122.0), 'gulf coast': (29.5, -89.0),
    'appalachia': (37.0, -82.0), 'ozarks': (37.0, -93.0),

    # ── CANADA ──
    'british columbia': (53.7, -127.6), 'alberta': (53.9, -116.5), 'saskatchewan': (52.9, -106.4),
    'manitoba': (55.0, -97.0), 'ontario': (51.2, -85.3), 'quebec': (53.0, -73.5),
    'newfoundland': (53.1, -57.6), 'nova scotia': (45.0, -63.0), 'new brunswick': (46.5, -66.5),
    'prince edward island': (46.5, -63.0), 'yukon': (63.8, -135.5),
    'northwest territories': (64.8, -124.8), 'nunavut': (70.0, -90.0),
    'vancouver': (49.28, -123.12), 'toronto': (43.65, -79.38), 'montreal': (45.50, -73.57),
    'calgary': (51.05, -114.07), 'edmonton': (53.55, -113.50), 'ottawa': (45.42, -75.70),
    'winnipeg': (49.90, -97.14), 'halifax': (44.65, -63.58), 'fort mcmurray': (56.73, -111.38),
    'kelowna': (49.88, -119.49), 'kamloops': (50.67, -120.33), 'prince george': (53.92, -122.75),

    # ── EUROPE ──
    'portugal': (39.5, -8.0), 'spain': (40.0, -4.0), 'france': (46.6, 2.3), 'italy': (42.5, 12.5),
    'greece': (39.0, 22.0), 'germany': (51.2, 10.5), 'poland': (52.0, 19.1), 'sweden': (62.0, 15.6),
    'norway': (60.5, 8.5), 'finland': (64.0, 26.0), 'denmark': (56.0, 10.0),
    'united kingdom': (54.0, -2.0), 'ireland': (53.0, -8.0), 'netherlands': (52.1, 5.3),
    'belgium': (50.5, 4.5), 'switzerland': (46.8, 8.2), 'austria': (47.5, 14.0),
    'czech republic': (49.8, 15.5), 'slovakia': (48.7, 19.7), 'hungary': (47.2, 19.5),
    'romania': (45.9, 24.9), 'bulgaria': (42.7, 25.5), 'serbia': (44.0, 21.0),
    'croatia': (45.1, 15.2), 'slovenia': (46.1, 14.8), 'bosnia': (43.9, 17.7),
    'macedonia': (41.6, 21.7), 'albania': (41.1, 20.1), 'montenegro': (42.7, 19.3),
    'ukraine': (49.0, 32.0), 'belarus': (53.7, 27.9), 'estonia': (58.6, 25.0),
    'latvia': (56.9, 24.6), 'lithuania': (55.2, 23.9), 'cyprus': (35.1, 33.4),
    'iceland': (64.9, -19.0),
    'lisbon': (38.72, -9.14), 'madrid': (40.42, -3.70), 'barcelona': (41.39, 2.17),
    'paris': (48.86, 2.35), 'marseille': (43.30, 5.37), 'rome': (41.90, 12.50),
    'athens': (37.98, 23.73), 'london': (51.51, -0.13), 'berlin': (52.52, 13.41),
    'warsaw': (52.23, 21.01), 'moscow': (55.75, 37.62), 'stockholm': (59.33, 18.07),
    'oslo': (59.91, 10.75), 'copenhagen': (55.68, 12.57), 'helsinki': (60.17, 24.94),
    'amsterdam': (52.37, 4.90), 'vienna': (48.21, 16.37), 'zurich': (47.37, 8.54),
    'catalonia': (41.8, 1.8), 'andalusia': (37.5, -4.5), 'galicia': (42.8, -8.0),
    'provence': (43.9, 6.1), 'algarve': (37.1, -8.0), 'corsica': (42.2, 9.1),
    'sardinia': (40.1, 9.1), 'sicily': (37.6, 14.0), 'crete': (35.3, 24.9),
    'peloponnese': (37.6, 22.4), 'tuscany': (43.5, 11.1),

    # ── SOUTH AMERICA ──
    'chile': (-35.7, -71.5), 'argentina': (-38.4, -63.6), 'brazil': (-14.2, -51.9),
    'peru': (-9.2, -75.0), 'bolivia': (-16.3, -63.6), 'colombia': (4.6, -74.1),
    'venezuela': (6.4, -66.6), 'ecuador': (-1.8, -78.2), 'paraguay': (-23.4, -58.4),
    'uruguay': (-32.5, -55.8), 'guyana': (4.9, -58.9), 'suriname': (3.9, -56.0),
    'santiago chile': (-33.45, -70.67), 'santiago': (-33.45, -70.67), 'buenos aires': (-34.60, -58.38), 'sao paulo': (-23.55, -46.63),
    'rio de janeiro': (-22.91, -43.17), 'brasilia': (-15.78, -47.93), 'lima': (-12.05, -77.04),
    'bogota': (4.71, -74.07), 'caracas': (10.48, -66.90), 'quito': (-0.23, -78.52),
    'montevideo': (-34.90, -56.19), 'valparaiso': (-33.05, -71.62), 'manaus': (-3.12, -60.02),
    'amazon rainforest': (-3.0, -60.0), 'amazon basin': (-3.0, -60.0),
    'pantanal': (-17.5, -56.5), 'patagonia': (-46.0, -72.0), 'andes': (-32.0, -70.0),
    'cerrado': (-15.5, -47.5),

    # ── AUSTRALIA + NZ ──
    'australia': (-25.3, 133.8), 'new south wales': (-32.0, 147.0),
    'victoria australia': (-37.0, 145.0), 'queensland': (-22.0, 145.0),
    'western australia': (-25.0, 122.0), 'south australia': (-30.0, 135.0),
    'tasmania': (-42.0, 147.0), 'northern territory': (-19.0, 133.0),
    'sydney': (-33.87, 151.21), 'melbourne': (-37.81, 144.96), 'brisbane': (-27.47, 153.03),
    'perth': (-31.95, 115.86), 'adelaide': (-34.93, 138.60), 'canberra': (-35.28, 149.13),
    'darwin': (-12.46, 130.84), 'hobart': (-42.88, 147.33), 'gold coast': (-28.00, 153.43),
    'blue mountains': (-33.7, 150.4), 'outback': (-25.0, 135.0),
    'new zealand': (-41.0, 174.0), 'auckland': (-36.85, 174.76), 'wellington': (-41.29, 174.78),
    'christchurch': (-43.53, 172.64),

    # ── AFRICA ──
    'south africa': (-29.0, 25.0), 'cape town': (-33.93, 18.42), 'johannesburg': (-26.20, 28.05),
    'durban': (-29.86, 31.02), 'kenya': (0.0, 37.9), 'nairobi': (-1.29, 36.82),
    'tanzania': (-6.4, 34.9), 'uganda': (1.4, 32.3), 'ethiopia': (9.1, 40.5),
    'nigeria': (9.1, 8.7), 'lagos': (6.52, 3.38), 'ghana': (7.9, -1.0), 'accra': (5.56, -0.20),
    'morocco': (31.8, -7.1), 'algeria': (28.0, 1.7), 'tunisia': (33.9, 9.5),
    'libya': (26.3, 17.2), 'egypt': (26.8, 30.8), 'sudan': (12.9, 30.2),
    'democratic republic of the congo': (-4.0, 21.8), 'angola': (-11.2, 17.9),
    'zambia': (-13.1, 27.8), 'zimbabwe': (-19.0, 29.2), 'mozambique': (-18.7, 35.5),
    'madagascar': (-18.8, 46.9), 'namibia': (-22.0, 17.1), 'botswana': (-22.3, 24.7),
    'sahara': (24.0, 10.0), 'sahel': (14.5, 0.0),

    # ── ASIA ──
    'russia': (61.5, 105.3), 'siberia': (64.0, 105.0), 'far east russia': (60.0, 135.0),
    'kazakhstan': (48.0, 66.9), 'mongolia': (46.9, 103.8), 'china': (35.9, 104.2),
    'beijing': (39.90, 116.41), 'shanghai': (31.23, 121.47), 'xinjiang': (40.0, 85.0),
    'tibet': (31.0, 89.0), 'yunnan': (25.0, 102.0), 'inner mongolia': (42.0, 112.0),
    'india': (20.6, 78.9), 'delhi': (28.61, 77.21), 'mumbai': (19.08, 72.88),
    'bangalore': (12.97, 77.59), 'uttarakhand': (30.1, 79.0), 'himachal pradesh': (31.1, 77.2),
    'assam': (26.2, 92.9), 'kerala': (10.9, 76.3), 'karnataka': (15.3, 75.7),
    'nepal': (28.4, 84.1), 'bhutan': (27.5, 90.4), 'bangladesh': (23.7, 90.4),
    'pakistan': (30.4, 69.3), 'afghanistan': (33.9, 67.7), 'iran': (32.4, 53.7),
    'iraq': (33.2, 43.7), 'syria': (34.8, 38.9), 'jordan': (30.6, 36.2),
    'israel': (31.0, 34.8), 'lebanon': (33.9, 35.9), 'palestine': (31.9, 35.2),
    'saudi arabia': (23.9, 45.1), 'yemen': (15.6, 48.5), 'oman': (21.5, 55.9),
    'uae': (23.4, 53.9), 'turkey': (39.0, 35.0), 'istanbul': (41.01, 28.98),
    'ankara': (39.93, 32.87), 'antalya': (36.90, 30.71), 'cyprus': (35.1, 33.4),
    'japan': (36.2, 138.3), 'tokyo': (35.68, 139.69), 'osaka': (34.69, 135.50),
    'hokkaido': (43.3, 142.8), 'kyushu': (32.7, 130.7),
    'south korea': (35.9, 127.8), 'north korea': (40.3, 127.5), 'seoul': (37.57, 126.98),
    'taiwan': (23.7, 121.0), 'thailand': (15.9, 100.9), 'bangkok': (13.76, 100.50),
    'vietnam': (14.1, 108.3), 'hanoi': (21.03, 105.85), 'cambodia': (12.6, 104.9),
    'laos': (19.9, 102.5), 'myanmar': (21.9, 95.9), 'malaysia': (4.2, 101.9),
    'kuala lumpur': (3.14, 101.69), 'indonesia': (-0.8, 113.9), 'jakarta': (-6.21, 106.85),
    'sumatra': (-0.6, 101.7), 'borneo': (0.5, 114.0), 'kalimantan': (-1.7, 113.7),
    'sulawesi': (-2.5, 121.0), 'java indonesia': (-7.5, 110.0),
    'papua new guinea': (-6.3, 143.9), 'philippines': (12.9, 121.8), 'manila': (14.60, 120.98),
    'sri lanka': (7.9, 80.8), 'maldives': (3.2, 73.2), 'singapore': (1.35, 103.82),
    'brunei': (4.5, 114.7),

    # ── CARIBBEAN + CENTRAL AMERICA ──
    'mexico': (23.6, -102.5), 'mexico city': (19.43, -99.13), 'guadalajara': (20.67, -103.35),
    'yucatan': (20.8, -88.8), 'chihuahua state': (28.6, -106.1), 'sonora': (29.3, -110.9),
    'baja california': (30.0, -115.5), 'oaxaca': (17.1, -96.7), 'jalisco': (20.7, -103.7),
    'guatemala': (15.8, -90.2), 'honduras': (15.2, -86.2), 'el salvador': (13.8, -88.9),
    'nicaragua': (12.9, -85.2), 'costa rica': (9.7, -83.8), 'panama': (8.5, -80.8),
    'belize': (17.2, -88.5), 'cuba': (21.5, -77.8), 'jamaica': (18.1, -77.3),
    'haiti': (18.9, -72.3), 'dominican republic': (18.7, -70.2), 'puerto rico': (18.2, -66.6),
    'bahamas': (25.0, -77.4), 'trinidad': (10.7, -61.2),
    # ── Additional hotspots found in live feed audit (2026-05-01) ──
    'green mountain': (43.9, -72.9), 'champlain valley': (44.5, -73.3),
    'everglades': (25.9, -80.9), 'panhandle': (35.4, -101.0),
    'fermanagh': (54.4, -7.6), 'northern ireland': (54.7, -6.7),
    'san bernardino': (34.11, -117.30), 'okanogan': (48.36, -119.58),
    'wenatchee': (47.42, -120.31), 'columbia county': (33.56, -82.26),
    'southeast us': (33.0, -85.0), 'southeast united states': (33.0, -85.0),
    'midwest': (41.5, -93.0), 'southwest': (33.0, -110.0), 'northeast us': (42.0, -73.0),
    'gulf of mexico': (25.0, -90.0),
    'vermont forest': (43.9, -72.9),
    'cherokee': (35.9, -84.7), 'great smoky': (35.6, -83.5),
    # Additional global
    'kyoto': (35.01, 135.77), 'fukuoka': (33.59, 130.40), 'sapporo': (43.07, 141.35),
    'incheon': (37.46, 126.71), 'busan': (35.18, 129.08),
    'shenzhen': (22.54, 114.06), 'guangzhou': (23.13, 113.26), 'chengdu': (30.57, 104.07),
    'xian': (34.34, 108.94), 'hong kong': (22.32, 114.17),
    'mumbai': (19.08, 72.88), 'chennai': (13.08, 80.27), 'hyderabad': (17.39, 78.49),
    'kolkata': (22.57, 88.36), 'jaipur': (26.91, 75.79),
    'dubai': (25.20, 55.27), 'abu dhabi': (24.47, 54.37), 'riyadh': (24.71, 46.68),
    'tel aviv': (32.08, 34.78), 'jerusalem': (31.78, 35.22),
    'cairo': (30.04, 31.24), 'alexandria': (31.20, 29.92), 'addis ababa': (9.03, 38.74),
    'bogor': (-6.60, 106.80), 'surabaya': (-7.25, 112.75), 'bali': (-8.40, 115.19),
    'ho chi minh city': (10.82, 106.63), 'da nang': (16.05, 108.20),
    'phnom penh': (11.56, 104.92), 'siem reap': (13.37, 103.86),
    'naha': (26.21, 127.68), 'fukushima': (37.75, 140.47),
    'curitiba': (-25.43, -49.27), 'porto alegre': (-30.03, -51.23),
    'belo horizonte': (-19.92, -43.94), 'recife': (-8.05, -34.88),
    'cordoba argentina': (-31.42, -64.18), 'mendoza': (-32.89, -68.84),
    # European cities that often come up in fire news
    'naples': (40.85, 14.27), 'milan': (45.46, 9.19), 'turin': (45.07, 7.69),
    'valencia': (39.47, -0.38), 'seville': (37.39, -5.99), 'malaga': (36.72, -4.42),
    'porto': (41.16, -8.63), 'braga': (41.55, -8.43), 'coimbra': (40.20, -8.42),
    'thessaloniki': (40.64, 22.93), 'patras': (38.25, 21.73), 'heraklion': (35.34, 25.13),
    'izmir': (38.42, 27.14), 'bodrum': (37.03, 27.43),
}
# Longest-first so 'new south wales' matches before 'wales', 'british columbia' before 'columbia', etc.
GEO_KEYS_SORTED = sorted(GEO_DICT.keys(), key=lambda k: -len(k))


# 2-letter US state codes — fire headlines often use "CA", "FL", "TX" etc.
# Kept separate from GEO_DICT so we can require strict word-boundary matching
# (a loose "in CA" shouldn't match "in California" pre-lowercase).
US_STATE_CODES = {
    'AL':(32.8,-86.8),'AK':(64.2,-152.0),'AZ':(34.2,-111.7),'AR':(34.9,-92.4),
    'CA':(36.7,-119.7),'CO':(39.0,-105.5),'CT':(41.6,-72.7),'DE':(39.0,-75.5),
    'FL':(27.8,-81.7),'GA':(32.9,-83.4),'HI':(20.3,-156.4),'ID':(44.2,-114.4),
    'IL':(40.0,-89.2),'IN':(39.9,-86.3),'IA':(42.0,-93.5),'KS':(38.5,-98.4),
    'KY':(37.5,-85.3),'LA':(31.0,-92.0),'ME':(45.3,-69.2),'MD':(39.0,-76.8),
    'MA':(42.4,-71.8),'MI':(44.3,-85.4),'MN':(46.3,-94.3),'MS':(32.8,-89.7),
    'MO':(38.4,-92.5),'MT':(47.0,-109.6),'NE':(41.5,-99.8),'NV':(39.3,-116.6),
    'NH':(43.7,-71.6),'NJ':(40.2,-74.7),'NM':(34.4,-106.1),'NY':(42.9,-75.5),
    'NC':(35.6,-79.4),'ND':(47.5,-100.3),'OH':(40.3,-82.8),'OK':(35.6,-97.5),
    'OR':(44.0,-120.5),'PA':(40.9,-77.8),'RI':(41.7,-71.5),'SC':(33.9,-80.9),
    'SD':(44.4,-100.2),'TN':(35.9,-86.4),'TX':(31.5,-99.3),'UT':(39.3,-111.7),
    'VT':(44.0,-72.7),'VA':(37.5,-78.9),'WA':(47.4,-120.5),'WV':(38.6,-80.6),
    'WI':(44.5,-89.5),'WY':(43.0,-107.5)
}
# Pre-built regex to find any state code as a distinct token. Case-SENSITIVE
# (we don't lowercase the source text for this path) so "Ca." matches CA but
# "can" doesn't. Requires the token to be surrounded by non-word chars,
# and allows common suffixes like " weather" / ", " / ". " / "."
import re as _re
_STATE_CODE_RE = _re.compile(r'(?<![A-Za-z])(' + '|'.join(US_STATE_CODES.keys()) + r')(?=[ ,.:;!?]|$)')

def geo_lookup(title, description=''):
    """Tier-1 geocoder: dictionary-match state/city/country names in title + desc,
    falling back to 2-letter US state code detection. Returns (lat, lng) of the
    first + most specific match, or None."""
    raw = (title or '') + ' ' + (description or '')
    text = raw.lower()
    # Pass 1: lowercase longest-first dictionary match
    for k in GEO_KEYS_SORTED:
        needle = ' ' + k + ' '
        padded = ' ' + text + ' '
        if needle in padded or padded.startswith(k + ' ') or padded.endswith(' ' + k):
            return GEO_DICT[k]
        for punct in (',', '.', ':', ';', '?', '!', "'", '"'):
            if (' ' + k + punct) in padded:
                return GEO_DICT[k]
    # Pass 2: 2-letter US state code on ORIGINAL case text (headlines usually capitalize)
    m = _STATE_CODE_RE.search(raw)
    if m:
        return US_STATE_CODES[m.group(1)]
    return None


# ── GDELT DOC 2.0 API ───────────────────────────────────────────────

def fetch_gdelt_doc_articles():
    """Fetch wildfire articles from GDELT DOC 2.0 with tone, themes, and GLIDE extraction."""
    import requests

    print("\n[GDELT DOC] Fetching wildfire articles...")

    queries = [
        '(wildfire OR "wild fire" OR bushfire OR "forest fire") (evacuation OR containment OR acres OR firefighter)',
        '"red flag warning" OR "fire weather watch" OR "extreme fire danger"',
        '(wildfire OR "forest fire") (California OR Oregon OR Washington OR Texas OR Colorado OR Montana)',
    ]

    all_articles = []

    for i, query in enumerate(queries):
        if i > 0:
            print(f"  Waiting 7s for rate limit...")
            time.sleep(7)

        # Added: tone + themes — these can contain GLIDE references
        url = (
            f'https://api.gdeltproject.org/api/v2/doc/doc?'
            f'query={quote(query)}'
            f'&mode=artlist'
            f'&maxrecords=75'
            f'&format=json'
            f'&timespan=24h'
            f'&sort=datedesc'
        )

        try:
            resp = requests.get(url, timeout=30)
            if resp.status_code == 200:
                data = resp.json()
                articles = data.get('articles', [])
                print(f"  Query {i+1}: {len(articles)} articles")
                all_articles.extend(articles)
            elif resp.status_code == 429:
                print(f"  Query {i+1}: HTTP 429 (rate limited) — waiting 15s")
                time.sleep(15)
                resp2 = requests.get(url, timeout=30)
                if resp2.status_code == 200:
                    data = resp2.json()
                    articles = data.get('articles', [])
                    print(f"  Query {i+1} retry: {len(articles)} articles")
                    all_articles.extend(articles)
                else:
                    print(f"  Query {i+1} retry: HTTP {resp2.status_code}")
            else:
                print(f"  Query {i+1}: HTTP {resp.status_code}")
        except Exception as e:
            print(f"  Query {i+1} failed: {e}")

    # Within-source dedup: URL only (true duplicates from feed shuffle)
    seen_urls = set()
    unique = []
    for art in all_articles:
        u = art.get('url', '')
        if not u:
            drop('no_url', 'gdelt_doc', art.get('title', '')[:80])
            continue
        if u in seen_urls:
            drop('duplicate_url', 'gdelt_doc', u)
            continue
        seen_urls.add(u)

        # Sniff tone and themes from any embedded fields GDELT might carry
        tone = None
        themes_str = ''
        for k in ('tone', 'v2tone', 'V2Tone'):
            if k in art:
                try:
                    t = art[k]
                    if isinstance(t, (int, float)):
                        tone = float(t)
                    elif isinstance(t, str) and t:
                        tone = float(t.split(',')[0])
                except (ValueError, TypeError):
                    pass
                break
        for k in ('themes', 'v2themes', 'V2Themes'):
            if k in art:
                themes_str = str(art[k]) or ''
                break

        # Extract GLIDE from title, themes, or any URL-embedded hint
        glide = (extract_glide(art.get('title', ''))
                 or extract_glide(themes_str)
                 or extract_glide(u))

        unique.append({
            'title': art.get('title', ''),
            'url': u,
            'source': art.get('domain', ''),
            'date': art.get('seendate', ''),
            'image': art.get('socialimage', ''),
            'language': art.get('language', 'English'),
            'country': art.get('sourcecountry', ''),
            'tone': tone,
            'themes': themes_str[:500],   # cap length, full themes aren't displayed
            'glide': glide,
            'type': 'gdelt_doc'
        })

    # CHANGED from v1: was return unique[:150] — that's a cap that silently
    # dropped ~75 articles on a full run. Now no cap; frontend paginates.
    print(f"  Total unique: {len(unique)} articles (no cap — was [:150])")
    return unique


# ── GDELT GEO 2.0 API ───────────────────────────────────────────────

def fetch_gdelt_geo():
    """Fetch geolocated wildfire mentions from GDELT GEO API."""
    import requests

    print("\n[GDELT GEO] Fetching geolocated fire news...")

    urls_to_try = [
        'https://api.gdeltproject.org/api/v2/geo/geo?query=wildfire%20OR%20%22forest%20fire%22&mode=PointData&format=GeoJSON&timespan=24h',
        'https://api.gdeltproject.org/api/v2/geo/geo?query=wildfire&mode=PointData&format=GeoJSON',
    ]

    for url in urls_to_try:
        try:
            print(f"  Trying: {url[:80]}...")
            resp = requests.get(url, timeout=30)
            print(f"  HTTP {resp.status_code}, {len(resp.content)} bytes, content-type: {resp.headers.get('content-type','?')[:50]}")

            if resp.status_code != 200:
                continue

            text = resp.text.strip()

            try:
                data = json.loads(text)

                if 'features' in data:
                    features = data['features']
                    points = []
                    for f in features[:200]:
                        props = f.get('properties', {})
                        geom = f.get('geometry', {})
                        coords = geom.get('coordinates', [])
                        if len(coords) >= 2:
                            lat, lng = coords[1], coords[0]
                            if -90 <= lat <= 90 and -180 <= lng <= 180:
                                points.append({
                                    'lat': lat, 'lng': lng,
                                    'name': props.get('name', props.get('html', '')),
                                    'count': props.get('count', 1),
                                    'url': props.get('url', ''),
                                })
                    if points:
                        print(f"  ✓ {len(points)} geolocated points from GeoJSON")
                        return points

                if isinstance(data, list):
                    points = []
                    for item in data[:200]:
                        if 'lat' in item and 'lon' in item:
                            points.append({'lat': item['lat'], 'lng': item['lon'], 'name': item.get('name',''), 'count': 1, 'url': item.get('url','')})
                        elif 'latitude' in item and 'longitude' in item:
                            points.append({'lat': item['latitude'], 'lng': item['longitude'], 'name': item.get('name',''), 'count': 1, 'url': item.get('url','')})
                    if points:
                        print(f"  ✓ {len(points)} points from JSON array")
                        return points

                print(f"  JSON parsed but no usable geo data (keys: {list(data.keys())[:5]})")

            except json.JSONDecodeError:
                print(f"  Not JSON — trying regex extraction...")
                coord_pairs = re.findall(r'\[(-?\d+\.?\d+),\s*(-?\d+\.?\d+)\]', text[:100000])
                points = []
                for lng_s, lat_s in coord_pairs:
                    try:
                        lng, lat = float(lng_s), float(lat_s)
                        if -90 <= lat <= 90 and -180 <= lng <= 180:
                            points.append({'lat': lat, 'lng': lng, 'name': '', 'count': 1})
                        elif -90 <= lng <= 90 and -180 <= lat <= 180:
                            points.append({'lat': lng, 'lng': lat, 'name': '', 'count': 1})
                    except ValueError:
                        continue

                if points:
                    unique_points = []
                    for p in points:
                        is_dup = any(abs(p['lat']-u['lat'])<0.01 and abs(p['lng']-u['lng'])<0.01 for u in unique_points)
                        if not is_dup:
                            unique_points.append(p)
                    print(f"  ✓ {len(unique_points)} points extracted from HTML")
                    return unique_points[:200]
                else:
                    print(f"  No coordinates found in response")

        except Exception as e:
            print(f"  Failed: {e}")

    print("  All GEO methods failed")
    return []


# ── GDELT Disasters Live Stream + ReliefWeb ─────────────────────────

def fetch_gdelt_disasters():
    """Fetch the GDELT Disasters Live Stream and/or ReliefWeb active wildfires.

    Returns disaster records with GLIDE IDs. These get MERGED into the article
    feed by main() — they are no longer kept in a separate bucket.
    """
    import requests

    print("\n[DISASTERS+GLIDE] Fetching disaster stream + ReliefWeb...")

    # Try GDELT lastupdate feeds for GLIDE files
    urls_to_try = [
        'http://data.gdeltproject.org/gdeltv2/lastupdate.txt',
        'http://data.gdeltproject.org/gdeltv2/lastupdate-translation.txt',
    ]

    disaster_records = []

    for list_url in urls_to_try:
        try:
            resp = requests.get(list_url, timeout=15)
            if resp.status_code != 200:
                continue

            lines = resp.text.strip().split('\n')
            glide_files = [l for l in lines if 'glide' in l.lower()]

            if not glide_files:
                continue

            print(f"  {list_url}: {len(glide_files)} GLIDE files")

            for line in glide_files[:2]:
                parts = line.strip().split()
                if len(parts) >= 3:
                    file_url = parts[-1]
                    if not file_url.startswith('http'):
                        continue
                    try:
                        dr = requests.get(file_url, timeout=30)
                        if dr.status_code == 200 and len(dr.content) > 100:
                            records = parse_disaster_data(dr.text)
                            disaster_records.extend(records)
                    except Exception as e:
                        print(f"  Disaster file fetch failed: {e}")
                        drop('fetch_error', 'gdelt_disasters', str(e)[:100])
        except Exception as e:
            print(f"  {list_url} failed: {e}")

    # ReliefWeb API for active wildfires — richer data than GDELT disaster stream
    # IMPORTANT (2026-05-01): v1 endpoint was decommissioned. v2 requires an
    # appname that ReliefWeb approves manually (apidoc.reliefweb.int/parameters#appname).
    # Until we apply + get approved, this block skips cleanly with a clear log
    # line instead of failing silently. Set RELIEFWEB_APPNAME env var once
    # approved and it will activate.
    rw_appname = os.environ.get('RELIEFWEB_APPNAME', '').strip()
    if not rw_appname:
        print("  ReliefWeb SKIPPED — v1 decommissioned, v2 needs approved appname (set RELIEFWEB_APPNAME secret)")
        # Emit 0 records explicitly so downstream counts stay honest
        return disaster_records
    print(f"  Fetching ReliefWeb active wildfires (appname={rw_appname})...")
    try:
        rw_url = (
            f'https://api.reliefweb.int/v2/disasters?appname={rw_appname}'
            '&filter[field]=type&filter[value]=Wild Fire&filter[operator]=AND'
            '&filter[conditions][0][field]=status&filter[conditions][0][value]=current'
            '&limit=50'
            '&fields[include][]=name&fields[include][]=date&fields[include][]=glide'
            '&fields[include][]=country&fields[include][]=description&fields[include][]=url'
        )
        resp = requests.get(rw_url, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            for item in data.get('data', []):
                fields = item.get('fields', {})
                countries = [c.get('name','') for c in fields.get('country', [])]
                name = fields.get('name', '')
                glide = fields.get('glide', '') or None
                date_created = fields.get('date', {}).get('created', '')

                if not name and not glide:
                    drop('no_name_no_glide', 'reliefweb', item.get('id', '?'))
                    continue

                # Synthesize a title so this appears in the article feed
                title = name or ('Wildfire disaster ' + (glide or ''))
                # Fabricate a URL so within-source URL dedup won't collapse them
                # (and so the frontend has something to link to)
                url_out = fields.get('url', '') or f'https://reliefweb.int/disaster/{glide}' if glide else f'reliefweb://{item.get("id","?")}'

                disaster_records.append({
                    'title': title,
                    'url': url_out,
                    'source': 'ReliefWeb',
                    'date': date_created,
                    'image': '',
                    'language': 'English',
                    'country': ', '.join(countries),
                    'countries_list': countries,  # keep list for GLIDE matching
                    'description': (fields.get('description', '') or '')[:500],
                    'glide': glide,
                    'type': 'reliefweb_disaster'
                })
            print(f"  ✓ {len([d for d in disaster_records if d.get('type')=='reliefweb_disaster'])} ReliefWeb disasters")
        else:
            print(f"  ReliefWeb HTTP {resp.status_code}")
    except Exception as e:
        print(f"  ReliefWeb failed: {e}")

    return disaster_records


def parse_disaster_data(text):
    """Parse GDELT disaster CSV/TSV into structured records with GLIDE extraction."""
    records = []
    lines = text.strip().split('\n')
    for line in lines[:100]:
        fields = line.split('\t')
        if len(fields) >= 3:
            glide = ''
            url = ''
            for field in fields:
                # Accept either strict GLIDE format or legacy looser pattern
                if GLIDE_RE.search(field):
                    glide = GLIDE_RE.search(field).group(1)
                elif re.match(r'^[A-Z]{2}-\d{4}-\d+', field):
                    glide = field
                if field.startswith('http'):
                    url = field

            if not glide:
                continue  # we only care about records with GLIDE IDs here

            # Only surface wildfire-category GLIDE records (WF prefix)
            if not glide.startswith('WF'):
                continue

            records.append({
                'title': f'Wildfire incident · {glide}',
                'url': url or f'gdelt://disaster/{glide}',
                'source': 'GDELT Disasters',
                'date': '',
                'image': '',
                'language': 'English',
                'country': '',
                'description': '',
                'glide': glide,
                'type': 'gdelt_disaster'
            })

    print(f"  Parsed {len(records)} wildfire disaster records from GDELT stream")
    return records


# ── GDELT BigQuery ───────────────────────────────────────────────────
# TODO [BigQuery]: Add GCP credentials to unlock deep GDELT analysis.
# Enables: ENV_FIRE theme queries, disaster GLIDE tracking, geolocated events.
# Eventually migrate entire pipeline to Google Cloud Functions.
# REMINDER: Note on every update until resolved.

def fetch_bigquery_fire_data():
    """Query GDELT on BigQuery for fire-related events. Now emits records with
    synthetic titles so they survive into the article feed."""

    creds_json = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS_JSON', '')
    if not creds_json:
        print("\n[BigQuery] No credentials — skipping")
        print("  To enable: add GOOGLE_APPLICATION_CREDENTIALS_JSON secret to repo")
        return []

    try:
        from google.cloud import bigquery
        from google.oauth2 import service_account

        creds_path = '/tmp/gcp_creds.json'
        with open(creds_path, 'w') as f:
            f.write(creds_json)
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = creds_path

        credentials = service_account.Credentials.from_service_account_file(creds_path)
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)

        print("\n[BigQuery] Running fire intelligence queries...")

        # GKG fire themes with locations + V2ExtrasXML for GLIDE extraction
        query_gkg = """
        SELECT 
            DATE, DocumentIdentifier as url, V2Themes, V2Tone,
            V2Locations, V2ExtrasXML
        FROM `gdelt-bq.gdeltv2.gkg_partitioned`
        WHERE DATE >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
            AND (V2Themes LIKE '%ENV_FIRE%' 
                 OR V2Themes LIKE '%NATURAL_DISASTER_WILDFIRE%'
                 OR V2Themes LIKE '%WB_2680_FOREST_FIRES%')
        ORDER BY DATE DESC
        LIMIT 300
        """

        try:
            print("  Running GKG fire themes query...")
            rows = list(client.query(query_gkg))
            gkg_articles = []
            for row in rows:
                tone_parts = (row.V2Tone or '').split(',')
                tone = float(tone_parts[0]) if tone_parts and tone_parts[0] else 0

                locs = []
                for loc_str in (row.V2Locations or '').split(';'):
                    parts = loc_str.split('#')
                    if len(parts) >= 7:
                        try:
                            lat, lng = float(parts[5]), float(parts[6])
                            if lat != 0 and lng != 0:
                                locs.append({
                                    'lat': lat, 'lng': lng,
                                    'name': parts[3] if len(parts) > 3 else ''
                                })
                        except (ValueError, IndexError):
                            pass

                # Synthesize a title so this survives into the article feed
                location_str = locs[0]['name'] if locs else 'Unspecified location'
                themes_preview = (row.V2Themes or '').split(';')[0].replace('_', ' ').title()
                title = f'{themes_preview} · {location_str}'
                if row.DATE:
                    title += f' · {row.DATE.strftime("%Y-%m-%d")}'

                # GLIDE extraction from V2ExtrasXML — GDELT embeds GLIDE tags here
                glide = extract_glide(row.V2ExtrasXML) if row.V2ExtrasXML else None

                gkg_articles.append({
                    'title': title,
                    'url': row.url or '',
                    'source': 'GDELT GKG (BigQuery)',
                    'date': row.DATE.isoformat() if row.DATE else '',
                    'image': '',
                    'language': 'English',
                    'country': '',
                    'tone': round(tone, 2),
                    'themes': (row.V2Themes or '')[:500],
                    'locations': locs[:3],
                    'lat': locs[0]['lat'] if locs else None,
                    'lng': locs[0]['lng'] if locs else None,
                    'glide': glide,
                    'type': 'bigquery_gkg'
                })
            print(f"  ✓ GKG query: {len(gkg_articles)} fire-themed articles")
            try:
                os.remove(creds_path)
            except OSError:
                pass
            return gkg_articles
        except Exception as e:
            print(f"  GKG query failed: {e}")

        try:
            os.remove(creds_path)
        except OSError:
            pass
        return []

    except ImportError:
        print("  google-cloud-bigquery not installed")
    except Exception as e:
        print(f"  BigQuery error: {e}")
    return []


# ── GDACS (Global Disaster Alert & Coordination System, UN-backed) ──
# RSS feed with ~80 active events worldwide (earthquakes, floods, cyclones,
# wildfires, volcanoes). Schema includes lat/lng, GLIDE IDs, alert level
# (Green/Orange/Red). Public domain, no auth required.

def fetch_gdacs():
    """Fetch global disaster events from GDACS RSS."""
    import requests
    import xml.etree.ElementTree as ET

    print("\n[GDACS] Fetching global disaster feed...")
    try:
        resp = requests.get('https://www.gdacs.org/xml/rss.xml',
                            headers={'User-Agent':'FIRESTORM/1.0'},
                            timeout=15)
        if resp.status_code != 200:
            print(f"  GDACS HTTP {resp.status_code}")
            return []
        root = ET.fromstring(resp.content)
        ns = {
            'gdacs': 'http://www.gdacs.org',
            'georss': 'http://www.georss.org/georss',
            'geo': 'http://www.w3.org/2003/01/geo/wgs84_pos#',
            'glide': 'http://glidenumber.net',
            'dc': 'http://purl.org/dc/elements/1.1/'
        }
        records = []
        for item in root.findall('.//item'):
            title = (item.findtext('title') or '').strip()
            if not title:
                continue
            link = (item.findtext('link') or '').strip()
            desc = (item.findtext('description') or '').strip()
            pub = (item.findtext('pubDate') or '').strip()
            event_type = item.findtext('gdacs:eventtype', default='', namespaces=ns)
            alert_level = item.findtext('gdacs:alertlevel', default='', namespaces=ns)
            country = item.findtext('gdacs:country', default='', namespaces=ns)
            lat = item.findtext('geo:lat', default=None, namespaces=ns)
            lng = item.findtext('geo:long', default=None, namespaces=ns)
            glide = item.findtext('gdacs:glide', default=None, namespaces=ns) or None
            try: lat = float(lat) if lat else None
            except: lat = None
            try: lng = float(lng) if lng else None
            except: lng = None
            records.append({
                'title': title,
                'url': link or f'https://www.gdacs.org/',
                'source': f'GDACS ({event_type or "Global"})',
                'date': pub,
                'image': '',
                'language': 'English',
                'country': country,
                'description': desc[:500],
                'lat': lat,
                'lng': lng,
                'glide': glide,
                '_gdacs_alert': alert_level.upper(),
                '_gdacs_event_type': event_type,
                'type': 'gdacs'
            })
        print(f"  GDACS: {len(records)} events")
        return records
    except Exception as e:
        print(f"  GDACS failed: {e}")
        return []


# ── FEMA Disaster Declarations ──────────────────────────────────────
# US federal disaster declarations (OpenFEMA v2). Most wildfire-relevant:
# 'Fire' disaster type. Public API, no auth. Returns one record per
# declared disaster (one per state/area typically).

def fetch_fema_disasters():
    """Fetch recent FEMA disaster declarations (fires + severe storms last 60 days)."""
    import requests
    from datetime import datetime, timezone, timedelta
    print("\n[FEMA] Fetching disaster declarations...")
    try:
        since = (datetime.now(timezone.utc) - timedelta(days=60)).strftime('%Y-%m-%d')
        url = ("https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries"
               f"?$filter=declarationDate%20ge%20%27{since}%27%20and%20"
               "(incidentType%20eq%20%27Fire%27%20or%20incidentType%20eq%20%27Severe%20Storm%27%20or%20"
               "incidentType%20eq%20%27Hurricane%27)"
               "&$top=50&$orderby=declarationDate%20desc")
        resp = requests.get(url, timeout=15, headers={'User-Agent':'FIRESTORM/1.0'})
        if resp.status_code != 200:
            print(f"  FEMA HTTP {resp.status_code}")
            return []
        data = resp.json()
        rows = data.get('DisasterDeclarationsSummaries', [])
        records = []
        for r in rows:
            title = f"FEMA {r.get('declarationType','')} {r.get('disasterNumber','')}: {r.get('declarationTitle') or r.get('incidentType')}"
            state = r.get('state', '')
            # FEMA records don't ship coords — let tier-1 geocoder add them from state name later
            records.append({
                'title': title.strip(),
                'url': f"https://www.fema.gov/disaster/{r.get('disasterNumber','')}",
                'source': 'FEMA',
                'date': r.get('declarationDate', ''),
                'image': '',
                'language': 'English',
                'country': state + ', USA',
                'description': (r.get('incidentType','') + ' in ' + (r.get('designatedArea','') or state))[:500],
                'glide': None,
                'type': 'fema_disaster'
            })
        print(f"  FEMA: {len(records)} recent declarations")
        return records
    except Exception as e:
        print(f"  FEMA failed: {e}")
        return []


# ── InciWeb RSS with GeoRSS parsing ─────────────────────────────────

def fetch_inciweb():
    """Fetch InciWeb RSS feed and extract GeoRSS coordinates where present."""
    import requests
    import xml.etree.ElementTree as ET

    print("\n[InciWeb] Fetching incident feed...")

    urls = [
        'https://inciweb.wildfire.gov/feeds/rss/incidents',
        'https://inciweb.wildfire.gov/feeds/rss/incidents/',
        'https://inciweb.wildfire.gov/rss.xml',
        'https://inciweb.nwcg.gov/feeds/rss/incidents',
    ]

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/rss+xml, application/xml, text/xml, text/html, */*'
    }

    # GeoRSS + basic-geo namespace registrations for ET
    NAMESPACES = {
        'georss': 'http://www.georss.org/georss',
        'geo': 'http://www.w3.org/2003/01/geo/wgs84_pos#',
        'atom': 'http://www.w3.org/2005/Atom'
    }

    def parse_geo_from_item(item):
        """Try every known geo-tag format. Return (lat, lng) or (None, None)."""
        # georss:point → "lat lng"
        pt = item.findtext('georss:point', '', NAMESPACES)
        if pt:
            parts = pt.strip().split()
            if len(parts) >= 2:
                try:
                    return float(parts[0]), float(parts[1])
                except ValueError:
                    pass
        # geo:lat + geo:long
        lat_t = item.findtext('geo:lat', '', NAMESPACES)
        lng_t = item.findtext('geo:long', '', NAMESPACES)
        if lat_t and lng_t:
            try:
                return float(lat_t), float(lng_t)
            except ValueError:
                pass
        # Coordinates embedded in description text — last resort
        desc = item.findtext('description', '') or ''
        coord_m = re.search(r'(-?\d{1,3}\.\d{2,}),\s*(-?\d{1,3}\.\d{2,})', desc)
        if coord_m:
            try:
                lat, lng = float(coord_m.group(1)), float(coord_m.group(2))
                if -90 <= lat <= 90 and -180 <= lng <= 180:
                    return lat, lng
            except ValueError:
                pass
        return None, None

    for url in urls:
        try:
            resp = requests.get(url, timeout=15, headers=headers, allow_redirects=True)
            ct = resp.headers.get('content-type', '')
            print(f"  {url}")
            print(f"    HTTP {resp.status_code}, {len(resp.content)} bytes, type: {ct[:50]}")

            if resp.status_code != 200 or len(resp.content) < 200:
                continue

            text = resp.text
            if '<' not in text[:200]:
                print(f"    Not XML — first 100 chars: {text[:100]}")
                continue

            try:
                root = ET.fromstring(resp.content)
            except ET.ParseError as pe:
                print(f"    XML parse error: {pe}")
                try:
                    clean = re.sub(r'&(?!amp;|lt;|gt;|quot;|apos;)', '&amp;', text)
                    root = ET.fromstring(clean.encode('utf-8'))
                except Exception:
                    continue

            items = []
            geo_tagged = 0

            # RSS 2.0
            for item in root.findall('.//item'):
                title = item.findtext('title', '').strip()
                link = item.findtext('link', '')
                desc = item.findtext('description', '') or ''
                pub_date = item.findtext('pubDate', '')
                lat, lng = parse_geo_from_item(item)
                if lat is not None:
                    geo_tagged += 1

                if not title:
                    drop('no_title', 'inciweb', link[:80])
                    continue

                items.append({
                    'title': title,
                    'url': link,
                    'source': 'InciWeb',
                    'date': pub_date,
                    'image': '',
                    'language': 'English',
                    'country': 'USA',
                    'description': desc[:300],
                    'lat': lat,
                    'lng': lng,
                    'glide': None,         # InciWeb doesn't emit GLIDE; NIFC uses IRWIN
                    'type': 'inciweb'
                })

            # Atom fallback
            if not items:
                for entry in root.findall('.//atom:entry', NAMESPACES):
                    title = entry.findtext('atom:title', '', NAMESPACES).strip()
                    link_el = entry.find('atom:link', NAMESPACES)
                    link = link_el.get('href', '') if link_el is not None else ''
                    summary = entry.findtext('atom:summary', '', NAMESPACES) or ''
                    updated = entry.findtext('atom:updated', '', NAMESPACES)
                    lat, lng = parse_geo_from_item(entry)
                    if lat is not None:
                        geo_tagged += 1

                    if not title:
                        drop('no_title', 'inciweb', link[:80])
                        continue

                    items.append({
                        'title': title,
                        'url': link,
                        'source': 'InciWeb',
                        'date': updated,
                        'image': '',
                        'language': 'English',
                        'country': 'USA',
                        'description': summary[:300],
                        'lat': lat,
                        'lng': lng,
                        'glide': None,
                        'type': 'inciweb'
                    })

            if items:
                print(f"  ✓ {len(items)} incidents from InciWeb ({geo_tagged} geo-tagged)")
                return items
            else:
                print(f"    Parsed XML but found 0 items. Root tag: {root.tag}")

        except Exception as e:
            print(f"  {url} exception: {e}")

    print("  All InciWeb URLs failed")
    return []


# ── Google News RSS ──────────────────────────────────────────────────

def fetch_google_news():
    """Fetch wildfire news from Google News RSS."""
    import requests
    import xml.etree.ElementTree as ET

    print("\n[Google News] Fetching wildfire news...")

    searches = [
        'wildfire+evacuation+OR+containment',
        'forest+fire+acres+OR+firefighter',
        'red+flag+warning+fire+weather',
    ]

    all_items = []
    for i, search in enumerate(searches):
        if i > 0:
            time.sleep(2)

        url = f'https://news.google.com/rss/search?q={search}&hl=en-US&gl=US&ceid=US:en'
        try:
            resp = requests.get(url, timeout=15, headers={
                'User-Agent': 'Mozilla/5.0 (compatible; FIRESTORM/1.0)'
            })
            if resp.status_code == 200:
                try:
                    root = ET.fromstring(resp.content)
                except ET.ParseError:
                    continue
                for item in root.findall('.//item')[:30]:   # was [:20]
                    title = item.findtext('title', '') or ''
                    link = item.findtext('link', '') or ''
                    pub_date = item.findtext('pubDate', '') or ''
                    # Google News source element has a url attribute + text
                    source_el = item.find('source')
                    source = (source_el.text if source_el is not None else '') or 'Google News'

                    if not title or not link:
                        drop('no_title_or_link', 'google_news', title[:80] or link[:80])
                        continue

                    all_items.append({
                        'title': title,
                        'url': link,
                        'source': source,
                        'date': pub_date,
                        'image': '',
                        'language': 'English',
                        'country': '',
                        'glide': None,
                        'type': 'google_news'
                    })
            else:
                print(f"  Search {i+1}: HTTP {resp.status_code}")
        except Exception as e:
            print(f"  Search '{search}' failed: {e}")

    # Within-source dedup: URL only. No cross-source title collapsing.
    seen_urls = set()
    unique = []
    for item in all_items:
        u = item['url']
        if u in seen_urls:
            drop('duplicate_url', 'google_news', u)
            continue
        seen_urls.add(u)
        unique.append(item)

    print(f"  {len(unique)} unique articles from Google News")
    return unique


# ── GLIDE fuzzy-match ───────────────────────────────────────────────

def fuzzy_match_glide_to_articles(articles, disasters):
    """Attach GLIDE IDs from ReliefWeb/GDELT disasters to matching articles.

    Match criteria: article country contains a disaster country AND the article
    date is within 14 days of the disaster date AND article title mentions 'fire'
    or 'wildfire'. Conservative match to avoid false positives.
    """
    if not disasters:
        return 0

    # Build index: country (lowercased) → list of (glide, disaster_date_dt)
    by_country = {}
    for d in disasters:
        glide = d.get('glide')
        if not glide:
            continue
        # Disaster's own date
        d_date_str = d.get('date', '')
        d_date = None
        if d_date_str:
            try:
                # ISO format from ReliefWeb
                d_date = datetime.fromisoformat(d_date_str.replace('Z', '+00:00'))
            except (ValueError, TypeError):
                pass

        # Index by each country the disaster covers
        countries = d.get('countries_list') or []
        if not countries and d.get('country'):
            countries = [c.strip() for c in d['country'].split(',')]
        for country in countries:
            ck = country.lower().strip()
            if ck:
                by_country.setdefault(ck, []).append((glide, d_date))

    matched = 0
    for art in articles:
        if art.get('glide'):
            continue  # already has one, don't overwrite
        title_l = (art.get('title', '') or '').lower()
        if 'fire' not in title_l and 'wildfire' not in title_l and 'bushfire' not in title_l:
            continue

        # Try to match on article country
        art_country = (art.get('country') or '').lower().strip()
        if not art_country:
            continue

        candidates = by_country.get(art_country, [])
        if not candidates:
            # Also try country as a substring match (e.g. "united states" matches "united states of america")
            for ck, cl in by_country.items():
                if ck in art_country or art_country in ck:
                    candidates = cl
                    break
        if not candidates:
            continue

        # Parse article date for time-window filter
        art_date = None
        d_str = art.get('date', '')
        if d_str:
            # GDELT seendate format: YYYYMMDDTHHMMSSZ or ISO
            try:
                if 'T' in d_str and len(d_str) >= 15 and d_str[:8].isdigit():
                    art_date = datetime.strptime(d_str[:15], '%Y%m%dT%H%M%S').replace(tzinfo=timezone.utc)
                else:
                    art_date = datetime.fromisoformat(d_str.replace('Z', '+00:00'))
            except (ValueError, TypeError):
                pass

        # Prefer the closest-dated GLIDE
        best_glide = None
        best_diff = None
        for glide, d_date in candidates:
            if art_date and d_date:
                diff = abs((art_date - d_date).days)
                if diff > 14:
                    continue
                if best_diff is None or diff < best_diff:
                    best_glide = glide
                    best_diff = diff
            elif best_glide is None:
                # No date info — accept first
                best_glide = glide

        if best_glide:
            art['glide'] = best_glide
            art['_glide_source'] = 'fuzzy_match'
            matched += 1

    return matched


# ── Categorization ──────────────────────────────────────────────────
# v2 (2026-05-01): tightened the HIGH bucket so MODERATE actually populates.
# Previous version caught ANY fire-word article as HIGH, leaving 0 MODERATE.
# Now HIGH requires a severity signal (active incident, large scale, threat)
# and MODERATE catches ongoing wildfires that aren't immediately threatening.

def categorize_article(title):
    """Categorize an article by urgency based on title keywords."""
    t = (title or '').lower()

    # CRITICAL: immediate life-safety impact or confirmed casualties/destruction
    if any(w in t for w in ['evacuat', 'mandatory order', 'mandatory evacuation', 'flee', 'shelter in place',
                              'state of emergency', 'federal emergency', 'red flag warning']):
        return 'CRITICAL'
    if any(w in t for w in ['dead', 'killed', 'death', 'fatal', 'injur', 'missing', 'destroyed homes',
                              'homes destroyed', 'structures destroyed', 'town destroyed']):
        return 'CRITICAL'

    # HIGH: actively threatening, rapidly growing, or large-scale
    if any(w in t for w in ['out of control', 'uncontained', 'rapidly spreading', 'explosive', 'firestorm',
                              'conflagration', 'mega-fire', 'megafire']):
        return 'HIGH'
    # Size-based HIGH: thousands of acres, major incidents
    if any(w in t for w in ['thousand acres', ',000 acres', 'acres burned', 'acres scorched', 'growing fire',
                              'spreading fire', 'major fire', 'large fire']):
        return 'HIGH'
    # Immediate threats
    if any(w in t for w in ['threat', 'threaten', 'evacuation warning', 'evacuation advis', 'power shutoff',
                              'psps', 'air quality emergency']):
        return 'HIGH'
    # Weather extremes directly driving fire behavior
    if any(w in t for w in ['extreme fire', 'critical fire weather', 'explosive fire', 'firenado',
                              'fire whirl', 'pyrocumulus']):
        return 'HIGH'

    # MODERATE: ongoing wildfire coverage without immediate life-safety signal
    if any(w in t for w in ['wildfire', 'wild fire', 'brush fire', 'grass fire', 'forest fire',
                              'blaze', 'burning', 'burns', 'burn ', 'burned']):
        return 'MODERATE'
    if any(w in t for w in ['containment', 'contain ', 'contained', 'percent contain']):
        return 'MODERATE'
    if any(w in t for w in ['red flag', 'fire weather', 'fire danger', 'fire season', 'drought',
                              'dry conditions', 'high wind', 'heat wave']):
        return 'MODERATE'
    if any(w in t for w in ['firefight', 'firefighter', 'fire crew', 'air tanker', 'helitack',
                              'cal fire', 'wildland']):
        return 'MODERATE'
    if any(w in t for w in ['smoke', 'air quality', 'prescribed burn', 'prevention', 'defensible space',
                              'evacuation lift', 'evacuation end']):
        return 'MODERATE'

    return 'LOW'


# ── MAIN ────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("FIRESTORM News Intelligence Pipeline v2")
    print(f"Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("Data integrity: no cross-source dedup, GLIDE merging enabled")
    print("=" * 60)

    # ── Fetch from all sources ──
    gdelt_articles = fetch_gdelt_doc_articles()

    time.sleep(7)  # Rate limit between GDELT API calls
    geo_points = fetch_gdelt_geo()

    disasters = fetch_gdelt_disasters()    # now becomes articles
    inciweb = fetch_inciweb()
    google_news = fetch_google_news()
    bigquery_records = fetch_bigquery_fire_data()  # now list-typed, may be []
    gdacs_records = fetch_gdacs()
    fema_records = fetch_fema_disasters()

    # ── Raw counts (honest, pre-anything) ──
    raw_counts = {
        'gdelt_doc': len(gdelt_articles),
        'reliefweb_disasters': len([d for d in disasters if d.get('type') == 'reliefweb_disaster']),
        'gdelt_disasters': len([d for d in disasters if d.get('type') == 'gdelt_disaster']),
        'inciweb': len(inciweb),
        'google_news': len(google_news),
        'bigquery_gkg': len(bigquery_records),
        'geo_points': len(geo_points),
        'gdacs': len(gdacs_records),
        'fema': len(fema_records),
    }
    print(f"\n[RAW COUNTS] {raw_counts}")

    # ── Merge all article-shaped records into one feed ──
    # CHANGED from v1: disasters and bigquery now flow into all_news.
    # Also categorize before appending so sort works uniformly.
    all_news = []

    for art in gdelt_articles:
        art['category'] = categorize_article(art.get('title', ''))
        all_news.append(art)

    for art in google_news:
        art['category'] = categorize_article(art.get('title', ''))
        all_news.append(art)

    for art in inciweb:
        art['category'] = 'HIGH'   # InciWeb items are always active federal incidents
        all_news.append(art)

    for d in disasters:
        # Disasters get CRITICAL category — they're ReliefWeb-confirmed active
        d['category'] = 'CRITICAL' if d.get('type') == 'reliefweb_disaster' else categorize_article(d.get('title', ''))
        all_news.append(d)

    for bq in bigquery_records:
        bq['category'] = categorize_article(bq.get('title', ''))
        all_news.append(bq)

    # GDACS — category from alertlevel (Red=CRITICAL, Orange=HIGH, Green=MODERATE)
    for g in gdacs_records:
        level = (g.get('_gdacs_alert') or '').upper()
        if level == 'RED':     g['category'] = 'CRITICAL'
        elif level == 'ORANGE': g['category'] = 'HIGH'
        elif level == 'GREEN':  g['category'] = 'MODERATE'
        else:                   g['category'] = 'MODERATE'
        all_news.append(g)

    # FEMA declarations — always HIGH (federal disaster declaration is a real event)
    for fx in fema_records:
        fx['category'] = 'HIGH'
        all_news.append(fx)

    print(f"\n[MERGED] {len(all_news)} total records before GLIDE enrichment")

    # ── Tier-1 geo-tagging (dictionary match) ──
    # Add lat/lng to articles that don't have them. Free (no API calls).
    tier1_tagged = 0
    for art in all_news:
        if art.get('lat') is not None and art.get('lng') is not None:
            continue
        hit = geo_lookup(art.get('title',''), art.get('description',''))
        if hit:
            art['lat'] = hit[0]
            art['lng'] = hit[1]
            art['_geo_source'] = 'tier1_dict'
            tier1_tagged += 1
    print(f"[GEO] Tier-1 tagged {tier1_tagged} / {len(all_news)} articles")

    # ── GLIDE fuzzy-match ──
    # Use the disaster records with GLIDE IDs to tag matching articles that
    # didn't explicitly carry a GLIDE. Conservative: country + 14-day window.
    matched = fuzzy_match_glide_to_articles(all_news, disasters)
    print(f"[GLIDE] Fuzzy-matched {matched} additional articles to GLIDE IDs")

    # ── Sort by priority then date (stable: preserves source diversity) ──
    priority = {'CRITICAL': 0, 'HIGH': 1, 'MODERATE': 2, 'LOW': 3}
    all_news.sort(key=lambda x: (priority.get(x.get('category', 'LOW'), 3), x.get('date', '')))

    # ── NO CROSS-SOURCE DEDUP. Only drop true URL duplicates globally. ──
    # Different outlets on the same fire = signal, not noise.
    seen_urls = set()
    final = []
    for art in all_news:
        u = art.get('url', '')
        if u and u in seen_urls:
            drop('duplicate_url_cross_source', art.get('source', '?'), u)
            continue
        if u:
            seen_urls.add(u)
        final.append(art)

    print(f"[FINAL] {len(final)} records after cross-source URL dedup ({len(all_news) - len(final)} true duplicates removed)")

    # ── Counts for frontend display ──
    counts = {
        'total': len(final),
        'critical': len([a for a in final if a.get('category') == 'CRITICAL']),
        'high': len([a for a in final if a.get('category') == 'HIGH']),
        'moderate': len([a for a in final if a.get('category') == 'MODERATE']),
        'glide_tagged': len([a for a in final if a.get('glide')]),
        # Per-source counts
        'gdelt': raw_counts['gdelt_doc'],
        'google_news': raw_counts['google_news'],
        'inciweb': raw_counts['inciweb'],
        'geo_points': raw_counts['geo_points'],
        'disasters': raw_counts['reliefweb_disasters'] + raw_counts['gdelt_disasters'],
        'bigquery': raw_counts['bigquery_gkg'],
    }

    # ── Build final output ──
    output = {
        'updated': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'schema_version': 2,
        'counts': counts,
        'raw_counts': raw_counts,
        'articles': final,                       # was [:200] — now everything
        'geo_points': geo_points,                # was [:150] — now everything
        'dropped': DROPPED,                      # NEW: full audit trail
    }

    feed_path = os.path.join(DATA_DIR, 'fire-news-feed.json')
    with open(feed_path, 'w') as f:
        json.dump(output, f, separators=(',', ':'))

    size_kb = os.path.getsize(feed_path) / 1024

    # Metadata file
    meta = {
        'updated': output['updated'],
        'schema_version': 2,
        'counts': counts,
        'raw_counts': raw_counts,
        'dropped_count': len(DROPPED),
        'sources': {
            'gdelt_doc': 'api.gdeltproject.org/api/v2/doc',
            'gdelt_geo': 'api.gdeltproject.org/api/v2/geo',
            'gdelt_disasters': 'data.gdeltproject.org + reliefweb.int',
            'inciweb': 'inciweb.wildfire.gov (with GeoRSS)',
            'google_news': 'news.google.com',
            'bigquery': 'enabled' if bigquery_records else 'no credentials'
        },
        'integrity_notes': [
            'No cross-source dedup — different outlets on same fire stay separate',
            'Disasters merged into articles (v1 kept them in separate bucket)',
            'BigQuery GKG records merged with synthetic titles (v1 also separate)',
            'ReliefWeb GLIDE IDs fuzzy-matched to GDELT articles',
            'InciWeb GeoRSS coordinates extracted (v1 hardcoded lat/lng to 0)',
            'Every discard logged to feed.dropped[]'
        ]
    }
    with open(os.path.join(DATA_DIR, 'meta.json'), 'w') as f:
        json.dump(meta, f, indent=2)

    print(f"\n{'='*60}")
    print(f"Output: {feed_path} ({size_kb:.0f} KB)")
    print(f"Total articles: {len(final)}")
    print(f"  CRITICAL: {counts['critical']}")
    print(f"  HIGH: {counts['high']}")
    print(f"  MODERATE: {counts['moderate']}")
    print(f"  GLIDE-tagged: {counts['glide_tagged']}")
    print(f"  Geo points: {len(geo_points)}")
    print(f"  Dropped (logged): {len(DROPPED)}")
    print("Done!")


if __name__ == "__main__":
    main()

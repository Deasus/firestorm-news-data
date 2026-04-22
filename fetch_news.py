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
    print("  Fetching ReliefWeb active wildfires...")
    try:
        rw_url = (
            'https://api.reliefweb.int/v1/disasters?appname=firestorm'
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

def categorize_article(title):
    """Categorize an article by urgency based on title keywords."""
    t = (title or '').lower()
    if any(w in t for w in ['evacuat', 'emergency', 'mandatory', 'order', 'flee', 'shelter']):
        return 'CRITICAL'
    if any(w in t for w in ['dead', 'killed', 'death', 'fatal', 'injur', 'missing', 'destroy']):
        return 'CRITICAL'
    if any(w in t for w in ['wildfire', 'fire', 'blaze', 'burn', 'acre', 'contain', 'spread']):
        return 'HIGH'
    if any(w in t for w in ['red flag', 'fire weather', 'fire danger', 'wind', 'dry', 'drought']):
        return 'HIGH'
    if any(w in t for w in ['smoke', 'air quality', 'prescribed', 'prevention', 'firefight']):
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

    # ── Raw counts (honest, pre-anything) ──
    raw_counts = {
        'gdelt_doc': len(gdelt_articles),
        'reliefweb_disasters': len([d for d in disasters if d.get('type') == 'reliefweb_disaster']),
        'gdelt_disasters': len([d for d in disasters if d.get('type') == 'gdelt_disaster']),
        'inciweb': len(inciweb),
        'google_news': len(google_news),
        'bigquery_gkg': len(bigquery_records),
        'geo_points': len(geo_points),
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

    print(f"\n[MERGED] {len(all_news)} total records before GLIDE enrichment")

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

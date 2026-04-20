"""
FIRESTORM News Intelligence Pipeline
=====================================
Aggregates wildfire intelligence from multiple sources into a single JSON feed.

Sources:
1. GDELT DOC 2.0 API — Full-text search for wildfire/fire news
2. GDELT GEO 2.0 API — Geolocated fire news for map plotting
3. GDELT Disasters Live Stream — UN OCHA/ReliefWeb tracked disasters
4. GDELT BigQuery — Deep historical analysis (requires Google Cloud credentials)
5. InciWeb RSS — Official federal wildfire incident updates
6. Google News RSS — Broad wildfire news coverage

Outputs JSON to data/ directory. Runs every 30 minutes via GitHub Actions.
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

# ── GDELT DOC 2.0 API ───────────────────────────────────────────────

def fetch_gdelt_doc_articles():
    """Fetch wildfire articles from GDELT DOC 2.0 API with rate limit handling."""
    import requests
    
    print("\n[GDELT DOC] Fetching wildfire articles...")
    
    queries = [
        '(wildfire OR "wild fire" OR bushfire OR "forest fire") (evacuation OR containment OR acres OR firefighter)',
        '"red flag warning" OR "fire weather watch" OR "extreme fire danger"',
        '(wildfire OR "forest fire") (California OR Oregon OR Washington OR Texas OR Colorado OR Montana)',
    ]
    
    all_articles = []
    
    for i, query in enumerate(queries):
        # GDELT rate limit: ~1 request per 5-6 seconds
        if i > 0:
            print(f"  Waiting 7s for rate limit...")
            time.sleep(7)
        
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
                # Retry once
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
    
    # Deduplicate by URL
    seen = set()
    unique = []
    for art in all_articles:
        url = art.get('url', '')
        if url and url not in seen:
            seen.add(url)
            unique.append({
                'title': art.get('title', ''),
                'url': art.get('url', ''),
                'source': art.get('domain', ''),
                'date': art.get('seendate', ''),
                'image': art.get('socialimage', ''),
                'language': art.get('language', 'English'),
                'country': art.get('sourcecountry', ''),
                'type': 'gdelt_doc'
            })
    
    print(f"  Total unique: {len(unique)} articles")
    return unique[:150]

# ── GDELT GEO 2.0 API ───────────────────────────────────────────────

def fetch_gdelt_geo():
    """Fetch geolocated wildfire mentions from GDELT GEO API."""
    import requests
    
    print("\n[GDELT GEO] Fetching geolocated fire news...")
    
    # The GEO API returns HTML by default with embedded data
    # We need to use format=GeoJSON (capital J) or parse the HTML
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
            
            # Try JSON parse
            try:
                data = json.loads(text)
                
                # Standard GeoJSON
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
                                })
                    if points:
                        print(f"  ✓ {len(points)} geolocated points from GeoJSON")
                        return points
                
                # Maybe it's a list of objects
                if isinstance(data, list):
                    points = []
                    for item in data[:200]:
                        if 'lat' in item and 'lon' in item:
                            points.append({'lat': item['lat'], 'lng': item['lon'], 'name': item.get('name',''), 'count': 1})
                        elif 'latitude' in item and 'longitude' in item:
                            points.append({'lat': item['latitude'], 'lng': item['longitude'], 'name': item.get('name',''), 'count': 1})
                    if points:
                        print(f"  ✓ {len(points)} points from JSON array")
                        return points
                        
                print(f"  JSON parsed but no usable geo data (keys: {list(data.keys())[:5]})")
                
            except json.JSONDecodeError:
                # Not JSON — try extracting coordinates from HTML/text
                print(f"  Not JSON — trying regex extraction...")
                # Look for patterns like [lng, lat] or lat,lng in the HTML
                coord_pairs = re.findall(r'\[(-?\d+\.?\d+),\s*(-?\d+\.?\d+)\]', text[:100000])
                points = []
                for lng_s, lat_s in coord_pairs:
                    try:
                        lng, lat = float(lng_s), float(lat_s)
                        # GeoJSON is [lng, lat] so swap if needed
                        if -90 <= lat <= 90 and -180 <= lng <= 180:
                            points.append({'lat': lat, 'lng': lng, 'name': '', 'count': 1})
                        elif -90 <= lng <= 90 and -180 <= lat <= 180:
                            points.append({'lat': lng, 'lng': lat, 'name': '', 'count': 1})
                    except ValueError:
                        continue
                
                if points:
                    # Deduplicate nearby points
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
    
    # Fallback: use GDELT DOC API with sourcecountry to approximate locations
    print("  All GEO methods failed — extracting locations from DOC articles")
    return []

# ── GDELT DISASTERS LIVE STREAM ──────────────────────────────────────

def fetch_gdelt_disasters():
    """Fetch the GDELT Disasters Live Stream."""
    import requests
    
    print("\n[GDELT DISASTERS] Fetching disaster live stream...")
    
    # The disaster stream files are listed in a separate lastupdate file
    urls_to_try = [
        'http://data.gdeltproject.org/gdeltv2/lastupdate.txt',
        'http://data.gdeltproject.org/gdeltv2/lastupdate-translation.txt',
    ]
    
    for list_url in urls_to_try:
        try:
            resp = requests.get(list_url, timeout=15)
            if resp.status_code != 200:
                print(f"  {list_url}: HTTP {resp.status_code}")
                continue
            
            lines = resp.text.strip().split('\n')
            print(f"  {list_url}: {len(lines)} files listed")
            
            # Look for any file with 'gkg' in the name (Global Knowledge Graph)
            gkg_files = [l for l in lines if '.gkg.' in l.lower()]
            glide_files = [l for l in lines if 'glide' in l.lower()]
            
            print(f"  GKG files: {len(gkg_files)}, GLIDE files: {len(glide_files)}")
            
            # If we have GLIDE files, fetch those (disaster-specific)
            target_files = glide_files if glide_files else []
            
            for line in target_files[:2]:
                parts = line.strip().split()
                if len(parts) >= 3:
                    file_url = parts[-1]
                    if not file_url.startswith('http'):
                        continue
                    print(f"  Fetching disaster file: {file_url[-40:]}")
                    try:
                        dr = requests.get(file_url, timeout=30)
                        if dr.status_code == 200 and len(dr.content) > 100:
                            records = parse_disaster_data(dr.text)
                            if records:
                                return records
                    except Exception as e:
                        print(f"  Disaster file fetch failed: {e}")
            
            if not glide_files:
                print("  No GLIDE disaster files found in feed")
                
        except Exception as e:
            print(f"  {list_url} failed: {e}")
    
    # Try the OCHA ReliefWeb API directly for active wildfires
    print("  Trying ReliefWeb API for active disasters...")
    try:
        rw_url = 'https://api.reliefweb.int/v1/disasters?appname=firestorm&filter[field]=type&filter[value]=Wild Fire&filter[operator]=AND&filter[conditions][0][field]=status&filter[conditions][0][value]=current&limit=20&fields[include][]=name&fields[include][]=date&fields[include][]=glide&fields[include][]=country'
        resp = requests.get(rw_url, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            disasters = []
            for item in data.get('data', []):
                fields = item.get('fields', {})
                countries = [c.get('name','') for c in fields.get('country', [])]
                disasters.append({
                    'name': fields.get('name', ''),
                    'glide': fields.get('glide', ''),
                    'date': fields.get('date', {}).get('created', ''),
                    'countries': ', '.join(countries),
                    'type': 'reliefweb_disaster'
                })
            if disasters:
                print(f"  ✓ {len(disasters)} active wildfire disasters from ReliefWeb")
                return disasters
    except Exception as e:
        print(f"  ReliefWeb failed: {e}")
    
    return []

def parse_disaster_data(text):
    """Parse GDELT disaster CSV/TSV into structured data."""
    records = []
    lines = text.strip().split('\n')
    for line in lines[:100]:
        fields = line.split('\t')
        if len(fields) >= 3:
            # Look for GLIDE numbers (WF = wildfire, FL = flood, etc.)
            glide = ''
            url = ''
            for field in fields:
                if re.match(r'[A-Z]{2}-\d{4}-\d+', field):
                    glide = field
                if field.startswith('http'):
                    url = field
            
            if glide or url:
                records.append({
                    'glide': glide,
                    'url': url,
                    'raw_fields': len(fields),
                    'type': 'disaster_stream'
                })
    
    wildfires = [r for r in records if r.get('glide', '').startswith('WF')]
    print(f"  Parsed {len(records)} disaster records, {len(wildfires)} wildfires")
    return records

# ── GDELT BigQuery ───────────────────────────────────────────────────
# TODO [BigQuery]: Add GCP credentials to unlock deep GDELT analysis.
# Enables: ENV_FIRE theme queries, disaster GLIDE tracking, geolocated events.
# Eventually migrate entire pipeline to Google Cloud Functions.
# REMINDER: Note on every update until resolved.

def fetch_bigquery_fire_data():
    """Query GDELT on BigQuery for fire-related events and coverage."""
    
    creds_json = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS_JSON', '')
    if not creds_json:
        print("\n[BigQuery] No credentials — skipping")
        print("  To enable: add GOOGLE_APPLICATION_CREDENTIALS_JSON secret to repo")
        return None
    
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
        results = {}
        
        # GKG fire themes with locations
        query_gkg = """
        SELECT 
            DATE, DocumentIdentifier as url, V2Themes, V2Tone,
            V2Locations
        FROM `gdelt-bq.gdeltv2.gkg_partitioned`
        WHERE DATE >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
            AND (V2Themes LIKE '%ENV_FIRE%' 
                 OR V2Themes LIKE '%NATURAL_DISASTER_WILDFIRE%'
                 OR V2Themes LIKE '%WB_2680_FOREST_FIRES%')
        ORDER BY DATE DESC
        LIMIT 200
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
                                locs.append({'lat': lat, 'lng': lng, 'name': parts[3] if len(parts) > 3 else ''})
                        except (ValueError, IndexError):
                            pass
                gkg_articles.append({
                    'date': row.DATE.isoformat() if row.DATE else '',
                    'url': row.url or '',
                    'tone': round(tone, 2),
                    'locations': locs[:3],
                    'type': 'bigquery_gkg'
                })
            results['gkg_fire'] = gkg_articles
            print(f"  ✓ GKG query: {len(gkg_articles)} fire-themed articles")
        except Exception as e:
            print(f"  GKG query failed: {e}")
        
        os.remove(creds_path)
        return results
        
    except ImportError:
        print("  google-cloud-bigquery not installed")
    except Exception as e:
        print(f"  BigQuery error: {e}")
    return None

# ── InciWeb RSS ──────────────────────────────────────────────────────

def fetch_inciweb():
    """Fetch InciWeb incident RSS feed."""
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
    
    for url in urls:
        try:
            resp = requests.get(url, timeout=15, headers=headers, allow_redirects=True)
            ct = resp.headers.get('content-type', '')
            print(f"  {url}")
            print(f"    HTTP {resp.status_code}, {len(resp.content)} bytes, type: {ct[:50]}")
            
            if resp.status_code != 200:
                continue
            
            if len(resp.content) < 200:
                print(f"    Response too small — skipping")
                continue
            
            text = resp.text
            
            # Check if it looks like XML/RSS
            if '<' not in text[:200]:
                print(f"    Not XML — first 100 chars: {text[:100]}")
                continue
            
            try:
                root = ET.fromstring(resp.content)
            except ET.ParseError as pe:
                print(f"    XML parse error: {pe}")
                # Try cleaning common issues
                try:
                    clean = re.sub(r'&(?!amp;|lt;|gt;|quot;|apos;)', '&amp;', text)
                    root = ET.fromstring(clean.encode('utf-8'))
                except:
                    continue
            
            items = []
            
            # Try RSS 2.0 format
            for item in root.findall('.//item'):
                title = item.findtext('title', '').strip()
                link = item.findtext('link', '')
                desc = item.findtext('description', '')
                pub_date = item.findtext('pubDate', '')
                if title:
                    items.append({
                        'title': title,
                        'url': link,
                        'description': (desc or '')[:300],
                        'date': pub_date,
                        'lat': 0, 'lng': 0,
                        'type': 'inciweb'
                    })
            
            # Try Atom format
            if not items:
                ns = {'atom': 'http://www.w3.org/2005/Atom'}
                for entry in root.findall('.//atom:entry', ns):
                    title = entry.findtext('atom:title', '', ns).strip()
                    link_el = entry.find('atom:link', ns)
                    link = link_el.get('href', '') if link_el is not None else ''
                    summary = entry.findtext('atom:summary', '', ns)
                    updated = entry.findtext('atom:updated', '', ns)
                    if title:
                        items.append({
                            'title': title,
                            'url': link,
                            'description': (summary or '')[:300],
                            'date': updated,
                            'lat': 0, 'lng': 0,
                            'type': 'inciweb'
                        })
            
            # Try generic entry/item search
            if not items:
                for tag in ['entry', 'item', 'record']:
                    for item in root.iter(tag):
                        title = ''
                        link = ''
                        for child in item:
                            tag_local = child.tag.split('}')[-1].lower()
                            if tag_local == 'title' and child.text:
                                title = child.text.strip()
                            elif tag_local == 'link':
                                link = child.get('href', child.text or '')
                        if title:
                            items.append({
                                'title': title, 'url': link,
                                'description': '', 'date': '',
                                'lat': 0, 'lng': 0, 'type': 'inciweb'
                            })
            
            if items:
                print(f"  ✓ {len(items)} incidents from InciWeb")
                return items[:50]
            else:
                print(f"    Parsed XML but found 0 items. Root tag: {root.tag}")
                # Show first few child tags for debugging
                children = [child.tag for child in root][:5]
                print(f"    Children: {children}")
                
        except Exception as e:
            print(f"  {url} exception: {e}")
    
    print("  All InciWeb URLs failed — trying NIFC current situations")
    
    # Fallback: try NIFC situation reports
    try:
        nifc_url = 'https://www.nifc.gov/fire-information/nfn'
        resp = requests.get(nifc_url, timeout=15, headers=headers)
        print(f"  NIFC fallback: HTTP {resp.status_code}, {len(resp.content)} bytes")
    except Exception as e:
        print(f"  NIFC fallback failed: {e}")
    
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
            time.sleep(2)  # Be polite to Google
        
        url = f'https://news.google.com/rss/search?q={search}&hl=en-US&gl=US&ceid=US:en'
        try:
            resp = requests.get(url, timeout=15, headers={
                'User-Agent': 'Mozilla/5.0 (compatible; FIRESTORM/1.0)'
            })
            if resp.status_code == 200:
                root = ET.fromstring(resp.content)
                for item in root.findall('.//item')[:20]:
                    title = item.findtext('title', '')
                    link = item.findtext('link', '')
                    pub_date = item.findtext('pubDate', '')
                    source = item.findtext('source', '')
                    
                    all_items.append({
                        'title': title,
                        'url': link,
                        'source': source,
                        'date': pub_date,
                        'type': 'google_news'
                    })
            else:
                print(f"  Search {i+1}: HTTP {resp.status_code}")
        except Exception as e:
            print(f"  Search '{search}' failed: {e}")
    
    # Deduplicate
    seen = set()
    unique = []
    for item in all_items:
        key = re.sub(r'[^a-z0-9]', '', item.get('title', '').lower())[:40]
        if key and key not in seen:
            seen.add(key)
            unique.append(item)
    
    print(f"  {len(unique)} unique articles from Google News")
    return unique[:60]

# ── AGGREGATE & OUTPUT ───────────────────────────────────────────────

def categorize_article(title):
    """Categorize an article by urgency based on title keywords."""
    t = title.lower()
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

def main():
    import requests
    
    print("=" * 60)
    print("FIRESTORM News Intelligence Pipeline")
    print(f"Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)
    
    gdelt_articles = fetch_gdelt_doc_articles()
    
    time.sleep(7)  # Rate limit between GDELT API calls
    geo_points = fetch_gdelt_geo()
    
    disasters = fetch_gdelt_disasters()
    inciweb = fetch_inciweb()
    google_news = fetch_google_news()
    bigquery_data = fetch_bigquery_fire_data()
    
    # Build combined feed
    all_news = []
    for art in gdelt_articles:
        art['category'] = categorize_article(art.get('title', ''))
        all_news.append(art)
    for art in google_news:
        art['category'] = categorize_article(art.get('title', ''))
        all_news.append(art)
    for art in inciweb:
        art['category'] = 'HIGH'
        all_news.append(art)
    
    # Sort by priority then date
    priority = {'CRITICAL': 0, 'HIGH': 1, 'MODERATE': 2, 'LOW': 3}
    all_news.sort(key=lambda x: (priority.get(x.get('category', 'LOW'), 3), x.get('date', '')))
    
    # Deduplicate
    seen_titles = set()
    deduped = []
    for art in all_news:
        title_key = re.sub(r'[^a-z0-9]', '', art.get('title', '').lower())[:40]
        if title_key and title_key not in seen_titles:
            seen_titles.add(title_key)
            deduped.append(art)
    
    # Build output
    output = {
        'updated': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'counts': {
            'total': len(deduped),
            'critical': len([a for a in deduped if a.get('category') == 'CRITICAL']),
            'high': len([a for a in deduped if a.get('category') == 'HIGH']),
            'moderate': len([a for a in deduped if a.get('category') == 'MODERATE']),
            'gdelt': len(gdelt_articles),
            'google_news': len(google_news),
            'inciweb': len(inciweb),
            'geo_points': len(geo_points),
            'disasters': len(disasters) if isinstance(disasters, list) else 0,
        },
        'articles': deduped[:200],
        'geo_points': geo_points[:150],
        'disasters': disasters[:50] if isinstance(disasters, list) else [],
    }
    
    if bigquery_data:
        output['bigquery'] = bigquery_data
    
    feed_path = os.path.join(DATA_DIR, 'fire-news-feed.json')
    with open(feed_path, 'w') as f:
        json.dump(output, f, separators=(',', ':'))
    
    size_kb = os.path.getsize(feed_path) / 1024
    
    # Write metadata
    meta = {
        'updated': output['updated'],
        'counts': output['counts'],
        'sources': {
            'gdelt_doc': 'api.gdeltproject.org/api/v2/doc',
            'gdelt_geo': 'api.gdeltproject.org/api/v2/geo',
            'gdelt_disasters': 'data.gdeltproject.org + reliefweb',
            'inciweb': 'inciweb.wildfire.gov',
            'google_news': 'news.google.com',
            'bigquery': 'enabled' if bigquery_data else 'no credentials'
        }
    }
    with open(os.path.join(DATA_DIR, 'meta.json'), 'w') as f:
        json.dump(meta, f, indent=2)
    
    print(f"\n{'='*60}")
    print(f"Output: {feed_path} ({size_kb:.0f} KB)")
    print(f"Total articles: {len(deduped)}")
    print(f"  CRITICAL: {output['counts']['critical']}")
    print(f"  HIGH: {output['counts']['high']}")
    print(f"  MODERATE: {output['counts']['moderate']}")
    print(f"  Geo points: {len(geo_points)}")
    print(f"  Disasters: {output['counts']['disasters']}")
    print("Done!")

if __name__ == "__main__":
    main()

"""
FIRESTORM News Intelligence Pipeline
=====================================
Aggregates wildfire intelligence from multiple sources into a single JSON feed.

Sources:
1. GDELT DOC 2.0 API — Full-text search for wildfire/fire news (CORS-friendly, no auth)
2. GDELT GEO 2.0 API — Geolocated fire news for map plotting
3. GDELT Disasters Live Stream — UN OCHA/ReliefWeb tracked disasters
4. GDELT BigQuery — Deep historical analysis (requires Google Cloud credentials)
5. InciWeb RSS — Official federal wildfire incident updates
6. Google News RSS — Broad wildfire news coverage

Outputs JSON files to data/ directory for FIRESTORM to fetch via GitHub raw URLs.
Runs every 15 minutes via GitHub Actions.
"""

import json
import os
import sys
import re
import hashlib
from datetime import datetime, timezone, timedelta
from urllib.parse import quote

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

# ── GDELT DOC 2.0 API ───────────────────────────────────────────────
# Direct CORS-friendly API — no proxy needed
# Searches full text of all monitored news in 65 languages

def fetch_gdelt_doc_articles():
    """Fetch wildfire articles from GDELT DOC 2.0 API."""
    import requests
    
    print("\n[GDELT DOC] Fetching wildfire articles...")
    
    # Multiple queries to get comprehensive coverage
    queries = [
        # Primary wildfire query
        '(wildfire OR "wild fire" OR bushfire OR "forest fire") (evacuation OR containment OR acres OR firefighter OR "fire department")',
        # Fire weather
        '"red flag warning" OR "fire weather watch" OR "extreme fire" OR "fire danger"',
        # Specific fire incidents (broader)
        '(wildfire OR "forest fire") (California OR Oregon OR Washington OR Texas OR Colorado OR Montana OR Arizona)',
    ]
    
    all_articles = []
    
    for i, query in enumerate(queries):
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
    return unique[:150]  # Cap at 150

# ── GDELT GEO 2.0 API ───────────────────────────────────────────────
# Returns geolocated news mentions for map plotting

def fetch_gdelt_geo():
    """Fetch geolocated wildfire mentions from GDELT GEO API."""
    import requests
    
    print("\n[GDELT GEO] Fetching geolocated fire news...")
    
    url = (
        'https://api.gdeltproject.org/api/v2/geo/geo?'
        'query=wildfire OR "forest fire" OR bushfire OR "fire evacuation"'
        '&mode=pointdata'
        '&format=geojson'
        '&timespan=24h'
    )
    
    try:
        resp = requests.get(url, timeout=30)
        if resp.status_code == 200:
            # GEO API returns GeoJSON
            text = resp.text
            # Try to parse as JSON
            try:
                data = json.loads(text)
                features = data.get('features', [])
                print(f"  {len(features)} geolocated mentions")
                
                # Extract points with metadata
                points = []
                for f in features[:200]:  # Cap at 200 points
                    props = f.get('properties', {})
                    geom = f.get('geometry', {})
                    coords = geom.get('coordinates', [0, 0])
                    points.append({
                        'lat': coords[1] if len(coords) > 1 else 0,
                        'lng': coords[0] if len(coords) > 0 else 0,
                        'name': props.get('name', ''),
                        'count': props.get('count', 1),
                        'url': props.get('url', ''),
                        'html': props.get('html', '')
                    })
                return points
            except json.JSONDecodeError:
                # GEO API might return HTML for some modes
                print(f"  Response is not JSON ({len(text)} chars)")
                return []
        else:
            print(f"  HTTP {resp.status_code}")
    except Exception as e:
        print(f"  Failed: {e}")
    
    return []

# ── GDELT DISASTERS LIVE STREAM ──────────────────────────────────────
# Pre-generated feeds of active OCHA/ReliefWeb disasters, updated every 15 min

def fetch_gdelt_disasters():
    """Fetch the GDELT Disasters Live Stream."""
    import requests
    
    print("\n[GDELT DISASTERS] Fetching disaster live stream...")
    
    # The disaster stream files are at data.gdeltproject.org
    # First get the last update file list
    try:
        # Get the master file list
        list_url = 'http://data.gdeltproject.org/gdeltv2/lastupdate.txt'
        resp = requests.get(list_url, timeout=15)
        if resp.status_code != 200:
            print(f"  lastupdate.txt HTTP {resp.status_code}")
            return []
        
        lines = resp.text.strip().split('\n')
        disaster_files = [l for l in lines if 'gkgglide' in l.lower() or 'glide' in l.lower()]
        
        if disaster_files:
            print(f"  Found {len(disaster_files)} disaster stream files")
            # Parse the file URL from the line (format: size hash url)
            for line in disaster_files:
                parts = line.strip().split()
                if len(parts) >= 3:
                    file_url = parts[-1]
                    print(f"  Fetching: {file_url}")
                    try:
                        dr = requests.get(file_url, timeout=30)
                        if dr.status_code == 200:
                            # Parse CSV data
                            return parse_disaster_csv(dr.text)
                    except Exception as e:
                        print(f"  Failed to fetch disaster file: {e}")
        else:
            print("  No disaster stream files in lastupdate.txt")
            
            # Try the disaster JSON feed directly
            disaster_json_url = 'http://data.gdeltproject.org/gdeltv2/lastupdate-glide.txt'
            try:
                dr = requests.get(disaster_json_url, timeout=15)
                if dr.status_code == 200:
                    lines = dr.text.strip().split('\n')
                    for line in lines:
                        parts = line.strip().split()
                        if len(parts) >= 3:
                            file_url = parts[-1]
                            if file_url.endswith('.json') or 'glide' in file_url:
                                print(f"  Trying: {file_url}")
                                jr = requests.get(file_url, timeout=30)
                                if jr.status_code == 200:
                                    try:
                                        return jr.json()
                                    except:
                                        return parse_disaster_csv(jr.text)
            except Exception as e:
                print(f"  Disaster JSON fallback failed: {e}")
    
    except Exception as e:
        print(f"  Failed: {e}")
    
    return []

def parse_disaster_csv(text):
    """Parse GDELT disaster CSV into structured data."""
    records = []
    lines = text.strip().split('\n')
    for line in lines[:100]:  # Cap at 100 records
        fields = line.split('\t')
        if len(fields) >= 5:
            records.append({
                'glide': fields[0] if fields[0].startswith('WF') or fields[0].startswith('FL') else '',
                'date': fields[1] if len(fields) > 1 else '',
                'url': fields[-1] if fields[-1].startswith('http') else '',
                'type': 'disaster_stream'
            })
    wildfires = [r for r in records if r.get('glide', '').startswith('WF')]
    print(f"  Parsed {len(records)} records, {len(wildfires)} wildfire-specific")
    return records

# ── GDELT BigQuery ───────────────────────────────────────────────────
# Deep analysis queries — requires GOOGLE_APPLICATION_CREDENTIALS

def fetch_bigquery_fire_data():
    """Query GDELT on BigQuery for fire-related events and coverage."""
    
    # Check if BigQuery credentials are available
    creds_json = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS_JSON', '')
    if not creds_json:
        print("\n[BigQuery] No credentials — skipping")
        print("  To enable: add GOOGLE_APPLICATION_CREDENTIALS_JSON secret to repo")
        return None
    
    try:
        from google.cloud import bigquery
        from google.oauth2 import service_account
        
        # Write credentials to temp file
        creds_path = '/tmp/gcp_creds.json'
        with open(creds_path, 'w') as f:
            f.write(creds_json)
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = creds_path
        
        credentials = service_account.Credentials.from_service_account_file(creds_path)
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        
        print("\n[BigQuery] Running fire intelligence queries...")
        
        results = {}
        
        # Query 1: Recent fire-themed GKG articles with locations
        query_gkg = """
        SELECT 
            DATE,
            DocumentIdentifier as url,
            SPLIT(V2Themes, ';') as themes,
            V2Tone,
            SPLIT(V2Locations, ';') as locations
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
            query_job = client.query(query_gkg)
            rows = list(query_job)
            
            gkg_articles = []
            for row in rows:
                tone_parts = (row.V2Tone or '').split(',')
                tone = float(tone_parts[0]) if tone_parts and tone_parts[0] else 0
                
                # Extract locations
                locs = []
                for loc_str in (row.locations or []):
                    parts = loc_str.split('#')
                    if len(parts) >= 7:
                        try:
                            lat = float(parts[5])
                            lng = float(parts[6])
                            name = parts[3]
                            if lat != 0 and lng != 0:
                                locs.append({'lat': lat, 'lng': lng, 'name': name})
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
            print(f"  GKG query: {len(gkg_articles)} fire-themed articles")
        except Exception as e:
            print(f"  GKG query failed: {e}")
        
        # Query 2: Fire events with actors and locations
        query_events = """
        SELECT 
            SQLDATE,
            Actor1Name,
            Actor2Name,
            EventCode,
            GoldsteinScale,
            NumMentions,
            AvgTone,
            ActionGeo_Lat,
            ActionGeo_Long,
            ActionGeo_FullName,
            SOURCEURL
        FROM `gdelt-bq.gdeltv2.events`
        WHERE SQLDATE >= FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
            AND (EventRootCode IN ('14', '18', '19', '20')  -- Appeal, Assist, Coerce, Use Force
                 OR Actor1Name LIKE '%FIRE%'
                 OR Actor2Name LIKE '%FIRE%')
            AND ActionGeo_CountryCode = 'US'
            AND ActionGeo_Lat IS NOT NULL
        ORDER BY NumMentions DESC
        LIMIT 100
        """
        
        try:
            print("  Running events query...")
            query_job = client.query(query_events)
            rows = list(query_job)
            
            events = []
            for row in rows:
                events.append({
                    'date': str(row.SQLDATE),
                    'actor1': row.Actor1Name or '',
                    'actor2': row.Actor2Name or '',
                    'event_code': row.EventCode or '',
                    'goldstein': float(row.GoldsteinScale) if row.GoldsteinScale else 0,
                    'mentions': int(row.NumMentions) if row.NumMentions else 0,
                    'tone': round(float(row.AvgTone), 2) if row.AvgTone else 0,
                    'lat': float(row.ActionGeo_Lat) if row.ActionGeo_Lat else 0,
                    'lng': float(row.ActionGeo_Long) if row.ActionGeo_Long else 0,
                    'location': row.ActionGeo_FullName or '',
                    'url': row.SOURCEURL or '',
                    'type': 'bigquery_event'
                })
            
            results['events'] = events
            print(f"  Events query: {len(events)} fire-related events")
        except Exception as e:
            print(f"  Events query failed: {e}")
        
        # Cleanup
        os.remove(creds_path)
        return results
        
    except ImportError:
        print("  google-cloud-bigquery not installed")
        return None
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
        'https://inciweb.nwcg.gov/feeds/rss/incidents',
    ]
    
    for url in urls:
        try:
            resp = requests.get(url, timeout=15, headers={'User-Agent': 'FIRESTORM/1.0'})
            if resp.status_code == 200:
                root = ET.fromstring(resp.content)
                items = []
                for item in root.findall('.//item')[:50]:
                    title = item.findtext('title', '')
                    link = item.findtext('link', '')
                    desc = item.findtext('description', '')
                    pub_date = item.findtext('pubDate', '')
                    
                    # Try to extract lat/lng from georss
                    lat, lng = 0, 0
                    point = item.find('{http://www.georss.org/georss}point')
                    if point is not None and point.text:
                        parts = point.text.strip().split()
                        if len(parts) == 2:
                            try:
                                lat, lng = float(parts[0]), float(parts[1])
                            except ValueError:
                                pass
                    
                    items.append({
                        'title': title,
                        'url': link,
                        'description': desc[:300],
                        'date': pub_date,
                        'lat': lat,
                        'lng': lng,
                        'type': 'inciweb'
                    })
                
                print(f"  {len(items)} incidents from InciWeb")
                return items
        except Exception as e:
            print(f"  {url} failed: {e}")
    
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
    for search in searches:
        url = f'https://news.google.com/rss/search?q={search}&hl=en-US&gl=US&ceid=US:en'
        try:
            resp = requests.get(url, timeout=15, headers={'User-Agent': 'FIRESTORM/1.0'})
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
        except Exception as e:
            print(f"  Search '{search}' failed: {e}")
    
    # Deduplicate
    seen = set()
    unique = []
    for item in all_items:
        key = item.get('title', '')[:50]
        if key and key not in seen:
            seen.add(key)
            unique.append(item)
    
    print(f"  {len(unique)} unique articles from Google News")
    return unique[:60]

# ── AGGREGATE & OUTPUT ───────────────────────────────────────────────

def categorize_article(title):
    """Categorize an article by urgency/type based on title keywords."""
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
    import requests  # ensure available
    
    print("=" * 60)
    print("FIRESTORM News Intelligence Pipeline")
    print(f"Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)
    
    # Collect from all sources
    gdelt_articles = fetch_gdelt_doc_articles()
    geo_points = fetch_gdelt_geo()
    disasters = fetch_gdelt_disasters()
    inciweb = fetch_inciweb()
    google_news = fetch_google_news()
    bigquery_data = fetch_bigquery_fire_data()
    
    # Build the combined news feed
    all_news = []
    
    # Add GDELT articles
    for art in gdelt_articles:
        art['category'] = categorize_article(art.get('title', ''))
        all_news.append(art)
    
    # Add Google News
    for art in google_news:
        art['category'] = categorize_article(art.get('title', ''))
        all_news.append(art)
    
    # Add InciWeb
    for art in inciweb:
        art['category'] = 'HIGH'  # All InciWeb entries are fire incidents
        all_news.append(art)
    
    # Sort by category priority then date
    priority = {'CRITICAL': 0, 'HIGH': 1, 'MODERATE': 2, 'LOW': 3}
    all_news.sort(key=lambda x: (priority.get(x.get('category', 'LOW'), 3), x.get('date', '')), reverse=False)
    
    # Final dedup by title similarity
    seen_titles = set()
    deduped = []
    for art in all_news:
        title_key = re.sub(r'[^a-z0-9]', '', art.get('title', '').lower())[:40]
        if title_key and title_key not in seen_titles:
            seen_titles.add(title_key)
            deduped.append(art)
    
    # Write outputs
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
        },
        'articles': deduped[:200],
        'geo_points': geo_points[:150],
        'disasters': disasters[:50] if isinstance(disasters, list) else [],
    }
    
    # Add BigQuery data if available
    if bigquery_data:
        output['bigquery'] = bigquery_data
    
    # Write main feed
    feed_path = os.path.join(DATA_DIR, 'fire-news-feed.json')
    with open(feed_path, 'w') as f:
        json.dump(output, f, separators=(',', ':'))
    
    size_kb = os.path.getsize(feed_path) / 1024
    print(f"\n{'='*60}")
    print(f"Output: {feed_path} ({size_kb:.0f} KB)")
    print(f"Total articles: {len(deduped)}")
    print(f"  CRITICAL: {output['counts']['critical']}")
    print(f"  HIGH: {output['counts']['high']}")
    print(f"  MODERATE: {output['counts']['moderate']}")
    print(f"  Geo points: {len(geo_points)}")
    print(f"  Disasters: {len(output['disasters'])}")
    if bigquery_data:
        for k, v in bigquery_data.items():
            print(f"  BigQuery {k}: {len(v)} records")
    
    # Write metadata
    meta = {
        'updated': output['updated'],
        'counts': output['counts'],
        'sources': {
            'gdelt_doc': 'api.gdeltproject.org/api/v2/doc',
            'gdelt_geo': 'api.gdeltproject.org/api/v2/geo',
            'gdelt_disasters': 'data.gdeltproject.org disaster stream',
            'inciweb': 'inciweb.wildfire.gov',
            'google_news': 'news.google.com',
            'bigquery': 'enabled' if bigquery_data else 'no credentials'
        }
    }
    with open(os.path.join(DATA_DIR, 'meta.json'), 'w') as f:
        json.dump(meta, f, indent=2)
    
    print("Done!")

if __name__ == "__main__":
    main()

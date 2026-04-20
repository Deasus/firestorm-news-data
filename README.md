# FIRESTORM Fire News Intelligence

Automated wildfire news intelligence pipeline for the FIRESTORM platform.

## What This Does

Every 15 minutes, a GitHub Action aggregates wildfire news from multiple sources into a single JSON feed.

## Data Sources

| Source | What It Provides | Update Frequency |
|--------|-----------------|-----------------|
| **GDELT DOC 2.0** | Full-text search across 65 languages for wildfire news | Every 15 min |
| **GDELT GEO 2.0** | Geolocated news mentions (lat/lng on map) | Every 15 min |
| **GDELT Disasters** | UN OCHA/ReliefWeb tracked wildfire disasters | Every 15 min |
| **InciWeb RSS** | Official federal wildfire incident updates | As published |
| **Google News RSS** | Broad wildfire coverage from major outlets | Continuous |
| **GDELT BigQuery** | Deep historical analysis (optional, needs GCP credentials) | On-demand |

## Output

`data/fire-news-feed.json` — Single JSON file containing:

```json
{
  "updated": "2026-04-20T01:00:00Z",
  "counts": { "total": 150, "critical": 5, "high": 40, ... },
  "articles": [ { "title": "...", "url": "...", "category": "CRITICAL", ... } ],
  "geo_points": [ { "lat": 34.2, "lng": -118.5, "name": "Los Angeles", ... } ],
  "disasters": [ { "glide": "WF-2026-000123-USA", ... } ]
}
```

## Usage in FIRESTORM

```javascript
fetch('https://raw.githubusercontent.com/Deasus/firestorm-news-data/main/data/fire-news-feed.json')
  .then(r => r.json())
  .then(feed => {
    // feed.articles — news items sorted by urgency
    // feed.geo_points — locations to plot on map
    // feed.disasters — active OCHA-tracked disasters
  });
```

## Article Categories

| Category | Description |
|----------|------------|
| `CRITICAL` | Evacuations, deaths, structures destroyed |
| `HIGH` | Active wildfires, fire weather warnings |
| `MODERATE` | Smoke, air quality, prescribed burns |
| `LOW` | General fire-related news |

## Enabling BigQuery (Optional)

For deeper analysis, add Google Cloud credentials:

1. Create a GCP service account with BigQuery read access
2. Go to repo **Settings** → **Secrets** → **Actions**
3. Add secret `GCP_CREDENTIALS` with the service account JSON

This enables queries against the full GDELT BigQuery dataset (petabytes of historical data, no rate limits).

## Cost

- **GitHub Actions**: Free (uses ~30 min/day of the 2,000 min/month free tier)
- **GDELT APIs**: Free, no API key needed
- **BigQuery** (optional): Google gives 1TB/month free processing

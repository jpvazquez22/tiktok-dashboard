# ── TikTok Dashboard Configuration ──────────────────────────────────────────
# Edit these values to match your setup.

import os
APIFY_API_TOKEN = os.environ.get("APIFY_API_TOKEN", "apify_api_JIzKrcjlqPjBoouk9gNg9nxwke7VbX3Tn40m")

# TikTok username(s) to track (without the @)
TIKTOK_USERNAMES = ["dragsimone", "eyeopenup_", "cleodipatra", "aphroditequeen011", "queentequila33"]

# How many recent posts to fetch per run.
# Set to 0 for unlimited (fetches all videos — may take longer and cost more Apify credits).
# Most TikTok profiles expose up to ~500 videos via scraping.
MAX_POSTS_PER_USER = 500

# Apify Actor ID for TikTok scraper (clockworks/tiktok-scraper)
TIKTOK_ACTOR_ID = "clockworks~tiktok-scraper"

# Refresh schedule: "hourly" | "daily" | "manual"
REFRESH_SCHEDULE = "daily"

# Daily refresh time (only used when REFRESH_SCHEDULE = "daily")
DAILY_REFRESH_HOUR = 9   # 9 AM
DAILY_REFRESH_MINUTE = 0

# Local server port (Railway overrides via PORT env var)
PORT = int(os.environ.get("PORT", 5050))

# SQLite database file location
# Stored in user's home directory to avoid filesystem restrictions.
import os as _os
_HOME = _os.path.expanduser("~")
DB_PATH = _os.path.join(_HOME, "tiktok_analytics.db")

# Log file
LOG_PATH = _os.path.join(_HOME, "tiktok_sync.log")

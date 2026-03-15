"""
app.py — TikTok Analytics Dashboard
Flask web server + APScheduler background sync.

Usage:
    python app.py
    Open http://localhost:5050 in your browser.
"""

import logging
import json
import atexit
from flask import Flask, render_template, jsonify, request

from apscheduler.schedulers.background import BackgroundScheduler

import db
import apify_client
from config import (
    TIKTOK_USERNAMES, REFRESH_SCHEDULE,
    DAILY_REFRESH_HOUR, DAILY_REFRESH_MINUTE,
    PORT, LOG_PATH
)

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ── Flask App ─────────────────────────────────────────────────────────────────
app = Flask(__name__)
app.config['TEMPLATES_AUTO_RELOAD'] = True


def format_num(n):
    """Format large numbers: 1200000 → 1.2M, 45000 → 45K"""
    try:
        n = int(n or 0)
    except (ValueError, TypeError):
        return "0"
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    elif n >= 1_000:
        return f"{n/1_000:.1f}K"
    return str(n)


# Register as both a template filter AND a global function
app.jinja_env.filters["format_num"] = format_num
app.jinja_env.globals["format_num"] = format_num


# ── Database Init ─────────────────────────────────────────────────────────────
db.init_db()
logger.info("Database initialized.")


# ── Sync Logic ────────────────────────────────────────────────────────────────
def run_sync(username: str):
    """Scrape TikTok data for one user and store in SQLite."""
    logger.info(f"Starting sync for @{username}...")
    try:
        videos, profile = apify_client.scrape_user(username)

        if profile:
            db.upsert_profile(profile)

        for video in videos:
            db.upsert_video(video)

        db.log_sync(username, len(videos), 0, "success")
        logger.info(f"Sync complete for @{username}: {len(videos)} videos saved.")

    except Exception as e:
        logger.error(f"Sync failed for @{username}: {e}")
        db.log_sync(username, 0, 0, "error", str(e))


def sync_all():
    """Sync all configured TikTok usernames."""
    for username in TIKTOK_USERNAMES:
        run_sync(username)


# ── Scheduler ─────────────────────────────────────────────────────────────────
scheduler = BackgroundScheduler()

if REFRESH_SCHEDULE == "hourly":
    scheduler.add_job(sync_all, "cron", minute=0)
    logger.info("Scheduler set to: hourly")
elif REFRESH_SCHEDULE == "daily":
    scheduler.add_job(
        sync_all, "cron",
        hour=DAILY_REFRESH_HOUR,
        minute=DAILY_REFRESH_MINUTE
    )
    logger.info(f"Scheduler set to: daily at {DAILY_REFRESH_HOUR:02d}:{DAILY_REFRESH_MINUTE:02d}")

scheduler.start()
atexit.register(lambda: scheduler.shutdown())


# ── Routes ────────────────────────────────────────────────────────────────────

RANGE_OPTIONS = {
    "7":   "Last 7 days",
    "30":  "Last 30 days",
    "60":  "Last 60 days",
    "90":  "Last 90 days",
    "365": "Last 365 days",
    "0":   "All time",
}

@app.route("/")
def dashboard():
    user_param = request.args.get("user", TIKTOK_USERNAMES[0])
    username   = user_param if user_param in TIKTOK_USERNAMES else TIKTOK_USERNAMES[0]
    days_str   = request.args.get("days", "30")
    days       = int(days_str) if days_str in RANGE_OPTIONS else 30
    days_label = RANGE_OPTIONS.get(str(days), "Last 30 days")

    profile     = db.get_profile(username)
    kpis        = db.get_kpis(username, days=days)
    videos      = db.get_recent_videos(username)
    top5        = db.get_top_videos(username, limit=5)
    sentiment   = db.get_sentiment_summary(username)
    eng_trend   = db.get_engagement_trend(username, days=days)
    views_trend = db.get_daily_views_gained(username, days=days)
    # Fall back to post-date totals (all-time) if no meaningful snapshot delta data exists yet
    using_snapshot_data = bool(views_trend) and sum(r.get('total_views', 0) for r in views_trend) > 0
    if not using_snapshot_data:
        views_trend = db.get_views_trend(username, days=0)  # all-time, from first video
    recent_syncs = db.get_recent_syncs(limit=5)

    has_data = bool(videos)

    return render_template(
        "dashboard.html",
        username=username,
        all_usernames=TIKTOK_USERNAMES,
        profile=profile,
        kpis=kpis,
        videos=videos,
        top5=top5,
        sentiment=sentiment,
        eng_trend_json=json.dumps(eng_trend),
        views_trend_json=json.dumps(views_trend),
        recent_syncs=recent_syncs,
        has_data=has_data,
        last_updated=profile.get("last_updated", "Never"),
        days=days,
        days_label=days_label,
        range_options=RANGE_OPTIONS,
        using_snapshot_data=using_snapshot_data,
    )


@app.route("/leaderboard")
def leaderboard():
    days_str = request.args.get("days", "30")
    days = int(days_str) if days_str in RANGE_OPTIONS else 30
    days_label = RANGE_OPTIONS.get(str(days), "Last 30 days")

    rows = []
    for username in TIKTOK_USERNAMES:
        profile = db.get_profile(username)
        kpis = db.get_leaderboard_kpis(username, days=days)
        rows.append({
            "username": username,
            "followers": profile.get("followers", 0),
            "display_name": profile.get("display_name", "@" + username),
            "total_videos":      kpis.get("total_videos", 0) or 0,
            "total_views":       kpis.get("total_views", 0) or 0,
            "total_likes":       kpis.get("total_likes", 0) or 0,
            "total_comments":    kpis.get("total_comments", 0) or 0,
            "total_shares":      kpis.get("total_shares", 0) or 0,
            "total_saves":       kpis.get("total_saves", 0) or 0,
            "avg_views":         kpis.get("avg_views", 0) or 0,
            "avg_engagement_pct": kpis.get("avg_engagement_pct", 0) or 0,
        })

    return render_template(
        "leaderboard.html",
        rows=rows,
        all_usernames=TIKTOK_USERNAMES,
        days=days,
        days_label=days_label,
        range_options=RANGE_OPTIONS,
    )


@app.route("/api/sync", methods=["POST"])
def api_sync():
    """Manually trigger a sync from the dashboard."""
    import threading
    t = threading.Thread(target=sync_all)
    t.daemon = True
    t.start()
    return jsonify({"status": "started", "message": "Sync started in background. Refresh in ~2 minutes."})


@app.route("/api/status")
def api_status():
    """Return latest sync status."""
    syncs = db.get_recent_syncs(limit=3)
    return jsonify({"syncs": syncs})


@app.route("/api/videos/<username>")
def api_videos(username):
    videos = db.get_recent_videos(username)
    return jsonify(videos)


if __name__ == "__main__":
    logger.info(f"Starting TikTok Dashboard on http://localhost:{PORT}")
    logger.info(f"Tracking: {', '.join(['@' + u for u in TIKTOK_USERNAMES])}")
    logger.info("Tip: Click 'Sync Now' in the dashboard to fetch your first batch of data.")
    app.run(debug=False, host="0.0.0.0", port=PORT, use_reloader=False)

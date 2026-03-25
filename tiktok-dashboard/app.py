"""
app.py — TikTok & Instagram Analytics Dashboard
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
import tiktok_scraper
import instagram_scraper
from config import (
    TIKTOK_USERNAMES, INSTAGRAM_MAP, REFRESH_SCHEDULE,
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
    """Scrape TikTok data for one user and store."""
    logger.info(f"Starting TikTok sync for @{username}...")
    try:
        videos, profile = tiktok_scraper.scrape_user(username)

        if profile:
            profile["platform"] = "tiktok"
            db.upsert_profile(profile)

        for video in videos:
            video["platform"] = "tiktok"
            db.upsert_video(video)

        db.log_sync(username, len(videos), 0, "success", platform="tiktok")
        logger.info(f"TikTok sync complete for @{username}: {len(videos)} videos saved.")

    except Exception as e:
        logger.error(f"TikTok sync failed for @{username}: {e}")
        db.log_sync(username, 0, 0, "error", str(e), platform="tiktok")


def run_ig_sync(username: str):
    """Scrape Instagram data for one user and store under their TikTok username."""
    ig_handle = INSTAGRAM_MAP.get(username)
    if not ig_handle:
        return  # no IG for this user

    logger.info(f"Starting Instagram sync for @{ig_handle} (stored as {username})...")
    try:
        posts, profile = instagram_scraper.scrape_user(ig_handle, canonical_username=username)

        if profile:
            db.upsert_profile(profile)

        for post in posts:
            db.upsert_video(post)

        db.log_sync(username, len(posts), 0, "success", platform="instagram")
        logger.info(f"Instagram sync complete for @{ig_handle}: {len(posts)} posts saved.")

    except Exception as e:
        logger.error(f"Instagram sync failed for @{ig_handle}: {e}")
        db.log_sync(username, 0, 0, "error", str(e), platform="instagram")


def sync_all():
    """Sync all configured TikTok usernames."""
    for username in TIKTOK_USERNAMES:
        run_sync(username)


def sync_all_ig():
    """Sync all configured Instagram usernames."""
    for username in TIKTOK_USERNAMES:
        run_ig_sync(username)


def sync_everything():
    """Sync both TikTok and Instagram."""
    sync_all()
    sync_all_ig()


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

    # Platform selection
    platform   = request.args.get("platform", "tiktok")
    has_ig     = INSTAGRAM_MAP.get(username) is not None
    ig_handle  = INSTAGRAM_MAP.get(username)
    if platform == "instagram" and not has_ig:
        platform = "tiktok"

    profile     = db.get_profile(username, platform=platform)
    kpis        = db.get_kpis(username, platform=platform, days=days)
    videos      = db.get_recent_videos(username, platform=platform)
    top5        = db.get_top_videos(username, platform=platform, limit=5)
    sentiment   = db.get_sentiment_summary(username, platform=platform)
    eng_trend   = db.get_engagement_trend(username, platform=platform, days=days)
    views_trend = db.get_daily_views_gained(username, platform=platform, days=days)
    using_snapshot_data = bool(views_trend) and sum(r.get('total_views', 0) for r in views_trend) > 0
    if not using_snapshot_data:
        views_trend = db.get_views_trend(username, platform=platform, days=0)
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
        platform=platform,
        has_ig=has_ig,
        ig_handle=ig_handle,
        instagram_map=INSTAGRAM_MAP,
    )


@app.route("/leaderboard")
def leaderboard():
    days_str = request.args.get("days", "30")
    days = int(days_str) if days_str in RANGE_OPTIONS else 30
    days_label = RANGE_OPTIONS.get(str(days), "Last 30 days")
    lb_platform = request.args.get("platform", "tiktok")

    rows = []
    for username in TIKTOK_USERNAMES:
        ig_handle = INSTAGRAM_MAP.get(username)
        has_ig = ig_handle is not None

        if lb_platform == "instagram" and not has_ig:
            continue

        if lb_platform == "combined":
            profile_tt = db.get_profile(username, platform="tiktok")
            profile_ig = db.get_profile(username, platform="instagram") if has_ig else {}
            kpis_tt = db.get_leaderboard_kpis(username, platform="tiktok", days=days)
            kpis_ig = db.get_leaderboard_kpis(username, platform="instagram", days=days) if has_ig else {}

            rows.append({
                "username": username,
                "has_ig": has_ig,
                "ig_handle": ig_handle,
                "followers": (profile_tt.get("followers", 0) or 0) + (profile_ig.get("followers", 0) or 0),
                "display_name": profile_tt.get("display_name", "@" + username),
                "total_videos":      (kpis_tt.get("total_videos", 0) or 0) + (kpis_ig.get("total_videos", 0) or 0),
                "total_views":       (kpis_tt.get("total_views", 0) or 0) + (kpis_ig.get("total_views", 0) or 0),
                "total_likes":       (kpis_tt.get("total_likes", 0) or 0) + (kpis_ig.get("total_likes", 0) or 0),
                "total_comments":    (kpis_tt.get("total_comments", 0) or 0) + (kpis_ig.get("total_comments", 0) or 0),
                "total_shares":      (kpis_tt.get("total_shares", 0) or 0) + (kpis_ig.get("total_shares", 0) or 0),
                "total_saves":       (kpis_tt.get("total_saves", 0) or 0) + (kpis_ig.get("total_saves", 0) or 0),
                "avg_views":         (kpis_tt.get("avg_views", 0) or 0),
                "avg_engagement_pct": (kpis_tt.get("avg_engagement_pct", 0) or 0),
            })
        else:
            profile = db.get_profile(username, platform=lb_platform)
            kpis = db.get_leaderboard_kpis(username, platform=lb_platform, days=days)
            rows.append({
                "username": username,
                "has_ig": has_ig,
                "ig_handle": ig_handle,
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
        lb_platform=lb_platform,
        instagram_map=INSTAGRAM_MAP,
    )


@app.route("/api/sync", methods=["POST"])
def api_sync():
    """Manually trigger a TikTok sync from the dashboard."""
    import threading
    t = threading.Thread(target=sync_all)
    t.daemon = True
    t.start()
    return jsonify({"status": "started", "message": "TikTok sync started in background. Refresh in ~2 minutes."})


@app.route("/api/sync/instagram", methods=["POST"])
def api_sync_ig():
    """Manually trigger an Instagram sync."""
    import threading
    t = threading.Thread(target=sync_all_ig)
    t.daemon = True
    t.start()
    return jsonify({"status": "started", "message": "Instagram sync started in background. This may take a few minutes."})


@app.route("/api/status")
def api_status():
    """Return latest sync status."""
    syncs = db.get_recent_syncs(limit=3)
    return jsonify({"syncs": syncs})


@app.route("/api/videos/<username>")
def api_videos(username):
    platform = request.args.get("platform", "tiktok")
    videos = db.get_recent_videos(username, platform=platform)
    return jsonify(videos)


if __name__ == "__main__":
    logger.info(f"Starting Analytics Dashboard on http://localhost:{PORT}")
    logger.info(f"Tracking: {', '.join(['@' + u for u in TIKTOK_USERNAMES])}")
    logger.info("Tip: Click 'Sync Now' in the dashboard to fetch your first batch of data.")
    app.run(debug=False, host="0.0.0.0", port=PORT, use_reloader=False)

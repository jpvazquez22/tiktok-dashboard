"""
db.py — Database setup, insertion, and query helpers.
Supports PostgreSQL (via DATABASE_URL) and SQLite (local fallback).
All tables include a 'platform' column ('tiktok' or 'instagram').
"""

import sqlite3
import os
from datetime import datetime, timedelta
from config import DB_PATH, DATABASE_URL

# ── Database backend detection ───────────────────────────────────────────────
USE_PG = bool(DATABASE_URL)

if USE_PG:
    import psycopg2
    import psycopg2.extras


def get_conn():
    if USE_PG:
        conn = psycopg2.connect(DATABASE_URL, sslmode="require")
        return conn
    else:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn


def _fetchone_dict(cursor):
    if USE_PG:
        cols = [desc[0] for desc in cursor.description] if cursor.description else []
        row = cursor.fetchone()
        return dict(zip(cols, row)) if row else None
    else:
        row = cursor.fetchone()
        return dict(row) if row else None


def _fetchall_dict(cursor):
    if USE_PG:
        cols = [desc[0] for desc in cursor.description] if cursor.description else []
        rows = cursor.fetchall()
        return [dict(zip(cols, r)) for r in rows]
    else:
        rows = cursor.fetchall()
        return [dict(r) for r in rows]


def init_db():
    """Create all tables if they don't exist, then run migrations."""
    conn = get_conn()
    c = conn.cursor()

    if USE_PG:
        c.execute("""
            CREATE TABLE IF NOT EXISTS video_snapshots (
                video_id    TEXT NOT NULL,
                username    TEXT NOT NULL,
                date        TEXT NOT NULL,
                views       INTEGER DEFAULT 0,
                likes       INTEGER DEFAULT 0,
                comments    INTEGER DEFAULT 0,
                shares      INTEGER DEFAULT 0,
                bookmarks   INTEGER DEFAULT 0,
                platform    TEXT DEFAULT 'tiktok',
                PRIMARY KEY (video_id, date, platform)
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS profiles (
                username        TEXT NOT NULL,
                display_name    TEXT,
                bio             TEXT,
                followers       INTEGER DEFAULT 0,
                following       INTEGER DEFAULT 0,
                total_videos    INTEGER DEFAULT 0,
                total_hearts    INTEGER DEFAULT 0,
                avatar_url      TEXT,
                last_updated    TEXT,
                platform        TEXT DEFAULT 'tiktok',
                PRIMARY KEY (username, platform)
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS videos (
                id              TEXT NOT NULL,
                username        TEXT,
                description     TEXT,
                url             TEXT,
                cover_url       TEXT,
                views           INTEGER DEFAULT 0,
                likes           INTEGER DEFAULT 0,
                comments        INTEGER DEFAULT 0,
                shares          INTEGER DEFAULT 0,
                bookmarks       INTEGER DEFAULT 0,
                engagement_rate REAL DEFAULT 0,
                music_name      TEXT,
                duration        INTEGER DEFAULT 0,
                posted_at       TEXT,
                recorded_at     TEXT,
                platform        TEXT DEFAULT 'tiktok',
                PRIMARY KEY (id, platform)
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS comments (
                id              TEXT NOT NULL,
                video_id        TEXT,
                username        TEXT,
                text            TEXT,
                likes           INTEGER DEFAULT 0,
                sentiment       REAL DEFAULT 0,
                sentiment_label TEXT,
                posted_at       TEXT,
                recorded_at     TEXT,
                platform        TEXT DEFAULT 'tiktok',
                PRIMARY KEY (id, platform)
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS sync_log (
                id              SERIAL PRIMARY KEY,
                ran_at          TEXT,
                username        TEXT,
                videos_fetched  INTEGER DEFAULT 0,
                comments_fetched INTEGER DEFAULT 0,
                status          TEXT,
                message         TEXT,
                platform        TEXT DEFAULT 'tiktok'
            )
        """)
        # Migration: add platform column to existing tables
        for table in ('profiles', 'videos', 'video_snapshots', 'comments', 'sync_log'):
            try:
                c.execute(f"ALTER TABLE {table} ADD COLUMN platform TEXT DEFAULT 'tiktok'")
                conn.commit()
            except Exception:
                conn.rollback()
        # Migration: update primary keys if needed (drop old, add new)
        # This is handled by CREATE TABLE IF NOT EXISTS with the new schema above
        # For existing tables, the platform column defaults to 'tiktok'
    else:
        c.executescript("""
            CREATE TABLE IF NOT EXISTS video_snapshots (
                video_id    TEXT NOT NULL,
                username    TEXT NOT NULL,
                date        TEXT NOT NULL,
                views       INTEGER DEFAULT 0,
                likes       INTEGER DEFAULT 0,
                comments    INTEGER DEFAULT 0,
                shares      INTEGER DEFAULT 0,
                bookmarks   INTEGER DEFAULT 0,
                platform    TEXT DEFAULT 'tiktok',
                PRIMARY KEY (video_id, date, platform)
            );

            CREATE TABLE IF NOT EXISTS profiles (
                username        TEXT NOT NULL,
                display_name    TEXT,
                bio             TEXT,
                followers       INTEGER DEFAULT 0,
                following       INTEGER DEFAULT 0,
                total_videos    INTEGER DEFAULT 0,
                total_hearts    INTEGER DEFAULT 0,
                avatar_url      TEXT,
                last_updated    TEXT,
                platform        TEXT DEFAULT 'tiktok',
                PRIMARY KEY (username, platform)
            );

            CREATE TABLE IF NOT EXISTS videos (
                id              TEXT NOT NULL,
                username        TEXT,
                description     TEXT,
                url             TEXT,
                cover_url       TEXT,
                views           INTEGER DEFAULT 0,
                likes           INTEGER DEFAULT 0,
                comments        INTEGER DEFAULT 0,
                shares          INTEGER DEFAULT 0,
                bookmarks       INTEGER DEFAULT 0,
                engagement_rate REAL DEFAULT 0,
                music_name      TEXT,
                duration        INTEGER DEFAULT 0,
                posted_at       TEXT,
                recorded_at     TEXT,
                platform        TEXT DEFAULT 'tiktok',
                PRIMARY KEY (id, platform)
            );

            CREATE TABLE IF NOT EXISTS comments (
                id              TEXT NOT NULL,
                video_id        TEXT,
                username        TEXT,
                text            TEXT,
                likes           INTEGER DEFAULT 0,
                sentiment       REAL DEFAULT 0,
                sentiment_label TEXT,
                posted_at       TEXT,
                recorded_at     TEXT,
                platform        TEXT DEFAULT 'tiktok',
                PRIMARY KEY (id, platform)
            );

            CREATE TABLE IF NOT EXISTS sync_log (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                ran_at          TEXT,
                username        TEXT,
                videos_fetched  INTEGER DEFAULT 0,
                comments_fetched INTEGER DEFAULT 0,
                status          TEXT,
                message         TEXT,
                platform        TEXT DEFAULT 'tiktok'
            );
        """)

        # Migration: add platform and bookmarks columns to existing DBs
        for col, table in [
            ("bookmarks", "video_snapshots"),
            ("platform", "video_snapshots"),
            ("platform", "profiles"),
            ("platform", "videos"),
            ("platform", "comments"),
            ("platform", "sync_log"),
        ]:
            try:
                default = "'tiktok'" if col == "platform" else "0"
                c.execute(f"ALTER TABLE {table} ADD COLUMN {col} TEXT DEFAULT {default}" if col == "platform"
                          else f"ALTER TABLE {table} ADD COLUMN {col} INTEGER DEFAULT {default}")
                conn.commit()
            except Exception:
                pass

    conn.commit()
    conn.close()


# ── Upsert Functions ─────────────────────────────────────────────────────────

def upsert_profile(data: dict):
    platform = data.get("platform", "tiktok")
    conn = get_conn()
    if USE_PG:
        conn.cursor().execute("""
            INSERT INTO profiles (username, display_name, bio, followers, following,
                                  total_videos, total_hearts, avatar_url, last_updated, platform)
            VALUES (%(username)s, %(display_name)s, %(bio)s, %(followers)s, %(following)s,
                    %(total_videos)s, %(total_hearts)s, %(avatar_url)s, %(last_updated)s, %(platform)s)
            ON CONFLICT(username, platform) DO UPDATE SET
                display_name = EXCLUDED.display_name,
                bio          = EXCLUDED.bio,
                followers    = EXCLUDED.followers,
                following    = EXCLUDED.following,
                total_videos = EXCLUDED.total_videos,
                total_hearts = EXCLUDED.total_hearts,
                avatar_url   = EXCLUDED.avatar_url,
                last_updated = EXCLUDED.last_updated
        """, {**data, "platform": platform})
    else:
        conn.execute("""
            INSERT INTO profiles (username, display_name, bio, followers, following,
                                  total_videos, total_hearts, avatar_url, last_updated, platform)
            VALUES (:username, :display_name, :bio, :followers, :following,
                    :total_videos, :total_hearts, :avatar_url, :last_updated, :platform)
            ON CONFLICT(username, platform) DO UPDATE SET
                display_name = excluded.display_name,
                bio          = excluded.bio,
                followers    = excluded.followers,
                following    = excluded.following,
                total_videos = excluded.total_videos,
                total_hearts = excluded.total_hearts,
                avatar_url   = excluded.avatar_url,
                last_updated = excluded.last_updated
        """, {**data, "platform": platform})
    conn.commit()
    conn.close()


def upsert_video(data: dict):
    platform = data.get("platform", "tiktok")
    conn = get_conn()
    if USE_PG:
        c = conn.cursor()
        c.execute("""
            INSERT INTO videos (id, username, description, url, cover_url,
                                views, likes, comments, shares, bookmarks,
                                engagement_rate, music_name, duration, posted_at, recorded_at, platform)
            VALUES (%(id)s, %(username)s, %(description)s, %(url)s, %(cover_url)s,
                    %(views)s, %(likes)s, %(comments)s, %(shares)s, %(bookmarks)s,
                    %(engagement_rate)s, %(music_name)s, %(duration)s, %(posted_at)s, %(recorded_at)s, %(platform)s)
            ON CONFLICT(id, platform) DO UPDATE SET
                views           = EXCLUDED.views,
                likes           = EXCLUDED.likes,
                comments        = EXCLUDED.comments,
                shares          = EXCLUDED.shares,
                bookmarks       = EXCLUDED.bookmarks,
                engagement_rate = EXCLUDED.engagement_rate,
                recorded_at     = EXCLUDED.recorded_at
        """, {**data, "platform": platform})
        today = datetime.utcnow().date().isoformat()
        c.execute("""
            INSERT INTO video_snapshots (video_id, username, date, views, likes, comments, shares, bookmarks, platform)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT(video_id, date, platform) DO UPDATE SET
                views     = EXCLUDED.views,
                likes     = EXCLUDED.likes,
                comments  = EXCLUDED.comments,
                shares    = EXCLUDED.shares,
                bookmarks = EXCLUDED.bookmarks
        """, (data['id'], data['username'], today,
              data['views'], data['likes'], data['comments'], data['shares'], data['bookmarks'], platform))
    else:
        conn.execute("""
            INSERT INTO videos (id, username, description, url, cover_url,
                                views, likes, comments, shares, bookmarks,
                                engagement_rate, music_name, duration, posted_at, recorded_at, platform)
            VALUES (:id, :username, :description, :url, :cover_url,
                    :views, :likes, :comments, :shares, :bookmarks,
                    :engagement_rate, :music_name, :duration, :posted_at, :recorded_at, :platform)
            ON CONFLICT(id, platform) DO UPDATE SET
                views           = excluded.views,
                likes           = excluded.likes,
                comments        = excluded.comments,
                shares          = excluded.shares,
                bookmarks       = excluded.bookmarks,
                engagement_rate = excluded.engagement_rate,
                recorded_at     = excluded.recorded_at
        """, {**data, "platform": platform})
        today = datetime.utcnow().date().isoformat()
        conn.execute("""
            INSERT INTO video_snapshots (video_id, username, date, views, likes, comments, shares, bookmarks, platform)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(video_id, date, platform) DO UPDATE SET
                views     = excluded.views,
                likes     = excluded.likes,
                comments  = excluded.comments,
                shares    = excluded.shares,
                bookmarks = excluded.bookmarks
        """, (data['id'], data['username'], today,
              data['views'], data['likes'], data['comments'], data['shares'], data['bookmarks'], platform))
    conn.commit()
    conn.close()


def upsert_comment(data: dict):
    platform = data.get("platform", "tiktok")
    conn = get_conn()
    if USE_PG:
        conn.cursor().execute("""
            INSERT INTO comments
                (id, video_id, username, text, likes, sentiment, sentiment_label, posted_at, recorded_at, platform)
            VALUES
                (%(id)s, %(video_id)s, %(username)s, %(text)s, %(likes)s, %(sentiment)s, %(sentiment_label)s, %(posted_at)s, %(recorded_at)s, %(platform)s)
            ON CONFLICT(id, platform) DO NOTHING
        """, {**data, "platform": platform})
    else:
        conn.execute("""
            INSERT OR IGNORE INTO comments
                (id, video_id, username, text, likes, sentiment, sentiment_label, posted_at, recorded_at, platform)
            VALUES
                (:id, :video_id, :username, :text, :likes, :sentiment, :sentiment_label, :posted_at, :recorded_at, :platform)
        """, {**data, "platform": platform})
    conn.commit()
    conn.close()


def log_sync(username, videos_fetched, comments_fetched, status, message="", platform="tiktok"):
    conn = get_conn()
    if USE_PG:
        conn.cursor().execute("""
            INSERT INTO sync_log (ran_at, username, videos_fetched, comments_fetched, status, message, platform)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (datetime.utcnow().isoformat(), username, videos_fetched, comments_fetched, status, message, platform))
    else:
        conn.execute("""
            INSERT INTO sync_log (ran_at, username, videos_fetched, comments_fetched, status, message, platform)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (datetime.utcnow().isoformat(), username, videos_fetched, comments_fetched, status, message, platform))
    conn.commit()
    conn.close()


# ── Dashboard Query Helpers ───────────────────────────────────────────────────

def get_profile(username, platform="tiktok"):
    conn = get_conn()
    c = conn.cursor()
    p = "%s" if USE_PG else "?"
    c.execute(f"SELECT * FROM profiles WHERE username = {p} AND platform = {p}", (username, platform))
    result = _fetchone_dict(c)
    conn.close()
    return result if result else {}


def get_recent_videos(username, platform="tiktok", limit=None):
    conn = get_conn()
    c = conn.cursor()
    p = "%s" if USE_PG else "?"
    if limit:
        c.execute(
            f"SELECT * FROM videos WHERE username = {p} AND platform = {p} ORDER BY posted_at DESC LIMIT {p}",
            (username, platform, limit)
        )
    else:
        c.execute(
            f"SELECT * FROM videos WHERE username = {p} AND platform = {p} ORDER BY posted_at DESC",
            (username, platform)
        )
    result = _fetchall_dict(c)
    conn.close()
    return result


def get_top_videos(username, platform="tiktok", limit=5):
    conn = get_conn()
    c = conn.cursor()
    p = "%s" if USE_PG else "?"
    c.execute(f"""
        SELECT * FROM videos WHERE username = {p} AND platform = {p}
        ORDER BY views DESC LIMIT {p}
    """, (username, platform, limit))
    result = _fetchall_dict(c)
    conn.close()
    return result


def get_engagement_trend(username, platform="tiktok", days=30):
    """Returns daily average engagement rate. days=0 means all-time."""
    conn = get_conn()
    c = conn.cursor()
    p = "%s" if USE_PG else "?"
    if days and days > 0:
        since = (datetime.utcnow() - timedelta(days=days)).isoformat()
        if USE_PG:
            c.execute(f"""
                SELECT posted_at::date as day,
                       ROUND(AVG(engagement_rate)::numeric, 4) as avg_eng,
                       SUM(views) as total_views,
                       COUNT(*) as video_count
                FROM videos
                WHERE username = {p} AND platform = {p} AND posted_at >= {p}
                GROUP BY day ORDER BY day ASC
            """, (username, platform, since))
        else:
            c.execute(f"""
                SELECT DATE(posted_at) as day,
                       ROUND(AVG(engagement_rate), 4) as avg_eng,
                       SUM(views) as total_views,
                       COUNT(*) as video_count
                FROM videos
                WHERE username = {p} AND platform = {p} AND posted_at >= {p}
                GROUP BY day ORDER BY day ASC
            """, (username, platform, since))
    else:
        if USE_PG:
            c.execute(f"""
                SELECT posted_at::date as day,
                       ROUND(AVG(engagement_rate)::numeric, 4) as avg_eng,
                       SUM(views) as total_views,
                       COUNT(*) as video_count
                FROM videos
                WHERE username = {p} AND platform = {p}
                GROUP BY day ORDER BY day ASC
            """, (username, platform))
        else:
            c.execute(f"""
                SELECT DATE(posted_at) as day,
                       ROUND(AVG(engagement_rate), 4) as avg_eng,
                       SUM(views) as total_views,
                       COUNT(*) as video_count
                FROM videos
                WHERE username = {p} AND platform = {p}
                GROUP BY day ORDER BY day ASC
            """, (username, platform))
    result = _fetchall_dict(c)
    conn.close()
    for r in result:
        if r.get('day') and not isinstance(r['day'], str):
            r['day'] = str(r['day'])
        if r.get('avg_eng') and hasattr(r['avg_eng'], '__float__'):
            r['avg_eng'] = float(r['avg_eng'])
    return result


def get_sentiment_summary(username, platform="tiktok"):
    """Returns count of positive/neutral/negative comments."""
    conn = get_conn()
    c = conn.cursor()
    p = "%s" if USE_PG else "?"
    c.execute(f"""
        SELECT sentiment_label, COUNT(*) as count
        FROM comments
        WHERE platform = {p} AND video_id IN (SELECT id FROM videos WHERE username = {p} AND platform = {p})
        GROUP BY sentiment_label
    """, (platform, username, platform))
    rows = _fetchall_dict(c)
    conn.close()
    result = {"positive": 0, "neutral": 0, "negative": 0}
    for r in rows:
        label = r["sentiment_label"] or "neutral"
        result[label] = r["count"]
    return result


def get_kpis(username, platform="tiktok", days=30):
    """Returns aggregate KPI metrics. days=0 means all-time."""
    conn = get_conn()
    c = conn.cursor()
    p = "%s" if USE_PG else "?"
    round_fn = "ROUND(AVG(views)) as avg_views, ROUND(AVG(engagement_rate)*100, 2) as avg_engagement_pct"
    if USE_PG:
        round_fn = "ROUND(AVG(views)) as avg_views, ROUND((AVG(engagement_rate)*100)::numeric, 2) as avg_engagement_pct"
    if days and days > 0:
        since = (datetime.utcnow() - timedelta(days=days)).isoformat()
        c.execute(f"""
            SELECT
                COUNT(*)             as total_videos,
                SUM(views)           as total_views,
                SUM(likes)           as total_likes,
                SUM(comments)        as total_comments,
                SUM(shares)          as total_shares,
                {round_fn}
            FROM videos
            WHERE username = {p} AND platform = {p} AND posted_at >= {p}
        """, (username, platform, since))
    else:
        c.execute(f"""
            SELECT
                COUNT(*)             as total_videos,
                SUM(views)           as total_views,
                SUM(likes)           as total_likes,
                SUM(comments)        as total_comments,
                SUM(shares)          as total_shares,
                {round_fn}
            FROM videos
            WHERE username = {p} AND platform = {p}
        """, (username, platform))
    result = _fetchone_dict(c)
    conn.close()
    if result:
        for key in ('avg_views', 'avg_engagement_pct'):
            if result.get(key) and hasattr(result[key], '__float__'):
                result[key] = float(result[key])
    return result if result else {}


def get_leaderboard_kpis(username, platform="tiktok", days=30):
    """Returns full KPI row including saves/bookmarks for leaderboard."""
    conn = get_conn()
    c = conn.cursor()
    p = "%s" if USE_PG else "?"
    round_fn = "ROUND(AVG(views), 0) as avg_views, ROUND(AVG(engagement_rate)*100, 2) as avg_engagement_pct"
    if USE_PG:
        round_fn = "ROUND(AVG(views)) as avg_views, ROUND((AVG(engagement_rate)*100)::numeric, 2) as avg_engagement_pct"
    if days and days > 0:
        since = (datetime.utcnow() - timedelta(days=days)).isoformat()
        c.execute(f"""
            SELECT
                COUNT(*)                           as total_videos,
                SUM(views)                         as total_views,
                SUM(likes)                         as total_likes,
                SUM(comments)                      as total_comments,
                SUM(shares)                        as total_shares,
                SUM(bookmarks)                     as total_saves,
                {round_fn}
            FROM videos
            WHERE username = {p} AND platform = {p} AND posted_at >= {p}
        """, (username, platform, since))
    else:
        c.execute(f"""
            SELECT
                COUNT(*)                           as total_videos,
                SUM(views)                         as total_views,
                SUM(likes)                         as total_likes,
                SUM(comments)                      as total_comments,
                SUM(shares)                        as total_shares,
                SUM(bookmarks)                     as total_saves,
                {round_fn}
            FROM videos
            WHERE username = {p} AND platform = {p}
        """, (username, platform))
    result = _fetchone_dict(c)
    conn.close()
    if result:
        for key in ('avg_views', 'avg_engagement_pct'):
            if result.get(key) and hasattr(result[key], '__float__'):
                result[key] = float(result[key])
    return result if result else {}


def get_recent_syncs(limit=10):
    conn = get_conn()
    c = conn.cursor()
    p = "%s" if USE_PG else "?"
    c.execute(f"""
        SELECT * FROM sync_log ORDER BY ran_at DESC LIMIT {p}
    """, (limit,))
    result = _fetchall_dict(c)
    conn.close()
    return result


def get_daily_views_gained(username, platform="tiktok", days=30):
    """Returns views gained per day using snapshot deltas."""
    conn = get_conn()
    c = conn.cursor()
    p = "%s" if USE_PG else "?"
    if days and days > 0:
        since = (datetime.utcnow() - timedelta(days=days)).date().isoformat()
        c.execute(f"""
            SELECT day,
                   SUM(CASE WHEN views_gained > 0 THEN views_gained ELSE 0 END) as total_views
            FROM (
                SELECT date as day,
                       views - LAG(views) OVER (PARTITION BY video_id ORDER BY date) as views_gained
                FROM video_snapshots
                WHERE username = {p} AND platform = {p} AND date >= {p}
            ) t
            WHERE day IS NOT NULL
            GROUP BY day ORDER BY day ASC
        """, (username, platform, since))
    else:
        c.execute(f"""
            SELECT day,
                   SUM(CASE WHEN views_gained > 0 THEN views_gained ELSE 0 END) as total_views
            FROM (
                SELECT date as day,
                       views - LAG(views) OVER (PARTITION BY video_id ORDER BY date) as views_gained
                FROM video_snapshots
                WHERE username = {p} AND platform = {p}
            ) t
            WHERE day IS NOT NULL
            GROUP BY day ORDER BY day ASC
        """, (username, platform))
    result = _fetchall_dict(c)
    conn.close()
    return result


def get_views_trend(username, platform="tiktok", days=30):
    """Returns daily total views for sparkline. days=0 means all-time."""
    conn = get_conn()
    c = conn.cursor()
    p = "%s" if USE_PG else "?"
    if days and days > 0:
        since = (datetime.utcnow() - timedelta(days=days)).isoformat()
        if USE_PG:
            c.execute(f"""
                SELECT posted_at::date as day, SUM(views) as total_views
                FROM videos
                WHERE username = {p} AND platform = {p} AND posted_at >= {p}
                GROUP BY day ORDER BY day ASC
            """, (username, platform, since))
        else:
            c.execute(f"""
                SELECT DATE(posted_at) as day, SUM(views) as total_views
                FROM videos
                WHERE username = {p} AND platform = {p} AND posted_at >= {p}
                GROUP BY day ORDER BY day ASC
            """, (username, platform, since))
    else:
        if USE_PG:
            c.execute(f"""
                SELECT posted_at::date as day, SUM(views) as total_views
                FROM videos
                WHERE username = {p} AND platform = {p}
                GROUP BY day ORDER BY day ASC
            """, (username, platform))
        else:
            c.execute(f"""
                SELECT DATE(posted_at) as day, SUM(views) as total_views
                FROM videos
                WHERE username = {p} AND platform = {p}
                GROUP BY day ORDER BY day ASC
            """, (username, platform))
    result = _fetchall_dict(c)
    conn.close()
    for r in result:
        if r.get('day') and not isinstance(r['day'], str):
            r['day'] = str(r['day'])
    return result

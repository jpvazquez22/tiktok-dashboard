"""
db.py — SQLite database setup, insertion, and query helpers.
"""

import sqlite3
import os
from datetime import datetime, timedelta
from config import DB_PATH


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Create all tables if they don't exist."""
    conn = get_conn()
    c = conn.cursor()

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
            PRIMARY KEY (video_id, date)
        );

        CREATE TABLE IF NOT EXISTS profiles (
            username        TEXT PRIMARY KEY,
            display_name    TEXT,
            bio             TEXT,
            followers       INTEGER DEFAULT 0,
            following       INTEGER DEFAULT 0,
            total_videos    INTEGER DEFAULT 0,
            total_hearts    INTEGER DEFAULT 0,
            avatar_url      TEXT,
            last_updated    TEXT
        );

        CREATE TABLE IF NOT EXISTS videos (
            id              TEXT PRIMARY KEY,
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
            recorded_at     TEXT
        );

        CREATE TABLE IF NOT EXISTS comments (
            id              TEXT PRIMARY KEY,
            video_id        TEXT,
            username        TEXT,
            text            TEXT,
            likes           INTEGER DEFAULT 0,
            sentiment       REAL DEFAULT 0,
            sentiment_label TEXT,
            posted_at       TEXT,
            recorded_at     TEXT
        );

        CREATE TABLE IF NOT EXISTS sync_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            ran_at          TEXT,
            username        TEXT,
            videos_fetched  INTEGER DEFAULT 0,
            comments_fetched INTEGER DEFAULT 0,
            status          TEXT,
            message         TEXT
        );
    """)

    # Migrate existing DBs that don't yet have the bookmarks column
    try:
        c.execute("ALTER TABLE video_snapshots ADD COLUMN bookmarks INTEGER DEFAULT 0")
        conn.commit()
    except Exception:
        pass  # column already exists

    conn.commit()
    conn.close()


def upsert_profile(data: dict):
    conn = get_conn()
    conn.execute("""
        INSERT INTO profiles (username, display_name, bio, followers, following,
                              total_videos, total_hearts, avatar_url, last_updated)
        VALUES (:username, :display_name, :bio, :followers, :following,
                :total_videos, :total_hearts, :avatar_url, :last_updated)
        ON CONFLICT(username) DO UPDATE SET
            display_name = excluded.display_name,
            bio          = excluded.bio,
            followers    = excluded.followers,
            following    = excluded.following,
            total_videos = excluded.total_videos,
            total_hearts = excluded.total_hearts,
            avatar_url   = excluded.avatar_url,
            last_updated = excluded.last_updated
    """, data)
    conn.commit()
    conn.close()


def upsert_video(data: dict):
    conn = get_conn()
    conn.execute("""
        INSERT INTO videos (id, username, description, url, cover_url,
                            views, likes, comments, shares, bookmarks,
                            engagement_rate, music_name, duration, posted_at, recorded_at)
        VALUES (:id, :username, :description, :url, :cover_url,
                :views, :likes, :comments, :shares, :bookmarks,
                :engagement_rate, :music_name, :duration, :posted_at, :recorded_at)
        ON CONFLICT(id) DO UPDATE SET
            views           = excluded.views,
            likes           = excluded.likes,
            comments        = excluded.comments,
            shares          = excluded.shares,
            bookmarks       = excluded.bookmarks,
            engagement_rate = excluded.engagement_rate,
            recorded_at     = excluded.recorded_at
    """, data)
    # Save daily snapshot for delta (views gained per day) tracking
    today = datetime.utcnow().date().isoformat()
    conn.execute("""
        INSERT INTO video_snapshots (video_id, username, date, views, likes, comments, shares, bookmarks)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(video_id, date) DO UPDATE SET
            views     = excluded.views,
            likes     = excluded.likes,
            comments  = excluded.comments,
            shares    = excluded.shares,
            bookmarks = excluded.bookmarks
    """, (data['id'], data['username'], today,
          data['views'], data['likes'], data['comments'], data['shares'], data['bookmarks']))
    conn.commit()
    conn.close()


def upsert_comment(data: dict):
    conn = get_conn()
    conn.execute("""
        INSERT OR IGNORE INTO comments
            (id, video_id, username, text, likes, sentiment, sentiment_label, posted_at, recorded_at)
        VALUES
            (:id, :video_id, :username, :text, :likes, :sentiment, :sentiment_label, :posted_at, :recorded_at)
    """, data)
    conn.commit()
    conn.close()


def log_sync(username, videos_fetched, comments_fetched, status, message=""):
    conn = get_conn()
    conn.execute("""
        INSERT INTO sync_log (ran_at, username, videos_fetched, comments_fetched, status, message)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (datetime.utcnow().isoformat(), username, videos_fetched, comments_fetched, status, message))
    conn.commit()
    conn.close()


# ── Dashboard Query Helpers ───────────────────────────────────────────────────

def get_profile(username):
    conn = get_conn()
    row = conn.execute("SELECT * FROM profiles WHERE username = ?", (username,)).fetchone()
    conn.close()
    return dict(row) if row else {}


def get_recent_videos(username, limit=None):
    conn = get_conn()
    if limit:
        rows = conn.execute(
            "SELECT * FROM videos WHERE username = ? ORDER BY posted_at DESC LIMIT ?",
            (username, limit)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM videos WHERE username = ? ORDER BY posted_at DESC",
            (username,)
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_top_videos(username, limit=5):
    conn = get_conn()
    rows = conn.execute("""
        SELECT * FROM videos WHERE username = ?
        ORDER BY views DESC LIMIT ?
    """, (username, limit)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_engagement_trend(username, days=30):
    """Returns daily average engagement rate. days=0 means all-time."""
    conn = get_conn()
    if days and days > 0:
        since = (datetime.utcnow() - timedelta(days=days)).isoformat()
        rows = conn.execute("""
            SELECT DATE(posted_at) as day,
                   ROUND(AVG(engagement_rate), 4) as avg_eng,
                   SUM(views) as total_views,
                   COUNT(*) as video_count
            FROM videos
            WHERE username = ? AND posted_at >= ?
            GROUP BY day ORDER BY day ASC
        """, (username, since)).fetchall()
    else:
        rows = conn.execute("""
            SELECT DATE(posted_at) as day,
                   ROUND(AVG(engagement_rate), 4) as avg_eng,
                   SUM(views) as total_views,
                   COUNT(*) as video_count
            FROM videos
            WHERE username = ?
            GROUP BY day ORDER BY day ASC
        """, (username,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_sentiment_summary(username):
    """Returns count of positive/neutral/negative comments."""
    conn = get_conn()
    rows = conn.execute("""
        SELECT sentiment_label, COUNT(*) as count
        FROM comments
        WHERE video_id IN (SELECT id FROM videos WHERE username = ?)
        GROUP BY sentiment_label
    """, (username,)).fetchall()
    conn.close()
    result = {"positive": 0, "neutral": 0, "negative": 0}
    for r in rows:
        label = r["sentiment_label"] or "neutral"
        result[label] = r["count"]
    return result


def get_kpis(username, days=30):
    """Returns aggregate KPI metrics. days=0 means all-time."""
    conn = get_conn()
    if days and days > 0:
        since = (datetime.utcnow() - timedelta(days=days)).isoformat()
        row = conn.execute("""
            SELECT
                COUNT(*)             as total_videos,
                SUM(views)           as total_views,
                SUM(likes)           as total_likes,
                SUM(comments)        as total_comments,
                SUM(shares)          as total_shares,
                ROUND(AVG(views), 0) as avg_views,
                ROUND(AVG(engagement_rate)*100, 2) as avg_engagement_pct
            FROM videos
            WHERE username = ? AND posted_at >= ?
        """, (username, since)).fetchone()
    else:
        row = conn.execute("""
            SELECT
                COUNT(*)             as total_videos,
                SUM(views)           as total_views,
                SUM(likes)           as total_likes,
                SUM(comments)        as total_comments,
                SUM(shares)          as total_shares,
                ROUND(AVG(views), 0) as avg_views,
                ROUND(AVG(engagement_rate)*100, 2) as avg_engagement_pct
            FROM videos
            WHERE username = ?
        """, (username,)).fetchone()
    conn.close()
    return dict(row) if row else {}


def get_leaderboard_kpis(username, days=30):
    """Returns full KPI row including saves/bookmarks for leaderboard."""
    conn = get_conn()
    if days and days > 0:
        since = (datetime.utcnow() - timedelta(days=days)).isoformat()
        row = conn.execute("""
            SELECT
                COUNT(*)                           as total_videos,
                SUM(views)                         as total_views,
                SUM(likes)                         as total_likes,
                SUM(comments)                      as total_comments,
                SUM(shares)                        as total_shares,
                SUM(bookmarks)                     as total_saves,
                ROUND(AVG(views), 0)               as avg_views,
                ROUND(AVG(engagement_rate)*100, 2) as avg_engagement_pct
            FROM videos
            WHERE username = ? AND posted_at >= ?
        """, (username, since)).fetchone()
    else:
        row = conn.execute("""
            SELECT
                COUNT(*)                           as total_videos,
                SUM(views)                         as total_views,
                SUM(likes)                         as total_likes,
                SUM(comments)                      as total_comments,
                SUM(shares)                        as total_shares,
                SUM(bookmarks)                     as total_saves,
                ROUND(AVG(views), 0)               as avg_views,
                ROUND(AVG(engagement_rate)*100, 2) as avg_engagement_pct
            FROM videos
            WHERE username = ?
        """, (username,)).fetchone()
    conn.close()
    return dict(row) if row else {}


def get_recent_syncs(limit=10):
    conn = get_conn()
    rows = conn.execute("""
        SELECT * FROM sync_log ORDER BY ran_at DESC LIMIT ?
    """, (limit,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_daily_views_gained(username, days=30):
    """
    Returns views gained per day using snapshot deltas.
    days=0 means all-time. Falls back to empty list if no snapshot data yet.
    """
    conn = get_conn()
    if days and days > 0:
        since = (datetime.utcnow() - timedelta(days=days)).date().isoformat()
        rows = conn.execute("""
            SELECT day,
                   SUM(CASE WHEN views_gained > 0 THEN views_gained ELSE 0 END) as total_views
            FROM (
                SELECT date as day,
                       views - LAG(views) OVER (PARTITION BY video_id ORDER BY date) as views_gained
                FROM video_snapshots
                WHERE username = ? AND date >= ?
            ) t
            WHERE day IS NOT NULL
            GROUP BY day ORDER BY day ASC
        """, (username, since)).fetchall()
    else:
        rows = conn.execute("""
            SELECT day,
                   SUM(CASE WHEN views_gained > 0 THEN views_gained ELSE 0 END) as total_views
            FROM (
                SELECT date as day,
                       views - LAG(views) OVER (PARTITION BY video_id ORDER BY date) as views_gained
                FROM video_snapshots
                WHERE username = ?
            ) t
            WHERE day IS NOT NULL
            GROUP BY day ORDER BY day ASC
        """, (username,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_views_trend(username, days=30):
    """Returns daily total views for sparkline. days=0 means all-time."""
    conn = get_conn()
    if days and days > 0:
        since = (datetime.utcnow() - timedelta(days=days)).isoformat()
        rows = conn.execute("""
            SELECT DATE(posted_at) as day, SUM(views) as total_views
            FROM videos
            WHERE username = ? AND posted_at >= ?
            GROUP BY day ORDER BY day ASC
        """, (username, since)).fetchall()
    else:
        rows = conn.execute("""
            SELECT DATE(posted_at) as day, SUM(views) as total_views
            FROM videos
            WHERE username = ?
            GROUP BY day ORDER BY day ASC
        """, (username,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]

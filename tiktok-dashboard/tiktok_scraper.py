"""
tiktok_scraper.py — Free TikTok scraper using direct HTTP requests.
Extracts embedded JSON data from TikTok profile pages.
Drop-in replacement for the old apify_client.py.

Public API:
    scrape_user(username) -> (list_of_videos, profile_dict)
"""

import json
import re
import time
import logging
import subprocess
from datetime import datetime

import requests

from config import MAX_POSTS_PER_USER, MS_TOKEN

logger = logging.getLogger(__name__)

# ── Rate limiting ────────────────────────────────────────────────────────────
_last_scrape_time = 0
MIN_SCRAPE_INTERVAL = 3  # seconds between users

# ── Retry config ─────────────────────────────────────────────────────────────
MAX_RETRIES = 2
RETRY_DELAY = 5  # seconds

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-Dest": "document",
    "Cache-Control": "no-cache",
}


# ── HTTP Scraper ─────────────────────────────────────────────────────────────

def _fetch_page_data(username: str) -> dict:
    """
    Fetch TikTok profile page and extract the embedded JSON data
    from the __UNIVERSAL_DATA_FOR_REHYDRATION__ script tag.
    """
    url = f"https://www.tiktok.com/@{username}"
    cookies = {}
    if MS_TOKEN:
        cookies["msToken"] = MS_TOKEN
    resp = requests.get(url, headers=HEADERS, cookies=cookies, timeout=30)
    resp.raise_for_status()

    # Try __UNIVERSAL_DATA_FOR_REHYDRATION__ first (newer TikTok pages)
    match = re.search(
        r'<script\s+id="__UNIVERSAL_DATA_FOR_REHYDRATION__"[^>]*>(.*?)</script>',
        resp.text,
        re.DOTALL,
    )
    if match:
        data = json.loads(match.group(1))
        return data

    # Fallback: try SIGI_STATE (older TikTok pages)
    match = re.search(
        r'<script\s+id="SIGI_STATE"[^>]*>(.*?)</script>',
        resp.text,
        re.DOTALL,
    )
    if match:
        data = json.loads(match.group(1))
        return data

    logger.warning(f"No embedded JSON found for @{username}")
    return {}


def _extract_profile_and_videos(data: dict, username: str) -> tuple[list, dict]:
    """Parse the embedded page data into our video and profile schemas."""
    videos = []
    profile = {}

    # Navigate the __UNIVERSAL_DATA_FOR_REHYDRATION__ structure
    default_scope = data.get("__DEFAULT_SCOPE__") or {}
    user_detail = default_scope.get("webapp.user-detail") or {}
    user_info = user_detail.get("userInfo") or {}

    # If we got userInfo, parse profile
    if user_info:
        profile = _parse_profile(user_info, username)

    # Try to get video items from the page data
    # Videos might be in different locations depending on TikTok's page structure
    # Method 1: webapp.user-detail may have videos
    item_list = default_scope.get("webapp.user-detail", {}).get("itemList") or []

    # Method 2: Check for video-feed in default scope
    if not item_list:
        video_feed = default_scope.get("webapp.video-feed") or {}
        item_list = video_feed.get("itemList") or []

    # Method 3: SIGI_STATE format
    if not item_list:
        item_module = data.get("ItemModule") or {}
        item_list = list(item_module.values())

    for item in item_list:
        try:
            videos.append(_parse_video(item, username))
        except Exception as e:
            logger.debug(f"Skipping video item: {e}")

    return videos, profile


def _scrape_with_yt_dlp(username: str, max_videos: int) -> list:
    """
    Fallback: use yt-dlp to fetch video metadata.
    Returns list of parsed video dicts.
    """
    videos = []
    try:
        import sys
        cmd = [
            sys.executable, "-m", "yt_dlp",
            "--dump-json",
            "--flat-playlist",
            "--playlist-end", str(max_videos),
            "--no-warnings",
            "--quiet",
            f"https://www.tiktok.com/@{username}",
        ]
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=120
        )
        if result.returncode != 0:
            logger.warning(f"yt-dlp failed for @{username}: {result.stderr[:200]}")
            return []

        for line in result.stdout.strip().split("\n"):
            if not line.strip():
                continue
            try:
                item = json.loads(line)
                videos.append(_parse_video_ytdlp(item, username))
            except Exception as e:
                logger.debug(f"Skipping yt-dlp item: {e}")

    except FileNotFoundError:
        logger.debug("yt-dlp not installed, skipping fallback")
    except subprocess.TimeoutExpired:
        logger.warning(f"yt-dlp timed out for @{username}")

    return videos


# ── Parsers ──────────────────────────────────────────────────────────────────

def _parse_video(item: dict, username: str) -> dict:
    """Normalize a TikTok embedded JSON video item into our video schema."""
    stats = item.get("stats") or item.get("statsV2") or {}
    video_meta = item.get("video") or {}
    music = item.get("music") or {}

    views     = int(stats.get("playCount", 0) or 0)
    likes     = int(stats.get("diggCount", 0) or 0)
    comments  = int(stats.get("commentCount", 0) or 0)
    shares    = int(stats.get("shareCount", 0) or 0)
    bookmarks = int(stats.get("collectCount", 0) or 0)

    eng_rate = (likes + comments + shares) / views if views > 0 else 0

    create_time = item.get("createTime")
    if isinstance(create_time, (int, float)):
        posted_at = datetime.utcfromtimestamp(create_time).isoformat()
    elif isinstance(create_time, str):
        posted_at = create_time
    else:
        posted_at = datetime.utcnow().isoformat()

    video_id = str(item.get("id", ""))

    return {
        "id":              video_id,
        "username":        username,
        "description":     (item.get("desc") or "")[:500],
        "url":             f"https://www.tiktok.com/@{username}/video/{video_id}",
        "cover_url":       video_meta.get("cover") or video_meta.get("dynamicCover") or video_meta.get("originCover") or "",
        "views":           views,
        "likes":           likes,
        "comments":        comments,
        "shares":          shares,
        "bookmarks":       bookmarks,
        "engagement_rate": round(eng_rate, 6),
        "music_name":      music.get("title") or music.get("musicName") or "",
        "duration":        int(video_meta.get("duration", 0) or 0),
        "posted_at":       posted_at,
        "recorded_at":     datetime.utcnow().isoformat(),
    }


def _parse_video_ytdlp(item: dict, username: str) -> dict:
    """Parse a yt-dlp JSON output line into our video schema."""
    views     = int(item.get("view_count", 0) or 0)
    likes     = int(item.get("like_count", 0) or 0)
    comments  = int(item.get("comment_count", 0) or 0)
    shares    = int(item.get("repost_count", 0) or 0)
    bookmarks = int(item.get("save_count", 0) or 0)

    eng_rate = (likes + comments + shares) / views if views > 0 else 0

    ts = item.get("timestamp")
    if isinstance(ts, (int, float)) and ts > 0:
        posted_at = datetime.utcfromtimestamp(ts).isoformat()
    else:
        upload_date = item.get("upload_date", "")
        if upload_date and len(upload_date) == 8:
            posted_at = f"{upload_date[:4]}-{upload_date[4:6]}-{upload_date[6:8]}T00:00:00"
        else:
            posted_at = datetime.utcnow().isoformat()

    video_id = str(item.get("id", ""))

    return {
        "id":              video_id,
        "username":        username,
        "description":     (item.get("title") or item.get("description") or "")[:500],
        "url":             item.get("webpage_url") or f"https://www.tiktok.com/@{username}/video/{video_id}",
        "cover_url":       item.get("thumbnail") or "",
        "views":           views,
        "likes":           likes,
        "comments":        comments,
        "shares":          shares,
        "bookmarks":       bookmarks,
        "engagement_rate": round(eng_rate, 6),
        "music_name":      item.get("track") or "",
        "duration":        int(item.get("duration", 0) or 0),
        "posted_at":       posted_at,
        "recorded_at":     datetime.utcnow().isoformat(),
    }


def _parse_profile(user_info: dict, username: str) -> dict:
    """Extract profile info from TikTok's embedded userInfo."""
    user = user_info.get("user") or {}
    stats = user_info.get("stats") or {}

    return {
        "username":      username,
        "display_name":  user.get("nickname") or user.get("uniqueId") or username,
        "bio":           user.get("signature") or "",
        "followers":     int(stats.get("followerCount", 0) or 0),
        "following":     int(stats.get("followingCount", 0) or 0),
        "total_videos":  int(stats.get("videoCount", 0) or 0),
        "total_hearts":  int(stats.get("heartCount") or stats.get("heart", 0) or 0),
        "avatar_url":    user.get("avatarLarger") or user.get("avatarThumb") or "",
        "last_updated":  datetime.utcnow().isoformat(),
    }


# ── Public sync API (called by app.py) ───────────────────────────────────────

def scrape_user(username: str) -> tuple[list, dict]:
    """
    Synchronous scraper. Same signature as the old apify_client.scrape_user().
    Returns (list_of_videos, profile_dict).

    Strategy:
      1. Fetch TikTok profile page and extract embedded JSON (profile + ~30 videos)
      2. If that fails or returns no videos, try yt-dlp as fallback
    """
    global _last_scrape_time

    # Rate limiting
    elapsed = time.time() - _last_scrape_time
    if elapsed < MIN_SCRAPE_INTERVAL:
        time.sleep(MIN_SCRAPE_INTERVAL - elapsed)

    logger.info(f"Starting HTTP scrape for @{username}...")

    # Retry loop
    for attempt in range(MAX_RETRIES + 1):
        try:
            # Method 1: Direct HTTP page scrape
            page_data = _fetch_page_data(username)
            videos, profile = _extract_profile_and_videos(page_data, username)

            # Method 2: If no videos from page, try yt-dlp
            if not videos:
                max_vids = MAX_POSTS_PER_USER if MAX_POSTS_PER_USER > 0 else 100
                logger.info(f"No videos from page data, trying yt-dlp for @{username}...")
                videos = _scrape_with_yt_dlp(username, max_vids)

            _last_scrape_time = time.time()

            if videos or profile:
                logger.info(f"Scraped {len(videos)} videos for @{username}")
                return videos, profile

            # Empty — retry
            if attempt < MAX_RETRIES:
                logger.warning(
                    f"Empty response for @{username}, retrying in {RETRY_DELAY * (attempt + 1)}s..."
                )
                time.sleep(RETRY_DELAY * (attempt + 1))

        except Exception as e:
            if attempt < MAX_RETRIES:
                logger.warning(f"Attempt {attempt + 1} failed for @{username}: {e}")
                time.sleep(RETRY_DELAY * (attempt + 1))
            else:
                logger.error(f"All {MAX_RETRIES + 1} attempts failed for @{username}: {e}")
                raise

    return [], {}

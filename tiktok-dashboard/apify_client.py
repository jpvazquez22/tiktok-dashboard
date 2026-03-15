"""
apify_client.py — Apify TikTok scraper integration.
Triggers runs, polls for completion, and returns structured data.
"""

import requests
import time
import logging
from datetime import datetime
from config import APIFY_API_TOKEN, TIKTOK_ACTOR_ID, MAX_POSTS_PER_USER

BASE_URL = "https://api.apify.com/v2"
HEADERS = {
    "Authorization": f"Bearer {APIFY_API_TOKEN}",
    "Content-Type": "application/json"
}

logger = logging.getLogger(__name__)


def trigger_run(username: str) -> str:
    """Start an Apify TikTok scraper run. Returns run ID."""
    url = f"{BASE_URL}/acts/{TIKTOK_ACTOR_ID}/runs"

    payload = {
        "profiles": [f"https://www.tiktok.com/@{username}"],
        "resultsPerPage": MAX_POSTS_PER_USER if MAX_POSTS_PER_USER > 0 else 9999,
        "shouldDownloadVideos": False,
        "shouldDownloadCovers": False,
        "shouldDownloadSubtitles": False,
        "shouldDownloadSlideshowImages": False,
    }

    resp = requests.post(url, json=payload, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    run_id = resp.json()["data"]["id"]
    logger.info(f"Apify run started: {run_id} for @{username}")
    return run_id


def wait_for_run(run_id: str, timeout_seconds: int = 600) -> bool:
    """Poll until run finishes. Returns True on success."""
    url = f"{BASE_URL}/actor-runs/{run_id}"
    deadline = time.time() + timeout_seconds

    while time.time() < deadline:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        status = resp.json()["data"]["status"]

        if status == "SUCCEEDED":
            logger.info(f"Run {run_id} succeeded")
            return True
        elif status in ("FAILED", "ABORTED", "TIMED-OUT"):
            logger.error(f"Run {run_id} ended with status: {status}")
            return False

        logger.debug(f"Run {run_id} status: {status} — waiting...")
        time.sleep(8)

    logger.error(f"Run {run_id} timed out after {timeout_seconds}s")
    return False


def fetch_results(run_id: str) -> list:
    """Download dataset items from a completed run."""
    url = f"{BASE_URL}/actor-runs/{run_id}/dataset/items"
    params = {"format": "json", "clean": "true"}
    resp = requests.get(url, headers=HEADERS, params=params, timeout=60)
    resp.raise_for_status()
    items = resp.json()
    logger.info(f"Fetched {len(items)} items from run {run_id}")
    return items


def parse_video(item: dict, username: str) -> dict:
    """Normalize an Apify TikTok item into our video schema."""
    views    = int(item.get("playCount", 0) or 0)
    likes    = int(item.get("diggCount", 0) or 0)
    comments = int(item.get("commentCount", 0) or 0)
    shares   = int(item.get("shareCount", 0) or 0)
    bookmarks = int(item.get("collectCount", 0) or 0)

    # Engagement rate = (likes + comments + shares) / views (avoid div/0)
    eng_rate = (likes + comments + shares) / views if views > 0 else 0

    # Parse timestamp
    create_time = item.get("createTime") or item.get("createTimeISO")
    if isinstance(create_time, int):
        posted_at = datetime.utcfromtimestamp(create_time).isoformat()
    elif isinstance(create_time, str):
        posted_at = create_time
    else:
        posted_at = datetime.utcnow().isoformat()

    # musicMeta is the actual field returned by clockworks actor
    music = item.get("musicMeta") or item.get("music") or {}
    music_name = music.get("musicName", "") or music.get("title", "") if isinstance(music, dict) else str(music)

    video_id = str(item.get("id", item.get("webVideoUrl", "")))

    return {
        "id":              video_id,
        "username":        username,
        "description":     (item.get("text") or item.get("desc") or "")[:500],
        "url":             item.get("webVideoUrl") or item.get("url") or "",
        "cover_url":       item.get("coverUrl") or "",
        "views":           views,
        "likes":           likes,
        "comments":        comments,
        "shares":          shares,
        "bookmarks":       bookmarks,
        "engagement_rate": round(eng_rate, 6),
        "music_name":      music_name,
        "duration":        int((item.get("videoMeta") or {}).get("duration", 0)),
        "posted_at":       posted_at,
        "recorded_at":     datetime.utcnow().isoformat(),
    }


def parse_profile(item: dict, username: str) -> dict:
    """Extract author/profile info from a video item."""
    author = item.get("authorMeta") or {}
    if not isinstance(author, dict):
        author = {}

    return {
        "username":      username,
        "display_name":  author.get("name") or author.get("nickName") or username,
        "bio":           author.get("signature") or "",
        "followers":     int(author.get("fans") or author.get("followerCount") or 0),
        "following":     int(author.get("following") or 0),
        "total_videos":  int(author.get("video") or author.get("videoCount") or 0),
        "total_hearts":  int(author.get("heart") or author.get("heartCount") or 0),
        "avatar_url":    author.get("avatar") or author.get("avatarThumb") or "",
        "last_updated":  datetime.utcnow().isoformat(),
    }


def scrape_user(username: str) -> tuple[list, dict]:
    """
    Full pipeline: trigger → wait → fetch → parse.
    Returns (list_of_videos, profile_dict).
    """
    run_id = trigger_run(username)
    success = wait_for_run(run_id)

    if not success:
        raise RuntimeError(f"Apify run failed for @{username}")

    items = fetch_results(run_id)
    if not items:
        return [], {}

    videos = [parse_video(item, username) for item in items]
    profile = parse_profile(items[0], username)

    return videos, profile

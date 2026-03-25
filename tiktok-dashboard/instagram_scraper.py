"""
instagram_scraper.py — Instagram scraper using instaloader.
Extracts profile info and recent posts for public profiles.

Public API:
    scrape_user(ig_username, canonical_username=None) -> (list_of_posts, profile_dict)
"""

import time
import logging
from datetime import datetime

from config import IG_MAX_POSTS_PER_USER

logger = logging.getLogger(__name__)

# ── Rate limiting ────────────────────────────────────────────────────────────
_last_scrape_time = 0
MIN_SCRAPE_INTERVAL = 30  # seconds between users (IG is aggressive)

# ── Retry config ─────────────────────────────────────────────────────────────
MAX_RETRIES = 1
RETRY_DELAY = 15


def _parse_post(post, canonical_username: str) -> dict:
    """Convert an instaloader Post object to our video schema."""
    views = post.video_view_count or 0 if post.is_video else 0
    likes = post.likes or 0
    comments = post.comments or 0
    shares = 0   # not available via scraping
    bookmarks = 0  # not available via scraping

    # Engagement rate: for reels use views, for photos use likes as proxy
    if views > 0:
        eng_rate = (likes + comments) / views
    elif likes > 0:
        eng_rate = comments / likes if likes > 0 else 0
    else:
        eng_rate = 0

    posted_at = post.date_utc.isoformat() if post.date_utc else datetime.utcnow().isoformat()

    post_id = post.shortcode or str(post.mediaid)

    return {
        "id":              f"ig_{post_id}",
        "username":        canonical_username,
        "description":     (post.caption or "")[:500],
        "url":             f"https://www.instagram.com/p/{post.shortcode}/",
        "cover_url":       post.url or "",
        "views":           views,
        "likes":           likes,
        "comments":        comments,
        "shares":          shares,
        "bookmarks":       bookmarks,
        "engagement_rate": round(eng_rate, 6),
        "music_name":      "",
        "duration":        int(post.video_duration or 0) if post.is_video else 0,
        "posted_at":       posted_at,
        "recorded_at":     datetime.utcnow().isoformat(),
        "platform":        "instagram",
    }


def _parse_profile(ig_profile, canonical_username: str) -> dict:
    """Convert an instaloader Profile object to our profile schema."""
    return {
        "username":      canonical_username,
        "display_name":  ig_profile.full_name or ig_profile.username,
        "bio":           ig_profile.biography or "",
        "followers":     ig_profile.followers or 0,
        "following":     ig_profile.followees or 0,
        "total_videos":  ig_profile.mediacount or 0,
        "total_hearts":  0,  # not available
        "avatar_url":    ig_profile.profile_pic_url or "",
        "last_updated":  datetime.utcnow().isoformat(),
        "platform":      "instagram",
    }


def scrape_user(ig_username: str, canonical_username: str = None) -> tuple:
    """
    Scrape Instagram profile and posts using instaloader.

    Args:
        ig_username: The actual Instagram handle to scrape
        canonical_username: The TikTok username to store data under (defaults to ig_username)

    Returns:
        (list_of_posts, profile_dict)
    """
    global _last_scrape_time

    if canonical_username is None:
        canonical_username = ig_username

    # Rate limiting
    elapsed = time.time() - _last_scrape_time
    if elapsed < MIN_SCRAPE_INTERVAL:
        time.sleep(MIN_SCRAPE_INTERVAL - elapsed)

    logger.info(f"Starting Instagram scrape for @{ig_username} (stored as {canonical_username})...")

    try:
        import instaloader
    except ImportError:
        logger.error("instaloader not installed. Run: pip install instaloader")
        return [], {}

    L = instaloader.Instaloader(
        download_pictures=False,
        download_videos=False,
        download_video_thumbnails=False,
        download_geotags=False,
        download_comments=False,
        save_metadata=False,
        compress_json=False,
        quiet=True,
    )

    for attempt in range(MAX_RETRIES + 1):
        try:
            profile = instaloader.Profile.from_username(L.context, ig_username)
            profile_data = _parse_profile(profile, canonical_username)

            posts = []
            max_posts = IG_MAX_POSTS_PER_USER if IG_MAX_POSTS_PER_USER > 0 else 50
            for i, post in enumerate(profile.get_posts()):
                if i >= max_posts:
                    break
                try:
                    posts.append(_parse_post(post, canonical_username))
                except Exception as e:
                    logger.debug(f"Skipping IG post: {e}")
                # Small delay between posts to avoid rate limiting
                if i > 0 and i % 10 == 0:
                    time.sleep(2)

            _last_scrape_time = time.time()
            logger.info(f"Scraped {len(posts)} IG posts for @{ig_username}")
            return posts, profile_data

        except Exception as e:
            if attempt < MAX_RETRIES:
                logger.warning(f"Attempt {attempt + 1} failed for IG @{ig_username}: {e}")
                time.sleep(RETRY_DELAY * (attempt + 1))
            else:
                logger.error(f"All attempts failed for IG @{ig_username}: {e}")
                _last_scrape_time = time.time()
                return [], {}

    return [], {}

"""Trending/hot detection and statistics computation."""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


@dataclass
class TrendingTopic:
    topic_id: str
    title: str
    url: str
    author: str
    hot_score: float
    view_velocity: float
    reply_velocity: float
    view_delta: int
    reply_delta: int
    is_new: bool


@dataclass
class SummaryStats:
    new_topics: int
    new_posts: int
    active_topics: int
    top_posters: list
    hour_histogram: list


class StatsEngine:
    def __init__(self, config, db):
        self.config = config
        self.db = db
        self.lookback_hours = config["digest"].get("trending_lookback_hours", 24)
        self.trending_count = config["digest"].get("trending_count", 10)

    def compute_trending(self, lookback_hours=None, limit=None):
        """Compute trending topics by hot_score. Returns list of TrendingTopic."""
        lookback = lookback_hours or self.lookback_hours
        limit = limit or self.trending_count

        rows = self.db.get_snapshots_for_trending(lookback)
        trending = []

        for row in rows:
            old_crawled = datetime.fromisoformat(row["old_crawled_at"])
            new_crawled = datetime.fromisoformat(row["new_crawled_at"])
            hours_elapsed = (new_crawled - old_crawled).total_seconds() / 3600

            if hours_elapsed < 0.25:
                continue

            view_delta = (row["new_views"] or 0) - (row["old_views"] or 0)
            reply_delta = (row["new_replies"] or 0) - (row["old_replies"] or 0)

            view_velocity = max(0, view_delta) / hours_elapsed
            reply_velocity = max(0, reply_delta) / hours_elapsed

            hot_score = (view_velocity * 1.0) + (reply_velocity * 50.0)

            # Bonus for new topics (first seen within lookback window)
            is_new = False
            first_seen = row["first_seen"]
            if first_seen:
                first_seen_dt = datetime.fromisoformat(first_seen)
                cutoff = datetime.utcnow() - timedelta(hours=lookback)
                if first_seen_dt > cutoff:
                    is_new = True
                    hot_score *= 1.5

            trending.append(TrendingTopic(
                topic_id=row["topic_id"],
                title=row["title"],
                url=row["url"],
                author=row["author"],
                hot_score=round(hot_score, 1),
                view_velocity=round(view_velocity, 1),
                reply_velocity=round(reply_velocity, 2),
                view_delta=max(0, view_delta),
                reply_delta=max(0, reply_delta),
                is_new=is_new,
            ))

        trending.sort(key=lambda t: t.hot_score, reverse=True)
        return trending[:limit]

    def compute_summary(self, since):
        """Compute summary stats for the period since the given timestamp."""
        since_str = since if isinstance(since, str) else since.isoformat()

        new_topics = len(self.db.get_new_topics_since(since_str))
        new_posts = len(self.db.get_new_posts_since(since_str))
        active_topics_rows = self.db.get_active_topics_since(since_str)
        active_topics = len(active_topics_rows)
        top_posters = self.db.get_top_posters(since_str, 10)
        hour_histogram = self.db.get_post_hour_histogram(since_str)

        return SummaryStats(
            new_topics=new_topics,
            new_posts=new_posts,
            active_topics=active_topics,
            top_posters=[(r["author"], r["post_count"]) for r in top_posters],
            hour_histogram=[(r["hour"], r["count"]) for r in hour_histogram],
        )

    def get_most_discussed(self, since, limit=10):
        """Topics with the most new replies in the period."""
        since_str = since if isinstance(since, str) else since.isoformat()
        rows = self.db.get_active_topics_since(since_str, limit)
        return [dict(r) for r in rows]

    def get_most_viewed(self, since, limit=10):
        """Topics with the biggest view count jump in the period."""
        since_str = since if isinstance(since, str) else since.isoformat()
        lookback = self.lookback_hours

        rows = self.db.get_snapshots_for_trending(lookback)
        viewed = []
        for row in rows:
            view_delta = (row["new_views"] or 0) - (row["old_views"] or 0)
            if view_delta > 0:
                viewed.append({
                    "topic_id": row["topic_id"],
                    "title": row["title"],
                    "url": row["url"],
                    "author": row["author"],
                    "view_delta": view_delta,
                })

        viewed.sort(key=lambda x: x["view_delta"], reverse=True)
        return viewed[:limit]

"""Keyword and user watch matching engine."""

import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class AlertMatch:
    post_id: str
    topic_id: str
    keyword: str
    author: str
    snippet: str
    url: str
    topic_title: str
    match_type: str  # "keyword" or "user"


class AlertEngine:
    def __init__(self, config, db):
        self.keywords = [k.lower() for k in config["alerts"].get("keywords", [])]
        self.watch_users = [u.lower() for u in config["alerts"].get("watch_users", [])]
        self.db = db

    def check_posts(self, new_posts):
        """Check a list of new post dicts for keyword/user matches. Returns list of AlertMatch."""
        if self.should_suppress():
            logger.info("Suppressing alerts (initial crawl not yet complete)")
            return []

        matches = []

        for post in new_posts:
            content_lower = (post.get("content") or "").lower()
            title_lower = (post.get("topic_title") or "").lower()
            author_lower = (post.get("author") or "").lower()

            # Keyword matching — check content and topic title
            for keyword in self.keywords:
                idx = content_lower.find(keyword)
                if idx != -1:
                    snippet = self._extract_snippet(post.get("content", ""), idx, len(keyword))
                    match = AlertMatch(
                        post_id=post["post_id"],
                        topic_id=post["topic_id"],
                        keyword=keyword,
                        author=post.get("author", ""),
                        snippet=snippet,
                        url=post.get("url", ""),
                        topic_title=post.get("topic_title", ""),
                        match_type="keyword",
                    )
                    matches.append(match)
                    self.db.insert_alert_match(post["post_id"], post["topic_id"], keyword, snippet)
                elif keyword in title_lower:
                    snippet = post.get("topic_title", "")
                    match = AlertMatch(
                        post_id=post["post_id"],
                        topic_id=post["topic_id"],
                        keyword=keyword,
                        author=post.get("author", ""),
                        snippet=f"[Title] {snippet}",
                        url=post.get("url", ""),
                        topic_title=post.get("topic_title", ""),
                        match_type="keyword",
                    )
                    matches.append(match)
                    self.db.insert_alert_match(post["post_id"], post["topic_id"], keyword, f"[Title] {snippet}")

            # User watch matching
            for user in self.watch_users:
                if author_lower == user:
                    snippet = (post.get("content") or "")[:200]
                    match = AlertMatch(
                        post_id=post["post_id"],
                        topic_id=post["topic_id"],
                        keyword=f"@{user}",
                        author=post.get("author", ""),
                        snippet=snippet,
                        url=post.get("url", ""),
                        topic_title=post.get("topic_title", ""),
                        match_type="user",
                    )
                    matches.append(match)
                    self.db.insert_alert_match(post["post_id"], post["topic_id"], f"@{user}", snippet)

        if matches:
            logger.info("Found %d alert matches in %d new posts", len(matches), len(new_posts))
        return matches

    def should_suppress(self):
        """Suppress alerts if this is the initial crawl."""
        return self.db.get_meta("initial_crawl_complete") is None

    @staticmethod
    def _extract_snippet(text, match_idx, match_len, context=100):
        """Extract ±100 chars around a match."""
        start = max(0, match_idx - context)
        end = min(len(text), match_idx + match_len + context)
        snippet = text[start:end]
        if start > 0:
            snippet = "..." + snippet
        if end < len(text):
            snippet = snippet + "..."
        return snippet

"""SQLite storage and queries for MTFCA Forum Monitor."""

import sqlite3
import logging
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, db_path="mtfca_monitor.db"):
        self.db_path = db_path
        if db_path != ":memory:":
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.init_db()

    def init_db(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS topics (
                topic_id        TEXT PRIMARY KEY,
                title           TEXT NOT NULL,
                author          TEXT,
                url             TEXT,
                first_seen      DATETIME DEFAULT CURRENT_TIMESTAMP,
                last_reply_date TEXT,
                last_reply_author TEXT
            );

            CREATE TABLE IF NOT EXISTS posts (
                post_id         TEXT PRIMARY KEY,
                topic_id        TEXT NOT NULL,
                author          TEXT,
                date            TEXT,
                content         TEXT,
                url             TEXT,
                first_seen      DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (topic_id) REFERENCES topics(topic_id)
            );

            CREATE TABLE IF NOT EXISTS topic_snapshots (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                topic_id        TEXT NOT NULL,
                replies         INTEGER,
                views           INTEGER,
                crawled_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (topic_id) REFERENCES topics(topic_id)
            );

            CREATE TABLE IF NOT EXISTS alert_matches (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                post_id         TEXT,
                topic_id        TEXT,
                matched_keyword TEXT,
                matched_text    TEXT,
                matched_at      DATETIME DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS digest_log (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                digest_type     TEXT,
                generated_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
                filepath        TEXT
            );

            CREATE TABLE IF NOT EXISTS meta (
                key             TEXT PRIMARY KEY,
                value           TEXT
            );
        """)
        self.conn.commit()

    # --- Topics ---

    def upsert_topic(self, topic_id, title, author, url, last_reply_date=None, last_reply_author=None):
        self.conn.execute("""
            INSERT INTO topics (topic_id, title, author, url, last_reply_date, last_reply_author)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(topic_id) DO UPDATE SET
                title = excluded.title,
                last_reply_date = excluded.last_reply_date,
                last_reply_author = excluded.last_reply_author
        """, (topic_id, title, author, url, last_reply_date, last_reply_author))
        self.conn.commit()

    def get_topic(self, topic_id):
        return self.conn.execute(
            "SELECT * FROM topics WHERE topic_id = ?", (topic_id,)
        ).fetchone()

    def get_topic_count(self):
        return self.conn.execute("SELECT COUNT(*) FROM topics").fetchone()[0]

    def get_new_topics_since(self, since):
        return self.conn.execute(
            "SELECT * FROM topics WHERE first_seen > ? ORDER BY first_seen DESC", (since,)
        ).fetchall()

    # --- Posts ---

    def insert_post(self, post_id, topic_id, author, date, content, url):
        """Insert a post, returns True if it was new (not a duplicate)."""
        cursor = self.conn.execute("""
            INSERT OR IGNORE INTO posts (post_id, topic_id, author, date, content, url)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (post_id, topic_id, author, date, content, url))
        self.conn.commit()
        return cursor.rowcount > 0

    def get_post_count(self):
        return self.conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]

    def get_new_posts_since(self, since):
        return self.conn.execute(
            "SELECT * FROM posts WHERE first_seen > ? ORDER BY first_seen DESC", (since,)
        ).fetchall()

    def get_posts_for_topic(self, topic_id):
        return self.conn.execute(
            "SELECT * FROM posts WHERE topic_id = ? ORDER BY date", (topic_id,)
        ).fetchall()

    # --- Snapshots ---

    def insert_snapshot(self, topic_id, replies, views):
        self.conn.execute("""
            INSERT INTO topic_snapshots (topic_id, replies, views)
            VALUES (?, ?, ?)
        """, (topic_id, replies, views))
        self.conn.commit()

    def get_last_snapshot(self, topic_id):
        """Get the most recent snapshot before the current one."""
        return self.conn.execute("""
            SELECT * FROM topic_snapshots
            WHERE topic_id = ?
            ORDER BY crawled_at DESC
            LIMIT 1 OFFSET 1
        """, (topic_id,)).fetchone()

    def get_snapshots_for_trending(self, lookback_hours):
        """Get earliest and latest snapshot per topic within the lookback window."""
        return self.conn.execute("""
            SELECT
                t.topic_id,
                t.title,
                t.url,
                t.author,
                t.first_seen,
                earliest.replies AS old_replies,
                earliest.views AS old_views,
                earliest.crawled_at AS old_crawled_at,
                latest.replies AS new_replies,
                latest.views AS new_views,
                latest.crawled_at AS new_crawled_at
            FROM topics t
            INNER JOIN (
                SELECT topic_id, replies, views, crawled_at,
                       ROW_NUMBER() OVER (PARTITION BY topic_id ORDER BY crawled_at ASC) AS rn
                FROM topic_snapshots
                WHERE crawled_at > datetime('now', ? || ' hours')
            ) earliest ON t.topic_id = earliest.topic_id AND earliest.rn = 1
            INNER JOIN (
                SELECT topic_id, replies, views, crawled_at,
                       ROW_NUMBER() OVER (PARTITION BY topic_id ORDER BY crawled_at DESC) AS rn
                FROM topic_snapshots
                WHERE crawled_at > datetime('now', ? || ' hours')
            ) latest ON t.topic_id = latest.topic_id AND latest.rn = 1
            WHERE earliest.crawled_at != latest.crawled_at
        """, (f"-{lookback_hours}", f"-{lookback_hours}")).fetchall()

    # --- Alerts ---

    def insert_alert_match(self, post_id, topic_id, keyword, snippet):
        self.conn.execute("""
            INSERT INTO alert_matches (post_id, topic_id, matched_keyword, matched_text)
            VALUES (?, ?, ?, ?)
        """, (post_id, topic_id, keyword, snippet))
        self.conn.commit()

    def get_alerts_since(self, since):
        return self.conn.execute("""
            SELECT am.*, t.title, t.url
            FROM alert_matches am
            LEFT JOIN topics t ON am.topic_id = t.topic_id
            WHERE am.matched_at > ?
            ORDER BY am.matched_at DESC
        """, (since,)).fetchall()

    # --- Digest ---

    def insert_digest_log(self, digest_type, filepath):
        self.conn.execute("""
            INSERT INTO digest_log (digest_type, filepath) VALUES (?, ?)
        """, (digest_type, filepath))
        self.conn.commit()

    def get_last_digest(self, digest_type):
        return self.conn.execute("""
            SELECT * FROM digest_log
            WHERE digest_type = ?
            ORDER BY generated_at DESC
            LIMIT 1
        """, (digest_type,)).fetchone()

    # --- Meta ---

    def get_meta(self, key):
        row = self.conn.execute(
            "SELECT value FROM meta WHERE key = ?", (key,)
        ).fetchone()
        return row["value"] if row else None

    def set_meta(self, key, value):
        self.conn.execute("""
            INSERT INTO meta (key, value) VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """, (key, value))
        self.conn.commit()

    # --- Stats helpers ---

    def get_top_posters(self, since, limit=10):
        return self.conn.execute("""
            SELECT author, COUNT(*) as post_count
            FROM posts WHERE first_seen > ?
            GROUP BY author ORDER BY post_count DESC LIMIT ?
        """, (since, limit)).fetchall()

    def get_post_hour_histogram(self, since):
        return self.conn.execute("""
            SELECT CAST(strftime('%H', date) AS INTEGER) as hour, COUNT(*) as count
            FROM posts WHERE first_seen > ? AND date IS NOT NULL
            GROUP BY hour ORDER BY hour
        """, (since,)).fetchall()

    def get_active_topics_since(self, since, limit=20):
        """Topics with the most new posts in the period."""
        return self.conn.execute("""
            SELECT p.topic_id, t.title, t.url, t.author, COUNT(*) as new_posts
            FROM posts p
            JOIN topics t ON p.topic_id = t.topic_id
            WHERE p.first_seen > ?
            GROUP BY p.topic_id
            ORDER BY new_posts DESC
            LIMIT ?
        """, (since, limit)).fetchall()

    # --- Cleanup ---

    def close(self):
        self.conn.close()

"""Scrapes MTFCA phpBB3 forum HTML, returns structured data."""

import re
import time
import logging
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")


@dataclass
class CrawlResult:
    topics_scanned: int = 0
    new_topics: int = 0
    new_posts: list = field(default_factory=list)
    errors: int = 0


class Crawler:
    def __init__(self, config, db):
        self.config = config
        self.db = db
        self.base_url = config["forum"]["base_url"]
        self.forum_id = config["forum"]["forum_id"]
        self.pages_to_scan = config["forum"]["pages_to_scan"]
        self.delay = config["forum"]["request_delay"]

        self.session = requests.Session()
        self.session.headers["User-Agent"] = "MTFCA-ForumMonitor/1.0 (Personal Use)"

    # --- HTTP ---

    def _fetch(self, url):
        """Fetch a URL with retry and rate limiting."""
        for attempt in range(3):
            try:
                resp = self.session.get(url, timeout=30)
                resp.raise_for_status()
                time.sleep(self.delay)
                return resp.text
            except requests.RequestException as e:
                if attempt < 2:
                    wait = self.delay * (2 ** attempt)
                    logger.warning("Request failed (attempt %d/3): %s — retrying in %.0fs", attempt + 1, e, wait)
                    time.sleep(wait)
                else:
                    logger.error("Request failed after 3 attempts: %s", e)
                    raise

    # --- URL helpers ---

    @staticmethod
    def _strip_sid(url):
        return re.sub(r"[&?]sid=[a-f0-9]+", "", url)

    def _abs_url(self, href):
        """Convert a relative phpBB href to an absolute URL."""
        href = self._strip_sid(href)
        if href.startswith("./"):
            return self.base_url + "/" + href[2:]
        if href.startswith("http"):
            return href
        return self.base_url + "/" + href

    # --- Date parsing ---

    def _parse_date(self, text):
        """Parse phpBB date strings to UTC ISO format."""
        if not text:
            return None

        text = text.strip()
        # Remove leading » or other separators
        text = re.sub(r"^[»›\s]+", "", text).strip()

        now_et = datetime.now(ET)

        # Handle relative dates
        if text.lower().startswith("today"):
            time_part = text.split(None, 1)[1] if " " in text else ""
            try:
                t = datetime.strptime(time_part.strip(), "%I:%M %p")
                dt = now_et.replace(hour=t.hour, minute=t.minute, second=0, microsecond=0)
                return dt.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                pass

        if text.lower().startswith("yesterday"):
            time_part = text.split(None, 1)[1] if " " in text else ""
            try:
                t = datetime.strptime(time_part.strip(), "%I:%M %p")
                dt = (now_et - timedelta(days=1)).replace(hour=t.hour, minute=t.minute, second=0, microsecond=0)
                return dt.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                pass

        # Full format: "Sun Mar 15, 2026 11:40 am"
        for fmt in (
            "%a %b %d, %Y %I:%M %p",
            "%b %d, %Y %I:%M %p",
            "%a %b %d, %Y",
            "%b %d, %Y",
        ):
            try:
                dt = datetime.strptime(text, fmt)
                dt = dt.replace(tzinfo=ET)
                return dt.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue

        logger.debug("Could not parse date: %r", text)
        return None

    # --- Topic listing scraper ---

    def scrape_topic_listing(self):
        """Scrape the forum topic listing pages. Returns list of topic dicts."""
        topics = []

        for page in range(self.pages_to_scan):
            start = page * 100
            url = f"{self.base_url}/viewforum.php?f={self.forum_id}"
            if start > 0:
                url += f"&start={start}"

            logger.info("Scraping topic listing page %d (start=%d)", page + 1, start)
            try:
                html = self._fetch(url)
            except requests.RequestException:
                continue

            soup = BeautifulSoup(html, "lxml")

            # Find all forumbg containers — skip the first one if it's announcements
            containers = soup.select("div.forumbg")
            for container in containers:
                # Skip announcement sections
                header = container.find_previous_sibling("div", class_="forabg") or container
                if "announce" in str(container.get("class", [])).lower():
                    continue

                for row in container.select("li.row"):
                    try:
                        topic = self._parse_topic_row(row)
                        if topic:
                            topics.append(topic)
                    except Exception as e:
                        logger.warning("Failed to parse topic row: %s", e)
                        continue

        return topics

    def _parse_topic_row(self, row):
        """Extract topic data from a single li.row element."""
        # Title and URL
        title_el = row.select_one("a.topictitle")
        if not title_el:
            return None

        title = title_el.get_text(strip=True)
        url = self._abs_url(title_el["href"])

        # Topic ID from URL
        m = re.search(r"t=(\d+)", url)
        if not m:
            return None
        topic_id = m.group(1)

        # Original author
        author = None
        # Look in the topic poster area — typically a div with class containing author info
        poster_area = row.select_one("div.topic-poster") or row
        author_el = poster_area.select_one("a.username, a.username-coloured")
        if not author_el:
            # Fallback: first username link in the row's dt area
            dt = row.select_one("dt")
            if dt:
                author_el = dt.select_one("a.username, a.username-coloured")
        if author_el:
            author = author_el.get_text(strip=True)

        # Reply count
        replies = 0
        replies_el = row.select_one("dd.posts")
        if replies_el:
            m = re.search(r"(\d+)", replies_el.get_text())
            if m:
                replies = int(m.group(1))

        # View count
        views = 0
        views_el = row.select_one("dd.views")
        if views_el:
            m = re.search(r"(\d+)", views_el.get_text())
            if m:
                views = int(m.group(1))

        # Last post info
        last_reply_date = None
        last_reply_author = None
        lastpost_el = row.select_one("dd.lastpost")
        if lastpost_el:
            lp_author_el = lastpost_el.select_one("a.username, a.username-coloured")
            if lp_author_el:
                last_reply_author = lp_author_el.get_text(strip=True)

            # Date is usually in a <time> element or as text after the author
            time_el = lastpost_el.select_one("time")
            if time_el:
                last_reply_date = self._parse_date(time_el.get("datetime") or time_el.get_text())
            else:
                # Try to extract date text from the lastpost dd
                text = lastpost_el.get_text(separator=" ", strip=True)
                # Remove the author name and "by" prefix
                text = re.sub(r"^by\s+\S+\s*", "", text, flags=re.IGNORECASE)
                last_reply_date = self._parse_date(text)

        return {
            "topic_id": topic_id,
            "title": title,
            "author": author,
            "url": url,
            "replies": replies,
            "views": views,
            "last_reply_date": last_reply_date,
            "last_reply_author": last_reply_author,
        }

    # --- Thread post scraper ---

    def scrape_thread_posts(self, topic_url, topic_id, reply_count):
        """Scrape posts from a thread's last page. Returns list of post dicts."""
        # Calculate last page start — assume ~10 posts per page as phpBB default
        posts_per_page = 10
        if reply_count > posts_per_page:
            start = (reply_count // posts_per_page) * posts_per_page
            url = topic_url + f"&start={start}"
        else:
            url = topic_url

        logger.debug("Scraping thread %s (replies=%d, url=%s)", topic_id, reply_count, url)
        try:
            html = self._fetch(url)
        except requests.RequestException:
            return []

        soup = BeautifulSoup(html, "lxml")
        posts = []

        for post_div in soup.select("div.post"):
            try:
                post = self._parse_post(post_div, topic_id)
                if post:
                    posts.append(post)
            except Exception as e:
                logger.warning("Failed to parse post in topic %s: %s", topic_id, e)
                continue

        return posts

    def _parse_post(self, post_div, topic_id):
        """Extract post data from a single div.post element."""
        # Post ID from the element's id attribute (e.g., "p391647")
        post_id_attr = post_div.get("id", "")
        if not post_id_attr:
            # Try parent or wrapper
            wrapper = post_div.find_parent(id=re.compile(r"^p\d+"))
            post_id_attr = wrapper.get("id", "") if wrapper else ""

        post_id = re.sub(r"^p", "", post_id_attr)
        if not post_id:
            return None

        # Author
        author = None
        author_el = post_div.select_one("a.username, a.username-coloured")
        if author_el:
            author = author_el.get_text(strip=True)

        # Date — in the .author div, after the » character
        date = None
        author_div = post_div.select_one(".author")
        if author_div:
            text = author_div.get_text(separator=" ", strip=True)
            # Extract text after »
            parts = text.split("»")
            if len(parts) > 1:
                date = self._parse_date(parts[-1].strip())
            else:
                # Try <time> element
                time_el = author_div.select_one("time")
                if time_el:
                    date = self._parse_date(time_el.get("datetime") or time_el.get_text())

        # Content
        content = None
        content_div = post_div.select_one("div.content")
        if content_div:
            content = content_div.get_text(separator="\n", strip=True)

        # URL
        url = f"{self.base_url}/viewtopic.php?f={self.forum_id}&t={topic_id}#p{post_id}"

        return {
            "post_id": post_id,
            "topic_id": topic_id,
            "author": author,
            "date": date,
            "content": content,
            "url": url,
        }

    # --- Main orchestrator ---

    def run(self):
        """Execute a full crawl cycle. Returns CrawlResult."""
        result = CrawlResult()

        # 1. Scrape topic listings
        topics = self.scrape_topic_listing()
        result.topics_scanned = len(topics)

        topics_to_fetch = []

        for topic in topics:
            tid = topic["topic_id"]

            # Check if this is a new topic
            existing = self.db.get_topic(tid)
            if not existing:
                result.new_topics += 1

            # Upsert topic
            self.db.upsert_topic(
                tid, topic["title"], topic["author"], topic["url"],
                topic["last_reply_date"], topic["last_reply_author"],
            )

            # Insert snapshot
            self.db.insert_snapshot(tid, topic["replies"], topic["views"])

            # Check if replies increased since last snapshot
            prev = self.db.get_last_snapshot(tid)
            if prev is None:
                # First time seeing this topic — fetch its posts
                topics_to_fetch.append(topic)
            elif topic["replies"] > prev["replies"]:
                # New replies since last crawl
                topics_to_fetch.append(topic)

        # 2. Fetch thread pages for topics with new activity
        logger.info("Found %d topics with new activity to fetch", len(topics_to_fetch))

        for topic in topics_to_fetch:
            try:
                posts = self.scrape_thread_posts(
                    topic["url"], topic["topic_id"], topic["replies"]
                )
                for post in posts:
                    was_new = self.db.insert_post(
                        post["post_id"], post["topic_id"],
                        post["author"], post["date"],
                        post["content"], post["url"],
                    )
                    if was_new:
                        # Attach topic title for alert checking
                        post["topic_title"] = topic["title"]
                        result.new_posts.append(post)
            except Exception as e:
                logger.error("Error fetching thread %s: %s", topic["topic_id"], e)
                result.errors += 1

        logger.info(
            "Crawl complete: %d topics scanned, %d new topics, %d new posts, %d errors",
            result.topics_scanned, result.new_topics, len(result.new_posts), result.errors,
        )
        return result

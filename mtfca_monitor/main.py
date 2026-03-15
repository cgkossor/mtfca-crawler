"""MTFCA Forum Monitor — Entry point and scheduler."""

import argparse
import logging
import signal
import sys
import threading
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path

import yaml

from .database import Database
from .crawler import Crawler
from .alerts import AlertEngine
from .stats import StatsEngine
from .digest import DigestGenerator
from .notifier import Notifier

logger = logging.getLogger("mtfca_monitor")


def load_config(path):
    with open(path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return config


def setup_logging():
    root = logging.getLogger("mtfca_monitor")
    root.setLevel(logging.DEBUG)

    # Console handler — INFO
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s %(message)s", datefmt="%H:%M:%S"))
    root.addHandler(ch)

    # File handler — DEBUG, rotating
    fh = RotatingFileHandler("monitor.log", maxBytes=5 * 1024 * 1024, backupCount=3)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    root.addHandler(fh)


def run_once(config, db):
    """Single crawl + alert cycle. Returns (crawl_result, alerts)."""
    notifier = Notifier(config)

    # Crawl
    crawler = Crawler(config, db)
    crawl_result = crawler.run()

    # Console summary
    notifier.console_summary(crawl_result)

    # First-run check — suppress alerts on initial population
    if db.get_meta("initial_crawl_complete") is None:
        db.set_meta("initial_crawl_complete", "true")
        logger.info("Initial crawl complete — alerts suppressed for this run")
        return crawl_result, []

    # Alerts
    alert_engine = AlertEngine(config, db)
    alerts = alert_engine.check_posts(crawl_result.new_posts)

    # Send immediate notifications for keyword alerts
    notifier.notify_alerts(alerts)

    return crawl_result, alerts


def check_digest_due(config, db):
    """Check if it's time to send a digest."""
    digest_cfg = config.get("digest", {})
    frequency = digest_cfg.get("frequency", "daily")
    digest_time = digest_cfg.get("time", "18:00")

    now = datetime.now()

    # Parse configured time
    try:
        hour, minute = map(int, digest_time.split(":"))
    except (ValueError, AttributeError):
        hour, minute = 18, 0

    # Have we passed the digest time today?
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if now < target:
        return False

    # Check last digest
    last = db.get_last_digest(frequency)
    if last is None:
        return True

    last_dt = datetime.fromisoformat(last["generated_at"])
    if frequency == "daily":
        return (now - last_dt) > timedelta(hours=20)
    elif frequency == "weekly":
        return (now - last_dt) > timedelta(days=6)

    return False


def run_digest(config, db):
    """Generate and send a digest."""
    stats_engine = StatsEngine(config, db)
    generator = DigestGenerator(config, db, stats_engine)
    notifier = Notifier(config)

    digest = generator.generate()
    notifier.notify_digest(digest)

    # Save filepath if HTML was generated
    output_dir = config.get("output", {}).get("html_file", {}).get("output_dir", "./output")
    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    filepath = str(Path(output_dir) / f"digest_{date_str}.html")
    db.insert_digest_log(digest.digest_type, filepath)

    logger.info("Digest generated and sent (%s)", digest.digest_type)


def cmd_run(config, db):
    """Single crawl + alerts, then exit."""
    run_once(config, db)


def cmd_digest(config, db):
    """Force-generate a digest now."""
    run_digest(config, db)


def cmd_stats(config, db):
    """Print current stats to console."""
    stats_engine = StatsEngine(config, db)
    now = datetime.utcnow()
    since_24h = (now - timedelta(hours=24)).isoformat()

    print(f"\n{'=' * 50}")
    print("MTFCA Forum Monitor — Stats")
    print(f"{'=' * 50}")
    print(f"Total topics:  {db.get_topic_count()}")
    print(f"Total posts:   {db.get_post_count()}")

    summary = stats_engine.compute_summary(since_24h)
    print(f"\nLast 24 hours:")
    print(f"  New topics:    {summary.new_topics}")
    print(f"  New posts:     {summary.new_posts}")
    print(f"  Active topics: {summary.active_topics}")

    trending = stats_engine.compute_trending()
    if trending:
        print(f"\nTrending (top {len(trending)}):")
        for i, t in enumerate(trending, 1):
            new = " [NEW]" if t.is_new else ""
            print(f"  {i}. {t.title}{new}")
            print(f"     Score: {t.hot_score} | +{t.reply_delta} replies | +{t.view_delta} views")

    if summary.top_posters:
        print("\nTop posters (24h):")
        for name, count in summary.top_posters[:5]:
            print(f"  {name}: {count} posts")

    print()


def cmd_monitor(config, db):
    """Continuous monitoring loop."""
    interval = config.get("schedule", {}).get("poll_interval_minutes", 15)
    shutdown = threading.Event()

    def handle_signal(signum, frame):
        logger.info("Received signal %d, shutting down gracefully...", signum)
        shutdown.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    logger.info("Starting monitor loop (interval: %d minutes)", interval)

    while not shutdown.is_set():
        try:
            run_once(config, db)

            if check_digest_due(config, db):
                run_digest(config, db)
        except Exception as e:
            logger.error("Error in monitor loop: %s", e, exc_info=True)

        shutdown.wait(timeout=interval * 60)

    logger.info("Monitor stopped")


def main():
    parser = argparse.ArgumentParser(description="MTFCA Forum Monitor")
    parser.add_argument("--config", default="config.yaml", help="Path to config file")

    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    subparsers.add_parser("run", help="Single crawl + alerts, then exit")
    subparsers.add_parser("digest", help="Force-generate a digest now")
    subparsers.add_parser("stats", help="Print current stats to console")
    subparsers.add_parser("monitor", help="Continuous 15-minute loop (default)")

    args = parser.parse_args()

    setup_logging()

    # Load config
    config_path = args.config
    if not Path(config_path).exists():
        logger.error("Config file not found: %s", config_path)
        sys.exit(1)

    config = load_config(config_path)

    # Initialize database
    db_path = config.get("database", {}).get("path", "mtfca_monitor.db")
    db = Database(db_path)

    try:
        command = args.command or "monitor"

        if command == "run":
            cmd_run(config, db)
        elif command == "digest":
            cmd_digest(config, db)
        elif command == "stats":
            cmd_stats(config, db)
        elif command == "monitor":
            cmd_monitor(config, db)
        else:
            parser.print_help()
    finally:
        db.close()


if __name__ == "__main__":
    main()

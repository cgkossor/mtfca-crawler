"""Generates daily/weekly digest reports as HTML and plain text."""

import logging
from datetime import datetime, timedelta
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class Digest:
    html: str
    text: str
    digest_type: str
    period_start: str
    period_end: str


class DigestGenerator:
    def __init__(self, config, db, stats_engine):
        self.config = config
        self.db = db
        self.stats = stats_engine

    def generate(self, digest_type=None):
        """Generate a full digest. Returns a Digest object."""
        digest_type = digest_type or self.config["digest"].get("frequency", "daily")
        now = datetime.utcnow()

        # Determine period
        last_digest = self.db.get_last_digest(digest_type)
        if last_digest:
            period_start = last_digest["generated_at"]
        else:
            days = 7 if digest_type == "weekly" else 1
            period_start = (now - timedelta(days=days)).isoformat()

        period_end = now.isoformat()

        # Gather data
        trending = self.stats.compute_trending()
        alerts = self.db.get_alerts_since(period_start)
        new_topics = self.db.get_new_topics_since(period_start)
        most_discussed = self.stats.get_most_discussed(period_start)
        most_viewed = self.stats.get_most_viewed(period_start)
        summary = self.stats.compute_summary(period_start)

        # Build HTML
        html = self._build_html(
            digest_type, period_start, period_end, now,
            trending, alerts, new_topics, most_discussed, most_viewed, summary,
        )

        # Build plain text
        text = self._build_text(
            digest_type, period_start, period_end, now,
            trending, alerts, new_topics, most_discussed, most_viewed, summary,
        )

        return Digest(
            html=html,
            text=text,
            digest_type=digest_type,
            period_start=period_start,
            period_end=period_end,
        )

    def _build_html(self, digest_type, period_start, period_end, now,
                    trending, alerts, new_topics, most_discussed, most_viewed, summary):
        start_short = period_start[:10]
        end_short = period_end[:10]
        title = f"MTFCA Forum Digest — {start_short} to {end_short}"

        sections = []

        # Trending
        if trending:
            rows = ""
            for i, t in enumerate(trending):
                bg = "#f8f9fa" if i % 2 == 0 else "#ffffff"
                badge_color = "#28a745" if t.hot_score < 50 else "#ffc107" if t.hot_score < 200 else "#dc3545"
                new_tag = ' <span style="background:#17a2b8;color:#fff;padding:1px 6px;border-radius:3px;font-size:11px;">NEW</span>' if t.is_new else ""
                rows += f"""<tr style="background:{bg}">
                    <td style="padding:8px"><a href="{t.url}" style="color:#1a73e8;text-decoration:none">{t.title}</a>{new_tag}</td>
                    <td style="padding:8px;text-align:center"><span style="background:{badge_color};color:#fff;padding:2px 8px;border-radius:10px;font-weight:bold">{t.hot_score}</span></td>
                    <td style="padding:8px;text-align:center">+{t.reply_delta}</td>
                    <td style="padding:8px;text-align:center">+{t.view_delta}</td>
                </tr>"""
            sections.append(f"""
                <h2 style="color:#e25822;margin-top:25px">Trending Now</h2>
                <table style="width:100%;border-collapse:collapse;font-size:14px">
                    <tr style="background:#343a40;color:#fff">
                        <th style="padding:8px;text-align:left">Topic</th>
                        <th style="padding:8px;width:80px">Score</th>
                        <th style="padding:8px;width:80px">Replies</th>
                        <th style="padding:8px;width:80px">Views</th>
                    </tr>
                    {rows}
                </table>""")

        # Keyword Alerts
        if alerts:
            alert_rows = ""
            for i, a in enumerate(alerts):
                bg = "#fff3cd" if i % 2 == 0 else "#fff8e1"
                alert_rows += f"""<tr style="background:{bg}">
                    <td style="padding:8px"><strong>{a['matched_keyword']}</strong></td>
                    <td style="padding:8px"><a href="{a['url']}" style="color:#1a73e8">{a['title']}</a></td>
                    <td style="padding:8px;font-size:12px;color:#666">{_truncate(a['matched_text'], 120)}</td>
                </tr>"""
            sections.append(f"""
                <h2 style="color:#dc3545;margin-top:25px">Keyword Alerts</h2>
                <table style="width:100%;border-collapse:collapse;font-size:14px">
                    <tr style="background:#856404;color:#fff">
                        <th style="padding:8px;text-align:left;width:120px">Keyword</th>
                        <th style="padding:8px;text-align:left">Topic</th>
                        <th style="padding:8px;text-align:left">Context</th>
                    </tr>
                    {alert_rows}
                </table>""")

        # New Threads
        if new_topics:
            topic_rows = ""
            for i, t in enumerate(new_topics):
                bg = "#f8f9fa" if i % 2 == 0 else "#ffffff"
                topic_rows += f"""<tr style="background:{bg}">
                    <td style="padding:8px"><a href="{t['url']}" style="color:#1a73e8">{t['title']}</a></td>
                    <td style="padding:8px">{t['author'] or 'Unknown'}</td>
                    <td style="padding:8px;font-size:12px;color:#666">{t['first_seen'][:16]}</td>
                </tr>"""
            sections.append(f"""
                <h2 style="color:#17a2b8;margin-top:25px">New Threads</h2>
                <table style="width:100%;border-collapse:collapse;font-size:14px">
                    <tr style="background:#343a40;color:#fff">
                        <th style="padding:8px;text-align:left">Topic</th>
                        <th style="padding:8px;text-align:left;width:150px">Author</th>
                        <th style="padding:8px;text-align:left;width:130px">Created</th>
                    </tr>
                    {topic_rows}
                </table>""")

        # Most Discussed
        if most_discussed:
            disc_rows = ""
            for i, t in enumerate(most_discussed):
                bg = "#f8f9fa" if i % 2 == 0 else "#ffffff"
                disc_rows += f"""<tr style="background:{bg}">
                    <td style="padding:8px"><a href="{t['url']}" style="color:#1a73e8">{t['title']}</a></td>
                    <td style="padding:8px;text-align:center">{t['new_posts']}</td>
                </tr>"""
            sections.append(f"""
                <h2 style="color:#6f42c1;margin-top:25px">Most Discussed</h2>
                <table style="width:100%;border-collapse:collapse;font-size:14px">
                    <tr style="background:#343a40;color:#fff">
                        <th style="padding:8px;text-align:left">Topic</th>
                        <th style="padding:8px;width:100px">New Posts</th>
                    </tr>
                    {disc_rows}
                </table>""")

        # Most Viewed
        if most_viewed:
            view_rows = ""
            for i, t in enumerate(most_viewed):
                bg = "#f8f9fa" if i % 2 == 0 else "#ffffff"
                view_rows += f"""<tr style="background:{bg}">
                    <td style="padding:8px"><a href="{t['url']}" style="color:#1a73e8">{t['title']}</a></td>
                    <td style="padding:8px;text-align:center">+{t['view_delta']}</td>
                </tr>"""
            sections.append(f"""
                <h2 style="color:#fd7e14;margin-top:25px">Most Viewed</h2>
                <table style="width:100%;border-collapse:collapse;font-size:14px">
                    <tr style="background:#343a40;color:#fff">
                        <th style="padding:8px;text-align:left">Topic</th>
                        <th style="padding:8px;width:100px">View Delta</th>
                    </tr>
                    {view_rows}
                </table>""")

        # Stats Block
        stats_html = f"""
            <h2 style="color:#343a40;margin-top:25px">Stats</h2>
            <div style="display:flex;gap:20px;flex-wrap:wrap;margin-bottom:15px">
                <div style="background:#e3f2fd;padding:15px 25px;border-radius:8px;text-align:center">
                    <div style="font-size:28px;font-weight:bold;color:#1565c0">{summary.new_topics}</div>
                    <div style="font-size:12px;color:#666">New Topics</div>
                </div>
                <div style="background:#e8f5e9;padding:15px 25px;border-radius:8px;text-align:center">
                    <div style="font-size:28px;font-weight:bold;color:#2e7d32">{summary.new_posts}</div>
                    <div style="font-size:12px;color:#666">New Posts</div>
                </div>
                <div style="background:#fff3e0;padding:15px 25px;border-radius:8px;text-align:center">
                    <div style="font-size:28px;font-weight:bold;color:#e65100">{summary.active_topics}</div>
                    <div style="font-size:12px;color:#666">Active Topics</div>
                </div>
            </div>"""

        if summary.top_posters:
            posters = "".join(
                f"<li><strong>{name}</strong> — {count} posts</li>"
                for name, count in summary.top_posters[:10]
            )
            stats_html += f"""
                <h3 style="color:#555;font-size:14px;margin-top:15px">Top Posters</h3>
                <ol style="font-size:13px;color:#333">{posters}</ol>"""

        if summary.hour_histogram:
            max_count = max(c for _, c in summary.hour_histogram) if summary.hour_histogram else 1
            bars = ""
            for hour, count in summary.hour_histogram:
                width = int((count / max_count) * 200)
                bars += f'<div style="display:flex;align-items:center;gap:5px;margin:2px 0"><span style="width:30px;font-size:11px;color:#888;text-align:right">{hour:02d}h</span><div style="background:#4caf50;height:14px;width:{width}px;border-radius:2px"></div><span style="font-size:11px;color:#888">{count}</span></div>'
            stats_html += f"""
                <h3 style="color:#555;font-size:14px;margin-top:15px">Activity by Hour (UTC)</h3>
                {bars}"""

        sections.append(stats_html)

        # Assemble full HTML
        body = "\n".join(sections)
        return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>{title}</title></head>
<body style="margin:0;padding:0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#f5f5f5">
    <div style="background:#1a1a2e;color:#eee;padding:25px 30px">
        <h1 style="margin:0;font-size:22px">{title}</h1>
        <p style="margin:5px 0 0;font-size:13px;color:#aaa">Generated {now.strftime('%Y-%m-%d %H:%M UTC')} | {digest_type.title()} Digest</p>
    </div>
    <div style="max-width:800px;margin:20px auto;padding:0 20px">
        {body}
    </div>
    <div style="background:#343a40;color:#aaa;padding:15px 30px;text-align:center;font-size:12px;margin-top:30px">
        Generated by MTFCA Forum Monitor
    </div>
</body>
</html>"""

    def _build_text(self, digest_type, period_start, period_end, now,
                    trending, alerts, new_topics, most_discussed, most_viewed, summary):
        lines = []
        start_short = period_start[:10]
        end_short = period_end[:10]

        lines.append(f"MTFCA Forum Digest — {start_short} to {end_short}")
        lines.append(f"Generated {now.strftime('%Y-%m-%d %H:%M UTC')} | {digest_type.title()} Digest")
        lines.append("=" * 60)

        if trending:
            lines.append("\nTRENDING NOW")
            lines.append("-" * 40)
            for i, t in enumerate(trending, 1):
                new = " [NEW]" if t.is_new else ""
                lines.append(f"  {i}. {t.title}{new}")
                lines.append(f"     Score: {t.hot_score} | +{t.reply_delta} replies | +{t.view_delta} views")
                lines.append(f"     {t.url}")

        if alerts:
            lines.append("\nKEYWORD ALERTS")
            lines.append("-" * 40)
            for a in alerts:
                lines.append(f'  [{a["matched_keyword"]}] {a["title"]}')
                lines.append(f'    {_truncate(a["matched_text"], 100)}')

        if new_topics:
            lines.append("\nNEW THREADS")
            lines.append("-" * 40)
            for t in new_topics:
                lines.append(f"  - {t['title']} (by {t['author'] or 'Unknown'})")

        if most_discussed:
            lines.append("\nMOST DISCUSSED")
            lines.append("-" * 40)
            for t in most_discussed:
                lines.append(f"  - {t['title']} ({t['new_posts']} new posts)")

        lines.append(f"\nSTATS")
        lines.append("-" * 40)
        lines.append(f"  New topics: {summary.new_topics}")
        lines.append(f"  New posts:  {summary.new_posts}")
        lines.append(f"  Active topics: {summary.active_topics}")

        if summary.top_posters:
            lines.append("\n  Top Posters:")
            for name, count in summary.top_posters[:10]:
                lines.append(f"    {name}: {count} posts")

        lines.append("\n" + "=" * 60)
        lines.append("Generated by MTFCA Forum Monitor")

        return "\n".join(lines)


def _truncate(text, length):
    if not text:
        return ""
    if len(text) <= length:
        return text
    return text[:length] + "..."

"""Multi-channel notification output: console, HTML file, email, webhook."""

import json
import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime
from pathlib import Path

import requests

logger = logging.getLogger(__name__)


class Notifier:
    def __init__(self, config):
        self.config = config
        self.output_cfg = config.get("output", {})

    # --- Dispatchers ---

    def notify_alerts(self, alerts):
        """Send immediate alert notifications via all enabled channels."""
        if not alerts:
            return

        if self._console_enabled():
            for alert in alerts:
                self.console_alert(alert)

        if self._email_enabled() and self.output_cfg["email"].get("send_immediate_alerts", True):
            self.send_email_alerts(alerts)

        if self._webhook_enabled():
            self.send_webhook_alerts(alerts)

        # Always log to file
        if self._html_enabled():
            self.save_alerts_log(alerts)

    def notify_digest(self, digest):
        """Send digest via all enabled channels."""
        if self._console_enabled():
            print(digest.text)

        if self._html_enabled():
            self.save_html_digest(digest)

        if self._email_enabled() and self.output_cfg["email"].get("send_digest", True):
            self.send_email_digest(digest)

        if self._webhook_enabled():
            self.send_webhook_digest(digest)

    # --- Console ---

    def console_alert(self, alert):
        """Print a single alert to console with ANSI colors."""
        use_color = self.output_cfg.get("console", {}).get("color", True)

        if alert.match_type == "keyword":
            if use_color:
                print(f"\033[91m[ALERT]\033[0m Keyword \"\033[91m{alert.keyword}\033[0m\" in "
                      f"\"{alert.topic_title}\" by {alert.author}")
            else:
                print(f"[ALERT] Keyword \"{alert.keyword}\" in \"{alert.topic_title}\" by {alert.author}")
        elif alert.match_type == "user":
            if use_color:
                print(f"\033[93m[WATCH]\033[0m User \033[93m{alert.author}\033[0m posted in "
                      f"\"{alert.topic_title}\"")
            else:
                print(f"[WATCH] User {alert.author} posted in \"{alert.topic_title}\"")

        print(f"  {alert.url}")

    def console_summary(self, crawl_result):
        """Print a brief crawl summary line."""
        now = datetime.now().strftime("%H:%M")
        print(f"[{now}] Crawled {crawl_result.topics_scanned} topics | "
              f"{len(crawl_result.new_posts)} new posts | "
              f"{crawl_result.new_topics} new topics | "
              f"{crawl_result.errors} errors")

    # --- HTML File ---

    def save_html_digest(self, digest):
        """Save digest HTML to output directory."""
        output_dir = Path(self.output_cfg.get("html_file", {}).get("output_dir", "./output"))
        output_dir.mkdir(parents=True, exist_ok=True)

        date_str = datetime.utcnow().strftime("%Y-%m-%d")
        filepath = output_dir / f"digest_{date_str}.html"
        filepath.write_text(digest.html, encoding="utf-8")
        logger.info("Digest saved to %s", filepath)

        if self.output_cfg.get("html_file", {}).get("auto_open", False):
            import webbrowser
            webbrowser.open(str(filepath))

        return str(filepath)

    def save_alerts_log(self, alerts):
        """Append alerts to alerts.log as JSON lines."""
        output_dir = Path(self.output_cfg.get("html_file", {}).get("output_dir", "./output"))
        output_dir.mkdir(parents=True, exist_ok=True)

        log_path = output_dir / "alerts.log"
        with open(log_path, "a", encoding="utf-8") as f:
            for alert in alerts:
                entry = {
                    "timestamp": datetime.utcnow().isoformat(),
                    "keyword": alert.keyword,
                    "match_type": alert.match_type,
                    "topic_title": alert.topic_title,
                    "author": alert.author,
                    "snippet": alert.snippet,
                    "url": alert.url,
                    "post_id": alert.post_id,
                    "topic_id": alert.topic_id,
                }
                f.write(json.dumps(entry) + "\n")

    # --- Email ---

    def send_email_alerts(self, alerts):
        """Send immediate alert email with all keyword matches from this crawl."""
        subject = f"MTFCA Alert: {len(alerts)} keyword match{'es' if len(alerts) != 1 else ''} found"

        # Build HTML body
        rows = ""
        for alert in alerts:
            color = "#dc3545" if alert.match_type == "keyword" else "#ffc107"
            rows += f"""<tr>
                <td style="padding:8px;border-bottom:1px solid #eee">
                    <span style="background:{color};color:#fff;padding:2px 6px;border-radius:3px;font-size:11px">
                        {alert.keyword}
                    </span>
                </td>
                <td style="padding:8px;border-bottom:1px solid #eee">
                    <a href="{alert.url}">{alert.topic_title}</a><br>
                    <small style="color:#666">by {alert.author}</small>
                </td>
                <td style="padding:8px;border-bottom:1px solid #eee;font-size:12px;color:#555">
                    {_truncate(alert.snippet, 150)}
                </td>
            </tr>"""

        html = f"""<html><body style="font-family:sans-serif">
            <h2 style="color:#dc3545">MTFCA Forum Alert</h2>
            <p>{len(alerts)} keyword match{'es' if len(alerts) != 1 else ''} found:</p>
            <table style="border-collapse:collapse;width:100%">
                <tr style="background:#f5f5f5">
                    <th style="padding:8px;text-align:left">Keyword</th>
                    <th style="padding:8px;text-align:left">Topic</th>
                    <th style="padding:8px;text-align:left">Context</th>
                </tr>
                {rows}
            </table>
            <p style="color:#888;font-size:12px;margin-top:20px">— MTFCA Forum Monitor</p>
        </body></html>"""

        # Plain text fallback
        text_lines = [f"MTFCA Forum Alert — {len(alerts)} match(es)\n"]
        for alert in alerts:
            text_lines.append(f"[{alert.keyword}] {alert.topic_title}")
            text_lines.append(f"  by {alert.author}: {_truncate(alert.snippet, 100)}")
            text_lines.append(f"  {alert.url}\n")

        self._send_email(subject, html, "\n".join(text_lines))

    def send_email_digest(self, digest):
        """Send the full digest as an email."""
        date_str = datetime.utcnow().strftime("%Y-%m-%d")
        subject = f"MTFCA Forum Digest — {date_str}"
        self._send_email(subject, digest.html, digest.text)

    def _send_email(self, subject, html_body, text_body):
        """Send a multipart email via SMTP/TLS."""
        email_cfg = self.output_cfg.get("email", {})
        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = email_cfg["from_address"]
            msg["To"] = ", ".join(email_cfg["to_addresses"])

            msg.attach(MIMEText(text_body, "plain"))
            msg.attach(MIMEText(html_body, "html"))

            with smtplib.SMTP(email_cfg["smtp_host"], email_cfg["smtp_port"]) as server:
                server.starttls()
                server.login(email_cfg["smtp_user"], email_cfg["smtp_password"])
                server.sendmail(
                    email_cfg["from_address"],
                    email_cfg["to_addresses"],
                    msg.as_string(),
                )
            logger.info("Email sent: %s", subject)
        except Exception as e:
            logger.error("Failed to send email: %s", e)

    # --- Webhook ---

    def send_webhook_alerts(self, alerts):
        """Send alerts to Discord/Slack webhook."""
        webhook_cfg = self.output_cfg.get("webhook", {})
        platform = webhook_cfg.get("platform", "discord")

        if platform == "discord":
            embeds = []
            for alert in alerts[:10]:  # Discord limit
                color = 0xDC3545 if alert.match_type == "keyword" else 0xFFC107
                embeds.append({
                    "title": f"[{alert.keyword}] {alert.topic_title}",
                    "description": _truncate(alert.snippet, 200),
                    "url": alert.url,
                    "color": color,
                    "fields": [
                        {"name": "Author", "value": alert.author, "inline": True},
                        {"name": "Type", "value": alert.match_type, "inline": True},
                    ],
                })
            self._post_webhook({"embeds": embeds})

        elif platform == "slack":
            blocks = [{"type": "header", "text": {"type": "plain_text", "text": f"MTFCA Alert: {len(alerts)} matches"}}]
            for alert in alerts[:10]:
                blocks.append({
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"*[{alert.keyword}]* <{alert.url}|{alert.topic_title}>\nby {alert.author}: {_truncate(alert.snippet, 150)}"},
                })
            self._post_webhook({"blocks": blocks})

    def send_webhook_digest(self, digest):
        """Send condensed digest to webhook."""
        webhook_cfg = self.output_cfg.get("webhook", {})
        platform = webhook_cfg.get("platform", "discord")

        # Just send a condensed summary
        if platform == "discord":
            self._post_webhook({
                "embeds": [{
                    "title": "MTFCA Forum Digest",
                    "description": digest.text[:2000],
                    "color": 0x1A1A2E,
                }]
            })
        elif platform == "slack":
            self._post_webhook({
                "blocks": [
                    {"type": "header", "text": {"type": "plain_text", "text": "MTFCA Forum Digest"}},
                    {"type": "section", "text": {"type": "mrkdwn", "text": digest.text[:2900]}},
                ]
            })

    def _post_webhook(self, payload):
        """POST JSON to the configured webhook URL."""
        url = self.output_cfg.get("webhook", {}).get("url", "")
        if not url:
            return
        try:
            resp = requests.post(url, json=payload, timeout=10)
            resp.raise_for_status()
            logger.info("Webhook sent successfully")
        except Exception as e:
            logger.error("Webhook failed: %s", e)

    # --- Channel checks ---

    def _console_enabled(self):
        return self.output_cfg.get("console", {}).get("enabled", True)

    def _html_enabled(self):
        return self.output_cfg.get("html_file", {}).get("enabled", True)

    def _email_enabled(self):
        return self.output_cfg.get("email", {}).get("enabled", False)

    def _webhook_enabled(self):
        return self.output_cfg.get("webhook", {}).get("enabled", False)


def _truncate(text, length):
    if not text:
        return ""
    if len(text) <= length:
        return text
    return text[:length] + "..."

# MTFCA Forum Monitor

Monitors the [Model T Ford Club of America phpBB3 forum](https://www.mtfca.com/phpBB3/viewforum.php?f=2) for new posts, keyword matches, and trending threads.

## Features

- **Keyword Alerts** — Instant email when a keyword is mentioned in any post or topic title (case-insensitive)
- **Trending Detection** — Tracks view/reply velocity to surface hot threads
- **Daily/Weekly Digest** — HTML email with trending topics, new threads, most discussed, and stats
- **Incremental Crawling** — Only fetches threads with new activity, minimizing requests
- **SQLite Storage** — Full history of topics, posts, and engagement snapshots

## Quick Start

```bash
# Clone
git clone https://github.com/cgkossor/mtfca-crawler.git
cd mtfca-crawler

# Setup
python3 -m venv venv
source venv/bin/activate          # Linux/Mac
# .\venv\Scripts\Activate.ps1    # Windows PowerShell
pip install -r requirements.txt

# Configure
cp config.yaml.example config.yaml
nano config.yaml                  # Fill in SMTP creds, keywords, recipients
```

## Usage

```bash
# Single crawl + keyword alerts
python -m mtfca_monitor.main run

# Print current stats (topic count, trending, top posters)
python -m mtfca_monitor.main stats

# Force-generate and send a digest now
python -m mtfca_monitor.main digest

# Continuous monitoring loop — runs every 15 minutes (default)
python -m mtfca_monitor.main monitor
```

### First Run

The first `run` populates the database with ~200 topics and suppresses alerts (so you don't get flooded). The second run onward will detect changes and fire keyword alerts.

```bash
python -m mtfca_monitor.main run     # Initial population (no alerts)
# ... wait 15+ minutes ...
python -m mtfca_monitor.main run     # Now detects new posts + sends alerts
```

## Configuration

See `config.yaml.example` for all options. Key settings:

```yaml
alerts:
  keywords:              # Instant email alert on match (case-insensitive)
    - "ecct"
    - "e-timer"
    - "hcct"
  watch_users:           # Alert when these users post anything
    - "SomeUsername"

digest:
  frequency: "daily"     # "daily" or "weekly"
  time: "20:00"          # When to send digest (24hr format)

output:
  email:
    enabled: true
    smtp_host: "smtp.gmail.com"
    smtp_port: 587
    smtp_user: "you@gmail.com"
    smtp_password: "your-app-password"    # Gmail App Password
    from_address: "you@gmail.com"
    to_addresses:
      - "recipient1@gmail.com"
      - "recipient2@example.com"
    send_immediate_alerts: true           # Keyword matches → instant email
    send_digest: true                     # Trending/stats → scheduled digest
```

### Gmail App Password

To use Gmail SMTP, you need an [App Password](https://myaccount.google.com/apppasswords) (not your regular password). Requires 2FA enabled on your Google account.

## VPS Deployment

Deployed as a systemd service on an Ubuntu VPS:

```bash
# On VPS — first time setup
cd /opt/hobbies/services
git clone https://github.com/cgkossor/mtfca-crawler.git mtfca-monitor
cd mtfca-monitor
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Configure
cp config.yaml.example config.yaml
nano config.yaml

# Create data directory
mkdir -p /opt/hobbies/data/mtfca-monitor

# Test it works
python -m mtfca_monitor.main run --config config.yaml

# Install and start systemd service
sudo cp mtfca-monitor.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now mtfca-monitor

# Check status
sudo systemctl status mtfca-monitor
sudo journalctl -u mtfca-monitor -f
```

### Deploying Updates

From your local machine (Windows PowerShell):

```powershell
.\deploy.ps1
```

This pushes to GitHub, SSHs into the VPS, pulls the latest code, and restarts the service.

## Architecture

```
mtfca_monitor/
├── main.py        — CLI entry point + scheduler loop
├── crawler.py     — Scrapes forum HTML (topic listings + thread pages)
├── database.py    — SQLite storage (topics, posts, snapshots, alerts)
├── alerts.py      — Keyword/user matching engine
├── stats.py       — Trending/hot detection algorithm
├── digest.py      — HTML + plain text digest generation
└── notifier.py    — Email, console, HTML file, webhook output
```

### How Trending Works

```
hot_score = (view_velocity × 1.0) + (reply_velocity × 50.0)
```

Reply velocity is weighted 50× higher than views because a new reply is a much stronger engagement signal. New topics with traction get a 1.5× bonus.

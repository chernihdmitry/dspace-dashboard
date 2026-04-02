# DSpace Dashboard

Web dashboard for DSpace: summary stats, downloads, submitters, and ORCID sync views with admin-only access.

Short guide to run the app, configure environment, and install a systemd unit.

## Requirements

- Python 3.9+
- Access to DSpace REST API, Solr, and PostgreSQL

## Quick start (manual)

```bash
cd /opt/dspace-dashboard
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Environment file (/etc/default/dspace-dashboard)

Create the environment file used by systemd:

```bash
sudo tee /etc/default/dspace-dashboard >/dev/null <<'EOF'
# Path to DSpace local.cfg
DSPACE_CONFIG_PATH=/dspace/config/local.cfg

# Flask session secret
SECRET_KEY=replace_with_random_64_chars

# Optional settings
CACHE_TTL_SECONDS=300
START_YEAR=2025
START_MONTH=1

# ORCID metadata field id (required, varies by DSpace instance)
ORCID_FIELD_ID=205
EOF
```

## Systemd unit

Create the unit file:

```bash
sudo tee /etc/systemd/system/dspace-dashboard.service >/dev/null <<'EOF'
[Unit]
Description=DSpace Dashboard (Flask via Gunicorn)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=dspace
Group=dspace
WorkingDirectory=/opt/dspace-dashboard

EnvironmentFile=/etc/default/dspace-dashboard

# Gunicorn слушает localhost или 0.0.0.0
ExecStart=/opt/dspace-dashboard/venv/bin/gunicorn \
  --workers 2 \
  --threads 4 \
  --timeout 60 \
  --bind localhost:8088 \
  app:app

Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
EOF
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now dspace-dashboard
```

Open http://localhost:8088 (or your reverse-proxy URL).

Logs:

```bash
sudo journalctl -u dspace-dashboard -f
```

## Item edits from DSpace logs

Dashboard section **"Редагування"** uses events parsed from DSpace logs.

### Parser daemon (manual run)

Use the long-running daemon for near-real-time updates:

```bash
cd /opt/dspace-dashboard
source .venv/bin/activate
python3 parser_daemon.py --log-glob "/dspace/log/dspace*.log" --log-level INFO
```

### Optional environment overrides

Set these in `/etc/default/dspace-dashboard` (used by both the dashboard and parser services) to fine-tune behavior:

- `DSPACE_EDIT_LOG_GLOB` — glob for log files (default `/dspace/log/dspace.log`).
- `DSPACE_EDIT_POLL_SECONDS` — sleep between iterations (default `5`).
- `DSPACE_EDIT_PENDING_SECONDS` — delay before confirming edit (default `180`).
- `DSPACE_EDIT_DEDUPE_SECONDS` — dedupe window for the same user+item edits (default `60`).
- `DSPACE_SYSTEM_EVENT_RETENTION_HOURS` — how long to keep workflow/system markers (default `48`).
- `DSPACE_REQUEST_CONTEXT_RETENTION_SECONDS` — in-memory request context TTL (default `900`).
- `DSPACE_EDIT_LOG_LEVEL` — log level for the daemon (`INFO`, `DEBUG`, etc.).

### Systemd unit for parser

Create a companion unit that runs alongside the web app:

```bash
sudo tee /etc/systemd/system/dspace-dashboard-parser.service >/dev/null <<'EOF'
[Unit]
Description=DSpace Dashboard Item Edit Parser
After=network-online.target postgresql.service
Wants=network-online.target

[Service]
Type=simple
User=dspace
Group=dspace
WorkingDirectory=/opt/dspace-dashboard
EnvironmentFile=/etc/default/dspace-dashboard
ExecStart=/opt/dspace-dashboard/.venv/bin/python3 parser_daemon.py \
  --log-glob "/dspace/log/dspace*.log"
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF
```

Enable and start the daemon (after deploying code and migrations):

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now dspace-dashboard-parser
```

Follow logs via `sudo journalctl -u dspace-dashboard-parser -f`.

Notes:
- daemon keeps parser state in PostgreSQL (inode + offset) and resumes automatically;
- only confirmed update events are counted (`ItemServiceImpl ::update_item:item_id=...`);
- after the first daemon run, refresh the dashboard page `/item-edits` to populate charts.

## SEO module (Google indexing and Scholar readiness)

Dashboard section **"SEO"** runs technical checks and (optionally) reads indexing data from Google Search Console.

Recommended: add to `/etc/default/dspace-dashboard` (environment):

```bash
GOOGLE_SEARCH_CONSOLE_ENABLED=true
GOOGLE_SEARCH_CONSOLE_CLIENT_ID="..."
GOOGLE_SEARCH_CONSOLE_CLIENT_SECRET="..."
GOOGLE_SEARCH_CONSOLE_REFRESH_TOKEN="..."
```

Search Console property URL is taken automatically from DSpace `local.cfg`:
- `dspace.ui.url` (preferred)
- `dspace.server.url` (fallback)

Notes:
- Search Console integration works with one administrator Google account and stored `refresh_token`.
- No user OAuth flow is used in dashboard UI.
- If Search Console is disabled or credentials are missing, SEO tab still runs robots/sitemap/HTML/PDF checks.
- Search Console API does not expose the same total `Indexed / Not indexed` numbers shown in the Search Console Page Indexing UI. The dashboard can reliably show sitemap data and Search Analytics metrics, but total indexing counts from the UI are not available via the public API.

Performance tuning (optional):

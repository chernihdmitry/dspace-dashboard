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

### Parser script

Run manually:

```bash
cd /opt/dspace-dashboard
source .venv/bin/activate
python3 parse_dspace_edit_logs.py --log-glob "/dspace/log/*.log"
```

Optional environment variable:

```bash
DSPACE_EDIT_LOG_GLOB=/dspace/log/*.log
```

### Cron example

Every 10 minutes:

```bash
*/10 * * * * cd /opt/dspace-dashboard && /opt/dspace-dashboard/.venv/bin/python3 parse_dspace_edit_logs.py --log-glob "/dspace/log/*.log" >> /opt/dspace-dashboard/logs/edit-parser.log 2>&1
```

Notes:
- parser reads logs incrementally (tracks inode + offset in DB);
- only confirmed update events are counted (`ItemServiceImpl ::update_item:item_id=...`);
- after first parser run, open the dashboard page `/item-edits`.

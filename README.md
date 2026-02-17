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

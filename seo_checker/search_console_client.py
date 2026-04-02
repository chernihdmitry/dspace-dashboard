import os
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote

import requests
from dspace_config import get_config_value

TOKEN_URL = "https://oauth2.googleapis.com/token"
SEARCH_CONSOLE_BASE = "https://www.googleapis.com/webmasters/v3"
DEFAULT_TIMEOUT = float(os.getenv("SEO_HTTP_TIMEOUT", "10"))


class GoogleSearchConsoleClient:
    def __init__(self):
        self.enabled = os.getenv("GOOGLE_SEARCH_CONSOLE_ENABLED", "false").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        self.client_id = os.getenv("GOOGLE_SEARCH_CONSOLE_CLIENT_ID", "").strip()
        self.client_secret = os.getenv("GOOGLE_SEARCH_CONSOLE_CLIENT_SECRET", "").strip()
        self.refresh_token = os.getenv("GOOGLE_SEARCH_CONSOLE_REFRESH_TOKEN", "").strip()
        self.site_url = self._normalize_site_url(
            get_config_value("dspace.ui.url", get_config_value("dspace.server.url", "")).strip()
        )
        self._access_token: Optional[str] = None

    @staticmethod
    def _normalize_site_url(site_url: str) -> str:
        value = (site_url or "").strip()
        if not value:
            return ""
        # URL-prefix properties in Search Console should have a trailing slash.
        if value.startswith(("http://", "https://")) and not value.endswith("/"):
            return value + "/"
        return value

    def is_configured(self) -> bool:
        return bool(
            self.enabled
            and self.client_id
            and self.client_secret
            and self.refresh_token
            and self.site_url
        )

    def _fetch_access_token(self) -> str:
        response = requests.post(
            TOKEN_URL,
            data={
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "refresh_token": self.refresh_token,
                "grant_type": "refresh_token",
            },
            timeout=DEFAULT_TIMEOUT,
        )
        response.raise_for_status()
        payload = response.json()
        token = payload.get("access_token", "")
        if not token:
            raise RuntimeError("Google OAuth token response does not contain access_token")
        self._access_token = token
        return token

    def _token(self) -> str:
        if self._access_token:
            return self._access_token
        return self._fetch_access_token()

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._token()}",
            "Accept": "application/json",
        }

    def list_sitemaps(self) -> List[Dict[str, object]]:
        site = quote(self.site_url, safe="")
        url = f"{SEARCH_CONSOLE_BASE}/sites/{site}/sitemaps"
        response = requests.get(url, headers=self._headers(), timeout=DEFAULT_TIMEOUT)
        response.raise_for_status()
        return response.json().get("sitemap", [])

    def _resolve_date_param(self, date_param: str) -> Tuple[date, date, str]:
        today = date.today()
        dp = (date_param or "last30").strip()

        if dp == "today":
            return today, today, "today"
        if dp == "yesterday":
            d = today - timedelta(days=1)
            return d, d, "yesterday"
        if dp == "last7":
            return today - timedelta(days=6), today, "last7"
        if dp == "last30":
            return today - timedelta(days=29), today, "last30"
        if dp == "last365":
            return today - timedelta(days=364), today, "last365"

        if "," in dp:
            left, right = [x.strip() for x in dp.split(",", 1)]
            start_dt = datetime.strptime(left, "%Y-%m-%d").date()
            end_dt = datetime.strptime(right, "%Y-%m-%d").date()
            if start_dt > end_dt:
                start_dt, end_dt = end_dt, start_dt
            return start_dt, end_dt, f"{start_dt.isoformat()},{end_dt.isoformat()}"

        return today - timedelta(days=29), today, "last30"

    def get_search_analytics_summary(self, date_param: str = "last30") -> Dict[str, object]:
        site = quote(self.site_url, safe="")
        url = f"{SEARCH_CONSOLE_BASE}/sites/{site}/searchAnalytics/query"
        start_date, end_date, resolved = self._resolve_date_param(date_param)

        response = requests.post(
            url,
            headers={**self._headers(), "Content-Type": "application/json"},
            json={
                "startDate": start_date.isoformat(),
                "endDate": end_date.isoformat(),
                "type": "web",
                "aggregationType": "auto",
                "rowLimit": 1,
            },
            timeout=DEFAULT_TIMEOUT,
        )
        response.raise_for_status()
        payload = response.json()
        rows = payload.get("rows", []) or []
        row = rows[0] if rows else {}

        return {
            "date_param": resolved,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "clicks": float(row.get("clicks", 0.0) or 0.0),
            "impressions": float(row.get("impressions", 0.0) or 0.0),
            "ctr": float(row.get("ctr", 0.0) or 0.0),
            "position": float(row.get("position", 0.0) or 0.0),
        }

    def get_top_pages(self, date_param: str = "last30", limit: int = 5) -> List[Dict[str, object]]:
        site = quote(self.site_url, safe="")
        url = f"{SEARCH_CONSOLE_BASE}/sites/{site}/searchAnalytics/query"
        start_date, end_date, _ = self._resolve_date_param(date_param)

        response = requests.post(
            url,
            headers={**self._headers(), "Content-Type": "application/json"},
            json={
                "startDate": start_date.isoformat(),
                "endDate": end_date.isoformat(),
                "type": "web",
                "dimensions": ["page"],
                "rowLimit": max(1, limit),
            },
            timeout=DEFAULT_TIMEOUT,
        )
        response.raise_for_status()
        rows = response.json().get("rows", []) or []

        result: List[Dict[str, object]] = []
        for row in rows[:limit]:
            keys = row.get("keys", []) or []
            result.append(
                {
                    "page": keys[0] if keys else "",
                    "clicks": float(row.get("clicks", 0.0) or 0.0),
                    "impressions": float(row.get("impressions", 0.0) or 0.0),
                    "ctr": float(row.get("ctr", 0.0) or 0.0),
                    "position": float(row.get("position", 0.0) or 0.0),
                }
            )
        return result

    def get_indexing_status(self) -> Dict[str, object]:
        sitemaps = self.list_sitemaps()
        submitted_total = 0
        indexed_total = 0
        has_indexed_values = False

        for sm in sitemaps:
            for content in sm.get("contents", []) or []:
                submitted_total += int(content.get("submitted", 0) or 0)
                raw_indexed = content.get("indexed")
                if raw_indexed not in (None, ""):
                    has_indexed_values = True
                    indexed_total += int(raw_indexed or 0)

        return {
            "indexed": max(0, indexed_total) if has_indexed_values else None,
            "not_indexed": max(0, submitted_total - indexed_total) if has_indexed_values else None,
            "submitted": submitted_total,
            "source": "sitemaps",
            "note": "",
        }


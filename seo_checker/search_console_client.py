import os
import logging
from datetime import date, datetime, timedelta
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import quote, urlparse

import requests
from dspace_config import get_config_value

TOKEN_URL = "https://oauth2.googleapis.com/token"
SEARCH_CONSOLE_BASE = "https://www.googleapis.com/webmasters/v3"
DEFAULT_TIMEOUT = float(os.getenv("SEO_HTTP_TIMEOUT", "10"))
LOGGER = logging.getLogger(__name__)


def _extract_hostname(site_url: str) -> str:
    parsed = urlparse((site_url or "").strip())
    return (parsed.hostname or "").strip().lower()


def _parent_domain(hostname: str) -> str:
    labels = [label for label in (hostname or "").split(".") if label]
    if len(labels) <= 2:
        return hostname
    return ".".join(labels[1:])


def _select_search_console_site_url(site_urls: Iterable[str], dspace_url: str) -> Optional[str]:
    available = [str(site_url).strip() for site_url in site_urls if str(site_url).strip()]
    available_set = set(available)
    normalized_dspace_url = GoogleSearchConsoleClient._normalize_site_url(dspace_url)
    hostname = _extract_hostname(normalized_dspace_url)

    candidates = [normalized_dspace_url]
    if hostname:
        candidates.append(f"sc-domain:{hostname}")
        parent = _parent_domain(hostname)
        if parent and parent != hostname:
            candidates.append(f"sc-domain:{parent}")

    for candidate in candidates:
        if candidate in available_set:
            return candidate
    return None


def resolve_search_console_site_url(service, dspace_url: str) -> str:
    """Resolve the best Search Console property for a DSpace URL.

    Supports both google-api-python-client style services and this module's
    requests-based client.
    """
    if hasattr(service, "sites"):
        payload = service.sites().list().execute()
        entries = payload.get("siteEntry", []) or []
    elif hasattr(service, "list_sites"):
        entries = service.list_sites()
    else:
        raise TypeError("service must provide sites().list().execute() or list_sites()")

    site_urls = [str(entry.get("siteUrl", "")).strip() for entry in entries if isinstance(entry, dict)]
    resolved = _select_search_console_site_url(site_urls, dspace_url)
    LOGGER.info("Search Console DSpace URL: %s", dspace_url)
    LOGGER.info("Available Search Console properties: %s", site_urls)
    LOGGER.info("Selected Search Console property: %s", resolved or "")
    if resolved:
        return resolved

    raise RuntimeError(
        "No matching Google Search Console property found for "
        f"DSpace URL {dspace_url!r}. Available properties: {', '.join(site_urls) or '(none)'}"
    )


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
        self.dspace_url = self._normalize_site_url(
            get_config_value("dspace.ui.url", get_config_value("dspace.server.url", "")).strip()
        )
        self.site_url = self.dspace_url
        self._resolved_site_url: Optional[str] = None
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
            and self.dspace_url
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

    def list_sites(self) -> List[Dict[str, object]]:
        url = f"{SEARCH_CONSOLE_BASE}/sites"
        response = requests.get(url, headers=self._headers(), timeout=DEFAULT_TIMEOUT)
        response.raise_for_status()
        return response.json().get("siteEntry", []) or []

    def _site_url(self) -> str:
        if self._resolved_site_url:
            return self._resolved_site_url
        self._resolved_site_url = resolve_search_console_site_url(self, self.dspace_url)
        self.site_url = self._resolved_site_url
        return self._resolved_site_url

    def list_sitemaps(self) -> List[Dict[str, object]]:
        site = quote(self._site_url(), safe="")
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
        site = quote(self._site_url(), safe="")
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
        site = quote(self._site_url(), safe="")
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

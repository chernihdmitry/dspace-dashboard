import os
from typing import Dict, List

from .search_console_client import GoogleSearchConsoleClient


def collect_google_index_data(sample_item_urls: List[str], date_param: str = "last30") -> Dict[str, object]:
    client = GoogleSearchConsoleClient()

    if not client.enabled:
        return {
            "enabled": False,
            "configured": False,
            "indexed": None,
            "not_indexed": None,
            "submitted": 0,
            "source": "unavailable",
            "note": "Google Search Console is disabled.",
            "search_analytics": None,
            "top_pages": [],
            "sitemaps": [],
            "issues": [],
            "site_url": "",
        }

    if not client.is_configured():
        return {
            "enabled": True,
            "configured": False,
            "indexed": None,
            "not_indexed": None,
            "submitted": 0,
            "source": "unavailable",
            "note": "Google Search Console credentials are incomplete.",
            "search_analytics": None,
            "top_pages": [],
            "sitemaps": [],
            "issues": [
                {
                    "severity": "error",
                    "component": "google",
                    "message": "Google Search Console is enabled but credentials are incomplete",
                }
            ],
            "site_url": client.site_url,
        }

    issues = []
    try:
        indexing = client.get_indexing_status()
        sitemaps = client.list_sitemaps()
        search_analytics = client.get_search_analytics_summary(date_param=date_param)
        top_pages = client.get_top_pages(date_param=date_param)
    except Exception as exc:
        return {
            "enabled": True,
            "configured": True,
            "indexed": None,
            "not_indexed": None,
            "submitted": 0,
            "source": "unavailable",
            "note": "Search Console request failed.",
            "search_analytics": None,
            "top_pages": [],
            "sitemaps": [],
            "issues": [
                {
                    "severity": "error",
                    "component": "google",
                    "message": f"Search Console request failed: {exc}",
                }
            ],
            "site_url": client.site_url,
        }

    return {
        "enabled": True,
        "configured": True,
        "indexed": indexing.get("indexed"),
        "not_indexed": indexing.get("not_indexed"),
        "submitted": int(indexing.get("submitted", 0)),
        "source": str(indexing.get("source", "sitemaps")),
        "note": str(indexing.get("note", "")),
        "search_analytics": search_analytics,
        "top_pages": top_pages,
        "sitemaps": sitemaps,
        "issues": issues,
        "site_url": client.site_url,
    }

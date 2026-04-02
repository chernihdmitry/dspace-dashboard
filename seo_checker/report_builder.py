from datetime import datetime
from typing import Dict, List


def _derive_status(issues: List[Dict[str, str]]) -> str:
    if any(issue.get("severity") == "error" for issue in issues):
        return "Critical"
    if any(issue.get("severity") == "warning" for issue in issues):
        return "Needs attention"
    return "Good"


def build_report(data: Dict[str, object]) -> Dict[str, object]:
    issues: List[Dict[str, str]] = []
    for key in (
        "google_issues",
        "robots_issues",
        "sitemap_issues",
        "html_issues",
        "pdf_issues",
        "domain_issues",
        "scholar_issues",
    ):
        issues.extend(data.get(key, []))

    status = _derive_status(issues)

    return {
        "status": status,
        "checked_at": datetime.utcnow().isoformat() + "Z",
        "google": {
            "indexed": data.get("google_indexed"),
            "not_indexed": data.get("google_not_indexed"),
            "submitted": int(data.get("google_submitted", 0)),
            "site_url": data.get("google_site_url", ""),
            "enabled": bool(data.get("google_enabled", False)),
            "configured": bool(data.get("google_configured", False)),
            "source": data.get("google_source", "unavailable"),
            "note": data.get("google_note", ""),
            "date_param": data.get("google_date_param", "last30"),
            "search_analytics": data.get("google_search_analytics"),
            "top_pages": data.get("google_top_pages", []),
            "sitemaps": data.get("google_sitemaps", []),
        },
        "scholar": {
            "estimate": int(data.get("scholar_estimate", 0)),
            "readiness": data.get("scholar_readiness", "Needs fixing"),
            "observed": bool(data.get("scholar_observed", False)),
            "query": data.get("scholar_query", ""),
            "disclaimer": data.get(
                "scholar_disclaimer",
                "Estimate is approximate and does not guarantee actual indexing in Google Scholar.",
            ),
        },
        "robots": {
            "status": data.get("robots_status", "Error"),
            "url": data.get("robots_url", ""),
            "blocked_paths": data.get("robots_blocked_paths", []),
            "http_status": data.get("robots_http_status"),
        },
        "sitemap": {
            "status": data.get("sitemap_status", "Error"),
            "url": data.get("sitemap_url", ""),
            "url_count": int(data.get("sitemap_url_count", 0)),
            "sample_item_urls": data.get("sample_item_urls", []),
        },
        "html": {
            "status": data.get("html_status", "Error"),
            "checks": data.get("html_checks", []),
        },
        "pdf": {
            "status": data.get("pdf_status", "Error"),
            "checks": data.get("pdf_checks", []),
        },
        "domain": {
            "status": data.get("domain_status", "Warning"),
            "item_hosts": data.get("domain_item_hosts", []),
            "pdf_hosts": data.get("domain_pdf_hosts", []),
        },
        "issues": issues,
    }

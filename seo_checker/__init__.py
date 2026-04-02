import os
import time
from typing import Dict

from dspace_config import get_config_value

from .google_index import collect_google_index_data
from .html_checker import check_html_pages
from .pdf_checker import check_domain_consistency, check_pdf
from .report_builder import build_report
from .robots_checker import check_robots
from .scholar_estimator import estimate_scholar_presence, scholar_readiness
from .sitemap_checker import check_sitemap


def _site_url() -> str:
    # Prefer explicit Search Console property URL, fallback to DSpace UI URL.
    return (
        get_config_value("google_search_console.site_url", "").strip()
        or get_config_value("dspace.ui.url", "").strip()
        or get_config_value("dspace.server.url", "").strip()
    )


def run_seo_check(date_param: str = "last30") -> Dict[str, object]:
    started = time.time()
    site_url = _site_url()
    if not site_url:
        raise RuntimeError("SEO check requires google_search_console.site_url or dspace.ui.url in local.cfg")

    timeout = float(os.getenv("SEO_HTTP_TIMEOUT", "10"))

    robots = check_robots(site_url, timeout=timeout)
    sitemap = check_sitemap(site_url, timeout=timeout)

    sample_item_urls = list(sitemap.get("sample_item_urls", []) or [])
    if not sample_item_urls:
        sample_item_urls = [site_url]

    html = check_html_pages(sample_item_urls, timeout=timeout)
    pdf_urls = list(html.get("pdf_urls", []) or [])
    pdf = check_pdf(pdf_urls, timeout=timeout)
    domain = check_domain_consistency(sample_item_urls, [row.get("url", "") for row in pdf.get("checks", [])])

    google = collect_google_index_data(sample_item_urls, date_param=date_param)
    scholar = estimate_scholar_presence(site_url, timeout=timeout)

    has_robots_block = bool(robots.get("blocked_paths"))
    scholar_state = scholar_readiness(
        robots_status=str(robots.get("status", "Error")),
        html_status=str(html.get("status", "Error")),
        pdf_status=str(pdf.get("status", "Error")),
        has_robots_block=has_robots_block,
    )

    report = build_report(
        {
            "google_enabled": google.get("enabled", False),
            "google_configured": google.get("configured", False),
            "google_site_url": google.get("site_url", ""),
            "google_indexed": google.get("indexed", 0),
            "google_not_indexed": google.get("not_indexed", 0),
            "google_submitted": google.get("submitted", 0),
            "google_source": google.get("source", "unavailable"),
            "google_note": google.get("note", ""),
            "google_search_analytics": google.get("search_analytics"),
            "google_top_pages": google.get("top_pages", []),
            "google_date_param": date_param,
            "google_sitemaps": google.get("sitemaps", []),
            "google_issues": google.get("issues", []),
            "scholar_estimate": scholar.get("estimate", 0),
            "scholar_observed": scholar.get("observed", False),
            "scholar_query": scholar.get("query", ""),
            "scholar_disclaimer": scholar.get("disclaimer", ""),
            "scholar_readiness": scholar_state,
            "scholar_issues": scholar.get("issues", []),
            "robots_status": robots.get("status", "Error"),
            "robots_url": robots.get("url", ""),
            "robots_http_status": robots.get("http_status"),
            "robots_blocked_paths": robots.get("blocked_paths", []),
            "robots_issues": robots.get("issues", []),
            "sitemap_status": sitemap.get("status", "Error"),
            "sitemap_url": sitemap.get("url", ""),
            "sitemap_url_count": sitemap.get("url_count", 0),
            "sample_item_urls": sample_item_urls,
            "sitemap_issues": sitemap.get("issues", []),
            "html_status": html.get("status", "Error"),
            "html_checks": html.get("checks", []),
            "html_issues": html.get("issues", []),
            "pdf_status": pdf.get("status", "Error"),
            "pdf_checks": pdf.get("checks", []),
            "pdf_issues": pdf.get("issues", []),
            "domain_status": domain.get("status", "Warning"),
            "domain_item_hosts": domain.get("item_hosts", []),
            "domain_pdf_hosts": domain.get("pdf_hosts", []),
            "domain_issues": domain.get("issues", []),
        }
    )

    report["duration_seconds"] = round(time.time() - started, 2)
    return report

import re
from typing import Dict, List

import requests

REQUIRED_CITATION_META = [
    "citation_title",
    "citation_author",
    "citation_publication_date",
    "citation_pdf_url",
]


def _extract_meta_names(html: str) -> Dict[str, str]:
    found: Dict[str, str] = {}
    lower_html = html.lower()
    for name in REQUIRED_CITATION_META:
        marker = f'name="{name}"'
        marker_alt = f"name='{name}'"
        if marker in lower_html or marker_alt in lower_html:
            found[name] = "present"
    return found


def _extract_citation_pdf_urls(html: str) -> List[str]:
    pattern = re.compile(
        r"<meta[^>]+name=[\"']citation_pdf_url[\"'][^>]+content=[\"']([^\"']+)[\"']",
        flags=re.IGNORECASE,
    )
    return [match.group(1).strip() for match in pattern.finditer(html) if match.group(1).strip()]


def check_html_pages(urls: List[str], timeout: float = 10.0) -> Dict[str, object]:
    checks = []
    pdf_urls: List[str] = []
    issues: List[Dict[str, str]] = []

    for url in urls[:5]:
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            html = response.text or ""
            meta_found = _extract_meta_names(html)
            found_pdf_urls = _extract_citation_pdf_urls(html)
            pdf_urls.extend(found_pdf_urls)

            missing = [name for name in REQUIRED_CITATION_META if name not in meta_found]
            has_ssr_content = len((response.text or "").strip()) > 500
            checks.append(
                {
                    "url": url,
                    "http_status": response.status_code,
                    "meta_found": sorted(meta_found.keys()),
                    "meta_missing": missing,
                    "ssr_content": has_ssr_content,
                    "citation_pdf_urls": found_pdf_urls,
                }
            )

            if missing:
                issues.append(
                    {
                        "severity": "warning",
                        "component": "html",
                        "message": f"Missing citation meta tags on {url}: {', '.join(missing)}",
                    }
                )
            if not has_ssr_content:
                issues.append(
                    {
                        "severity": "error",
                        "component": "html",
                        "message": f"Page looks JS-only or empty for crawlers: {url}",
                    }
                )
        except Exception as exc:
            checks.append(
                {
                    "url": url,
                    "http_status": None,
                    "meta_found": [],
                    "meta_missing": list(REQUIRED_CITATION_META),
                    "ssr_content": False,
                    "citation_pdf_urls": [],
                    "error": str(exc),
                }
            )
            issues.append(
                {
                    "severity": "error",
                    "component": "html",
                    "message": f"Cannot fetch item page {url}: {exc}",
                }
            )

    status = "OK"
    if any(issue["severity"] == "error" for issue in issues):
        status = "Error"
    elif issues:
        status = "Warning"

    return {
        "status": status,
        "checks": checks,
        "pdf_urls": list(dict.fromkeys(pdf_urls)),
        "issues": issues,
    }

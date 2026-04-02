from typing import Dict, List
from urllib.parse import urlparse

import requests


def check_pdf(sample_pdf_urls: List[str], timeout: float = 10.0) -> Dict[str, object]:
    issues: List[Dict[str, str]] = []
    checks = []

    for url in sample_pdf_urls[:5]:
        try:
            response = requests.get(url, timeout=timeout, stream=True)
            response.raise_for_status()
            content_type = (response.headers.get("Content-Type") or "").lower()
            content_length = int(response.headers.get("Content-Length") or 0)

            chunk = b""
            for part in response.iter_content(chunk_size=4096):
                chunk += part
                if len(chunk) > 200000:
                    break

            has_text_markers = b"/Font" in chunk or b"/Contents" in chunk or b"BT" in chunk

            checks.append(
                {
                    "url": url,
                    "http_status": response.status_code,
                    "content_type": content_type,
                    "size_bytes": content_length,
                    "has_text_markers": has_text_markers,
                    "is_pdf": None,
                }
            )
            if content_length and content_length < 1024:
                issues.append(
                    {
                        "severity": "warning",
                        "component": "pdf",
                        "message": f"PDF file is unusually small: {url}",
                    }
                )
        except Exception as exc:
            checks.append(
                {
                    "url": url,
                    "http_status": None,
                    "content_type": "",
                    "size_bytes": 0,
                    "has_text_markers": False,
                    "is_pdf": None,
                    "error": str(exc),
                }
            )
            issues.append(
                {
                    "severity": "error",
                    "component": "pdf",
                    "message": f"Cannot fetch PDF URL {url}: {exc}",
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
        "issues": issues,
    }


def check_domain_consistency(item_urls: List[str], pdf_urls: List[str]) -> Dict[str, object]:
    issues: List[Dict[str, str]] = []
    item_hosts = {urlparse(url).netloc.lower() for url in item_urls if url}
    pdf_hosts = {urlparse(url).netloc.lower() for url in pdf_urls if url}

    status = "OK"
    if item_hosts and pdf_hosts and item_hosts != pdf_hosts:
        status = "Warning"
        issues.append(
            {
                "severity": "warning",
                "component": "domain",
                "message": f"Item and PDF hosts differ: items={sorted(item_hosts)} pdf={sorted(pdf_hosts)}",
            }
        )

    return {
        "status": status,
        "item_hosts": sorted(item_hosts),
        "pdf_hosts": sorted(pdf_hosts),
        "issues": issues,
    }

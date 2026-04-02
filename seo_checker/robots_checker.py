from typing import Dict, List
from urllib.parse import urljoin, urlparse

import requests


def check_robots(site_url: str, timeout: float = 10.0) -> Dict[str, object]:
    robots_url = urljoin(site_url.rstrip("/") + "/", "robots.txt")
    issues: List[Dict[str, str]] = []

    try:
        response = requests.get(robots_url, timeout=timeout)
    except Exception as exc:
        return {
            "status": "Error",
            "url": robots_url,
            "available": False,
            "http_status": None,
            "blocked_paths": ["/items/", "/handle/", ".pdf"],
            "issues": [
                {
                    "severity": "error",
                    "component": "robots",
                    "message": f"robots.txt is not reachable: {exc}",
                }
            ],
        }

    body = response.text if response.ok else ""
    normalized = body.lower()
    blocked = []
    for marker in ("/items/", "/handle/", ".pdf"):
        if f"disallow: {marker}" in normalized:
            blocked.append(marker)

    if response.status_code >= 400:
        issues.append(
            {
                "severity": "error",
                "component": "robots",
                "message": f"robots.txt returns HTTP {response.status_code}",
            }
        )
    if blocked:
        issues.append(
            {
                "severity": "error",
                "component": "robots",
                "message": f"robots.txt blocks indexing paths: {', '.join(blocked)}",
            }
        )

    status = "OK"
    if blocked or response.status_code >= 400:
        status = "Error"
    elif "sitemap:" not in normalized:
        status = "Warning"
        issues.append(
            {
                "severity": "warning",
                "component": "robots",
                "message": "robots.txt does not contain Sitemap directive",
            }
        )

    parsed = urlparse(site_url)
    return {
        "status": status,
        "url": robots_url,
        "available": response.ok,
        "http_status": response.status_code,
        "host": parsed.netloc,
        "blocked_paths": blocked,
        "issues": issues,
    }

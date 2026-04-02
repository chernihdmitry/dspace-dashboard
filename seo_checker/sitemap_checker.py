from typing import Dict, List, Set
from urllib.parse import urljoin
import xml.etree.ElementTree as ET

import requests


def _strip_ns(tag: str) -> str:
    return tag.split("}")[-1] if "}" in tag else tag


def _looks_like_item_url(url: str) -> bool:
    lowered = url.lower()
    return "/items/" in lowered or "/handle/" in lowered


def _discover_sitemap_urls(site_url: str, timeout: float) -> List[str]:
    robots_url = urljoin(site_url.rstrip("/") + "/", "robots.txt")
    discovered: List[str] = []

    try:
        response = requests.get(robots_url, timeout=timeout)
        if response.ok:
            for raw_line in response.text.splitlines():
                line = raw_line.strip()
                if not line:
                    continue
                if line.lower().startswith("sitemap:"):
                    candidate = line.split(":", 1)[1].strip()
                    if candidate:
                        discovered.append(urljoin(site_url.rstrip("/") + "/", candidate))
    except Exception:
        # sitemap discovery from robots is optional; fallback to default sitemap.xml
        pass

    if not discovered:
        discovered.append(urljoin(site_url.rstrip("/") + "/", "sitemap.xml"))

    # Common DSpace-style split sitemaps: /sitemap0.xml, /sitemap1.xml, ...
    # We probe a reasonable range and keep only URLs that respond with 200.
    for idx in range(0, 50):
        candidate = urljoin(site_url.rstrip("/") + "/", f"sitemap{idx}.xml")
        try:
            probe = requests.get(candidate, timeout=timeout)
            if probe.status_code == 200:
                discovered.append(candidate)
        except Exception:
            continue

    # Preserve order, remove duplicates
    return list(dict.fromkeys(discovered))


def check_sitemap(site_url: str, timeout: float = 10.0, max_urls: int = 5000) -> Dict[str, object]:
    sitemap_candidates = _discover_sitemap_urls(site_url, timeout)
    issues: List[Dict[str, str]] = []
    found_urls: List[str] = []
    visited: Set[str] = set()

    def parse_sitemap(url: str):
        if url in visited or len(found_urls) >= max_urls:
            return
        visited.add(url)

        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        root = ET.fromstring(response.content)
        root_tag = _strip_ns(root.tag)

        if root_tag == "sitemapindex":
            for child in root:
                if _strip_ns(child.tag) != "sitemap":
                    continue
                loc = None
                for node in child:
                    if _strip_ns(node.tag) == "loc":
                        loc = (node.text or "").strip()
                        break
                if loc:
                    parse_sitemap(loc)
            return

        if root_tag == "urlset":
            for child in root:
                if _strip_ns(child.tag) != "url":
                    continue
                for node in child:
                    if _strip_ns(node.tag) == "loc":
                        loc = (node.text or "").strip()
                        if loc:
                            found_urls.append(loc)
                        break
            return

        raise ValueError(f"Unknown sitemap root tag: {root.tag}")

    successful_sitemaps: List[str] = []
    last_exc: Exception = Exception("No sitemap candidates")
    for candidate in sitemap_candidates:
        try:
            parse_sitemap(candidate)
            successful_sitemaps.append(candidate)
        except Exception as exc:
            last_exc = exc

    if not successful_sitemaps:
        return {
            "status": "Error",
            "url": sitemap_candidates[0] if sitemap_candidates else "",
            "urls": [],
            "valid": False,
            "url_count": 0,
            "sample_item_urls": [],
            "issues": [
                {
                    "severity": "error",
                    "component": "sitemap",
                    "message": f"sitemap check failed: {last_exc}",
                }
            ],
        }

    if not found_urls:
        issues.append(
            {
                "severity": "warning",
                "component": "sitemap",
                "message": "sitemap exists but contains no URLs",
            }
        )

    sample_item_urls: List[str] = []
    for url in found_urls:
        if _looks_like_item_url(url):
            sample_item_urls.append(url)
        if len(sample_item_urls) >= 5:
            break

    status = "OK"
    if issues:
        status = "Warning"

    return {
        "status": status,
        "url": successful_sitemaps[0],
        "urls": successful_sitemaps,
        "valid": True,
        "url_count": len(found_urls),
        "sample_item_urls": sample_item_urls,
        "issues": issues,
    }

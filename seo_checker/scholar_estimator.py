from typing import Dict, List
from urllib.parse import urlparse
import re

import requests


SCHOLAR_SEARCH_URL = "https://scholar.google.com/scholar"


def _parse_scholar_count(html: str) -> int:
    # Google Scholar renders result totals in different localized formats.
    patterns = [
        r"About\s+([\d\s,\.\u00a0\u202f]+)\s+result(?:s)?",
        r"About\s+([\d\s,\.\u00a0\u202f]+)\s+results",
        r"Results\s*:?\s*about\s*([\d\s,\.\u00a0\u202f]+)",
        r"Результатов\s*:?\s*примерно\s*([\d\s,\.\u00a0\u202f]+)",
        r"Результатов\s*:?\s*([\d\s,\.\u00a0\u202f]+)",
        r"Приблизна\s+кількість\s+результатів\s*:?\s*([\d\s,\.\u00a0\u202f]+)",
        r"Результати\s*:?\s*([\d\s,\.\u00a0\u202f]+)",
        r"Примерно\s+([\d\s,\.\u00a0\u202f]+)\s+результ",
    ]

    for pattern in patterns:
        match = re.search(pattern, html, flags=re.IGNORECASE)
        if not match:
            continue
        raw = match.group(1)
        digits_only = re.sub(r"[^\d]", "", raw)
        if digits_only:
            return int(digits_only)
    return 0


def estimate_scholar_presence(site_url: str, timeout: float = 10.0) -> Dict[str, object]:
    host = urlparse(site_url).netloc
    query = f"site:{host}"
    issues: List[Dict[str, str]] = []

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; dspace-dashboard-seo-checker/1.0)",
    }

    estimate = 0
    observed = False
    try:
        response = requests.get(
            SCHOLAR_SEARCH_URL,
            params={"q": query, "hl": "uk", "as_sdt": "0,5", "btnG": ""},
            headers=headers,
            timeout=timeout,
        )
        response.raise_for_status()
        text = response.text
        lowered = text.lower()
        if "unusual traffic" in lowered or "необычный трафик" in lowered:
            issues.append(
                {
                    "severity": "warning",
                    "component": "scholar",
                    "message": "Scholar blocked automated request (unusual traffic), estimate may be unavailable",
                }
            )

        estimate = _parse_scholar_count(text)
        if estimate > 0:
            observed = True
        else:
            issues.append(
                {
                    "severity": "warning",
                    "component": "scholar",
                    "message": "Scholar estimate unavailable (likely anti-bot or markup change)",
                }
            )
    except Exception as exc:
        issues.append(
            {
                "severity": "warning",
                "component": "scholar",
                "message": f"Scholar probe failed: {exc}",
            }
        )

    return {
        "query": query,
        "estimate": estimate,
        "observed": observed,
        "disclaimer": "Estimate is approximate and does not guarantee actual indexing in Google Scholar.",
        "issues": issues,
    }


def scholar_readiness(robots_status: str, html_status: str, pdf_status: str, has_robots_block: bool) -> str:
    if has_robots_block or html_status == "Error" or pdf_status == "Error":
        return "Broken"
    if robots_status == "Warning" or html_status == "Warning" or pdf_status == "Warning":
        return "Needs fixing"
    return "Good"

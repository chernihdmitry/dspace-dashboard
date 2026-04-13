from typing import Dict, List
from urllib.parse import urlparse
import re
import os

import requests


SCHOLAR_SEARCH_URL = "https://scholar.google.com/scholar"
SCHOLAR_HOME_URL = "https://scholar.google.com/"


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

    base_headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Upgrade-Insecure-Requests": "1",
    }
    user_agents = [
        (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        (
            "Mozilla/5.0 (X11; Linux x86_64; rv:124.0) "
            "Gecko/20100101 Firefox/124.0"
        ),
    ]

    proxy_url = os.getenv("SCHOLAR_PROXY_URL", "").strip()
    proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else None

    estimate = 0
    observed = False
    try:
        response = None
        last_status = None
        for user_agent in user_agents:
            with requests.Session() as session:
                headers = dict(base_headers)
                headers["User-Agent"] = user_agent

                # Prime cookies like a normal browser before search query.
                try:
                    session.get(
                        SCHOLAR_HOME_URL,
                        headers=headers,
                        timeout=timeout,
                        proxies=proxies,
                    )
                except Exception:
                    pass

                search_headers = dict(headers)
                search_headers["Referer"] = SCHOLAR_HOME_URL

                response = session.get(
                    SCHOLAR_SEARCH_URL,
                    params={"q": query, "hl": "uk", "as_sdt": "0,5", "btnG": ""},
                    headers=search_headers,
                    timeout=timeout,
                    proxies=proxies,
                )
                last_status = response.status_code
                if response.status_code < 400:
                    break

        if response is None:
            raise RuntimeError("Scholar request failed before receiving a response")

        if response.status_code >= 400:
            if response.status_code == 403:
                issues.append(
                    {
                        "severity": "warning",
                        "component": "scholar",
                        "message": "Scholar probe blocked with 403 (likely cloud/DC IP reputation or anti-bot challenge)",
                    }
                )
            else:
                issues.append(
                    {
                        "severity": "warning",
                        "component": "scholar",
                        "message": f"Scholar probe failed with HTTP {last_status}",
                    }
                )
        else:
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

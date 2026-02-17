import os
from typing import Dict, Optional
from dotenv import load_dotenv

# Load environment variables (/.env and /etc/default/dspace-dashboard)
if os.path.exists("/etc/default/dspace-dashboard"):
    load_dotenv("/etc/default/dspace-dashboard")
load_dotenv()

_CONFIG_CACHE: Optional[Dict[str, str]] = None
_CONFIG_PATH: Optional[str] = None


def get_config_path() -> str:
    return os.getenv("DSPACE_CONFIG_PATH", "/dspace/config/local.cfg")


def _read_config_file(path: str) -> Dict[str, str]:
    if not os.path.exists(path):
        return {}

    def _parse_lines(lines) -> Dict[str, str]:
        parsed: Dict[str, str] = {}
        for raw_line in lines:
            line = raw_line.strip()
            if not line or line.startswith("#") or line.startswith("!"):
                continue
            if "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip().lstrip("\ufeff")
            value = value.strip()

            if len(value) >= 2 and (
                (value.startswith('"') and value.endswith('"'))
                or (value.startswith("'") and value.endswith("'"))
            ):
                value = value[1:-1]

            if key:
                parsed[key] = value
        return parsed

    encodings = ("utf-8", "utf-16", "latin-1")
    for encoding in encodings:
        try:
            with open(path, "r", encoding=encoding, errors="replace") as handle:
                parsed = _parse_lines(handle)
                if parsed:
                    return parsed
        except Exception:
            continue
    return {}


def _load_config() -> Dict[str, str]:
    global _CONFIG_CACHE, _CONFIG_PATH

    path = get_config_path()
    _CONFIG_CACHE = _read_config_file(path)
    _CONFIG_PATH = path
    return _CONFIG_CACHE


def get_config_value(key: str, default: str = "") -> str:
    config = _load_config()
    value = config.get(key)
    if value is None or value == "":
        return default
    return value

"""
Matomo Analytics API Client
Получение данных через Matomo Reporting API
"""
import os
import requests
from datetime import datetime
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv
from dspace_config import get_config_value

# Загрузка переменных окружения из файлов
if os.path.exists("/etc/default/dspace-dashboard"):
    load_dotenv("/etc/default/dspace-dashboard")
load_dotenv()  # загружает .env если есть


# -----------------------------
# Config
# -----------------------------

MATOMO_BASE_URL = get_config_value("matomo.tracker.url", os.getenv("MATOMO_BASE_URL", "")).rstrip("/")
MATOMO_SITE_ID = get_config_value("matomo.request.siteid", os.getenv("MATOMO_SITE_ID", ""))
MATOMO_TOKEN_AUTH = get_config_value("matomo.async-client.token", os.getenv("MATOMO_TOKEN_AUTH", ""))
MATOMO_ENABLED = get_config_value("matomo.enabled", os.getenv("MATOMO_ENABLED", "")).strip().lower()
MATOMO_TIMEOUT = float(os.getenv("MATOMO_TIMEOUT", "10"))


# Cache для последних запросов (простой in-memory кеш на 60 секунд)
_cache = {}
_cache_ttl = {}


def _is_cache_valid(key: str) -> bool:
    """Проверка валидности кеша (60 секунд)"""
    if key not in _cache_ttl:
        return False
    return (datetime.now() - _cache_ttl[key]).total_seconds() < 60


def _get_from_cache(key: str) -> Optional[Any]:
    """Получить из кеша, если валидно"""
    if _is_cache_valid(key):
        return _cache.get(key)
    return None


def _set_to_cache(key: str, value: Any):
    """Сохранить в кеш"""
    _cache[key] = value
    _cache_ttl[key] = datetime.now()


# -----------------------------
# Matomo API Request
# -----------------------------

def _matomo_request(method: str, params: Dict[str, Any]) -> Any:
    """
    Выполняет POST запрос к Matomo API
    
    Args:
        method: Matomo API метод (например 'VisitsSummary.get')
        params: Дополнительные параметры запроса
    
    Returns:
        Ответ API в формате JSON
    """
    if not MATOMO_BASE_URL or not MATOMO_SITE_ID or not MATOMO_TOKEN_AUTH:
        raise ValueError("Matomo not configured. Check MATOMO_BASE_URL, MATOMO_SITE_ID, MATOMO_TOKEN_AUTH env variables")
    
    url = f"{MATOMO_BASE_URL}/index.php"
    
    # Базовые параметры для всех запросов
    data = {
        "module": "API",
        "method": method,
        "idSite": MATOMO_SITE_ID,
        "format": "JSON",
        "token_auth": MATOMO_TOKEN_AUTH,
        **params
    }
    
    try:
        response = requests.post(url, data=data, timeout=MATOMO_TIMEOUT)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.Timeout as exc:
        raise Exception(f"Matomo API timeout ({MATOMO_TIMEOUT}s)") from exc
    except requests.exceptions.RequestException as exc:
        raise Exception(f"Matomo API error: {str(exc)}") from exc


# -----------------------------
# Public API Methods
# -----------------------------

def get_visits_summary(period: str = "day", date: str = "yesterday", segment: str = None) -> Dict[str, Any]:
    """
    Получить базовую статистику посещений
    
    Args:
        period: Период ('day', 'week', 'month', 'year', 'range')
        date: Дата ('yesterday', 'today', 'last7', 'last30', '2024-01-01,2024-01-31')
        segment: Matomo segment для фільтрації (e.g., 'resolution!=unknown')
    
    Returns:
        Dict с ключами: nb_visits, nb_uniq_visitors, nb_pageviews, nb_actions, nb_downloads
    """
    cache_key = f"visits_summary_{period}_{date}_{segment}"
    cached = _get_from_cache(cache_key)
    if cached is not None:
        return cached
    
    params = {
        "period": period,
        "date": date
    }
    if segment:
        params["segment"] = segment
    
    result = _matomo_request("VisitsSummary.get", params)
    
    # Нормализуем формат ответа
    if isinstance(result, dict):
        result = {
            "nb_visits": int(result.get("nb_visits", 0)),
            "nb_uniq_visitors": int(result.get("nb_uniq_visitors", 0)),
            # Matomo может вернуть nb_pageviews или nb_actions
            "nb_pageviews": int(result.get("nb_pageviews", result.get("nb_actions", 0))),
            "nb_actions": int(result.get("nb_actions", 0)),
            "nb_downloads": int(result.get("nb_downloads", 0))
        }
    
    _set_to_cache(cache_key, result)
    return result


def get_top_countries(period: str = "day", date: str = "yesterday", limit: int = 10, segment: str = None) -> List[Dict[str, Any]]:
    """
    Получить топ стран по посещениям
    
    Args:
        period: Период ('day', 'week', 'month', 'year', 'range')
        date: Дата ('yesterday', 'today', 'last7', 'last30')
        limit: Количество стран
        segment: Matomo segment для фільтрації
    
    Returns:
        List[Dict] с ключами: label (название страны), nb_visits, nb_uniq_visitors, nb_actions
    """
    cache_key = f"top_countries_{period}_{date}_{limit}_{segment}"
    cached = _get_from_cache(cache_key)
    if cached is not None:
        return cached
    
    params = {
        "period": period,
        "date": date,
        "filter_limit": limit
    }
    if segment:
        params["segment"] = segment
    
    result = _matomo_request("UserCountry.getCountry", params)
    
    # Matomo возвращает список стран
    countries = []
    if isinstance(result, list):
        for country in result:
            # Для period=week/month/year используем sum_daily_nb_uniq_visitors,
            # для period=day используем nb_uniq_visitors
            uniq_visitors = country.get("nb_uniq_visitors") or country.get("sum_daily_nb_uniq_visitors", 0)
            
            countries.append({
                "label": country.get("label", "Unknown"),
                "code": country.get("code", ""),  # ISO код страны (ua, us, tw, ae)
                "logo": country.get("logo", ""),  # Путь к PNG-иконке флага (plugins/Morpheus/icons/dist/flags/ua.png)
                "nb_visits": int(country.get("nb_visits", 0)),
                "nb_uniq_visitors": int(uniq_visitors),
                "nb_actions": int(country.get("nb_actions", 0)),
                "nb_pageviews": int(country.get("nb_pageviews", country.get("nb_actions", 0)))
            })
    
    _set_to_cache(cache_key, countries)
    return countries


def get_actions_data(period: str = "day", date: str = "yesterday", segment: str = None) -> Dict[str, int]:
    """
    Получить данные о действиях (просмотры страниц, загрузки, поиски)
    
    Args:
        period: Период ('day', 'week', 'month', 'year', 'range')
        date: Дата ('yesterday', 'today', 'last7', 'last30')
        segment: Matomo segment для фільтрації
    
    Returns:
        Dict с ключами: nb_pageviews, nb_downloads, nb_searches
    """
    cache_key = f"actions_{period}_{date}_{segment}"
    cached = _get_from_cache(cache_key)
    if cached is not None:
        return cached
    
    params = {
        "period": period,
        "date": date
    }
    if segment:
        params["segment"] = segment
    
    result = _matomo_request("Actions.get", params)
    
    data = {
        "nb_pageviews": 0,
        "nb_downloads": 0,
        "nb_searches": 0
    }
    if isinstance(result, dict):
        data["nb_pageviews"] = int(result.get("nb_pageviews", 0))
        data["nb_downloads"] = int(result.get("nb_downloads", 0))
        data["nb_searches"] = int(result.get("nb_searches", 0))
    
    _set_to_cache(cache_key, data)
    return data


def get_summary_data(date: str = "yesterday", exclude_technical: bool = False) -> Dict[str, Any]:
    """
    Агрегатор данных для фронтенда
    Получает KPI метрики и топ стран одним вызовом
    
    Args:
        date: Дата ('yesterday', 'today', 'last7', 'last30', 'last365', '2024-01-01,2024-01-31')
        exclude_technical: Виключити технічний трафік (resolution=unknown)
    
    Returns:
        Dict с ключами: metrics (KPI), countries (топ стран), period, date
    """
    # Формуємо segment для фільтрації (resolution!=unknown)
    segment = "resolution!=unknown" if exclude_technical else None
    # Проверяем, является ли date диапазоном (формат: YYYY-MM-DD,YYYY-MM-DD)
    if ',' in date:
        matomo_period = "range"
        matomo_date = date
    # Автоматически определяем правильный period на основе date
    # period=week/month/year дает данные за текущую неделю/месяц/год (как в интерфейсе Matomo)
    # date=today означает "текущая неделя/месяц/год включая сегодня"
    elif date == "last7":
        matomo_period = "week"
        matomo_date = "today"
    elif date == "last30":
        matomo_period = "month"
        matomo_date = "today"
    elif date == "last365":
        matomo_period = "year"
        matomo_date = "today"
    else:
        matomo_period = "day"
        matomo_date = date
    
    try:
        metrics = get_visits_summary(matomo_period, matomo_date, segment=segment)
        countries = get_top_countries(matomo_period, matomo_date, limit=10, segment=segment)
        
        # Получаем просмотры страниц, загрузки и поиски из Actions.get
        # (VisitsSummary.get не возвращает nb_pageviews, только nb_actions)
        actions_data = get_actions_data(matomo_period, matomo_date, segment=segment)
        metrics["nb_pageviews"] = actions_data["nb_pageviews"]
        metrics["nb_downloads"] = actions_data["nb_downloads"]
        metrics["nb_searches"] = actions_data["nb_searches"]
        
        return {
            "success": True,
            "metrics": metrics,
            "countries": countries,
            "period": matomo_period,
            "date": date,
            "matomo_base_url": MATOMO_BASE_URL
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "metrics": {
                "nb_visits": 0,
                "nb_uniq_visitors": 0,
                "nb_pageviews": 0,
                "nb_actions": 0,
                "nb_downloads": 0,
                "nb_searches": 0
            },
            "countries": [],
            "period": matomo_period,
            "date": date
        }


def is_configured() -> bool:
    """Проверка, настроен ли Matomo"""
    if MATOMO_ENABLED in {"false", "0", "no", "off"}:
        return False
    if MATOMO_ENABLED in {"true", "1", "yes", "on"}:
        return bool(MATOMO_BASE_URL and MATOMO_SITE_ID and MATOMO_TOKEN_AUTH)
    return bool(MATOMO_BASE_URL and MATOMO_SITE_ID and MATOMO_TOKEN_AUTH)

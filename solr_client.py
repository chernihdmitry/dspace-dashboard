import os
import json
import requests
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from dspace_config import get_config_value


# -----------------------------
# Config
# -----------------------------

SOLR_URL = get_config_value(
    "solr.server",
    os.getenv("SOLR_URL", "http://localhost:8983/solr"),
).rstrip("/")
SOLR_TIMEOUT = float(os.getenv("SOLR_TIMEOUT", "8"))

SOLR_SEARCH_URL = f"{SOLR_URL}/search/select"
SOLR_STATS_URL  = f"{SOLR_URL}/statistics/select"

def _build_api_base(server_url: str) -> str:
    if not server_url:
        return ""
    base = server_url.rstrip("/")
    if base.endswith("/api"):
        return base
    return f"{base}/api"


def _get_api_base() -> str:
    server_url = get_config_value(
        "dspace.server.url",
        os.getenv("REST_BASE_URL", ""),
    ).rstrip("/")
    return _build_api_base(server_url).rstrip("/")


# -----------------------------
# HTTP helpers
# -----------------------------

def _get(url: str, params: dict):
    r = requests.get(url, params=params, timeout=SOLR_TIMEOUT)
    r.raise_for_status()
    return r.json()


def iso_z(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def month_range(year: int, month: int):
    start = datetime(year, month, 1, 0, 0, 0)
    end = (start + relativedelta(months=1)) - relativedelta(seconds=1)
    return start, end


# -----------------------------
# DSpace REST root info
# -----------------------------

def dspace_root_info():
    """
    Возвращает JSON с /server/api (root).
    Требует dspace.server.url в local.cfg (например https://host/server).
    """
    api_base = _get_api_base()
    if not api_base:
        raise RuntimeError(
            "dspace.server.url is not set in local.cfg and REST_BASE_URL is empty."
        )

    url = api_base

    r = requests.get(url, timeout=6)
    r.raise_for_status()
    return r.json()


# -----------------------------
# Repository totals (info page)
# -----------------------------

def repo_totals():
    main_docs_query = {
        "q": "archived:true",
        "fq": [
            "discoverable:true",
            "withdrawn:false",
            "-entityType:Person",
        ],
        "rows": 0,
        "q.op": "AND",
    }
    person_docs_query = {
        "q": "archived:true",
        "fq": [
            "discoverable:true",
            "withdrawn:false",
            "entityType:Person",
        ],
        "rows": 0,
        "q.op": "AND",
    }

    total_docs = _get(SOLR_SEARCH_URL, main_docs_query)["response"]["numFound"]
    person_profiles = _get(SOLR_SEARCH_URL, person_docs_query)["response"]["numFound"]

    first_params = {"q": "archived:true", "sort": "dc.date.accessioned_dt asc", "rows": 2}
    last_params  = {"q": "archived:true", "sort": "dc.date.accessioned_dt desc", "rows": 1}

    first_json = _get(SOLR_SEARCH_URL, first_params)
    last_json  = _get(SOLR_SEARCH_URL, last_params)

    docs_first = first_json.get("response", {}).get("docs", [])
    docs_last  = last_json.get("response", {}).get("docs", [])

    first_date = (
        docs_first[1].get("dc.date.accessioned_dt") if len(docs_first) > 1 else
        docs_first[0].get("dc.date.accessioned_dt") if len(docs_first) == 1 else
        None
    )
    last_date = docs_last[0].get("dc.date.accessioned_dt") if docs_last else None

    facet_params = {
        "q": "*:*",
        "rows": 0,
        "facet": "true",
        "facet.field": ["dc.language.iso", "dc.type"],
        "facet.limit": 200,
        "facet.mincount": 1,
    }
    f = _get(SOLR_SEARCH_URL, facet_params).get("facet_counts", {}).get("facet_fields", {})

    def flat_to_dict(lst):
        out = {}
        if not lst:
            return out
        for i in range(0, len(lst), 2):
            out[str(lst[i])] = int(lst[i + 1])
        return out

    return {
        "total_docs": int(total_docs),
        "person_profiles": int(person_profiles),
        "first_date": first_date,
        "last_date": last_date,
        "langs": flat_to_dict(f.get("dc.language.iso")),
        "types": flat_to_dict(f.get("dc.type")),
    }


# -----------------------------
# Submitted: last days + sparkline
# -----------------------------

def submitted_count(start_dt: datetime, end_dt: datetime) -> int:
    params = {
        "q": "archived:true",
        "fq": [
            "-entityType:Person",
            "discoverable:true",
            "withdrawn:false",
            f"dc.date.accessioned_dt:[{iso_z(start_dt)} TO {iso_z(end_dt)}]",
        ],
        "rows": 0,
        "q.op": "AND",
        "indent": "true",
    }
    return int(_get(SOLR_SEARCH_URL, params)["response"]["numFound"])


def submitted_last_days(days: int = 7) -> int:
    end = datetime.utcnow().replace(hour=23, minute=59, second=59, microsecond=0)
    start = end - timedelta(days=days - 1)
    return submitted_count(start, end)


def submitted_sparkline(days: int = 30):
    end = datetime.utcnow().replace(hour=23, minute=59, second=59, microsecond=0)
    start = (end - timedelta(days=days - 1)).replace(hour=0, minute=0, second=0, microsecond=0)

    facet = {
        "by_day": {
            "type": "range",
            "field": "dc.date.accessioned_dt",
            "start": iso_z(start),
            "end": iso_z(end + relativedelta(seconds=1)),  # exclusive end
            "gap": "+1DAY",
        }
    }

    params = {
        "q": "archived:true",
        "fq": [
            "-entityType:Person",
            "discoverable:true",
            "withdrawn:false",
            f"dc.date.accessioned_dt:[{iso_z(start)} TO {iso_z(end)}]",
        ],
        "rows": 0,
        "json.facet": json.dumps(facet),
    }

    j = _get(SOLR_SEARCH_URL, params)
    buckets = j.get("facets", {}).get("by_day", {}).get("buckets", [])

    labels, values = [], []
    for b in buckets:
        val = b.get("val", "")
        labels.append(val[:10])
        values.append(int(b.get("count", 0)))
    return labels, values


# -----------------------------
# Statistics core: downloads/views
# -----------------------------

def stats_count(q: str, fq: list[str]) -> int:
    params = {
        "q": q,
        "fq": fq,
        "rows": 0,
        "q.op": "AND",
        "wt": "json",
    }
    return int(_get(SOLR_STATS_URL, params)["response"]["numFound"])


def downloads_count(start_dt: datetime, end_dt: datetime) -> int:
    """
    DSpace Solr statistics core.
    Usually:
      - downloads are bitstream views: type:0 + bundleName:"ORIGINAL" + statistics_type:view
    But some setups store downloads under statistics_type:download.
    We'll try view first, then fallback to download.
    """
    fq_common = [
        'bundleName:"ORIGINAL"',  # IMPORTANT: multiValued, needs quotes (Windows особенно)
        "isBot:false",
        f"time:[{iso_z(start_dt)} TO {iso_z(end_dt)}]",
    ]

    # Most common (your curl подтверждает, что statistics_type=view есть)
    cnt = stats_count(q="type:0", fq=fq_common + ["statistics_type:view"])
    if cnt > 0:
        return cnt

    # Fallback for some DSpace installs
    cnt2 = stats_count(q="type:0", fq=fq_common + ["statistics_type:download"])
    return cnt2


def views_count(start_dt: datetime, end_dt: datetime) -> int:
    """
    Item page views (usually type:2) in statistics core.
    """
    return stats_count(
        q="type:2",
        fq=[
            "isBot:false",
            "statistics_type:view",
            f"time:[{iso_z(start_dt)} TO {iso_z(end_dt)}]",
        ],
    )



# -----------------------------
# Month daily stats (3 GET with json.facet)
# -----------------------------

def month_daily_stats(year: int, month: int):
    start, end = month_range(year, month)
    end_excl = end + relativedelta(seconds=1)

    def _range_facet(field: str):
        return {
            "by_day": {
                "type": "range",
                "field": field,
                "start": iso_z(start),
                "end": iso_z(end_excl),
                "gap": "+1DAY",
            }
        }

    # Submitted
    params_sub = {
        "q": "archived:true",
        "fq": [
            "-entityType:Person",
            "discoverable:true",
            "withdrawn:false",
            f"dc.date.accessioned_dt:[{iso_z(start)} TO {iso_z(end)}]",
        ],
        "rows": 0,
        "json.facet": json.dumps(_range_facet("dc.date.accessioned_dt")),
    }
    buckets_sub = _get(SOLR_SEARCH_URL, params_sub).get("facets", {}).get("by_day", {}).get("buckets", [])

    # Views
    params_views = {
        "q": "type:2",
        "fq": [
            "isBot:false",
            "statistics_type:view",
            f"time:[{iso_z(start)} TO {iso_z(end)}]",
        ],
        "rows": 0,
        "json.facet": json.dumps(_range_facet("time")),
    }
    buckets_views = _get(SOLR_STATS_URL, params_views).get("facets", {}).get("by_day", {}).get("buckets", [])

    # Downloads
    params_down = {
        "q": "type:0",
        "fq": [
            "bundleName:ORIGINAL",
            "isBot:false",
            "statistics_type:view",
            f"time:[{iso_z(start)} TO {iso_z(end)}]",
        ],
        "rows": 0,
        "json.facet": json.dumps(_range_facet("time")),
    }
    buckets_down = _get(SOLR_STATS_URL, params_down).get("facets", {}).get("by_day", {}).get("buckets", [])

    def buckets_to_map(buckets):
        mp = {}
        for b in buckets or []:
            val = b.get("val")
            if isinstance(val, str) and len(val) >= 10:
                mp[val[:10]] = int(b.get("count", 0))
        return mp

    mp_sub = buckets_to_map(buckets_sub)
    mp_views = buckets_to_map(buckets_views)
    mp_down = buckets_to_map(buckets_down)

    out = []
    cur = start
    while cur <= end:
        key = cur.strftime("%Y-%m-%d")
        out.append({
            "day": int(cur.strftime("%d")),
            "submitted": mp_sub.get(key, 0),
            "views": mp_views.get(key, 0),
            "downloads": mp_down.get(key, 0),
        })
        cur = cur + relativedelta(days=1)

    return out


# -----------------------------
# Monthly stats table
# -----------------------------

def monthly_stats(start_year: int, start_month: int):
    today = date.today()
    cur = datetime(start_year, start_month, 1)
    end = datetime(today.year, today.month, 1)

    rows = []
    while cur <= end:
        y, m = cur.year, cur.month
        start_dt, end_dt = month_range(y, m)

        rows.append({
            "year": y,
            "month": m,
            "submitted": submitted_count(start_dt, end_dt),
            "views": views_count(start_dt, end_dt),
            "downloads": downloads_count(start_dt, end_dt),
        })

        cur = cur + relativedelta(months=1)

    return rows


def stats_for_month(year: int, month: int):
    """Получить статистику за конкретный месяц"""
    start_dt, end_dt = month_range(year, month)
    
    return {
        "year": year,
        "month": month,
        "views": views_count(start_dt, end_dt),
        "downloads": downloads_count(start_dt, end_dt),
    }


def stats_for_year(year: int):
    """Получить статистику за весь год (суммарно)"""
    today = date.today()
    
    start = datetime(year, 1, 1, 0, 0, 0)
    if year == today.year:
        end = datetime.combine(today, datetime.max.time())
    else:
        end = datetime(year, 12, 31, 23, 59, 59)
    
    return {
        "year": year,
        "month": 0,
        "views": views_count(start, end),
        "downloads": downloads_count(start, end),
    }


def stats_year_by_months(year: int):
    """Получить статистику по каждому месяцу года для таблицы"""
    today = date.today()
    months_data = []
    
    for month in range(1, 13):
        # Если это будущий месяц - пропускаем
        if year == today.year and month > today.month:
            break
            
        start_dt, end_dt = month_range(year, month)
        
        months_data.append({
            "month": month,
            "views": views_count(start_dt, end_dt),
            "downloads": downloads_count(start_dt, end_dt),
        })
    
    return months_data


def stats_dynamics_for_year(year: int):
    """Получить динамику по месяцам для графика за год"""
    today = date.today()
    months = []
    
    for month in range(1, 13):
        # Если это будущий месяц - пропускаем
        if year == today.year and month > today.month:
            break
            
        start_dt, end_dt = month_range(year, month)
        
        months.append({
            "month": month,
            "views": views_count(start_dt, end_dt),
            "downloads": downloads_count(start_dt, end_dt),
        })
    
    return months


# -----------------------------
# Submitters (terms facet)
# -----------------------------

def submitters_for_month(year: int, month: int, limit: int = 200):
    start, end = month_range(year, month)

    facet = {
        "submitters": {
            "type": "terms",
            "field": "submitter_keyword",
            "limit": limit,
            "mincount": 1,
            "sort": "count desc",
        }
    }

    params = {
        "q": "archived:true",
        "fq": [
            "-entityType:Person",
            "discoverable:true",
            "withdrawn:false",
            f"dc.date.accessioned_dt:[{iso_z(start)} TO {iso_z(end)}]",
        ],
        "rows": 0,
        "json.facet": json.dumps(facet),
    }

    j = _get(SOLR_SEARCH_URL, params)
    buckets = j.get("facets", {}).get("submitters", {}).get("buckets", [])
    # унифицируем под шаблоны: key/count
    return [{"submitter": b.get("val", ""), "count": int(b.get("count", 0))} for b in buckets]


def submitters_for_year(year: int, limit: int = 200):
    """Получить данные по отправителям за весь год"""
    start = datetime(year, 1, 1, 0, 0, 0)
    today = date.today()
    
    # Если это текущий год, берём данные до сегодня
    if year == today.year:
        end = datetime.combine(today, datetime.max.time())
    else:
        end = datetime(year, 12, 31, 23, 59, 59)

    facet = {
        "submitters": {
            "type": "terms",
            "field": "submitter_keyword",
            "limit": limit,
            "mincount": 1,
            "sort": "count desc",
        }
    }

    params = {
        "q": "archived:true",
        "fq": [
            "-entityType:Person",
            "discoverable:true",
            "withdrawn:false",
            f"dc.date.accessioned_dt:[{iso_z(start)} TO {iso_z(end)}]",
        ],
        "rows": 0,
        "json.facet": json.dumps(facet),
    }

    j = _get(SOLR_SEARCH_URL, params)
    buckets = j.get("facets", {}).get("submitters", {}).get("buckets", [])
    return [{"submitter": b.get("val", ""), "count": int(b.get("count", 0))} for b in buckets]


def submitters_heatmap_data(year: int, limit: int = 50):
    """
    Получить данные для тепловой карты отправителей по месяцам
    за конкретный год (12 месяцев)
    
    Возвращает:
    {
        "months": ["Січень", "Лютий", ...],
        "submitters": ["User1", "User2", ...],
        "data": [[count1, count2, ...], [count3, count4, ...], ...]
            где data[submitter_index][month_index] = количество документов
    }
    """
    today = date.today()
    
    # Собираем все месяцы года (1-12)
    months = []
    monthly_submitters = {}  # {month_key: {submitter: count}}
    
    for month in range(1, 13):
        # Если это будущий месяц - пропускаем
        if year == today.year and month > today.month:
            break
            
        # Получаем данные за месяц
        try:
            submitters_data = submitters_for_month(year, month, limit=limit)
            
            month_key = f"{year}-{month:02d}"
            monthly_submitters[month_key] = {}
            
            for item in submitters_data:
                submitter = item.get("submitter", "")
                count = item.get("count", 0)
                if submitter:
                    monthly_submitters[month_key][submitter] = count
                    
        except Exception:
            # Пропускаем месяц если ошибка
            pass
        
        months.append(date(year, month, 1))
    
    # Собираем уникальный список всех отправителей
    all_submitters = set()
    for month_data in monthly_submitters.values():
        all_submitters.update(month_data.keys())
    
    # Сортируем отправителей по общему количеству документов
    submitter_totals = {}
    for submitter in all_submitters:
        total = sum(
            month_data.get(submitter, 0) 
            for month_data in monthly_submitters.values()
        )
        submitter_totals[submitter] = total
    
    sorted_submitters = sorted(
        submitter_totals.items(), 
        key=lambda x: x[1], 
        reverse=True
    )[:limit]
    
    submitters_list = [s[0] for s in sorted_submitters]
    
    # Формируем матрицу данных
    data_matrix = []
    for submitter in submitters_list:
        row = []
        for month_date in months:
            month_key = f"{month_date.year}-{month_date.month:02d}"
            count = monthly_submitters.get(month_key, {}).get(submitter, 0)
            row.append(count)
        data_matrix.append(row)
    
    # Названия месяцев на украинском (без года, т.к. год выбран в селекторе)
    month_names_ua = {
        1: 'Січень', 2: 'Лютий', 3: 'Березень', 4: 'Квітень',
        5: 'Травень', 6: 'Червень', 7: 'Липень', 8: 'Серпень',
        9: 'Вересень', 10: 'Жовтень', 11: 'Листопад', 12: 'Грудень'
    }
    
    month_labels = [
        month_names_ua[m.month]
        for m in months
    ]
    
    return {
        "months": month_labels,
        "submitters": submitters_list,
        "data": data_matrix,
    }


# -----------------------------
# Edited documents (experiments)
# -----------------------------

def _edited_docs_count(
    start_dt: datetime,
    end_dt: datetime,
    last_modified_field: str,
    accession_field: str,
):
    """
    Count items where last_modified_field is within range and after accession_field.
    Uses a function range query: ms(last_modified) - ms(accession) > 0.
    """
    fq = [
        "-entityType:Person",
        "discoverable:true",
        "withdrawn:false",
        f"{last_modified_field}:[{iso_z(start_dt)} TO {iso_z(end_dt)}]",
        f"{{!frange l=1}}sub(ms({last_modified_field}),ms({accession_field}))",
    ]

    params = {
        "q": "archived:true",
        "fq": fq,
        "rows": 0,
    }

    return int(_get(SOLR_SEARCH_URL, params)["response"]["numFound"])


def edited_docs_attempts(year: int, month: int):
    """
    Try the preferred Solr field combination for edited docs.
    Returns a single attempt with count or error.
    """
    if month == 0:
        start = datetime(year, 1, 1, 0, 0, 0)
        today = date.today()
        if year == today.year:
            end = datetime.combine(today, datetime.max.time())
        else:
            end = datetime(year, 12, 31, 23, 59, 59)
    else:
        start, end = month_range(year, month)

    attempts = []
    last_modified = "lastModified"
    accession = "dc.date.accessioned_dt"
    label = f"{last_modified} > {accession}"
    try:
        count = _edited_docs_count(start, end, last_modified, accession)
        attempts.append({
            "label": label,
            "count": count,
            "error": None,
        })
    except Exception as exc:
        attempts.append({
            "label": label,
            "count": None,
            "error": str(exc),
        })

    return attempts


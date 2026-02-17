import os
import time
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import psycopg

from dspace_config import get_config_value

_cache: Dict[str, Any] = {}
_cache_ttl: Dict[str, float] = {}
_metadata_field_cache: Dict[str, Optional[int]] = {}



def _cache_ttl_seconds() -> int:
    return int(os.getenv("CACHE_TTL_SECONDS", "300"))


def _cache_get(key: str):
    ttl = _cache_ttl_seconds()
    expires_at = _cache_ttl.get(key)
    if expires_at is None or expires_at < time.time():
        _cache.pop(key, None)
        _cache_ttl.pop(key, None)
        return None
    return _cache.get(key)


def _cache_set(key: str, value: Any):
    ttl = _cache_ttl_seconds()
    _cache[key] = value
    _cache_ttl[key] = time.time() + ttl


def _parse_db_url(url: str) -> Optional[Dict[str, Any]]:
    if not url:
        return None

    if url.startswith("jdbc:"):
        url = url[len("jdbc:"):]

    parsed = urlparse(url)
    if parsed.scheme not in ("postgres", "postgresql"):
        return None

    dbname = parsed.path.lstrip("/")
    if not dbname:
        return None

    return {
        "host": parsed.hostname or "localhost",
        "port": parsed.port or 5432,
        "dbname": dbname,
    }


def _get_db_params() -> Optional[Dict[str, Any]]:
    url = get_config_value("db.url", "")
    user = get_config_value("db.username", "")
    password = get_config_value("db.password", "")

    parsed = _parse_db_url(url)
    if not parsed:
        return None

    params = {
        **parsed,
        "user": user,
        "password": password,
    }
    return params


def _connect():
    params = _get_db_params()
    if not params:
        raise RuntimeError("Database configuration is missing in local.cfg")
    return psycopg.connect(**params)


def _period_range(year: int, month: int):
    if month == 0:
        start = datetime(year, 1, 1, 0, 0, 0)
        today = date.today()
        if year == today.year:
            end = datetime.combine(today, datetime.max.time())
        else:
            end = datetime(year, 12, 31, 23, 59, 59)
        return start, end

    start = datetime(year, month, 1, 0, 0, 0)
    if month == 12:
        end = datetime(year + 1, 1, 1, 0, 0, 0) - timedelta(seconds=1)
    else:
        end = datetime(year, month + 1, 1, 0, 0, 0) - timedelta(seconds=1)
    return start, end


def _fetch_columns(cur, table: str) -> List[str]:
    cur.execute(
        "select column_name from information_schema.columns where table_name=%s",
        (table,),
    )
    return [row[0] for row in cur.fetchall()]


def _metadata_field_id(schema: str, element: str, qualifier: Optional[str]) -> Optional[int]:
    cache_key = f"{schema}:{element}:{qualifier}"
    if cache_key in _metadata_field_cache:
        return _metadata_field_cache[cache_key]

    sql = (
        "select mfr.metadata_field_id "
        "from metadatafieldregistry mfr "
        "join metadataschemaregistry msr on mfr.metadata_schema_id = msr.metadata_schema_id "
        "where msr.short_id = %s and mfr.element = %s "
    )
    params: List[Any] = [schema, element]

    if qualifier is None:
        sql += "and mfr.qualifier is null "
    else:
        sql += "and mfr.qualifier = %s "
        params.append(qualifier)

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            row = cur.fetchone()
            if row:
                value = int(row[0])
                _metadata_field_cache[cache_key] = value
                return value
    _metadata_field_cache[cache_key] = None
    return None


def _orcid_field_id() -> Optional[int]:
    raw = os.getenv("ORCID_FIELD_ID", "")
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _excluded_collection_uuid() -> str:
    return get_config_value("researcher-profile.collection.uuid", "").strip()


def _collection_titles_by_uuid(collection_uuids: List[str]) -> Dict[str, str]:
    if not collection_uuids:
        return {}

    title_id = _metadata_field_id("dc", "title", None)
    if not title_id:
        return {}

    sql = (
        "select mv.dspace_object_id::text, max(mv.text_value) "
        "from metadatavalue mv "
        "where mv.metadata_field_id = %s "
        "and mv.dspace_object_id = any(%s) "
        "group by mv.dspace_object_id"
    )

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (title_id, collection_uuids))
            return {row[0]: row[1] for row in cur.fetchall()}




def _submitter_collections_query(include_submitter_filter: bool, exclude_collection: bool) -> str:
    filter_clause = " and e.uuid::text = %s " if include_submitter_filter else " "
    exclusion_clause = " and i.owning_collection::text <> %s " if exclude_collection else " "
    return (
        "with accessioned as ("
        "  select mv.dspace_object_id as item_uuid, "
        "         substring(mv.text_value from '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]+)?(Z|[+-][0-9]{2}:?[0-9]{2})?')::timestamptz as accessioned_at "
        "  from metadatavalue mv "
        "  where mv.metadata_field_id = %s "
        "    and mv.text_value ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}'"
        "), collection_titles as ("
        "  select mv.dspace_object_id as collection_uuid, "
        "         max(mv.text_value) as title "
        "  from metadatavalue mv "
        "  where mv.metadata_field_id = %s "
        "  group by mv.dspace_object_id"
        "), eperson_names as ("
        "  select mv.dspace_object_id as eperson_uuid, "
        "         max(case when mv.metadata_field_id = %s then mv.text_value end) as firstname, "
        "         max(case when mv.metadata_field_id = %s then mv.text_value end) as lastname "
        "  from metadatavalue mv "
        "  where mv.metadata_field_id in (%s, %s) "
        "  group by mv.dspace_object_id"
        ") "
        "select e.uuid as submitter_uuid, "
        "       coalesce(trim(en.firstname || ' ' || en.lastname), e.email) as submitter_name, "
        "       coalesce(ct.title, c.uuid::text) as collection_name, "
        "       count(distinct i.uuid) "
        "from item i "
        "join accessioned a on a.item_uuid = i.uuid "
        "join eperson e on e.uuid = i.submitter_id "
        "join collection c on c.uuid = i.owning_collection "
        "left join collection_titles ct on ct.collection_uuid = c.uuid "
        "left join eperson_names en on en.eperson_uuid = e.uuid "
        "where i.in_archive = true and i.withdrawn = false and i.discoverable = true "
        "  and a.accessioned_at >= %s and a.accessioned_at <= %s "
        + exclusion_clause +
        filter_clause +
        "group by submitter_uuid, submitter_name, collection_name "
        "order by submitter_name asc, count(distinct i.uuid) desc"
    )


def submitter_totals_by_period(year: int, month: int):
    cache_key = f"submitters:totals:{year}:{month}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    start_dt, end_dt = _period_range(year, month)
    excluded_uuid = _excluded_collection_uuid()
    sql = _submitter_collections_query(False, bool(excluded_uuid))

    accessioned_id = _metadata_field_id("dc", "date", "accessioned")
    title_id = _metadata_field_id("dc", "title", None)
    firstname_id = _metadata_field_id("eperson", "firstname", None)
    lastname_id = _metadata_field_id("eperson", "lastname", None)

    if not all([accessioned_id, title_id, firstname_id, lastname_id]):
        raise RuntimeError("Metadata field registry is missing required fields")

    rows = []
    with _connect() as conn:
        with conn.cursor() as cur:
            params: List[Any] = [
                accessioned_id,
                title_id,
                firstname_id,
                lastname_id,
                firstname_id,
                lastname_id,
                start_dt,
                end_dt,
            ]
            if excluded_uuid:
                params.append(excluded_uuid)
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()

    grouped: Dict[str, Dict[str, Any]] = {}
    for submitter_uuid, submitter_name, collection, count in rows:
        key = str(submitter_uuid)
        if key not in grouped:
            grouped[key] = {
                "uuid": key,
                "submitter": submitter_name,
                "total": 0,
            }
        grouped[key]["total"] += int(count)

    submitters = list(grouped.values())
    submitters.sort(key=lambda x: x["total"], reverse=True)
    _cache_set(cache_key, submitters)
    return submitters


def submitter_collections_for_submitter(year: int, month: int, submitter_uuid: str):
    cache_key = f"submitters:detail:{submitter_uuid}:{year}:{month}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    start_dt, end_dt = _period_range(year, month)
    excluded_uuid = _excluded_collection_uuid()
    accessioned_id = _metadata_field_id("dc", "date", "accessioned")
    if not accessioned_id:
        raise RuntimeError("Metadata field registry is missing required fields")

    sql = (
        "with accessioned as ("
        "  select mv.dspace_object_id as item_uuid, "
        "         substring(mv.text_value from '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]+)?(Z|[+-][0-9]{2}:?[0-9]{2})?')::timestamptz as accessioned_at "
        "  from metadatavalue mv "
        "  where mv.metadata_field_id = %s "
        "    and mv.text_value ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}'"
        ") "
        "select i.owning_collection::text, count(distinct i.uuid) "
        "from item i "
        "join accessioned a on a.item_uuid = i.uuid "
        "where i.in_archive = true and i.withdrawn = false and i.discoverable = true "
        "  and a.accessioned_at >= %s and a.accessioned_at <= %s "
        + ("  and i.owning_collection::text <> %s " if excluded_uuid else "") +
        "  and i.submitter_id::text = %s "
        "group by i.owning_collection "
        "order by count(distinct i.uuid) desc"
    )

    rows = []
    with _connect() as conn:
        with conn.cursor() as cur:
            params: List[Any] = [accessioned_id, start_dt, end_dt]
            if excluded_uuid:
                params.append(excluded_uuid)
            params.append(submitter_uuid)
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()

    collection_ids = [row[0] for row in rows]
    titles = _collection_titles_by_uuid(collection_ids)

    collections = []
    for collection_id, count in rows:
        collections.append({
            "collection_id": collection_id,
            "collection": titles.get(collection_id, collection_id),
            "count": int(count),
        })

    submitter_name = submitter_name_by_uuid(submitter_uuid) or submitter_uuid

    result = {
        "submitter": submitter_name,
        "collections": collections,
    }
    _cache_set(cache_key, result)
    return result


def submitter_name_by_uuid(submitter_uuid: str) -> Optional[str]:
    cache_key = f"submitters:name:{submitter_uuid}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    firstname_id = _metadata_field_id("eperson", "firstname", None)
    lastname_id = _metadata_field_id("eperson", "lastname", None)

    if not firstname_id or not lastname_id:
        return None

    sql = (
        "with eperson_names as ("
        "  select mv.dspace_object_id as eperson_uuid, "
        "         max(case when mv.metadata_field_id = %s then mv.text_value end) as firstname, "
        "         max(case when mv.metadata_field_id = %s then mv.text_value end) as lastname "
        "  from metadatavalue mv "
        "  where mv.metadata_field_id in (%s, %s) "
        "  group by mv.dspace_object_id"
        ") "
        "select coalesce(trim(en.firstname || ' ' || en.lastname), e.email) "
        "from eperson e "
        "left join eperson_names en on en.eperson_uuid = e.uuid "
        "where e.uuid::text = %s"
    )

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    firstname_id,
                    lastname_id,
                    firstname_id,
                    lastname_id,
                    submitter_uuid,
                ),
            )
            row = cur.fetchone()
            if row:
                _cache_set(cache_key, row[0])
                return row[0]
    return None


def submitter_collection_items(year: int, month: int, submitter_uuid: str, collection_uuid: str):
    cache_key = f"submitters:items:{submitter_uuid}:{collection_uuid}:{year}:{month}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    start_dt, end_dt = _period_range(year, month)
    accessioned_id = _metadata_field_id("dc", "date", "accessioned")
    title_id = _metadata_field_id("dc", "title", None)
    if not accessioned_id or not title_id:
        raise RuntimeError("Metadata field registry is missing required fields")

    sql = (
        "with accessioned as ("
        "  select mv.dspace_object_id as item_uuid, "
        "         substring(mv.text_value from '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]+)?(Z|[+-][0-9]{2}:?[0-9]{2})?')::timestamptz as accessioned_at "
        "  from metadatavalue mv "
        "  where mv.metadata_field_id = %s "
        "    and mv.text_value ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}'"
        ") "
        "select i.uuid::text "
        "from item i "
        "join accessioned a on a.item_uuid = i.uuid "
        "where i.in_archive = true and i.withdrawn = false and i.discoverable = true "
        "  and a.accessioned_at >= %s and a.accessioned_at <= %s "
        "  and i.submitter_id::text = %s "
        "  and i.owning_collection::text = %s "
        "order by i.last_modified desc"
    )

    collection_name = _collection_titles_by_uuid([collection_uuid]).get(collection_uuid, collection_uuid)

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (accessioned_id, start_dt, end_dt, submitter_uuid, collection_uuid),
            )
            item_ids = [row[0] for row in cur.fetchall()]

            if not item_ids:
                result = {"collection": collection_name, "items": []}
                _cache_set(cache_key, result)
                return result

            cur.execute(
                "select mv.dspace_object_id::text, max(mv.text_value) "
                "from metadatavalue mv "
                "where mv.metadata_field_id = %s "
                "  and mv.dspace_object_id = any(%s) "
                "group by mv.dspace_object_id",
                (title_id, item_ids),
            )
            titles = {row[0]: row[1] for row in cur.fetchall()}

    items = [
        {"uuid": item_id, "title": titles.get(item_id, item_id)}
        for item_id in item_ids
    ]

    result = {"collection": collection_name, "items": items}
    _cache_set(cache_key, result)
    return result


def researcher_profiles_by_period(year: int, month: int, collection_uuid: str):
    cache_key = f"orcid:profiles:{collection_uuid}:{year}:{month}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    start_dt, end_dt = _period_range(year, month)

    title_id = _metadata_field_id("dc", "title", None)
    if not title_id:
        raise RuntimeError("Metadata field registry is missing required fields")

    orcid_id = _orcid_field_id()
    if orcid_id is None:
        raise RuntimeError("ORCID_FIELD_ID is not set in the environment")

    sql = (
        "with profile_items as ("
        "  select i.uuid as owner_id "
        "  from item i "
        "  where i.owning_collection::text = %s "
        "    and i.in_archive = true and i.withdrawn = false and i.discoverable = true"
        "), latest as ("
        "  select distinct on (owner_id, entity_id) "
        "         owner_id, entity_id, timestamp_last_attempt, status "
        "  from orcid_history "
        "  where owner_id in (select owner_id from profile_items) "
        "    and timestamp_last_attempt >= %s and timestamp_last_attempt <= %s "
        "  order by owner_id, entity_id, timestamp_last_attempt desc"
        "), profile_titles as ("
        "  select mv.dspace_object_id as item_uuid, "
        "         max(mv.text_value) as title "
        "  from metadatavalue mv "
        "  where mv.metadata_field_id = %s "
        "  group by mv.dspace_object_id"
        "), profile_orcid as ("
        "  select mv.dspace_object_id as item_uuid, "
        "         max(mv.text_value) as orcid "
        "  from metadatavalue mv "
        "  where mv.metadata_field_id = %s "
        "  group by mv.dspace_object_id"
        ") "
        "select l.owner_id::text as owner_id, "
        "       coalesce(pt.title, l.owner_id::text) as profile_name, "
        "       count(distinct l.entity_id) as publications, "
        "       max(po.orcid) as orcid "
        "from latest l "
        "join item i on i.uuid::text = l.owner_id::text "
        "left join profile_titles pt on pt.item_uuid = i.uuid "
        "left join profile_orcid po on po.item_uuid = i.uuid "
        "where l.status in (200, 201) "
        "group by l.owner_id, profile_name "
        "order by publications desc, profile_name asc"
    )

    rows = []
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (collection_uuid, start_dt, end_dt, title_id, orcid_id),
            )
            rows = cur.fetchall()

    result = [
        {
            "owner_id": row[0],
            "profile": row[1],
            "count": int(row[2]),
            "orcid": row[3],
        }
        for row in rows
    ]
    _cache_set(cache_key, result)
    return result


def researcher_profile_name(owner_id: str, collection_uuid: str):
    cache_key = f"orcid:profile-name:{collection_uuid}:{owner_id}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    title_id = _metadata_field_id("dc", "title", None)
    if not title_id:
        raise RuntimeError("Metadata field registry is missing required fields")

    name = None
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "select max(mv.text_value) "
                "from item i "
                "left join metadatavalue mv on mv.dspace_object_id = i.uuid "
                "  and mv.metadata_field_id = %s "
                "where i.uuid::text = %s "
                "  and i.owning_collection::text = %s "
                "  and i.in_archive = true and i.withdrawn = false and i.discoverable = true",
                (title_id, owner_id, collection_uuid),
            )
            row = cur.fetchone()
            if row:
                name = row[0]

    _cache_set(cache_key, name)
    return name


def researcher_profile_publications(year: int, month: int, owner_id: str):
    cache_key = f"orcid:publications:{owner_id}:{year}:{month}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    start_dt, end_dt = _period_range(year, month)

    title_id = _metadata_field_id("dc", "title", None)
    if not title_id:
        raise RuntimeError("Metadata field registry is missing required fields")

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "select distinct on (owner_id, entity_id) entity_id::text, status, timestamp_last_attempt "
                "from orcid_history "
                "where owner_id::text = %s "
                "  and timestamp_last_attempt >= %s and timestamp_last_attempt <= %s "
                "order by owner_id, entity_id, timestamp_last_attempt desc",
                (owner_id, start_dt, end_dt),
            )
            entity_rows = cur.fetchall()

            entity_ids = [row[0] for row in entity_rows if row[1] in (200, 201)]
            if not entity_ids:
                _cache_set(cache_key, [])
                return []

            last_attempt_map = {
                row[0]: row[2]
                for row in entity_rows
                if row[1] in (200, 201)
            }

            cur.execute(
                "select mv.dspace_object_id::text, max(mv.text_value) "
                "from metadatavalue mv "
                "where mv.metadata_field_id = %s "
                "  and mv.dspace_object_id = any(%s) "
                "group by mv.dspace_object_id",
                (title_id, entity_ids),
            )
            titles = {row[0]: row[1] for row in cur.fetchall()}

    publications = []
    for entity_id in entity_ids:
        publications.append({
            "uuid": entity_id,
            "title": titles.get(entity_id, entity_id),
            "date": last_attempt_map.get(entity_id),
        })

    publications.sort(key=lambda item: (item["date"] is None, item["date"], item["title"].lower()))
    _cache_set(cache_key, publications)
    return publications

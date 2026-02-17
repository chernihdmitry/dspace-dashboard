import os
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import psycopg

from dspace_config import get_config_value


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


def edited_docs_by_editor_attempts(year: int, month: int, limit: int = 200):
    start_dt, end_dt = _period_range(year, month)
    attempts: List[Dict[str, Any]] = []

    with _connect() as conn:
        with conn.cursor() as cur:
            item_cols = _fetch_columns(cur, "item")

    item_uuid_col = "uuid" if "uuid" in item_cols else "item_id"

    def _run_query(label: str, sql: str, params: tuple):
        try:
            with _connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, params)
                    rows = [
                        {"editor": r[0], "count": int(r[1])}
                        for r in cur.fetchall()
                    ]
            attempts.append({
                "label": label,
                "rows": rows,
                "error": None,
            })
        except Exception as exc:
            attempts.append({
                "label": label,
                "rows": [],
                "error": str(exc),
            })

    # Attempt 1: versioning tables (editor == submitter)
    sql_version_submitter = (
        "select e.email, count(distinct vi.item_id) "
        "from versionhistory vh "
        "join versionitem vi on vi.versionhistory_id = vh.versionhistory_id "
        f"join item i on i.{item_uuid_col}::text = vi.item_id::text "
        "join eperson e on e.uuid::text = vi.eperson_id::text "
        "where vh.version_date >= %s and vh.version_date <= %s "
        "and i.in_archive = true and i.withdrawn = false and i.discoverable = true "
        "and vi.eperson_id::text = i.submitter_id::text "
        "group by e.email "
        "order by count(distinct vi.item_id) desc "
        "limit %s"
    )
    _run_query(
        "versioning: editor == submitter",
        sql_version_submitter,
        (start_dt, end_dt, limit),
    )

    # Attempt 2: versioning tables (any editor)
    sql_version_any = (
        "select e.email, count(distinct vi.item_id) "
        "from versionhistory vh "
        "join versionitem vi on vi.versionhistory_id = vh.versionhistory_id "
        f"join item i on i.{item_uuid_col}::text = vi.item_id::text "
        "join eperson e on e.uuid::text = vi.eperson_id::text "
        "where vh.version_date >= %s and vh.version_date <= %s "
        "and i.in_archive = true and i.withdrawn = false and i.discoverable = true "
        "group by e.email "
        "order by count(distinct vi.item_id) desc "
        "limit %s"
    )
    _run_query(
        "versioning: any editor",
        sql_version_any,
        (start_dt, end_dt, limit),
    )

    # Attempt 3: item.last_modified_by if present
    last_modified_by_col = None
    for candidate in ("last_modified_by", "last_modified_by_id", "modified_by"):
        if candidate in item_cols:
            last_modified_by_col = candidate
            break

    accession_col = None
    for candidate in ("dateaccessioned", "date_accessioned", "accession_date"):
        if candidate in item_cols:
            accession_col = candidate
            break

    if last_modified_by_col and accession_col and "last_modified" in item_cols:
        sql_item_modified = (
            f"select e.email, count(distinct i.{item_uuid_col}) "
            f"from item i "
            f"join eperson e on e.uuid::text = i.{last_modified_by_col}::text "
            f"where i.last_modified >= %s and i.last_modified <= %s "
            f"and i.in_archive = true and i.withdrawn = false and i.discoverable = true "
            f"and i.last_modified > i.{accession_col} "
            f"and i.{last_modified_by_col}::text = i.submitter_id::text "
            f"group by e.email "
            f"order by count(distinct i.item_id) desc "
            f"limit %s"
        )
        _run_query(
            "item.last_modified_by == submitter",
            sql_item_modified,
            (start_dt, end_dt, limit),
        )

    return attempts


def _submitter_collections_query(include_submitter_filter: bool) -> str:
    filter_clause = " and e.uuid::text = %s " if include_submitter_filter else " "
    return (
        "with accessioned as ("
        "  select mv.dspace_object_id as item_uuid, "
        "         mv.text_value::timestamptz as accessioned_at "
        "  from metadatavalue mv "
        "  join metadatafieldregistry mfr on mv.metadata_field_id = mfr.metadata_field_id "
        "  join metadataschemaregistry msr on mfr.metadata_schema_id = msr.metadata_schema_id "
        "  where msr.short_id = 'dc' and mfr.element = 'date' and mfr.qualifier = 'accessioned' "
        "    and mv.text_value ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'"
        "), collection_titles as ("
        "  select mv.dspace_object_id as collection_uuid, "
        "         max(mv.text_value) as title "
        "  from metadatavalue mv "
        "  join metadatafieldregistry mfr on mv.metadata_field_id = mfr.metadata_field_id "
        "  join metadataschemaregistry msr on mfr.metadata_schema_id = msr.metadata_schema_id "
        "  where msr.short_id = 'dc' and mfr.element = 'title' "
        "    and (mfr.qualifier is null or mfr.qualifier = '') "
        "  group by mv.dspace_object_id"
        "), eperson_names as ("
        "  select mv.dspace_object_id as eperson_uuid, "
        "         max(case when mfr.element = 'firstname' then mv.text_value end) as firstname, "
        "         max(case when mfr.element = 'lastname' then mv.text_value end) as lastname "
        "  from metadatavalue mv "
        "  join metadatafieldregistry mfr on mv.metadata_field_id = mfr.metadata_field_id "
        "  where mfr.element in ('firstname', 'lastname') "
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
        + filter_clause +
        "group by submitter_uuid, submitter_name, collection_name "
        "order by submitter_name asc, count(distinct i.uuid) desc"
    )


def submitter_totals_by_period(year: int, month: int):
    start_dt, end_dt = _period_range(year, month)
    sql = _submitter_collections_query(False)

    rows = []
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (start_dt, end_dt))
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
    return submitters


def submitter_collections_for_submitter(year: int, month: int, submitter_uuid: str):
    start_dt, end_dt = _period_range(year, month)
    sql = _submitter_collections_query(True)

    rows = []
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (start_dt, end_dt, submitter_uuid))
            rows = cur.fetchall()

    collections = []
    submitter_name = None
    for _, name, collection, count in rows:
        submitter_name = name
        collections.append({
            "collection": collection,
            "count": int(count),
        })

    if not submitter_name:
        submitter_name = submitter_name_by_uuid(submitter_uuid)

    return {
        "submitter": submitter_name or submitter_uuid,
        "collections": collections,
    }


def submitter_name_by_uuid(submitter_uuid: str) -> Optional[str]:
    sql = (
        "with eperson_names as ("
        "  select mv.dspace_object_id as eperson_uuid, "
        "         max(case when mfr.element = 'firstname' then mv.text_value end) as firstname, "
        "         max(case when mfr.element = 'lastname' then mv.text_value end) as lastname "
        "  from metadatavalue mv "
        "  join metadatafieldregistry mfr on mv.metadata_field_id = mfr.metadata_field_id "
        "  where mfr.element in ('firstname', 'lastname') "
        "  group by mv.dspace_object_id"
        ") "
        "select coalesce(trim(en.firstname || ' ' || en.lastname), e.email) "
        "from eperson e "
        "left join eperson_names en on en.eperson_uuid = e.uuid "
        "where e.uuid::text = %s"
    )

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (submitter_uuid,))
            row = cur.fetchone()
            if row:
                return row[0]
    return None

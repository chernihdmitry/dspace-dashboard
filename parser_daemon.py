#!/usr/bin/env python3
import argparse
import glob
import hashlib
import logging
import os
import signal
import sys
import time
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse
import re

import psycopg

from dspace_config import get_config_value


# ----------------------------
# Config defaults
# ----------------------------

DEFAULT_LOG_GLOB = os.getenv("DSPACE_EDIT_LOG_GLOB", "/dspace/log/dspace.log")
DEFAULT_PARSER_NAME = "dspace_item_edits_daemon"
DEFAULT_POLL_SECONDS = int(os.getenv("DSPACE_EDIT_POLL_SECONDS", "5"))
DEFAULT_PENDING_SECONDS = int(os.getenv("DSPACE_EDIT_PENDING_SECONDS", "180"))
DEFAULT_DEDUPE_SECONDS = int(os.getenv("DSPACE_EDIT_DEDUPE_SECONDS", "60"))
DEFAULT_SYSTEM_RETENTION_HOURS = int(os.getenv("DSPACE_SYSTEM_EVENT_RETENTION_HOURS", "48"))
DEFAULT_REQUEST_CONTEXT_RETENTION_SECONDS = int(
    os.getenv("DSPACE_REQUEST_CONTEXT_RETENTION_SECONDS", "900")
)


# ----------------------------
# Regex
# ----------------------------

REQUEST_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+\s+\w+\s+\S+\s+(?P<req>\S+)\s+"
    r"org\.dspace\.app\.rest\.utils\.DSpaceAPIRequestLoggingFilter\s+@\s+"
    r"Before request \[(?P<method>[A-Z]+)\s+(?P<path>[^\]]+)\]"
)

UPDATE_ITEM_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+\s+\w+\s+\S+\s+(?P<req>\S+)\s+"
    r"org\.dspace\.content\.ItemServiceImpl\s+@\s+"
    r"(?P<user>[^:]+)::update_item:item_id=(?P<item>[A-Fa-f0-9\-]{36})"
)

ARCHIVE_ITEM_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+\s+\w+\s+\S+\s+(?P<req>\S+)\s+"
    r"org\.dspace\.xmlworkflow\.XmlWorkflowServiceImpl\s+@\s+"
    r"(?P<user>[^:]+)::archive_item:.*item_id=(?P<item>[A-Fa-f0-9\-]{36})"
)

INSTALL_ITEM_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+\s+\w+\s+\S+\s+(?P<req>\S+)\s+"
    r"org\.dspace\.xmlworkflow\.XmlWorkflowServiceImpl\s+@\s+"
    r"(?P<user>[^:]+)::install_item:.*item_id=(?P<item>[A-Fa-f0-9\-]{36})"
)

DELETE_WORKSPACE_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+\s+\w+\s+\S+\s+(?P<req>\S+)\s+"
    r"org\.dspace\.content\.WorkspaceItemServiceImpl\s+@\s+"
    r"(?P<user>[^:]+)::delete_workspace_item:.*item_id=(?P<item>[A-Fa-f0-9\-]{36})"
)

# Иногда полезно считать и это служебным маркером, если захочешь расширить:
ADD_ITEM_TO_COLLECTION_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+\s+\w+\s+\S+\s+(?P<req>\S+)\s+"
    r"org\.dspace\.content\.CollectionServiceImpl\s+@\s+"
    r"(?P<user>[^:]+)::add_item:collection_id=[A-Fa-f0-9\-]{36},item_id=(?P<item>[A-Fa-f0-9\-]{36})"
)


# ----------------------------
# Globals
# ----------------------------

STOP_REQUESTED = False


def _handle_signal(signum, frame):
    global STOP_REQUESTED
    STOP_REQUESTED = True


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


# ----------------------------
# Helpers
# ----------------------------

def _parse_ts(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")


def _parse_db_url(url: str) -> Optional[Dict[str, object]]:
    if not url:
        return None

    if url.startswith("jdbc:"):
        url = url[len("jdbc:") :]

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


def _db_connect():
    parsed = _parse_db_url(get_config_value("db.url", ""))
    if not parsed:
        raise RuntimeError("db.url is missing or invalid in DSpace local.cfg")

    return psycopg.connect(
        **parsed,
        user=get_config_value("db.username", ""),
        password=get_config_value("db.password", ""),
        autocommit=False,
    )


def _line_hash(inode: int, line_start: int, line: str) -> str:
    return hashlib.sha1(
        f"{inode}:{line_start}:{line}".encode("utf-8", errors="ignore")
    ).hexdigest()


def _iter_files(log_glob: str) -> Iterable[str]:
    paths = sorted(glob.glob(log_glob))
    for path in paths:
        if not os.path.isfile(path):
            continue
        if path.endswith(".gz"):
            continue
        yield path


# ----------------------------
# Schema
# ----------------------------

def _ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            create table if not exists dashboard_item_edit_events (
                id bigserial primary key,
                event_ts timestamp not null,
                user_email text not null,
                item_uuid uuid not null,
                action text not null default 'update_item',
                source_file text not null,
                source_offset bigint not null,
                line_hash text not null unique,
                created_at timestamp not null default now()
            )
            """
        )
        cur.execute(
            "create index if not exists idx_dashboard_item_edit_events_ts on dashboard_item_edit_events(event_ts)"
        )
        cur.execute(
            "create index if not exists idx_dashboard_item_edit_events_user on dashboard_item_edit_events(user_email)"
        )
        cur.execute(
            "create index if not exists idx_dashboard_item_edit_events_item on dashboard_item_edit_events(item_uuid)"
        )

        cur.execute(
            """
            create table if not exists dashboard_log_parser_state (
                parser_name text not null,
                file_path text not null,
                inode bigint not null,
                file_offset bigint not null,
                updated_at timestamp not null default now(),
                primary key (parser_name, file_path)
            )
            """
        )

        cur.execute(
            """
            create table if not exists dashboard_item_edit_pending (
                id bigserial primary key,
                event_ts timestamp not null,
                user_email text not null,
                item_uuid uuid not null,
                request_id text,
                source_file text not null,
                source_offset bigint not null,
                line_hash text not null unique,
                created_at timestamp not null default now()
            )
            """
        )
        cur.execute(
            "create index if not exists idx_dashboard_item_edit_pending_ts on dashboard_item_edit_pending(event_ts)"
        )
        cur.execute(
            "create index if not exists idx_dashboard_item_edit_pending_item on dashboard_item_edit_pending(item_uuid)"
        )

        cur.execute(
            """
            create table if not exists dashboard_item_system_events (
                id bigserial primary key,
                event_ts timestamp not null,
                item_uuid uuid not null,
                event_type text not null,
                request_id text,
                source_file text not null,
                source_offset bigint not null,
                line_hash text not null unique,
                created_at timestamp not null default now()
            )
            """
        )
        cur.execute(
            "create index if not exists idx_dashboard_item_system_events_ts on dashboard_item_system_events(event_ts)"
        )
        cur.execute(
            "create index if not exists idx_dashboard_item_system_events_item on dashboard_item_system_events(item_uuid)"
        )
        cur.execute(
            "create index if not exists idx_dashboard_item_system_events_type on dashboard_item_system_events(event_type)"
        )

    conn.commit()


# ----------------------------
# Parser state
# ----------------------------

def _load_state(conn, parser_name: str, file_path: str) -> Optional[Tuple[int, int]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select inode, file_offset
            from dashboard_log_parser_state
            where parser_name = %s and file_path = %s
            """,
            (parser_name, file_path),
        )
        row = cur.fetchone()
        if not row:
            return None
        return int(row[0]), int(row[1])


def _save_state(conn, parser_name: str, file_path: str, inode: int, offset: int):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into dashboard_log_parser_state(parser_name, file_path, inode, file_offset, updated_at)
            values (%s, %s, %s, %s, now())
            on conflict (parser_name, file_path)
            do update set inode = excluded.inode,
                          file_offset = excluded.file_offset,
                          updated_at = now()
            """,
            (parser_name, file_path, inode, offset),
        )


# ----------------------------
# DB insert helpers
# ----------------------------

def _insert_pending(
    conn,
    event_ts: datetime,
    user_email: str,
    item_uuid: str,
    request_id: Optional[str],
    source_file: str,
    source_offset: int,
    line_hash: str,
) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into dashboard_item_edit_pending(
                event_ts, user_email, item_uuid, request_id,
                source_file, source_offset, line_hash
            )
            values (%s, %s, %s::uuid, %s, %s, %s, %s)
            on conflict (line_hash) do nothing
            """,
            (event_ts, user_email, item_uuid, request_id, source_file, source_offset, line_hash),
        )
        return cur.rowcount > 0


def _insert_system_event(
    conn,
    event_ts: datetime,
    item_uuid: str,
    event_type: str,
    request_id: Optional[str],
    source_file: str,
    source_offset: int,
    line_hash: str,
) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into dashboard_item_system_events(
                event_ts, item_uuid, event_type, request_id,
                source_file, source_offset, line_hash
            )
            values (%s, %s::uuid, %s, %s, %s, %s, %s)
            on conflict (line_hash) do nothing
            """,
            (event_ts, item_uuid, event_type, request_id, source_file, source_offset, line_hash),
        )
        return cur.rowcount > 0


def _insert_final_event(
    conn,
    event_ts: datetime,
    user_email: str,
    item_uuid: str,
    source_file: str,
    source_offset: int,
    line_hash: str,
) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into dashboard_item_edit_events(
                event_ts, user_email, item_uuid, action,
                source_file, source_offset, line_hash
            )
            values (%s, %s, %s::uuid, 'update_item', %s, %s, %s)
            on conflict (line_hash) do nothing
            """,
            (event_ts, user_email, item_uuid, source_file, source_offset, line_hash),
        )
        return cur.rowcount > 0


# ----------------------------
# Finalize pending logic
# ----------------------------

def _pending_has_nearby_system_event(
    conn,
    item_uuid: str,
    event_ts: datetime,
    window_seconds: int,
) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            select 1
            from dashboard_item_system_events s
            where s.item_uuid = %s::uuid
            and s.event_ts between %s - (%s * interval '1 second')
                                and %s + (%s * interval '1 second')
            limit 1
            """,
            (item_uuid, event_ts, window_seconds, event_ts, window_seconds),
        )
        return cur.fetchone() is not None


def _has_recent_final_duplicate(
    conn,
    event_ts: datetime,
    user_email: str,
    item_uuid: str,
    dedupe_seconds: int,
) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            select 1
            from dashboard_item_edit_events e
            where lower(e.user_email) = lower(%s)
            and e.item_uuid = %s::uuid
            and e.event_ts between %s - (%s * interval '1 second') and %s
            limit 1
            """,
            (user_email, item_uuid, event_ts, dedupe_seconds, event_ts),
        )
        return cur.fetchone() is not None


def _finalize_pending(conn, pending_seconds: int, dedupe_seconds: int) -> Tuple[int, int]:
    """
    Возвращает:
    - finalized_count
    - discarded_count
    """
    now = datetime.now()
    threshold = now - timedelta(seconds=pending_seconds)

    with conn.cursor() as cur:
        cur.execute(
            """
            select id, event_ts, user_email, item_uuid::text, source_file, source_offset, line_hash
            from dashboard_item_edit_pending
            where event_ts <= %s
            order by event_ts asc, id asc
            """,
            (threshold,),
        )
        pending_rows = cur.fetchall()

    finalized = 0
    discarded = 0
    in_batch_last_kept: Dict[Tuple[str, str], datetime] = {}

    for row in pending_rows:
        pending_id = int(row[0])
        event_ts = row[1]
        user_email = row[2]
        item_uuid = row[3]
        source_file = row[4]
        source_offset = int(row[5])
        line_hash = row[6]

        # 1. Шум сабмита/workflow — рядом есть system event
        if _pending_has_nearby_system_event(conn, item_uuid, event_ts, pending_seconds):
            with conn.cursor() as cur:
                cur.execute(
                    "delete from dashboard_item_edit_pending where id = %s",
                    (pending_id,),
                )
            discarded += 1
            continue

        # 2. Дедупликация против уже сохранённых final events
        if _has_recent_final_duplicate(conn, event_ts, user_email, item_uuid, dedupe_seconds):
            with conn.cursor() as cur:
                cur.execute(
                    "delete from dashboard_item_edit_pending where id = %s",
                    (pending_id,),
                )
            discarded += 1
            continue

        # 3. Дедупликация внутри текущей пачки finalize
        batch_key = (user_email.lower(), item_uuid)
        prev_kept_ts = in_batch_last_kept.get(batch_key)
        if prev_kept_ts and (event_ts - prev_kept_ts).total_seconds() <= dedupe_seconds:
            with conn.cursor() as cur:
                cur.execute(
                    "delete from dashboard_item_edit_pending where id = %s",
                    (pending_id,),
                )
            discarded += 1
            continue

        # 4. Всё ок — переносим в финальную таблицу
        inserted = _insert_final_event(
            conn=conn,
            event_ts=event_ts,
            user_email=user_email,
            item_uuid=item_uuid,
            source_file=source_file,
            source_offset=source_offset,
            line_hash=line_hash,
        )

        with conn.cursor() as cur:
            cur.execute(
                "delete from dashboard_item_edit_pending where id = %s",
                (pending_id,),
            )

        if inserted:
            finalized += 1
            in_batch_last_kept[batch_key] = event_ts
        else:
            # если line_hash уже был — просто убираем pending
            discarded += 1

    return finalized, discarded


def _cleanup_old_system_events(conn, retention_hours: int) -> int:
    cutoff = datetime.now() - timedelta(hours=retention_hours)
    with conn.cursor() as cur:
        cur.execute(
            "delete from dashboard_item_system_events where event_ts < %s",
            (cutoff,),
        )
        return cur.rowcount


# ----------------------------
# One-pass parse iteration
# ----------------------------

def _parse_file_iteration(
    conn,
    parser_name: str,
    file_path: str,
    request_context: Dict[str, Tuple[str, float]],
) -> Tuple[int, int, int, int]:
    """
    Возвращает:
    lines_read, pending_inserted, system_inserted, final_skipped_by_request_context
    """
    stat = os.stat(file_path)
    inode = int(stat.st_ino)
    file_size = int(stat.st_size)

    state = _load_state(conn, parser_name, file_path)
    start_offset = 0

    if state:
        state_inode, state_offset = state
        if state_inode == inode and 0 <= state_offset <= file_size:
            start_offset = state_offset

    lines_read = 0
    pending_inserted = 0
    system_inserted = 0
    skipped_by_request_context = 0

    with open(file_path, "rb") as handle:
        handle.seek(start_offset)

        while True:
            line_start = handle.tell()
            raw_line = handle.readline()
            if not raw_line:
                break

            lines_read += 1
            line = raw_line.decode("utf-8", errors="replace").strip()

            # request context
            m = REQUEST_RE.match(line)
            if m:
                req_id = m.group("req")
                path = m.group("path")
                now_epoch = time.time()

                if "/server/api/submission/" in path:
                    request_context[req_id] = ("submission", now_epoch)
                elif "/server/api/workflow/" in path:
                    request_context[req_id] = ("workflow", now_epoch)
                else:
                    request_context.setdefault(req_id, ("normal", now_epoch))
                continue

            # system events with item_uuid
            matched_system = False
            for rx, event_type in (
                (ARCHIVE_ITEM_RE, "archive_item"),
                (INSTALL_ITEM_RE, "install_item"),
                (DELETE_WORKSPACE_RE, "delete_workspace_item"),
                (ADD_ITEM_TO_COLLECTION_RE, "collection_add_item"),
            ):
                m = rx.match(line)
                if m:
                    event_ts = _parse_ts(m.group("ts"))
                    req_id = m.group("req")
                    item_uuid = m.group("item").strip()
                    h = _line_hash(inode, line_start, line)

                    if _insert_system_event(
                        conn=conn,
                        event_ts=event_ts,
                        item_uuid=item_uuid,
                        event_type=event_type,
                        request_id=req_id,
                        source_file=file_path,
                        source_offset=line_start,
                        line_hash=h,
                    ):
                        system_inserted += 1

                    matched_system = True
                    break

            if matched_system:
                continue

            # update_item candidate
            m = UPDATE_ITEM_RE.match(line)
            if not m:
                continue

            event_ts = _parse_ts(m.group("ts"))
            req_id = m.group("req")
            user_email = m.group("user").strip()
            item_uuid = m.group("item").strip()

            # request context says submission/workflow => skip immediately
            req_ctx = request_context.get(req_id)
            if req_ctx and req_ctx[0] in {"submission", "workflow"}:
                skipped_by_request_context += 1
                continue

            h = _line_hash(inode, line_start, line)
            if _insert_pending(
                conn=conn,
                event_ts=event_ts,
                user_email=user_email,
                item_uuid=item_uuid,
                request_id=req_id,
                source_file=file_path,
                source_offset=line_start,
                line_hash=h,
            ):
                pending_inserted += 1

        end_offset = handle.tell()

    _save_state(conn, parser_name, file_path, inode, end_offset)
    return lines_read, pending_inserted, system_inserted, skipped_by_request_context


def _prune_request_context(
    request_context: Dict[str, Tuple[str, float]],
    retention_seconds: int,
) -> int:
    now_epoch = time.time()
    to_delete = [
        req_id
        for req_id, (_, seen_at) in request_context.items()
        if (now_epoch - seen_at) > retention_seconds
    ]
    for req_id in to_delete:
        request_context.pop(req_id, None)
    return len(to_delete)


# ----------------------------
# Main daemon
# ----------------------------

def run_daemon(
    log_glob: str,
    parser_name: str,
    poll_seconds: int,
    pending_seconds: int,
    dedupe_seconds: int,
    system_retention_hours: int,
    request_context_retention_seconds: int,
):
    request_context: Dict[str, Tuple[str, float]] = {}

    logging.info("Starting parser daemon")
    logging.info(
        "config: log_glob=%s parser_name=%s poll=%s pending=%s dedupe=%s",
        log_glob,
        parser_name,
        poll_seconds,
        pending_seconds,
        dedupe_seconds,
    )

    with _db_connect() as conn:
        _ensure_schema(conn)

        while not STOP_REQUESTED:
            try:
                files = list(_iter_files(log_glob))
                total_lines = 0
                total_pending = 0
                total_system = 0
                total_skipped_reqctx = 0

                for file_path in files:
                    lines_read, pending_inserted, system_inserted, skipped_reqctx = _parse_file_iteration(
                        conn=conn,
                        parser_name=parser_name,
                        file_path=file_path,
                        request_context=request_context,
                    )
                    total_lines += lines_read
                    total_pending += pending_inserted
                    total_system += system_inserted
                    total_skipped_reqctx += skipped_reqctx

                finalized, discarded = _finalize_pending(
                    conn=conn,
                    pending_seconds=pending_seconds,
                    dedupe_seconds=dedupe_seconds,
                )

                deleted_system = _cleanup_old_system_events(
                    conn=conn,
                    retention_hours=system_retention_hours,
                )

                pruned_reqctx = _prune_request_context(
                    request_context=request_context,
                    retention_seconds=request_context_retention_seconds,
                )

                conn.commit()

                logging.info(
                    "iteration: files=%s lines=%s pending+%s system+%s skipped_reqctx=%s finalized=%s discarded=%s cleanup_system=%s prune_reqctx=%s reqctx_size=%s",
                    len(files),
                    total_lines,
                    total_pending,
                    total_system,
                    total_skipped_reqctx,
                    finalized,
                    discarded,
                    deleted_system,
                    pruned_reqctx,
                    len(request_context),
                )

            except Exception:
                conn.rollback()
                logging.exception("Iteration failed")

            slept = 0
            while slept < poll_seconds and not STOP_REQUESTED:
                time.sleep(1)
                slept += 1

    logging.info("Parser daemon stopped")


def main():
    parser = argparse.ArgumentParser(
        description="DSpace log parser daemon for real item edit events"
    )
    parser.add_argument(
        "--log-glob",
        default=DEFAULT_LOG_GLOB,
        help=f"Glob for DSpace log files (default: {DEFAULT_LOG_GLOB})",
    )
    parser.add_argument(
        "--parser-name",
        default=DEFAULT_PARSER_NAME,
        help=f"Parser state key (default: {DEFAULT_PARSER_NAME})",
    )
    parser.add_argument(
        "--poll-seconds",
        type=int,
        default=DEFAULT_POLL_SECONDS,
        help=f"Polling interval in seconds (default: {DEFAULT_POLL_SECONDS})",
    )
    parser.add_argument(
        "--pending-seconds",
        type=int,
        default=DEFAULT_PENDING_SECONDS,
        help=f"Delay before classifying update_item as real edit (default: {DEFAULT_PENDING_SECONDS})",
    )
    parser.add_argument(
        "--dedupe-seconds",
        type=int,
        default=DEFAULT_DEDUPE_SECONDS,
        help=f"Dedupe window for same user+item (default: {DEFAULT_DEDUPE_SECONDS})",
    )
    parser.add_argument(
        "--system-retention-hours",
        type=int,
        default=DEFAULT_SYSTEM_RETENTION_HOURS,
        help=f"Retention for dashboard_item_system_events (default: {DEFAULT_SYSTEM_RETENTION_HOURS})",
    )
    parser.add_argument(
        "--request-context-retention-seconds",
        type=int,
        default=DEFAULT_REQUEST_CONTEXT_RETENTION_SECONDS,
        help=f"Retention for in-memory request context (default: {DEFAULT_REQUEST_CONTEXT_RETENTION_SECONDS})",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("DSPACE_EDIT_LOG_LEVEL", "INFO"),
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stdout,
    )

    run_daemon(
        log_glob=args.log_glob,
        parser_name=args.parser_name,
        poll_seconds=args.poll_seconds,
        pending_seconds=args.pending_seconds,
        dedupe_seconds=args.dedupe_seconds,
        system_retention_hours=args.system_retention_hours,
        request_context_retention_seconds=args.request_context_retention_seconds,
    )


if __name__ == "__main__":
    raise SystemExit(main())
#!/usr/bin/env python3
import argparse
import glob
import hashlib
import os
import re
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

import psycopg

from dspace_config import get_config_value

UPDATE_ITEM_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+\s+\w+\s+\S+\s+\S+\s+"
    r"org\.dspace\.content\.ItemServiceImpl\s+@\s+"
    r"(?P<user>[^:]+)::update_item:item_id=(?P<item>[A-Fa-f0-9\-]{36})"
)


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
    )


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
    conn.commit()


def _load_state(conn, parser_name: str, file_path: str) -> Optional[Tuple[int, int]]:
    with conn.cursor() as cur:
        cur.execute(
            "select inode, file_offset from dashboard_log_parser_state where parser_name = %s and file_path = %s",
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
            do update set inode = excluded.inode, file_offset = excluded.file_offset, updated_at = now()
            """,
            (parser_name, file_path, inode, offset),
        )


def _iter_files(log_glob: str) -> Iterable[str]:
    paths = sorted(glob.glob(log_glob))
    for path in paths:
        if not os.path.isfile(path):
            continue
        if path.endswith(".gz"):
            continue
        yield path


def _parse_line(line: str) -> Optional[Tuple[datetime, str, str]]:
    match = UPDATE_ITEM_RE.match(line.strip())
    if not match:
        return None

    ts = datetime.strptime(match.group("ts"), "%Y-%m-%d %H:%M:%S")
    return ts, match.group("user").strip(), match.group("item").strip()


def _collect_events_for_file(
    file_path: str,
    inode: int,
    start_offset: int,
) -> Tuple[List[Tuple[datetime, str, str, str, int, str]], int, int]:
    events: List[Tuple[datetime, str, str, str, int, str]] = []
    lines_read = 0

    with open(file_path, "rb") as handle:
        handle.seek(start_offset)

        while True:
            line_start = handle.tell()
            raw_line = handle.readline()
            if not raw_line:
                break

            lines_read += 1
            line = raw_line.decode("utf-8", errors="replace")
            parsed = _parse_line(line)
            if not parsed:
                continue

            event_ts, user_email, item_uuid = parsed
            line_hash = hashlib.sha1(
                f"{inode}:{line_start}:{line}".encode("utf-8", errors="ignore")
            ).hexdigest()
            events.append(
                (
                    event_ts,
                    user_email,
                    item_uuid,
                    file_path,
                    line_start,
                    line_hash,
                )
            )

        end_offset = handle.tell()

    return events, lines_read, end_offset


def _insert_events(conn, rows: List[Tuple[datetime, str, str, str, int, str]]) -> int:
    if not rows:
        return 0

    inserted = 0
    with conn.cursor() as cur:
        for row in rows:
            cur.execute(
                """
                insert into dashboard_item_edit_events(
                    event_ts, user_email, item_uuid, action, source_file, source_offset, line_hash
                )
                values (%s, %s, %s::uuid, 'update_item', %s, %s, %s)
                on conflict (line_hash) do nothing
                """,
                row,
            )
            if cur.rowcount > 0:
                inserted += 1
    return inserted


def main():
    parser = argparse.ArgumentParser(
        description="Parse DSpace logs and persist item update events per user"
    )
    parser.add_argument(
        "--log-glob",
        default=os.getenv("DSPACE_EDIT_LOG_GLOB", "/dspace/log/*.log"),
        help="Glob for DSpace log files (default: /dspace/log/*.log)",
    )
    parser.add_argument(
        "--parser-name",
        default="dspace_item_edits",
        help="Parser state key for incremental reads",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and report without writing to DB",
    )
    args = parser.parse_args()

    files = list(_iter_files(args.log_glob))
    if not files:
        print(f"No files matched: {args.log_glob}")
        return 0

    total_lines = 0
    total_parsed = 0
    total_inserted = 0

    with _db_connect() as conn:
        _ensure_schema(conn)

        for file_path in files:
            stat = os.stat(file_path)
            inode = int(stat.st_ino)
            file_size = int(stat.st_size)

            state = _load_state(conn, args.parser_name, file_path)
            start_offset = 0
            if state:
                state_inode, state_offset = state
                if state_inode == inode and 0 <= state_offset <= file_size:
                    start_offset = state_offset

            rows, lines_read, end_offset = _collect_events_for_file(file_path, inode, start_offset)

            total_lines += lines_read
            total_parsed += len(rows)

            inserted = 0
            if not args.dry_run:
                inserted = _insert_events(conn, rows)
                _save_state(conn, args.parser_name, file_path, inode, end_offset)
                conn.commit()

            total_inserted += inserted
            print(
                f"{file_path}: lines={lines_read}, parsed={len(rows)}, inserted={inserted}, offset={start_offset}->{end_offset}"
            )

    print(
        f"Done: files={len(files)}, lines={total_lines}, parsed={total_parsed}, inserted={total_inserted}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

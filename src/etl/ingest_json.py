# src/etl/ingest_json.py
"""
Incremental ingestion of new .json files into SQLite (sessions/hits).

Поддерживаемые форматы входных файлов:
1) list[dict]                                -> либо sessions, либо hits
2) {"sessions": [...], "hits": [...]}        -> mixed
3) {"2022-01-02": [ {...}, {...} ]}          -> sessions/hits (как у тебя)
4) {"2022-01-01": [...], "2022-01-02": [...]}-> тоже ок (мы склеим списки)

Важно:
- Пустые файлы (0 строк) НЕ ломают пайплайн. Их логируем как success с 0 строк.
- Пайплайн не падает из-за одного файла: ошибки логируются, обработка продолжается.
- В режиме --force-reload удаляем старую запись ingestion_log по hash и грузим заново.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple


# ---------------------------
# sha256 файла
# ---------------------------
def file_sha256(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


# ---------------------------
# загрузка json
# ---------------------------
def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


# ---------------------------
# ingestion_log
# ---------------------------
def ensure_ingestion_log(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ingestion_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_name TEXT NOT NULL,
            file_path TEXT NOT NULL,
            file_hash TEXT NOT NULL,
            file_size_bytes INTEGER NOT NULL,
            file_mtime TEXT NOT NULL,
            loaded_at TEXT NOT NULL,
            status TEXT NOT NULL,              -- success | skipped | failed
            table_name TEXT,                   -- sessions | hits | mixed | unknown | unknown_empty
            rows_total INTEGER DEFAULT 0,
            rows_inserted INTEGER DEFAULT 0,
            error TEXT
        );
        """
    )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_ingestion_log_file_hash ON ingestion_log(file_hash);"
    )
    conn.commit()


def ensure_uniqueness(conn: sqlite3.Connection) -> None:
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_hits_hit_id_unique ON hits(hit_id);")
    conn.commit()


# ---------------------------
# эвристика: определить тип по имени файла
# ---------------------------
def kind_from_filename(name: str) -> str:
    low = name.lower()
    if "hits" in low:
        return "hits"
    if "sessions" in low:
        return "sessions"
    return "unknown"


# ---------------------------
# normalize payload -> list[dict]
# ---------------------------
def normalize_to_list_of_dict(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]

    if isinstance(payload, dict):
        # формат {"2022-01-02": [ ... ]} или несколько дат
        all_values_are_lists = all(isinstance(v, list) for v in payload.values())
        if all_values_are_lists and len(payload) >= 1:
            rows: List[Dict[str, Any]] = []
            for v in payload.values():
                rows.extend([r for r in v if isinstance(r, dict)])
            return rows

        # одиночная запись
        return [payload]

    return []


# ---------------------------
# split: sessions/hits/mixed
# ---------------------------
def split_payload(payload: Any) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], str]:
    # mixed-формат
    if isinstance(payload, dict) and ("sessions" in payload or "hits" in payload):
        sess = payload.get("sessions", [])
        hits = payload.get("hits", [])
        sessions_rows = [r for r in sess if isinstance(r, dict)] if isinstance(sess, list) else []
        hits_rows = [r for r in hits if isinstance(r, dict)] if isinstance(hits, list) else []
        return sessions_rows, hits_rows, "mixed"

    rows = normalize_to_list_of_dict(payload)
    if not rows:
        return [], [], "unknown"

    hits_keys = {"hit_date", "hit_time", "hit_number", "hit_type", "event_action", "hit_page_path"}
    sessions_keys = {"visit_date", "visit_time", "visit_number", "utm_source", "device_category", "geo_city"}

    hits_score = 0
    sess_score = 0
    for r in rows[:2000]:
        keys = set(r.keys())
        if keys & hits_keys:
            hits_score += 1
        if keys & sessions_keys:
            sess_score += 1

    if hits_score > sess_score:
        return [], rows, "hits"
    else:
        return rows, [], "sessions"


# ---------------------------
# insert rows
# ---------------------------
def insert_rows(
    conn: sqlite3.Connection, table: str, rows: List[Dict[str, Any]], batch_size: int = 5000
) -> Tuple[int, int]:
    if not rows:
        return 0, 0

    cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table});").fetchall()]
    cols_set = set(cols)

    cleaned: List[Dict[str, Any]] = [{k: r.get(k) for k in r.keys() if k in cols_set} for r in rows]
    if not cleaned or all(len(r) == 0 for r in cleaned):
        return len(rows), 0

    used_cols = [c for c in cols if any(c in r for r in cleaned)]
    placeholders = ", ".join(["?"] * len(used_cols))
    colnames_sql = ", ".join(used_cols)
    sql = f"INSERT OR IGNORE INTO {table} ({colnames_sql}) VALUES ({placeholders});"

    before = conn.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]

    total = len(cleaned)
    for i in range(0, total, batch_size):
        batch = cleaned[i : i + batch_size]
        values = [[r.get(c) for c in used_cols] for r in batch]
        conn.executemany(sql, values)

    after = conn.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]
    return len(rows), int(after - before)


# ---------------------------
# ingestion одного файла
# ---------------------------
def ingest_file(conn: sqlite3.Connection, path: Path, force_reload: bool = False) -> None:
    fhash = file_sha256(path)
    stat = path.stat()
    loaded_at = datetime.now().isoformat(timespec="seconds")
    file_kind_hint = kind_from_filename(path.name)

    # force_reload: убираем старую запись по hash (иначе unique index не даст записать заново)
    if force_reload:
        conn.execute("DELETE FROM ingestion_log WHERE file_hash = ?;", (fhash,))
        conn.commit()

    # если НЕ force_reload и файл уже успешно грузили — пропускаем
    if not force_reload:
        exists = conn.execute(
            "SELECT 1 FROM ingestion_log WHERE file_hash = ? AND status = 'success' LIMIT 1;",
            (fhash,),
        ).fetchone()
        if exists:
            return

    try:
        payload = load_json(path)
        sess_rows, hits_rows, kind = split_payload(payload)

        # кейс: файл пустой (например 2022-01-01 -> [])
        if kind == "unknown" and (file_kind_hint in ("sessions", "hits")):
            # логируем как успех с 0 строк — это нормальная ситуация
            conn.execute(
                """
                INSERT INTO ingestion_log
                (file_name, file_path, file_hash, file_size_bytes, file_mtime, loaded_at,
                 status, table_name, rows_total, rows_inserted, error)
                VALUES (?, ?, ?, ?, ?, ?, 'success', ?, 0, 0, NULL);
                """,
                (
                    path.name,
                    str(path),
                    fhash,
                    int(stat.st_size),
                    datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds"),
                    loaded_at,
                    f"{file_kind_hint}_empty",
                ),
            )
            conn.commit()
            return

        # если реально непонятный/битый файл — логируем fail и идём дальше
        if kind == "unknown":
            conn.execute(
                """
                INSERT INTO ingestion_log
                (file_name, file_path, file_hash, file_size_bytes, file_mtime, loaded_at,
                 status, table_name, rows_total, rows_inserted, error)
                VALUES (?, ?, ?, ?, ?, ?, 'failed', 'unknown', 0, 0, ?);
                """,
                (
                    path.name,
                    str(path),
                    fhash,
                    int(stat.st_size),
                    datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds"),
                    loaded_at,
                    "Unknown JSON structure (no rows detected)",
                ),
            )
            conn.commit()
            return

        rows_total = 0
        rows_inserted = 0

        with conn:
            if kind in ("sessions", "mixed") and sess_rows:
                t, ins = insert_rows(conn, "sessions", sess_rows)
                rows_total += t
                rows_inserted += ins

            if kind in ("hits", "mixed") and hits_rows:
                t, ins = insert_rows(conn, "hits", hits_rows)
                rows_total += t
                rows_inserted += ins

            conn.execute(
                """
                INSERT INTO ingestion_log
                (file_name, file_path, file_hash, file_size_bytes, file_mtime, loaded_at,
                 status, table_name, rows_total, rows_inserted, error)
                VALUES (?, ?, ?, ?, ?, ?, 'success', ?, ?, ?, NULL);
                """,
                (
                    path.name,
                    str(path),
                    fhash,
                    int(stat.st_size),
                    datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds"),
                    loaded_at,
                    kind,
                    int(rows_total),
                    int(rows_inserted),
                ),
            )

    except Exception as e:
        # не роняем пайплайн — только логируем ошибку
        try:
            conn.execute("DELETE FROM ingestion_log WHERE file_hash = ?;", (fhash,))
            conn.execute(
                """
                INSERT INTO ingestion_log
                (file_name, file_path, file_hash, file_size_bytes, file_mtime, loaded_at,
                 status, table_name, rows_total, rows_inserted, error)
                VALUES (?, ?, ?, ?, ?, ?, 'failed', ?, 0, 0, ?);
                """,
                (
                    path.name,
                    str(path),
                    fhash,
                    int(stat.st_size),
                    datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds"),
                    loaded_at,
                    file_kind_hint,
                    str(e),
                ),
            )
            conn.commit()
        except Exception:
            pass


# ---------------------------
# CLI
# ---------------------------
def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="data/ga.sqlite", help="Path to SQLite database")
    parser.add_argument("--input-dir", default="data/incoming_json", help="Folder with incoming *.json")
    parser.add_argument("--force-reload", action="store_true", help="Reload even if file hash exists in log")
    args = parser.parse_args()

    db_path = Path(args.db)
    input_dir = Path(args.input_dir)

    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path}")
    if not input_dir.exists():
        raise FileNotFoundError(f"Input dir not found: {input_dir}")

    json_files = sorted(input_dir.glob("*.json"))

    print(f"DB: {db_path}")
    print(f"Input dir: {input_dir}")
    print(f"Found JSON files: {len(json_files)}")
    print(f"Force reload: {args.force_reload}")

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")

    ensure_ingestion_log(conn)
    ensure_uniqueness(conn)

    for p in json_files:
        ingest_file(conn, p, force_reload=args.force_reload)

    rows = conn.execute(
        """
        SELECT id, loaded_at, file_name, status, table_name, rows_total, rows_inserted, substr(error, 1, 120)
        FROM ingestion_log
        ORDER BY id DESC
        LIMIT 15;
        """
    ).fetchall()

    print("\nLast ingestion_log rows:")
    for r in rows:
        print(r)

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()

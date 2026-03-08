"""
src/etl/load_pkl_to_sqlite.py

Этап 3.2 — загрузка исходных ga_sessions и ga_hits в SQLite.

Задача (как ты сформулировала):
- пересоздаём базу "с нуля" (через --reset)
- sessions загружаем из PKL (если есть; иначе CSV)
- hits загружаем ТОЛЬКО из CSV (чанками), PKL для hits НЕ используем

Что делает:
- (опционально) очищает таблицы sessions/hits (через --reset)
- читает sessions из data/raw (предпочтительно pkl)
- читает hits из data/raw ТОЛЬКО CSV чанками
- приводит типы и базово чистит строки
- загружает в SQLite пачками (chunks)
- после загрузки hits удаляет "сироты" (hits без соответствующей sessions)
  и включает FK обратно

Запуск:
    python -m src.etl.load_pkl_to_sqlite --reset
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

import pandas as pd

from src.etl.db_sqlite import connect, get_project_root


# -----------------------------
# Пути к данным (строго по структуре проекта)
# -----------------------------
def get_raw_dir() -> Path:
    """Папка с исходными файлами."""
    return get_project_root() / "data" / "raw"


# -----------------------------
# Загрузка файлов (sessions: pkl предпочтительнее csv)
# -----------------------------
def read_best(path_pkl: Path, path_csv: Path) -> pd.DataFrame:
    """
    Читает pkl если он существует, иначе csv.
    """
    if path_pkl.exists():
        return pd.read_pickle(path_pkl)
    if path_csv.exists():
        return pd.read_csv(path_csv)
    raise FileNotFoundError(f"Neither file exists: {path_pkl} or {path_csv}")


# -----------------------------
# Чистка / типизация (минимально, как в ноутбуках)
# -----------------------------
def clean_sessions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Минимальная чистка sessions:
    - visit_date -> datetime -> ISO string YYYY-MM-DD
    - visit_time -> string HH:MM:SS (как есть)
    - visit_number -> Int64
    - строки: trim
    """
    df = df.copy()

    # Дата: к datetime, затем берём только date, сохраняем как строку
    df["visit_date"] = pd.to_datetime(df["visit_date"], errors="coerce").dt.date.astype("string")

    # Время оставляем строкой
    df["visit_time"] = df["visit_time"].astype("string")

    # visit_number -> Int64 (nullable)
    df["visit_number"] = pd.to_numeric(df["visit_number"], errors="coerce").astype("Int64")

    # Трим строк
    for col in df.columns:
        if pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].astype("string").str.strip()

    return df


def clean_hits(df: pd.DataFrame) -> pd.DataFrame:
    """
    Минимальная чистка hits:
    - hit_date -> datetime -> ISO string YYYY-MM-DD
    - hit_time -> Int64 (nullable)
    - hit_number -> Int64
    - event_value НЕ грузим (в данных он пустой)
    - строки: trim
    """
    df = df.copy()

    # Удаляем event_value (у тебя missing rate 100%)
    if "event_value" in df.columns:
        df = df.drop(columns=["event_value"])

    # Даты
    df["hit_date"] = pd.to_datetime(df["hit_date"], errors="coerce").dt.date.astype("string")

    # hit_time: часто float/NaN -> Int64
    df["hit_time"] = pd.to_numeric(df["hit_time"], errors="coerce").astype("Int64")

    # hit_number -> Int64
    df["hit_number"] = pd.to_numeric(df["hit_number"], errors="coerce").astype("Int64")

    # Трим строк
    for col in df.columns:
        if pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].astype("string").str.strip()

    return df


# -----------------------------
# Загрузка в SQLite
# -----------------------------
def reset_tables(conn: sqlite3.Connection) -> None:
    """
    Очищает таблицы hits и sessions.
    Важно: сначала hits, потом sessions (из-за FK).
    """
    conn.execute("DELETE FROM hits;")
    conn.execute("DELETE FROM sessions;")
    conn.commit()


def load_sessions(conn: sqlite3.Connection) -> int:
    """
    Загружает sessions в таблицу sessions.
    Возвращает количество загруженных строк.
    """
    raw_dir = get_raw_dir()

    sess_pkl = raw_dir / "ga_sessions.pkl"
    sess_csv = raw_dir / "ga_sessions.csv"

    sessions = read_best(sess_pkl, sess_csv)
    sessions = clean_sessions(sessions)

    # Загружаем пачками
    sessions.to_sql("sessions", conn, if_exists="append", index=False, chunksize=50_000)

    return len(sessions)


def load_hits_from_csv(conn: sqlite3.Connection, chunksize_read: int = 200_000) -> int:
    """
    Загружает hits из CSV чанками (ga_hits-001.csv).
    Возвращает количество загруженных строк.
    """
    raw_dir = get_raw_dir()
    hits_csv = raw_dir / "ga_hits-001.csv"

    if not hits_csv.exists():
        raise FileNotFoundError(f"Hits CSV not found: {hits_csv}")

    total = 0

    # Колонки, которые реально грузим в БД (hit_id AUTOINCREMENT в таблице)
    cols_for_db = [
        "session_id",
        "hit_date",
        "hit_time",
        "hit_number",
        "hit_type",
        "hit_referer",
        "hit_page_path",
        "event_category",
        "event_action",
        "event_label",
    ]

    # Читаем CSV чанками, чтобы не упасть по памяти
    for chunk in pd.read_csv(hits_csv, chunksize=chunksize_read):
        chunk = clean_hits(chunk)

        # На всякий случай: если в чанке нет какой-то колонки — упадём с понятной ошибкой
        missing_cols = [c for c in cols_for_db if c not in chunk.columns]
        if missing_cols:
            raise ValueError(f"Hits CSV chunk missing columns: {missing_cols}")

        chunk = chunk[cols_for_db]

        # Пишем в SQLite
        chunk.to_sql("hits", conn, if_exists="append", index=False, chunksize=50_000)
        total += len(chunk)

        print(f"Loaded hits: {total:,}")

    return total


def delete_orphan_hits(conn: sqlite3.Connection) -> int:
    """
    Удаляет строки hits, у которых session_id отсутствует в sessions.
    Возвращает количество удалённых строк.
    """
    cur = conn.execute(
        """
        SELECT COUNT(*)
        FROM hits h
        LEFT JOIN sessions s ON s.session_id = h.session_id
        WHERE s.session_id IS NULL;
        """
    )
    orphan_cnt = cur.fetchone()[0]

    conn.execute(
        """
        DELETE FROM hits
        WHERE session_id NOT IN (SELECT session_id FROM sessions);
        """
    )
    conn.commit()
    return orphan_cnt


def sanity_checks(conn: sqlite3.Connection) -> None:
    """Мини-проверки после загрузки."""
    sess_cnt = conn.execute("SELECT COUNT(*) FROM sessions;").fetchone()[0]
    hits_cnt = conn.execute("SELECT COUNT(*) FROM hits;").fetchone()[0]
    orphan_hits = conn.execute(
        "SELECT COUNT(*) FROM hits WHERE session_id NOT IN (SELECT session_id FROM sessions);"
    ).fetchone()[0]
    distinct_hits_sessions = conn.execute("SELECT COUNT(DISTINCT session_id) FROM hits;").fetchone()[0]

    print("\n--- Sanity checks ---")
    print(f"sessions rows: {sess_cnt:,}")
    print(f"hits rows: {hits_cnt:,}")
    print(f"distinct session_id in hits: {distinct_hits_sessions:,}")
    print(f"orphan hits (should be 0): {orphan_hits:,}")


def main(reset: bool) -> None:
    """
    Полная загрузка под твою задачу:
    - при --reset очищаем таблицы
    - грузим sessions (pkl предпочтительнее)
    - грузим hits только из CSV
    - чистим orphan hits
    """
    with connect() as conn:
        if reset:
            print("🧹 Reset tables: hits, sessions")
            reset_tables(conn)

        # Сначала sessions (иначе FK на hits будет мешать)
        print("📥 Loading sessions (PKL preferred) ...")
        n_sessions = load_sessions(conn)
        print(f"✅ sessions loaded: {n_sessions:,}")

        # На время массовой загрузки hits отключаем FK, потом удаляем сироты
        conn.execute("PRAGMA foreign_keys = OFF;")
        print("⚙️ PRAGMA foreign_keys = OFF (bulk load hits)")

        print("📥 Loading hits from CSV ...")
        n_hits = load_hits_from_csv(conn, chunksize_read=200_000)
        print(f"✅ hits (csv) loaded: {n_hits:,}")

        # Чистим сироты (если они есть)
        deleted = delete_orphan_hits(conn)
        print(f"🧹 Deleted orphan hits: {deleted:,}")

        conn.execute("PRAGMA foreign_keys = ON;")
        print("⚙️ PRAGMA foreign_keys = ON")

        sanity_checks(conn)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Очистить таблицы sessions и hits перед загрузкой (для пересоздания с нуля)",
    )
    args = parser.parse_args()

    main(reset=args.reset)

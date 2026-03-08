"""
src/etl/load_sessions_to_sqlite.py

Этап 3.2a — загрузка ga_sessions в SQLite (ТОЛЬКО из PKL).

Что делает:
- (опционально) очищает таблицу sessions (через --reset)
- читает sessions из data/raw/ga_sessions.pkl
- минимально чистит и приводит типы (как в ноутбуках)
- загружает в SQLite пачками (chunks)
- делает sanity-check по количеству строк

Запуск:
    python -m src.etl.load_sessions_to_sqlite --reset
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
# Чистка / типизация (минимально)
# -----------------------------
def clean_sessions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Минимальная чистка sessions:
    - visit_date -> datetime -> ISO string YYYY-MM-DD
    - visit_time -> string HH:MM:SS (как есть)
    - visit_number -> Int64 (nullable)
    - строки: trim
    """
    df = df.copy()

    # Дата: приводим к datetime, берём только date, сохраняем как строку (SQLite-friendly)
    df["visit_date"] = pd.to_datetime(df["visit_date"], errors="coerce").dt.date.astype("string")

    # Время оставляем строкой
    df["visit_time"] = df["visit_time"].astype("string")

    # visit_number -> Int64 (nullable)
    df["visit_number"] = pd.to_numeric(df["visit_number"], errors="coerce").astype("Int64")

    # Тримим все object/string колонки
    for col in df.columns:
        if pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].astype("string").str.strip()

    return df


# -----------------------------
# SQLite helpers
# -----------------------------
def reset_sessions(conn: sqlite3.Connection) -> None:
    """
    Очищает таблицу sessions.
    Важно: если включены FK и в hits уже есть строки — будет ошибка.
    Поэтому либо грузим sessions ДО hits, либо предварительно чистим hits.
    """
    conn.execute("DELETE FROM sessions;")
    conn.commit()


def load_sessions_from_pkl(conn: sqlite3.Connection, chunksize_sql: int = 50_000) -> int:
    """
    Загружает sessions из PKL в таблицу sessions.
    Возвращает количество загруженных строк.
    """
    raw_dir = get_raw_dir()
    sess_pkl = raw_dir / "ga_sessions.pkl"

    # Явно требуем PKL (как ты просила)
    if not sess_pkl.exists():
        raise FileNotFoundError(f"Sessions PKL not found: {sess_pkl}")

    print(f"📦 Reading sessions PKL: {sess_pkl}")
    sessions = pd.read_pickle(sess_pkl)

    print("🧼 Cleaning sessions ...")
    sessions = clean_sessions(sessions)

    print("🗄️ Writing to SQLite (sessions) ...")
    sessions.to_sql("sessions", conn, if_exists="append", index=False, chunksize=chunksize_sql)

    return len(sessions)


def sanity_checks(conn: sqlite3.Connection) -> None:
    """Мини-проверки после загрузки."""
    sess_cnt = conn.execute("SELECT COUNT(*) FROM sessions;").fetchone()[0]
    date_min, date_max = conn.execute(
        "SELECT MIN(visit_date), MAX(visit_date) FROM sessions;"
    ).fetchone()

    print("\n--- Sanity checks ---")
    print(f"sessions rows: {sess_cnt:,}")
    print(f"visit_date min/max: {date_min} / {date_max}")


def main(reset: bool) -> None:
    with connect() as conn:
        # На всякий случай отключаем FK на время чистки/загрузки sessions
        # (если hits уже загружены, reset sessions может упасть).
        conn.execute("PRAGMA foreign_keys = OFF;")

        if reset:
            print("🧹 Reset: clearing sessions table")
            reset_sessions(conn)

        n = load_sessions_from_pkl(conn)
        print(f"✅ sessions loaded: {n:,}")

        conn.execute("PRAGMA foreign_keys = ON;")
        sanity_checks(conn)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Очистить таблицу sessions перед загрузкой",
    )
    args = parser.parse_args()

    main(reset=args.reset)

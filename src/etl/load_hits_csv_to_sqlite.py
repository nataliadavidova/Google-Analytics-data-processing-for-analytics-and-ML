"""
src/etl/load_hits_csv_to_sqlite.py

Этап 3.x — загрузка ga_hits в SQLite из CSV (чанками).

Что делает:
- (опционально) очищает таблицу hits
- читает hits из data/raw/ga_hits-001.csv чанками
- базово чистит/типизирует как в ноутбуке
- грузит в SQLite пачками (to_sql chunks)

Запуск:
    # обычная загрузка (добавит к тому, что есть)
    python -m src.etl.load_hits_csv_to_sqlite

    # загрузка "с нуля" только hits (очистит hits)
    python -m src.etl.load_hits_csv_to_sqlite --reset-hits
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

import pandas as pd

from src.etl.db_sqlite import connect, get_project_root


def get_raw_dir() -> Path:
    """Папка с исходными файлами."""
    return get_project_root() / "data" / "raw"


def clean_hits(df: pd.DataFrame) -> pd.DataFrame:
    """
    Минимальная чистка hits (как в ноутбуках):
    - drop event_value (в данных он пустой / 100% missing)
    - hit_date -> 'YYYY-MM-DD' (string)
    - hit_time -> Int64 (nullable)
    - hit_number -> Int64 (nullable)
    - строки: trim
    """
    df = df.copy()

    # 1) Удаляем event_value — он бесполезный (в твоих данных 100% пропусков)
    if "event_value" in df.columns:
        df = df.drop(columns=["event_value"])

    # 2) Дата: приводим к datetime, потом к строке 'YYYY-MM-DD'
    df["hit_date"] = pd.to_datetime(df["hit_date"], errors="coerce").dt.date.astype("string")

    # 3) Время: часто float/NaN -> nullable Int64
    df["hit_time"] = pd.to_numeric(df["hit_time"], errors="coerce").astype("Int64")

    # 4) Номер хита: nullable Int64 (на всякий случай)
    df["hit_number"] = pd.to_numeric(df["hit_number"], errors="coerce").astype("Int64")

    # 5) Тримим все строковые колонки
    for col in df.columns:
        if pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].astype("string").str.strip()

    return df


def reset_hits_table(conn: sqlite3.Connection) -> None:
    """Очищает таблицу hits."""
    conn.execute("DELETE FROM hits;")
    conn.commit()


def load_hits_from_csv(conn: sqlite3.Connection, chunksize_read: int = 200_000) -> int:
    """
    Загружает hits из data/raw/ga_hits-001.csv чанками.
    Возвращает количество загруженных строк.
    """
    raw_dir = get_raw_dir()
    hits_csv = raw_dir / "ga_hits-001.csv"

    if not hits_csv.exists():
        raise FileNotFoundError(f"Not found: {hits_csv}")

    total = 0

    # ВАЖНО: грузим только те колонки, которые есть в таблице hits (без hit_id)
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

    # На время загрузки отключаем FK, чтобы не падало (если в hits есть session_id,
    # которых нет в sessions, SQLite иначе выдаст FOREIGN KEY constraint failed)
    conn.execute("PRAGMA foreign_keys = OFF;")
    print("⚙️ PRAGMA foreign_keys = OFF (bulk load hits)")

    for i, chunk in enumerate(pd.read_csv(hits_csv, chunksize=chunksize_read), start=1):
        # Чистим + приводим типы
        chunk = clean_hits(chunk)

        # Выбираем строго нужные столбцы в правильном порядке
        chunk = chunk[cols_for_db]

        # Пишем в БД
        chunk.to_sql(
            "hits",
            conn,
            if_exists="append",
            index=False,
            chunksize=50_000,
        )

        total += len(chunk)
        print(f"✅ Loaded chunk #{i}: +{len(chunk):,} rows (total {total:,})")

    conn.execute("PRAGMA foreign_keys = ON;")
    print("⚙️ PRAGMA foreign_keys = ON")

    return total


def sanity_checks(conn: sqlite3.Connection) -> None:
    """Мини-проверки после загрузки."""
    hits_cnt = conn.execute("SELECT COUNT(*) FROM hits;").fetchone()[0]
    distinct_sessions = conn.execute("SELECT COUNT(DISTINCT session_id) FROM hits;").fetchone()[0]

    print("\n--- Sanity checks ---")
    print("hits rows:", hits_cnt)
    print("distinct session_id in hits:", distinct_sessions)


def main(reset_hits: bool) -> None:
    with connect() as conn:
        if reset_hits:
            print("🧹 Reset table: hits")
            reset_hits_table(conn)

        print("📥 Loading hits from CSV ...")
        n_hits = load_hits_from_csv(conn)
        print(f"🎉 Done! hits loaded: {n_hits:,}")

        sanity_checks(conn)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--reset-hits",
        action="store_true",
        help="Очистить таблицу hits перед загрузкой",
    )
    args = parser.parse_args()

    main(reset_hits=args.reset_hits)

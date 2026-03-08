"""
src/etl/db_sqlite.py

Утилиты для работы с SQLite в проекте:
- создание/инициализация БД по схеме из sql/schema.sql
- получение подключения

Структура проекта (важно):
- schema: sql/schema.sql
- db file: data/ga.sqlite
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable


def get_project_root() -> Path:
    """
    Возвращает корень проекта.
    Этот файл лежит в src/etl/db_sqlite.py -> корень на 3 уровня выше: src -> project_root.
    """
    return Path(__file__).resolve().parents[2]


def get_db_path() -> Path:
    """Путь до файла SQLite БД по стандарту проекта."""
    return get_project_root() / "data" / "ga.sqlite"


def get_schema_path() -> Path:
    """Путь до schema.sql по стандарту проекта."""
    return get_project_root() / "sql" / "schema.sql"


def connect(db_path: Path | None = None) -> sqlite3.Connection:
    """
    Создаёт подключение к SQLite.
    Если db_path не передан — используем data/ga.sqlite.
    """
    if db_path is None:
        db_path = get_db_path()

    # Гарантируем, что директория существует
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)

    # Рекомендуемые настройки для SQLite
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")

    return conn


def init_db(db_path: Path | None = None, schema_path: Path | None = None) -> None:
    """
    Инициализирует БД: создаёт файл (если нет) и применяет схему.
    Схема берётся из sql/schema.sql (если schema_path не указан).
    """
    if db_path is None:
        db_path = get_db_path()
    if schema_path is None:
        schema_path = get_schema_path()

    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")

    schema_sql = schema_path.read_text(encoding="utf-8")

    with connect(db_path) as conn:
        conn.executescript(schema_sql)

        # Проверка: какие таблицы есть в БД
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
        ).fetchall()

    print(f"✅ DB initialized: {db_path}")
    print("Tables:", [t[0] for t in tables])


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    """Проверяет существование таблицы в БД."""
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1;",
        (table_name,),
    ).fetchone()
    return row is not None


def fetch_all_tables(conn: sqlite3.Connection) -> list[str]:
    """Возвращает список таблиц в БД."""
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
    ).fetchall()
    return [r[0] for r in rows]


if __name__ == "__main__":
    # Позволяет запускать инициализацию так:
    # python -m src.etl.db_sqlite
    init_db()

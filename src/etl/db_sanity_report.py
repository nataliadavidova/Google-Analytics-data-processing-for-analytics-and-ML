"""
src/etl/db_sanity_report.py

Этап 3.x — sanity checks для SQLite-базы + сохранение отчёта в файл.

Что делает:
- читает метрики качества из SQLite (sessions/hits/session_features при наличии)
- считает:
  * counts (sessions, hits)
  * диапазоны дат
  * orphan hits (hits без sessions)
  * sessions_without_hits
  * дубли по (session_id, hit_number) и max_rows_per_pair
  * top event_action
  * распределения таргетов в session_features (если таблица есть)
- печатает кратко в консоль
- сохраняет markdown отчёт в reports/db_sanity_report.md (по умолчанию)

Запуск:
    python -m src.etl.db_sanity_report
    python -m src.etl.db_sanity_report --out reports/sanity_2026-02-05.md
"""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

import sqlite3

from src.etl.db_sqlite import connect, get_project_root


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    cur = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1;",
        (table_name,),
    )
    return cur.fetchone() is not None


def fetch_one(conn: sqlite3.Connection, sql: str, params: Tuple[Any, ...] = ()) -> Any:
    return conn.execute(sql, params).fetchone()[0]


def fetch_all(conn: sqlite3.Connection, sql: str, params: Tuple[Any, ...] = ()) -> List[Tuple[Any, ...]]:
    return conn.execute(sql, params).fetchall()


def build_metrics(conn: sqlite3.Connection) -> Dict[str, Any]:
    metrics: Dict[str, Any] = {}

    # --- базовые таблицы ---
    metrics["has_sessions"] = table_exists(conn, "sessions")
    metrics["has_hits"] = table_exists(conn, "hits")
    metrics["has_session_features"] = table_exists(conn, "session_features")

    if metrics["has_sessions"]:
        metrics["sessions_cnt"] = fetch_one(conn, "SELECT COUNT(*) FROM sessions;")
        metrics["sessions_date_min"] = fetch_one(conn, "SELECT MIN(visit_date) FROM sessions;")
        metrics["sessions_date_max"] = fetch_one(conn, "SELECT MAX(visit_date) FROM sessions;")
    else:
        metrics["sessions_cnt"] = None

    if metrics["has_hits"]:
        metrics["hits_cnt"] = fetch_one(conn, "SELECT COUNT(*) FROM hits;")
        metrics["hits_date_min"] = fetch_one(conn, "SELECT MIN(hit_date) FROM hits;")
        metrics["hits_date_max"] = fetch_one(conn, "SELECT MAX(hit_date) FROM hits;")
        metrics["hits_distinct_sessions"] = fetch_one(conn, "SELECT COUNT(DISTINCT session_id) FROM hits;")

        # orphan hits
        metrics["orphan_hits"] = fetch_one(
            conn,
            """
            SELECT COUNT(*)
            FROM hits h
            LEFT JOIN sessions s ON s.session_id = h.session_id
            WHERE s.session_id IS NULL;
            """,
        )
        metrics["orphan_sessions"] = fetch_one(
            conn,
            """
            SELECT COUNT(DISTINCT h.session_id)
            FROM hits h
            LEFT JOIN sessions s ON s.session_id = h.session_id
            WHERE s.session_id IS NULL;
            """,
        )

        if metrics["hits_cnt"]:
            metrics["orphan_hits_pct"] = round(100.0 * metrics["orphan_hits"] / metrics["hits_cnt"], 4)
        else:
            metrics["orphan_hits_pct"] = None

        # sessions without hits
        if metrics["has_sessions"]:
            metrics["sessions_without_hits"] = fetch_one(
                conn,
                """
                SELECT COUNT(*)
                FROM sessions s
                LEFT JOIN hits h ON h.session_id = s.session_id
                WHERE h.session_id IS NULL;
                """,
            )
        else:
            metrics["sessions_without_hits"] = None

        # duplicates by (session_id, hit_number)
        metrics["dup_pairs"] = fetch_one(
            conn,
            """
            SELECT COUNT(*) AS dup_pairs
            FROM (
              SELECT session_id, hit_number
              FROM hits
              GROUP BY session_id, hit_number
              HAVING COUNT(*) > 1
            );
            """,
        )

        metrics["unique_pairs"] = fetch_one(
            conn,
            """
            WITH pairs AS (
              SELECT session_id, hit_number
              FROM hits
              GROUP BY session_id, hit_number
            )
            SELECT COUNT(*) FROM pairs;
            """,
        )

        if metrics["unique_pairs"]:
            metrics["dup_pairs_pct"] = round(100.0 * metrics["dup_pairs"] / metrics["unique_pairs"], 4)
        else:
            metrics["dup_pairs_pct"] = None

        metrics["max_rows_per_pair"] = fetch_one(
            conn,
            """
            SELECT MAX(cnt) AS max_rows_per_pair
            FROM (
              SELECT session_id, hit_number, COUNT(*) AS cnt
              FROM hits
              GROUP BY session_id, hit_number
            );
            """,
        )

        # top event_action
        metrics["top_event_action"] = fetch_all(
            conn,
            """
            SELECT event_action, COUNT(*) AS cnt
            FROM hits
            GROUP BY event_action
            ORDER BY cnt DESC
            LIMIT 20;
            """,
        )

        # top orphan session_ids (по количеству строк)
        metrics["top_orphan_session_ids"] = fetch_all(
            conn,
            """
            SELECT h.session_id, COUNT(*) AS cnt
            FROM hits h
            LEFT JOIN sessions s ON s.session_id = h.session_id
            WHERE s.session_id IS NULL
            GROUP BY h.session_id
            ORDER BY cnt DESC
            LIMIT 20;
            """,
        )
    else:
        metrics["hits_cnt"] = None

    # --- session_features targets ---
    if metrics["has_session_features"]:
        metrics["session_features_cnt"] = fetch_one(conn, "SELECT COUNT(*) FROM session_features;")

        # target_showed_number_ads distribution
        metrics["target_showed_number_ads_dist"] = fetch_all(
            conn,
            """
            SELECT target_showed_number_ads, COUNT(*) AS cnt
            FROM session_features
            GROUP BY target_showed_number_ads
            ORDER BY target_showed_number_ads;
            """,
        )

        # target_funnel_go_to_then_showed distribution
        metrics["target_funnel_go_to_then_showed_dist"] = fetch_all(
            conn,
            """
            SELECT target_funnel_go_to_then_showed, COUNT(*) AS cnt
            FROM session_features
            GROUP BY target_funnel_go_to_then_showed
            ORDER BY target_funnel_go_to_then_showed;
            """,
        )
    else:
        metrics["session_features_cnt"] = None

    return metrics


def render_markdown(metrics: Dict[str, Any]) -> str:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def fmt(v: Any) -> str:
        return "—" if v is None else str(v)

    lines: List[str] = []
    lines.append(f"# DB sanity report\n")
    lines.append(f"- Generated at: **{now}**\n")

    lines.append("## Tables\n")
    lines.append(f"- sessions: **{fmt(metrics.get('has_sessions'))}**")
    lines.append(f"- hits: **{fmt(metrics.get('has_hits'))}**")
    lines.append(f"- session_features: **{fmt(metrics.get('has_session_features'))}**\n")

    lines.append("## Row counts\n")
    lines.append(f"- sessions_cnt: **{fmt(metrics.get('sessions_cnt'))}**")
    lines.append(f"- hits_cnt: **{fmt(metrics.get('hits_cnt'))}**")
    lines.append(f"- session_features_cnt: **{fmt(metrics.get('session_features_cnt'))}**\n")

    lines.append("## Date ranges\n")
    lines.append(f"- sessions visit_date: **{fmt(metrics.get('sessions_date_min'))} → {fmt(metrics.get('sessions_date_max'))}**")
    lines.append(f"- hits hit_date: **{fmt(metrics.get('hits_date_min'))} → {fmt(metrics.get('hits_date_max'))}**\n")

    if metrics.get("has_hits"):
        lines.append("## Integrity checks\n")
        lines.append(f"- orphan_hits (hits без sessions): **{fmt(metrics.get('orphan_hits'))}**")
        lines.append(f"- orphan_sessions (уникальных session_id в orphan hits): **{fmt(metrics.get('orphan_sessions'))}**")
        lines.append(f"- orphan_hits_pct: **{fmt(metrics.get('orphan_hits_pct'))}%**")
        lines.append(f"- sessions_without_hits: **{fmt(metrics.get('sessions_without_hits'))}**\n")

        lines.append("## Duplicates in hits\n")
        lines.append("- Проверка: дубль = `COUNT(*) > 1` для пары `(session_id, hit_number)`.\n")
        lines.append(f"- unique_pairs: **{fmt(metrics.get('unique_pairs'))}**")
        lines.append(f"- dup_pairs: **{fmt(metrics.get('dup_pairs'))}**")
        lines.append(f"- dup_pairs_pct: **{fmt(metrics.get('dup_pairs_pct'))}%**")
        lines.append(f"- max_rows_per_pair: **{fmt(metrics.get('max_rows_per_pair'))}**\n")

        lines.append("## Top event_action (Top-20)\n")
        lines.append("| event_action | cnt |")
        lines.append("|---|---:|")
        for action, cnt in metrics.get("top_event_action", []):
            lines.append(f"| {action} | {cnt} |")
        lines.append("")

        lines.append("## Top orphan session_id (Top-20)\n")
        lines.append("| session_id | orphan_hits_cnt |")
        lines.append("|---|---:|")
        for sid, cnt in metrics.get("top_orphan_session_ids", []):
            lines.append(f"| {sid} | {cnt} |")
        lines.append("")

    if metrics.get("has_session_features"):
        lines.append("## Targets in session_features\n")

        lines.append("### target_showed_number_ads\n")
        lines.append("| value | cnt |")
        lines.append("|---:|---:|")
        for v, cnt in metrics.get("target_showed_number_ads_dist", []):
            lines.append(f"| {v} | {cnt} |")
        lines.append("")

        lines.append("### target_funnel_go_to_then_showed\n")
        lines.append("| value | cnt |")
        lines.append("|---:|---:|")
        for v, cnt in metrics.get("target_funnel_go_to_then_showed_dist", []):
            lines.append(f"| {v} | {cnt} |")
        lines.append("")

    lines.append("## Notes\n")
    lines.append("- `orphan_hits` > 0 означает: в hits есть события с session_id, которых нет в sessions (в исходных данных это возможно).")
    lines.append("- `dup_pairs` > 0 означает: в рамках одной session_id один и тот же hit_number встречается больше одного раза (в исходных данных это тоже бывает).")
    lines.append("- Для аналитики/ML это обычно нормально, важно лишь **зафиксировать правила** обработки (например, агрегировать на уровень сессии).")

    return "\n".join(lines) + "\n"


def main(out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with connect() as conn:
        metrics = build_metrics(conn)

    md = render_markdown(metrics)
    out_path.write_text(md, encoding="utf-8")

    # Коротко в консоль
    print("✅ Report saved:", out_path)
    print("sessions:", metrics.get("sessions_cnt"), "| hits:", metrics.get("hits_cnt"))
    if metrics.get("has_hits"):
        print(
            "orphan_hits:",
            metrics.get("orphan_hits"),
            f"({metrics.get('orphan_hits_pct')}%)",
            "| dup_pairs:",
            metrics.get("dup_pairs"),
            f"({metrics.get('dup_pairs_pct')}%)",
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--out",
        type=str,
        default=str(get_project_root() / "reports" / "db_sanity_report.md"),
        help="Путь, куда сохранить markdown-отчёт",
    )
    args = parser.parse_args()

    main(out_path=Path(args.out))

# src/etl/build_session_features.py

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from src.etl.db_sqlite import connect


def create_indexes_for_speed(conn: sqlite3.Connection) -> None:
    """Индексы на исходных таблицах, чтобы агрегации были быстрее."""
    conn.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_hits_session_id   ON hits(session_id);
        CREATE INDEX IF NOT EXISTS idx_hits_event_action ON hits(event_action);
        CREATE INDEX IF NOT EXISTS idx_hits_hit_number   ON hits(hit_number);

        CREATE INDEX IF NOT EXISTS idx_sessions_visit_date ON sessions(visit_date);
        CREATE INDEX IF NOT EXISTS idx_sessions_utm_medium ON sessions(utm_medium);
        """
    )
    conn.commit()


def drop_table(conn: sqlite3.Connection, table_name: str) -> None:
    """Удаляем таблицу (если есть), чтобы пересоздать с нуля."""
    conn.execute(f"DROP TABLE IF EXISTS {table_name};")
    conn.commit()


def build_session_features(conn: sqlite3.Connection) -> None:
    """Создаёт и наполняет витрину session_features."""
    sql = """
    CREATE TABLE IF NOT EXISTS session_features (
        session_id TEXT PRIMARY KEY,

        visit_date TEXT,
        utm_source TEXT,
        utm_medium TEXT,
        utm_campaign TEXT,
        device_category TEXT,
        device_os TEXT,
        geo_country TEXT,
        geo_city TEXT,

        hits_cnt INTEGER,
        uniq_actions_cnt INTEGER,
        has_go_to_car_card INTEGER,
        has_showed_number_ads INTEGER,
        has_quiz_show INTEGER,

        target_showed_number_ads INTEGER,
        target_funnel_go_to_then_showed INTEGER
    );

    INSERT INTO session_features (
        session_id,
        visit_date, utm_source, utm_medium, utm_campaign,
        device_category, device_os, geo_country, geo_city,
        hits_cnt, uniq_actions_cnt,
        has_go_to_car_card, has_showed_number_ads, has_quiz_show,
        target_showed_number_ads, target_funnel_go_to_then_showed
    )
    WITH hits_agg AS (
        SELECT
            session_id,
            COUNT(*) AS hits_cnt,
            COUNT(DISTINCT event_action) AS uniq_actions_cnt,
            MAX(CASE WHEN event_action = 'go_to_car_card' THEN 1 ELSE 0 END) AS has_go_to_car_card,
            MAX(CASE WHEN event_action = 'showed_number_ads' THEN 1 ELSE 0 END) AS has_showed_number_ads,
            MAX(CASE WHEN event_action = 'quiz_show' THEN 1 ELSE 0 END) AS has_quiz_show,

            CASE
                WHEN
                    MIN(CASE WHEN event_action = 'go_to_car_card' THEN hit_number END) IS NOT NULL
                    AND
                    MIN(CASE WHEN event_action = 'showed_number_ads' THEN hit_number END) IS NOT NULL
                    AND
                    MIN(CASE WHEN event_action = 'showed_number_ads' THEN hit_number END)
                    >
                    MIN(CASE WHEN event_action = 'go_to_car_card' THEN hit_number END)
                THEN 1 ELSE 0
            END AS target_funnel_go_to_then_showed
        FROM hits
        GROUP BY session_id
    )
    SELECT
        s.session_id,
        s.visit_date, s.utm_source, s.utm_medium, s.utm_campaign,
        s.device_category, s.device_os, s.geo_country, s.geo_city,

        COALESCE(h.hits_cnt, 0) AS hits_cnt,
        COALESCE(h.uniq_actions_cnt, 0) AS uniq_actions_cnt,
        COALESCE(h.has_go_to_car_card, 0) AS has_go_to_car_card,
        COALESCE(h.has_showed_number_ads, 0) AS has_showed_number_ads,
        COALESCE(h.has_quiz_show, 0) AS has_quiz_show,

        COALESCE(h.has_showed_number_ads, 0) AS target_showed_number_ads,
        COALESCE(h.target_funnel_go_to_then_showed, 0) AS target_funnel_go_to_then_showed
    FROM sessions s
    LEFT JOIN hits_agg h ON h.session_id = s.session_id
    ;
    """
    conn.executescript(sql)
    conn.commit()


def create_feature_indexes(conn: sqlite3.Connection) -> None:
    """Индексы на витрине."""
    conn.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_sf_visit_date ON session_features(visit_date);
        CREATE INDEX IF NOT EXISTS idx_sf_utm_medium ON session_features(utm_medium);
        CREATE INDEX IF NOT EXISTS idx_sf_target_ads ON session_features(target_showed_number_ads);
        CREATE INDEX IF NOT EXISTS idx_sf_target_funnel ON session_features(target_funnel_go_to_then_showed);
        """
    )
    conn.commit()


def sanity_checks(conn: sqlite3.Connection) -> None:
    """Быстрые проверки качества витрины."""
    sessions_cnt = conn.execute("SELECT COUNT(*) FROM sessions;").fetchone()[0]
    hits_cnt = conn.execute("SELECT COUNT(*) FROM hits;").fetchone()[0]
    sf_cnt = conn.execute("SELECT COUNT(*) FROM session_features;").fetchone()[0]

    max_visit = conn.execute("SELECT MAX(visit_date) FROM sessions;").fetchone()[0]
    max_hit = conn.execute("SELECT MAX(hit_date) FROM hits;").fetchone()[0]
    max_sf = conn.execute("SELECT MAX(visit_date) FROM session_features;").fetchone()[0]

    cr_showed = conn.execute("SELECT ROUND(AVG(target_showed_number_ads), 6) FROM session_features;").fetchone()[0]
    cr_funnel = conn.execute("SELECT ROUND(AVG(target_funnel_go_to_then_showed), 6) FROM session_features;").fetchone()[0]

    sessions_without_hits = conn.execute("SELECT COUNT(*) FROM session_features WHERE hits_cnt = 0;").fetchone()[0]

    print("\n--- Sanity checks ---")
    print(f"sessions rows: {sessions_cnt:,}")
    print(f"hits rows: {hits_cnt:,}")
    print(f"session_features rows: {sf_cnt:,}  (должно == sessions rows)")
    print(f"MAX dates: sessions={max_visit} | hits={max_hit} | session_features={max_sf}")
    print(f"CR target_showed_number_ads: {cr_showed}")
    print(f"CR target_funnel_go_to_then_showed: {cr_funnel}")
    print(f"sessions without hits: {sessions_without_hits:,}")


def main(db: str, recreate: bool) -> None:
    # connect умеет принимать db_path, если мы это передадим
    with connect(db_path=Path(db)) as conn:
        conn.execute("PRAGMA foreign_keys = ON;")

        create_indexes_for_speed(conn)

        if recreate:
            print("🧹 Recreate mode: dropping session_features")
            drop_table(conn, "session_features")

        print("🧱 Building session_features ...")
        build_session_features(conn)

        print("⚙️ Creating indexes on session_features ...")
        create_feature_indexes(conn)

        print("✅ session_features created.")
        sanity_checks(conn)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="data/ga.sqlite", help="Path to SQLite database")
    parser.add_argument("--recreate", action="store_true", help="Drop + rebuild session_features")
    args = parser.parse_args()

    main(db=args.db, recreate=args.recreate)

"""
dags/ga_etl_dag.py

DAG: GA incremental ETL (JSON -> SQLite) + rebuild mart + checks + markdown report

Что делает по расписанию:
1) ingest_json: подхватывает новые *.json из data/incoming_json и дозагружает в data/ga.sqlite
2) build_session_features: пересоздаёт витрину session_features
3) dq_checks: минимальные проверки качества данных (упадёт, если критичная ошибка)
4) build_report: формирует markdown-отчёт для человека и кладёт в reports/

Почему так:
- методичку закрываем: регулярная загрузка новых JSON + обновление витрины + проверка + отчёт
- всё воспроизводимо: один DAG = один сценарий "end-to-end"
"""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


# ---------
# Константы путей внутри контейнера
# ---------
PROJECT_DIR = "/opt/airflow/project"
DB_PATH = f"{PROJECT_DIR}/data/ga.sqlite"
INCOMING_DIR = f"{PROJECT_DIR}/data/incoming_json"
REPORTS_DIR = f"{PROJECT_DIR}/reports"


# ---------
# Вспомогательная функция: простые проверки качества (поднимаем исключение -> DAG красный)
# ---------
def dq_checks() -> None:
    """
    Проверяем минимально важное:
    - таблицы существуют
    - session_features по числу строк == sessions
    - MAX даты согласованы (session_features не "отстаёт")
    """
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"DB not found: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    try:
        # 1) таблицы существуют
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table';"
        ).fetchall()}
        required = {"sessions", "hits", "session_features", "ingestion_log"}
        missing = required - tables
        if missing:
            raise RuntimeError(f"Missing tables in DB: {sorted(missing)}")

        # 2) sessions == session_features
        sessions_cnt = conn.execute("SELECT COUNT(*) FROM sessions;").fetchone()[0]
        sf_cnt = conn.execute("SELECT COUNT(*) FROM session_features;").fetchone()[0]
        if sessions_cnt != sf_cnt:
            raise RuntimeError(f"Rowcount mismatch: sessions={sessions_cnt} vs session_features={sf_cnt}")

        # 3) MAX даты не должны отставать
        max_sess = conn.execute("SELECT MAX(visit_date) FROM sessions;").fetchone()[0]
        max_sf = conn.execute("SELECT MAX(visit_date) FROM session_features;").fetchone()[0]
        if max_sess and max_sf and (max_sf < max_sess):
            raise RuntimeError(f"session_features is behind sessions: max_sf={max_sf} < max_sessions={max_sess}")

    finally:
        conn.close()


# ---------
# Вспомогательная функция: собрать markdown-отчёт
# ---------
def build_report(ds_nodash: str, **context) -> None:
    """
    Формирует понятный человеку отчёт:
    - что загрузили (по ingestion_log)
    - сколько строк всего в sessions/hits
    - до каких дат дошли
    - ключевые sanity метрики по витрине
    """
    os.makedirs(REPORTS_DIR, exist_ok=True)  # reports уже есть, это просто safety

    report_path = os.path.join(REPORTS_DIR, f"airflow_etl_report_{ds_nodash}.md")

    conn = sqlite3.connect(DB_PATH)
    try:
        # Общие цифры
        sessions_min, sessions_max, sessions_cnt = conn.execute(
            "SELECT MIN(visit_date), MAX(visit_date), COUNT(*) FROM sessions;"
        ).fetchone()
        hits_min, hits_max, hits_cnt = conn.execute(
            "SELECT MIN(hit_date), MAX(hit_date), COUNT(*) FROM hits;"
        ).fetchone()
        sf_min, sf_max, sf_cnt = conn.execute(
            "SELECT MIN(visit_date), MAX(visit_date), COUNT(*) FROM session_features;"
        ).fetchone()

        # Конверсии по таргетам
        cr_ads = conn.execute(
            "SELECT ROUND(AVG(target_showed_number_ads), 6) FROM session_features;"
        ).fetchone()[0]
        cr_funnel = conn.execute(
            "SELECT ROUND(AVG(target_funnel_go_to_then_showed), 6) FROM session_features;"
        ).fetchone()[0]

        # Сессии без хитов
        sessions_without_hits = conn.execute(
            "SELECT COUNT(*) FROM session_features WHERE hits_cnt = 0;"
        ).fetchone()[0]

        # Что было загружено "последним заходом"
        # Берём последние 30 строк лога: видно какие файлы обработались и сколько строк вставили
        last_logs = conn.execute(
            """
            SELECT loaded_at, file_name, status, table_name, rows_total, rows_inserted
            FROM ingestion_log
            ORDER BY id DESC
            LIMIT 30;
            """
        ).fetchall()

        # Топ источников/кампаний по конверсии (очень “бизнесово”)
        top_mediums = conn.execute(
            """
            SELECT
              COALESCE(utm_medium, '(null)') AS utm_medium,
              COUNT(*) AS sessions,
              SUM(target_showed_number_ads) AS conversions,
              ROUND(1.0 * SUM(target_showed_number_ads) / COUNT(*), 4) AS cr
            FROM session_features
            GROUP BY utm_medium
            HAVING COUNT(*) >= 1000
            ORDER BY cr DESC
            LIMIT 10;
            """
        ).fetchall()

    finally:
        conn.close()

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    md = []
    md.append(f"# ETL отчёт (Airflow) — {ds_nodash}\n")
    md.append(f"_Сформировано: {now}_\n")

    md.append("## Что сделал пайплайн\n")
    md.append("- Подхватил новые JSON из `data/incoming_json/` и дозагрузил их в SQLite `data/ga.sqlite`.\n")
    md.append("- Пересоздал витрину `session_features` (1 строка = 1 сессия).\n")
    md.append("- Выполнил sanity-checks (равенство размеров, актуальность дат и базовые метрики).\n")

    md.append("## Итог по данным (после обновления)\n")
    md.append("| Таблица | Мин. дата | Макс. дата | Строк |\n")
    md.append("|---|---:|---:|---:|\n")
    md.append(f"| sessions | {sessions_min} | {sessions_max} | {sessions_cnt:,} |\n")
    md.append(f"| hits | {hits_min} | {hits_max} | {hits_cnt:,} |\n")
    md.append(f"| session_features | {sf_min} | {sf_max} | {sf_cnt:,} |\n")

    md.append("\n## Ключевые “человеческие” метрики витрины\n")
    md.append(f"- **CR (показ номера / целевое действие)**: `{cr_ads}`  \n")
    md.append(f"- **CR (воронка go_to_car_card → showed_number_ads)**: `{cr_funnel}`  \n")
    md.append(f"- **Сессий без хитов**: `{sessions_without_hits:,}` (это нормально для части данных, но если резко растёт — тревожный сигнал)\n")

    md.append("\n## ТОП-10 utm_medium по конверсии (showed_number_ads)\n")
    md.append("| utm_medium | sessions | conversions | CR |\n")
    md.append("|---|---:|---:|---:|\n")
    for utm_medium, sess, conv, cr in top_mediums:
        md.append(f"| {utm_medium} | {sess:,} | {conv:,} | {cr} |\n")

    md.append("\n## Последние записи ingestion_log (что реально загрузилось)\n")
    md.append("| loaded_at | file_name | status | table_name | rows_total | rows_inserted |\n")
    md.append("|---|---|---|---|---:|---:|\n")
    for loaded_at, file_name, status, table_name, rows_total, rows_inserted in last_logs:
        md.append(f"| {loaded_at} | {file_name} | {status} | {table_name} | {rows_total} | {rows_inserted} |\n")

    md.append("\n---\n")
    md.append("### Как это читать менеджеру\n")
    md.append("- **Макс. дата** показывает, “до какого дня” данные загружены.\n")
    md.append("- **rows_inserted** в ingestion_log показывает, сколько строк реально добавилось из файла.\n")
    md.append("- Если `session_features` по датам и размерам совпадает с `sessions` — витрина обновилась корректно.\n")

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("".join(md))

    print(f"✅ Report written: {report_path}")


# ---------
# DAG
# ---------
default_args = {"owner": "airflow"}

with DAG(
    dag_id="ga_etl_dag",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="0 * * * *",  # раз в час (можем поменять на daily)
    catchup=False,
    max_active_runs=1,
    tags=["ga", "etl", "sqlite"],
) as dag:

    # 1) Инкрементальная загрузка JSON -> SQLite
    ingest_json = BashOperator(
        task_id="ingest_json",
        bash_command=(
            "cd /opt/airflow/project && "
            "python -m src.etl.ingest_json "
            "--db data/ga.sqlite "
            "--input-dir data/incoming_json"
        ),
    )

    # 2) Обновление витрины (полный пересчёт)
    build_mart = BashOperator(
        task_id="build_session_features",
        bash_command=(
            "cd /opt/airflow/project && "
            "python -m src.etl.build_session_features "
            "--db data/ga.sqlite "
            "--recreate"
        ),
    )

    # 3) Проверки качества (если упадёт — это сигнал, что витрина/данные неконсистентны)
    checks = PythonOperator(
        task_id="dq_checks",
        python_callable=dq_checks,
    )

    # 4) Отчёт (markdown)
    report = PythonOperator(
        task_id="build_report",
        python_callable=build_report,
        op_kwargs={"ds_nodash": "{{ ds_nodash }}"},
    )

    ingest_json >> build_mart >> checks >> report

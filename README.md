# Final Project (GA sessions/hits) — DA + ML + DE

## Structure
- notebooks/ — подготовка, EDA, метрики, ML
- src/etl — загрузка данных и инкремент из json в SQLite
- src/ml — фичи, обучение, инференс
- dags/ — Airflow DAG-и
- sql/ — схема SQLite

## Setup
1) Create venv, install requirements
2) Copy .env.example -> .env and adjust paths if needed

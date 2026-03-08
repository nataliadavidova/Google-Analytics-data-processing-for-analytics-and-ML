from __future__ import annotations

"""
final_project_ga: model training (target_any_goal) with hashing + XGBoost

Что делает этот скрипт
- Читает данные из SQLite: data/ga.sqlite
- Строит таргет target_any_goal по методичке:
  target_any_goal = 1, если в hits есть event_action из списка GOAL_ACTIONS
- Берёт только фичи визита: utm_*, device_*, geo_*
- Делает time split (80/20 по visit_date)
- Обучает модель: HashingVectorizer + XGBoost
- Сохраняет артефакты в data/models:
  - model_xgb_hash.joblib
  - vectorizer_hash.joblib
  - artifacts.json

Запуск:
  source .venv/bin/activate
  python -m src.ml.train
"""

import json
import sqlite3
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

import joblib
import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import HashingVectorizer
from sklearn.metrics import average_precision_score, roc_auc_score
from xgboost import XGBClassifier

# -------------------------
# Paths
# -------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]  # .../final_project_ga
DB_PATH = PROJECT_ROOT / "data" / "ga.sqlite"
ART_DIR = PROJECT_ROOT / "data" / "models"
ART_DIR.mkdir(parents=True, exist_ok=True)

# -------------------------
# Target (по методичке)
# -------------------------
GOAL_ACTIONS = [
    "sub_car_claim_click",
    "sub_car_claim_submit_click",
    "sub_open_dialog_click",
    "sub_custom_question_submit_click",
    "sub_call_number_click",
    "sub_callback_submit_click",
    "sub_submit_success",
    "sub_car_request_submit_click",
]

# -------------------------
# Features (строго по методичке)
# -------------------------
FEATURE_COLS = [
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "device_category",
    "device_os",
    "geo_country",
    "geo_city",
]


@dataclass
class ArtifactsMeta:
    model_type: str
    n_features: int
    threshold: float
    feature_cols: List[str]
    goal_actions: List[str]
    split: Dict[str, Any]
    metrics: Dict[str, Any]


def _row_to_tokens(row: Dict[str, Any], feature_cols: List[str]) -> str:
    """
    dict -> "utm_source=google device_os=ios geo_city=moscow ..."
    """
    parts: List[str] = []
    for c in feature_cols:
        v = row.get(c, None)
        if v is None:
            continue
        s = str(v).strip()
        if not s or s.lower() in {"nan", "none", "<na>"}:
            continue
        s = s.replace(" ", "_")
        parts.append(f"{c}={s}")
    return " ".join(parts)


def load_dataset_from_sqlite(db_path: Path) -> pd.DataFrame:
    """
    Возвращает df на уровне sessions:
      session_id, visit_date, utm_*, device_*, geo_*, target_any_goal
    """
    con = sqlite3.connect(db_path)

    sessions = pd.read_sql_query(
        f"""
        SELECT
            session_id,
            visit_date,
            {", ".join(FEATURE_COLS)}
        FROM sessions
        """,
        con,
    )

    # таргет: любая цель из GOAL_ACTIONS
    placeholders = ",".join(["?"] * len(GOAL_ACTIONS))
    goals = pd.read_sql_query(
        f"""
        SELECT
            session_id,
            1 AS target_any_goal
        FROM hits
        WHERE event_action IN ({placeholders})
        GROUP BY session_id
        """,
        con,
        params=GOAL_ACTIONS,
    )

    con.close()

    df = sessions.merge(goals, on="session_id", how="left")
    df["target_any_goal"] = df["target_any_goal"].fillna(0).astype(int)

    df["visit_date"] = pd.to_datetime(df["visit_date"], errors="coerce")
    df = df.dropna(subset=["visit_date"]).sort_values("visit_date").reset_index(drop=True)
    return df


def time_split(df: pd.DataFrame, train_frac: float = 0.8) -> Tuple[pd.DataFrame, pd.DataFrame]:
    split_idx = int(len(df) * train_frac)
    return df.iloc[:split_idx].copy(), df.iloc[split_idx:].copy()


def train() -> None:
    print("=== Train hashing + XGBoost for target_any_goal ===")
    print("DB:", DB_PATH)

    df = load_dataset_from_sqlite(DB_PATH)
    train_df, valid_df = time_split(df, 0.8)

    y_train = train_df["target_any_goal"].to_numpy()
    y_valid = valid_df["target_any_goal"].to_numpy()

    print("Train shape:", train_df.shape, "| Valid shape:", valid_df.shape)
    print("Pos rate train:", float(y_train.mean()), "| valid:", float(y_valid.mean()))

    # Hashing
    n_features = 2**18
    vectorizer = HashingVectorizer(
        n_features=n_features,
        alternate_sign=False,
        norm=None,
        binary=False,
        token_pattern=r"[^ ]+",
    )

    Xtr_text = [_row_to_tokens(r, FEATURE_COLS) for r in train_df[FEATURE_COLS].to_dict(orient="records")]
    Xva_text = [_row_to_tokens(r, FEATURE_COLS) for r in valid_df[FEATURE_COLS].to_dict(orient="records")]

    X_train = vectorizer.transform(Xtr_text)
    X_valid = vectorizer.transform(Xva_text)

    # Disbalance handling
    pos = float(y_train.sum())
    neg = float((y_train == 0).sum())
    scale_pos_weight = (neg / pos) if pos > 0 else 1.0

    model = XGBClassifier(
        n_estimators=600,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        reg_lambda=1.0,
        objective="binary:logistic",
        eval_metric="auc",
        tree_method="hist",
        n_jobs=8,
        random_state=42,
        scale_pos_weight=scale_pos_weight,
    )

    model.fit(X_train, y_train)

    proba = model.predict_proba(X_valid)[:, 1]
    roc = roc_auc_score(y_valid, proba)
    pr = average_precision_score(y_valid, proba)

    print(f"Valid ROC-AUC: {roc:.5f}")
    print(f"Valid PR-AUC : {pr:.5f}")

    # Порог для сервиса (по умолчанию, простой и прозрачный)
    threshold = 0.5

    meta = ArtifactsMeta(
        model_type="hashing + xgboost",
        n_features=n_features,
        threshold=threshold,
        feature_cols=FEATURE_COLS,
        goal_actions=GOAL_ACTIONS,
        split={
            "type": "time_split",
            "train_frac": 0.8,
            "train_min_date": str(train_df["visit_date"].min()),
            "train_max_date": str(train_df["visit_date"].max()),
            "valid_min_date": str(valid_df["visit_date"].min()),
            "valid_max_date": str(valid_df["visit_date"].max()),
            "train_size": int(len(train_df)),
            "valid_size": int(len(valid_df)),
        },
        metrics={
            "roc_auc": float(roc),
            "pr_auc": float(pr),
            "base_rate_valid": float(y_valid.mean()),
            "base_rate_train": float(y_train.mean()),
        },
    )

    # Save artifacts
    joblib.dump(model, ART_DIR / "model_xgb_hash.joblib")
    joblib.dump(vectorizer, ART_DIR / "vectorizer_hash.joblib")
    (ART_DIR / "artifacts.json").write_text(
        json.dumps(asdict(meta), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print("Saved artifacts to:", ART_DIR)
    print("- model_xgb_hash.joblib")
    print("- vectorizer_hash.joblib")
    print("- artifacts.json")


def main():
    train()


if __name__ == "__main__":
    main()

from __future__ import annotations

"""
final_project_ga: Prediction SERVICE (FastAPI) for target_any_goal

ТЗ методички:
- На вход: одна строка визита с атрибутами utm_*, device_*, geo_*
- На выход: 0/1 (1 — если пользователь совершит любое целевое действие)

Артефакты должны быть заранее обучены и сохранены в data/models/:
- model_xgb_hash.joblib
- vectorizer_hash.joblib
- artifacts.json

Запуск сервиса:
  uvicorn src.ml.predict:app --host 0.0.0.0 --port 8000 --reload

Проверка:
curl -X POST "http://127.0.0.1:8000/predict" \
  -H "Content-Type: application/json" \
  -d '{
    "utm_source": "google",
    "utm_medium": "cpc",
    "utm_campaign": "brand_search",
    "device_category": "mobile",
    "device_os": "android",
    "geo_country": "Russia",
    "geo_city": "Moscow"
  }'
"""

import json
from pathlib import Path
from typing import Any, Dict, Tuple

import joblib
from fastapi import FastAPI, HTTPException

# -------------------------
# Paths
# -------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]  # .../final_project_ga
ART_DIR = PROJECT_ROOT / "data" / "models"

MODEL_PATH = ART_DIR / "model_xgb_hash.joblib"
VECT_PATH = ART_DIR / "vectorizer_hash.joblib"
META_PATH = ART_DIR / "artifacts.json"


# -------------------------
# Helpers
# -------------------------
def _row_to_tokens(row: Dict[str, Any], feature_cols: list[str]) -> str:
    """
    dict -> "utm_source=google device_os=ios geo_city=moscow ..."
    """
    parts: list[str] = []
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


def load_artifacts() -> Tuple[Any, Any, Dict[str, Any]]:
    """
    Загружает артефакты. Бросает понятную ошибку, если чего-то нет.
    """
    missing = [p for p in [MODEL_PATH, VECT_PATH, META_PATH] if not p.exists()]
    if missing:
        raise FileNotFoundError(
            "Missing artifacts in data/models/. Train first:\n"
            "  python -m src.ml.train\n"
            f"Missing: {', '.join(str(p) for p in missing)}"
        )

    model = joblib.load(MODEL_PATH)
    vectorizer = joblib.load(VECT_PATH)
    meta = json.loads(META_PATH.read_text(encoding="utf-8"))
    return model, vectorizer, meta


def predict_one(row: Dict[str, Any]) -> int:
    """
    Главная функция по ТЗ: принимает строку визита -> возвращает 0/1.
    """
    model, vectorizer, meta = load_artifacts()
    feature_cols = meta["feature_cols"]
    threshold = float(meta["threshold"])

    tokens = _row_to_tokens(row, feature_cols)
    X = vectorizer.transform([tokens])
    proba = float(model.predict_proba(X)[:, 1][0])
    return 1 if proba >= threshold else 0


def predict_proba_one(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Для отладки: возвращает и вероятность, и порог.
    """
    model, vectorizer, meta = load_artifacts()
    feature_cols = meta["feature_cols"]
    threshold = float(meta["threshold"])

    tokens = _row_to_tokens(row, feature_cols)
    X = vectorizer.transform([tokens])
    proba = float(model.predict_proba(X)[:, 1][0])
    pred = 1 if proba >= threshold else 0
    return {"pred": pred, "proba": proba, "threshold": threshold}


# -------------------------
# FastAPI app (SERVICE)
# -------------------------
app = FastAPI(title="final_project_ga - prediction service (target_any_goal)")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/predict")
def api_predict(payload: Dict[str, Any]) -> int:
    """
    Принимаем JSON визита напрямую (без обёртки).
    Возвращаем строго число 0/1.
    """
    try:
        return predict_one(payload)
    except FileNotFoundError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Prediction failed: {e}")


@app.post("/predict_proba")
def api_predict_proba(payload: Dict[str, Any]) -> Dict[str, Any]:
    try:
        return predict_proba_one(payload)
    except FileNotFoundError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Prediction failed: {e}")

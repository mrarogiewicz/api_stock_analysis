import os
import re
import time
import requests
import pandas as pd
from io import StringIO
from datetime import datetime
from typing import Optional
from fastapi import FastAPI, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel

app = FastAPI()

VALID_API_KEY = os.environ.get("API_KEY")
SCHEMA_GIST_URL = os.environ.get("SCHEMA_GIST_URL")

SCRAPE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

security = HTTPBearer()

UNIT_MULTIPLIERS = {
    "Raw": 1,
    "Thousands": 1_000,
    "Millions": 1_000_000,
    "Billions": 1_000_000_000,
}

# --- Schema cache ---
_schema_cache = {
    "data": None,
    "loaded_at": 0,
}
CACHE_TTL = 300  # sekúnd


def get_schema() -> dict:
    now = time.time()
    if _schema_cache["data"] is None or (now - _schema_cache["loaded_at"]) > CACHE_TTL:
        if not SCHEMA_GIST_URL:
            raise RuntimeError("SCHEMA_GIST_URL nie je nastavená.")
        response = requests.get(SCHEMA_GIST_URL, timeout=5)
        response.raise_for_status()
        _schema_cache["data"] = response.json()
        _schema_cache["loaded_at"] = now
    return _schema_cache["data"]


# --- Models ---

class FinancialRequest(BaseModel):
    ticker: str
    period: Optional[str] = None  # None = annual aj quarterly, "annual" alebo "quarterly"


# --- Helpers ---

def check_auth(credentials: HTTPAuthorizationCredentials = Depends(security)):
    if credentials.credentials != VALID_API_KEY:
        raise HTTPException(status_code=403, detail="Nesprávny API kľúč.")


def detect_unit(html: str) -> str:
    match = re.search(
        r'title="Change number units"[^>]*>.*?<span[^>]*>\s*(Raw|Thousands|Millions|Billions)\s*</span>',
        html,
        re.DOTALL
    )
    if match:
        return match.group(1)
    return "Raw"


def parse_column_date(col_name: str) -> str | None:
    """
    Parsuje názov stĺpca ako 'Dec '25 Dec 31, 2025' na ISO dátum '2025-12-31'.
    """
    match = re.search(r'(\w+ \d+, \d{4})$', str(col_name))
    if match:
        try:
            dt = datetime.strptime(match.group(1), "%b %d, %Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            return None
    return None


def try_parse_number(value) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.replace(",", "").replace("%", "").strip()
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def is_percentage_value(value) -> bool:
    return isinstance(value, str) and "%" in value


def apply_schema(records: list, schema: dict, multiplier: int) -> list:
    """
    Filtruje riadky podľa schémy, aplikuje multiplikátor a formátuje dátumy.
    - Partial, case-insensitive match na názov metriky.
    - Každá metrika obsahuje 'unit' z Gistu.
    - Kľúče values sú ISO dátumy (2025-12-31).
    """
    result = []
    for metric_name, config in schema.items():
        record = next(
            (r for r in records if metric_name.lower() in str(r.get("metric", "")).lower()),
            None
        )
        if record is None:
            continue

        row = dict(record)
        unit = config.get("unit", "raw")
        should_multiply = config.get("multiply", False)

        values = {}
        for col, val in row.items():
            if col == "metric":
                continue
            iso_date = parse_column_date(col)
            if iso_date is None:
                continue

            if should_multiply and multiplier != 1 and not is_percentage_value(val):
                parsed = try_parse_number(val)
                if parsed is not None:
                    values[iso_date] = parsed * multiplier
                else:
                    values[iso_date] = val
            else:
                parsed = try_parse_number(val)
                values[iso_date] = parsed if parsed is not None else val

        result.append({
            "metric": row["metric"],
            "unit": unit,
            "values": values
        })

    return result


def build_url(base: str, period: str) -> str:
    return base if period == "annual" else f"{base}?p=quarterly"


def fetch_and_parse(url: str) -> tuple[list, str]:
    """Stiahne stránku, deteguje unit, vráti (records, unit)."""
    response = requests.get(url, headers=SCRAPE_HEADERS)
    response.raise_for_status()

    unit = detect_unit(response.text)
    tables = pd.read_html(StringIO(response.text))
    df = tables[0]

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[1] if col[1] else col[0] for col in df.columns]

    df.rename(columns={df.columns[0]: "metric"}, inplace=True)
    return df.to_dict(orient="records"), unit


def fetch_period_data(base_url: str, period: str, schema: dict) -> dict:
    """Stiahne a spracuje dáta pre jeden period."""
    url = build_url(base_url, period)
    records, unit = fetch_and_parse(url)
    multiplier = UNIT_MULTIPLIERS.get(unit, 1)
    data = apply_schema(records, schema, multiplier)
    return {"data": data}


def build_response(ticker: str, base_url: str, schema: dict, period: Optional[str]) -> dict:
    """Stiahne dáta pre jeden alebo oba periody podľa parametra."""
    periods_to_fetch = ["annual", "quarterly"] if period is None else [period]
    result = {"ticker": ticker.upper(), "periods": {}}
    for p in periods_to_fetch:
        result["periods"][p] = fetch_period_data(base_url, p, schema)
    return result


# --- Endpoints ---

@app.get("/keepalive")
def keepalive():
    return "keeping alive..."


@app.post("/ratios")
def get_ratios(body: FinancialRequest, _=Depends(check_auth)):
    try:
        base_url = f"https://stockanalysis.com/stocks/{body.ticker.lower()}/financials/ratios/"
        schema = get_schema().get("ratios", {})
        return build_response(body.ticker, base_url, schema, body.period)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Chyba: {e}")


@app.post("/income")
def get_income(body: FinancialRequest, _=Depends(check_auth)):
    try:
        base_url = f"https://stockanalysis.com/stocks/{body.ticker.lower()}/financials/"
        schema = get_schema().get("income", {})
        return build_response(body.ticker, base_url, schema, body.period)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Chyba: {e}")


@app.post("/balance")
def get_balance(body: FinancialRequest, _=Depends(check_auth)):
    try:
        base_url = f"https://stockanalysis.com/stocks/{body.ticker.lower()}/financials/balance-sheet/"
        schema = get_schema().get("balance", {})
        return build_response(body.ticker, base_url, schema, body.period)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Chyba: {e}")


@app.post("/cashflow")
def get_cashflow(body: FinancialRequest, _=Depends(check_auth)):
    try:
        base_url = f"https://stockanalysis.com/stocks/{body.ticker.lower()}/financials/cash-flow-statement/"
        schema = get_schema().get("cashflow", {})
        return build_response(body.ticker, base_url, schema, body.period)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Chyba: {e}")

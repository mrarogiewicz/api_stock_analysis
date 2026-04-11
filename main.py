import os
import re
import requests
import pandas as pd
from io import StringIO
from fastapi import FastAPI, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel

app = FastAPI()

VALID_API_KEY = os.environ.get("API_KEY")

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

MONETARY_RATIO_METRICS = {"Market Cap", "Enterprise Value"}


# --- Models ---

class FinancialRequest(BaseModel):
    ticker: str
    period: str = "annual"


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


def scrape_table(url: str) -> list:
    response = requests.get(url, headers=SCRAPE_HEADERS)
    response.raise_for_status()

    tables = pd.read_html(StringIO(response.text))
    df = tables[0]

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[1] if col[1] else col[0] for col in df.columns]

    df.rename(columns={df.columns[0]: "metric"}, inplace=True)
    return df.to_dict(orient="records")


def scrape_ratios_table(url: str) -> tuple[list, str]:
    response = requests.get(url, headers=SCRAPE_HEADERS)
    response.raise_for_status()

    unit = detect_unit(response.text)
    multiplier = UNIT_MULTIPLIERS[unit]

    tables = pd.read_html(StringIO(response.text))
    df = tables[0]

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[1] if col[1] else col[0] for col in df.columns]

    df.rename(columns={df.columns[0]: "metric"}, inplace=True)
    records = df.to_dict(orient="records")

    if multiplier != 1:
        for record in records:
            metric_name = str(record.get("metric", ""))
            if any(monetary in metric_name for monetary in MONETARY_RATIO_METRICS):
                for col, val in record.items():
                    if col == "metric":
                        continue
                    parsed = try_parse_number(val)
                    if parsed is not None:
                        record[col] = parsed * multiplier

    return records, unit


def build_url(base: str, period: str) -> str:
    return base if period == "annual" else f"{base}?p=quarterly"


# --- Endpoints ---

@app.get("/keepalive")
def keepalive():
    return "keeping alive..."


@app.post("/ratios")
def get_ratios(body: FinancialRequest, _=Depends(check_auth)):
    try:
        url = build_url(
            f"https://stockanalysis.com/stocks/{body.ticker.lower()}/financials/ratios/",
            body.period
        )
        data, detected_unit = scrape_ratios_table(url)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Chyba: {e}")
    return {
        "ticker": body.ticker.upper(),
        "period": body.period,
        "detected_unit": detected_unit,
        "data": data
    }


@app.post("/income")
def get_income(body: FinancialRequest, _=Depends(check_auth)):
    try:
        url = build_url(f"https://stockanalysis.com/stocks/{body.ticker.lower()}/financials/", body.period)
        data = scrape_table(url)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Chyba: {e}")
    return {"ticker": body.ticker.upper(), "period": body.period, "data": data}


@app.post("/balance")
def get_balance(body: FinancialRequest, _=Depends(check_auth)):
    try:
        url = build_url(f"https://stockanalysis.com/stocks/{body.ticker.lower()}/financials/balance-sheet/", body.period)
        data = scrape_table(url)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Chyba: {e}")
    return {"ticker": body.ticker.upper(), "period": body.period, "data": data}


@app.post("/cashflow")
def get_cashflow(body: FinancialRequest, _=Depends(check_auth)):
    try:
        url = build_url(f"https://stockanalysis.com/stocks/{body.ticker.lower()}/financials/cash-flow-statement/", body.period)
        data = scrape_table(url)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Chyba: {e}")
    return {"ticker": body.ticker.upper(), "period": body.period, "data": data}

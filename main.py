import os
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


# --- Models ---

class FinancialRequest(BaseModel):
    ticker: str
    period: str = "annual"  # "quarterly" alebo "annual"


# --- Helpers ---

def check_auth(credentials: HTTPAuthorizationCredentials = Depends(security)):
    if credentials.credentials != VALID_API_KEY:
        raise HTTPException(status_code=403, detail="Nesprávny API kľúč.")


def scrape_table(url: str) -> list:
    response = requests.get(url, headers=SCRAPE_HEADERS)
    response.raise_for_status()

    tables = pd.read_html(StringIO(response.text))
    df = tables[0]

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[1] if col[1] else col[0] for col in df.columns]

    df.rename(columns={df.columns[0]: "metric"}, inplace=True)

    return df.to_dict(orient="records")


def build_url(base: str, period: str) -> str:
    return base if period == "annual" else f"{base}?p=quarterly"


# --- Endpoints ---

@app.get("/keepalive")
def keepalive():
    return "keeping alive..."


@app.post("/ratios")
def get_ratios(body: FinancialRequest, _=Depends(check_auth)):
    try:
        url = build_url(f"https://stockanalysis.com/stocks/{body.ticker.lower()}/financials/ratios/", body.period)
        data = scrape_table(url)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Chyba: {e}")
    return {"ticker": body.ticker.upper(), "period": body.period, "data": data}


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

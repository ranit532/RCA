from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from decimal import Decimal
import logging
import numpy as np
import pandas as pd

from src.streamlit_backend import (
    initialize_snowflake_session,
    get_overview_metrics,
    get_dq_results,
    get_recon_results,
    get_audit_trail,
    get_dyd_status,
)

logger = logging.getLogger("api_server")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

app = FastAPI(title="RCA PoC API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

session = None

@app.on_event("startup")
def startup_event():
    global session
    session = initialize_snowflake_session()
    if session is None:
        logger.error("Could not initialize Snowflake session.")

@app.on_event("shutdown")
def shutdown_event():
    global session
    if session is not None:
        try:
            session.close()
        except Exception as exc:
            logger.warning(f"Error closing Snowflake session: {exc}")


def ensure_session():
    if session is None:
        raise HTTPException(status_code=503, detail="Snowflake session is not initialized.")
    return session


def df_to_json(df: pd.DataFrame):
    if df.empty:
        return []
    records = df.fillna("").to_dict(orient="records")
    for record in records:
        for key, value in record.items():
            if isinstance(value, (pd.Timestamp,)):
                record[key] = value.isoformat()
            elif isinstance(value, Decimal):
                record[key] = float(value)
            elif isinstance(value, (np.integer, np.floating)):
                record[key] = value.item()
            elif isinstance(value, (np.bool_,)):
                record[key] = bool(value)
    return records


@app.get("/")
def root():
    return {"status": "RCA PoC API is running", "version": "1.0.0"}

@app.get("/overview")
def overview():
    session = ensure_session()
    metrics = get_overview_metrics(session)
    return JSONResponse(content=metrics)

@app.get("/dq")
def dq_results():
    session = ensure_session()
    df = get_dq_results(session)
    return JSONResponse(content=df_to_json(df))

@app.get("/reconciliation")
def reconciliation_results():
    session = ensure_session()
    df = get_recon_results(session)
    return JSONResponse(content=df_to_json(df))

@app.get("/audit")
def audit_trail():
    session = ensure_session()
    df = get_audit_trail(session)
    return JSONResponse(content=df_to_json(df))

@app.get("/dyd")
def dyd_status():
    session = ensure_session()
    details = get_dyd_status(session)
    return JSONResponse(content=details)

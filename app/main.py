import os
import uuid
import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional, Literal, Union

from fastapi import FastAPI, Header, HTTPException, Response
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field
from fastapi.openapi.utils import get_openapi  # keep for Authorize button
from app.runner import scrape_append_to_csv, scrape_search_to_csv


# ---------------------------
# Config / API key handling
# ---------------------------

API_KEY = os.getenv("SCRAPER_API_KEY", "").strip()
if not API_KEY:
    # still allow running without a key in dev, but warn in logs
    print("WARNING: SCRAPER_API_KEY is not set; /docs and endpoints will be open")

app = FastAPI(title="Minimal Scraper", version="1.0.0")

# Simple x-api-key guard
async def require_api_key(x_api_key: Optional[str]) -> None:
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


# ---------------------------
# Models
# ---------------------------

class SelectorField(BaseModel):
    name: str
    selector: str
    type: Literal["text", "attr", "html"] = "text"
    attr: Optional[str] = None


class SearchConfig(BaseModel):
    # core search pieces
    input_selector: str
    submit_selector: str
    results_selector: Optional[str] = None
    detail_ready_selector: str
    back_to_search_selector: Optional[str] = None

    # NEW: disclaimer support
    disclaimer_selector: Optional[str] = None
    disclaimer_click_each: bool = False


class AppendPayload(BaseModel):
    mode: Literal["append"] = Field("append", frozen=True)
    base_url: str
    input_list: List[str]
    selectors: List[SelectorField]
    headless: bool = True
    delay_ms_min: int = 200
    delay_ms_max: int = 500
    timeout_ms: int = 20000


class SearchPayload(BaseModel):
    mode: Literal["search"] = Field("search", frozen=True)
    start_url: str
    input_list: List[str]
    selectors: List[SelectorField]
    search: SearchConfig
    headless: bool = True
    delay_ms_min: int = 200
    delay_ms_max: int = 500
    timeout_ms: int = 20000


RunRequest = Union[AppendPayload, SearchPayload]


class RunStatus(BaseModel):
    id: str
    status: Literal["queued", "running", "done", "error"]
    created_at: str
    finished_at: Optional[str] = None
    stats: Dict[str, int] = {"total": 0, "ok": 0, "err": 0}
    error: str = ""
    output_path: Optional[str] = None
    payload: Dict[str, Any] = {}


# ---------------------------
# In-memory run registry
# ---------------------------

RUNS: Dict[str, RunStatus] = {}


def _now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


async def _do(run_id: str, payload: RunRequest):
    st = RUNS[run_id]
    st.status = "running"
    RUNS[run_id] = st

    try:
        if isinstance(payload, AppendPayload):
            output_path, total, ok, err = await scrape_append_to_csv(
                base_url=payload.base_url,
                input_list=payload.input_list,
                selectors=[s.model_dump() for s in payload.selectors],
                headless=payload.headless,
                delay_ms_min=payload.delay_ms_min,
                delay_ms_max=payload.delay_ms_max,
                timeout_ms=payload.timeout_ms,
            )
        else:
            # IMPORTANT: pass the entire search object as a single argument
            output_path, total, ok, err = await scrape_search_to_csv(
                start_url=payload.start_url,
                input_list=payload.input_list,
                selectors=[s.model_dump() for s in payload.selectors],
                search=payload.search.model_dump(),
                headless=payload.headless,
                delay_ms_min=payload.delay_ms_min,
                delay_ms_max=payload.delay_ms_max,
                timeout_ms=payload.timeout_ms,
            )

        st.status = "done"
        st.output_path = output_path
        st.stats = {"total": total, "ok": ok, "err": err}
    except Exception as e:
        st.status = "error"
        st.error = f"{type(e).__name__}: {e}\n" \
                   f"sys.executable={os.sys.executable}\n" \
                   f"sys.prefix={os.sys.prefix}\n" \
                   f"PYTHONHOME={os.getenv('PYTHONHOME')}\n" \
                   f"PYTHONPATH={os.getenv('PYTHONPATH')}\n"
    finally:
        st.finished_at = _now_iso()
        RUNS[run_id] = st


# ---------------------------
# Routes
# ---------------------------

@app.get("/healthz")
async def healthz():
    return {"ok": True}


@app.post("/run", response_model=RunStatus)
async def create_run(payload: RunRequest, x_api_key: Optional[str] = Header(default=None)):
    await require_api_key(x_api_key)

    run_id = str(uuid.uuid4())
    st = RunStatus(
        id=run_id,
        status="queued",
        created_at=_now_iso(),
        payload=payload.model_dump(),
    )
    RUNS[run_id] = st

    # kick off task
    asyncio.create_task(_do(run_id, payload))

    return RUNS[run_id]


@app.get("/runs/{run_id}", response_model=RunStatus)
async def runs_status(run_id: str, x_api_key: Optional[str] = Header(default=None)):
    await require_api_key(x_api_key)
    st = RUNS.get(run_id)
    if not st:
        raise HTTPException(404, "Run not found")
    return st


@app.get("/runs/{run_id}/download")
async def runs_download(run_id: str, x_api_key: Optional[str] = Header(default=None)):
    await require_api_key(x_api_key)
    st = RUNS.get(run_id)
    if not st:
        raise HTTPException(404, "Run not found")
    if st.status != "done" or not st.output_path:
        raise HTTPException(400, "Run not completed or no output")
    if not os.path.exists(st.output_path):
        raise HTTPException(404, "Output file not found")
    return FileResponse(st.output_path, filename=os.path.basename(st.output_path))


# ---------------------------
# Swagger: API key "Authorize" button
# ---------------------------

def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        routes=app.routes,
    )
    # Define header-based API key
    openapi_schema.setdefault("components", {}).setdefault("securitySchemes", {}).update({
        "ApiKeyAuth": {
            "type": "apiKey",
            "in": "header",
            "name": "x-api-key",
        }
    })
    # Apply globally
    openapi_schema["security"] = [{"ApiKeyAuth": []}]
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi  # type: ignore

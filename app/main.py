# app/main.py
from __future__ import annotations

import uuid
import asyncio
import os
import sys
import traceback
from datetime import datetime
from typing import List, Optional, Literal

# Windows needs the Proactor event loop for Playwright subprocesses
if sys.platform.startswith("win"):
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    except Exception:
        pass
from fastapi.openapi.utils import get_openapi
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, PlainTextResponse
from pydantic import BaseModel, Field, HttpUrl
from loguru import logger

from .state import new_run, update_run, get_run
from .runner import scrape_append_to_csv, scrape_search_to_csv

app = FastAPI(title="Minimal Scraper Orchestrator")
# --- Swagger "Authorize" button for x-api-key ---
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema

    openapi_schema = get_openapi(
        title=app.title if hasattr(app, "title") else "API",
        version="1.0.0",
        description="Minimal Scraper API",
        routes=app.routes,
    )

    # Ensure components exists
    openapi_schema.setdefault("components", {})
    openapi_schema["components"].setdefault("securitySchemes", {})

    # Declare x-api-key header scheme
    openapi_schema["components"]["securitySchemes"]["ApiKeyAuth"] = {
        "type": "apiKey",
        "in": "header",
        "name": "x-api-key",
    }

    # Apply globally so the green "Authorize" shows up
    openapi_schema["security"] = [{"ApiKeyAuth": []}]

    app.openapi_schema = openapi_schema
    return openapi_schema

# Activate the custom OpenAPI
app.openapi = custom_openapi  # noqa: E305
# --- end Swagger "Authorize" patch ---
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --------- Models ---------

class Selector(BaseModel):
    name: str
    selector: str
    type: Literal["text", "attr"] = "text"
    attr: Optional[str] = None


class SearchConfig(BaseModel):
    input_selector: str
    submit_selector: Optional[str] = None
    results_selector: Optional[str] = None
    detail_ready_selector: Optional[str] = None
    back_to_search_selector: Optional[str] = None


class RunRequest(BaseModel):
    mode: Literal["append", "search"] = "append"

    # Common
    input_list: List[str] = Field(..., min_items=1)
    selectors: List[Selector] = Field(..., min_items=1)

    # Mode A
    base_url: Optional[HttpUrl | str] = None

    # Mode B
    start_url: Optional[HttpUrl | str] = None
    search: Optional[SearchConfig] = None

    # runtime
    headless: bool = True
    delay_ms_min: int = 300
    delay_ms_max: int = 900
    timeout_ms: int = 30000


class RunResponse(BaseModel):
    run_id: str
    status: str


# --------- Debug/env ---------

@app.get("/env", response_class=PlainTextResponse)
def env():
    lines = []
    lines.append(f"sys.executable = {sys.executable}")
    lines.append(f"sys.prefix     = {sys.prefix}")
    try:
        import playwright  # type: ignore
        from importlib.metadata import version, PackageNotFoundError
        try:
            pv = version("playwright")
        except PackageNotFoundError:
            pv = "unknown"
        lines.append(f"playwright.version  = {pv}")
        lines.append(f"playwright.__file__ = {getattr(playwright, '__file__', None)}")
    except Exception as e:
        lines.append(f"playwright import failed: {type(e).__name__}: {e}")
    lines.append("PYTHONHOME=" + str(os.environ.get("PYTHONHOME")))
    lines.append("PYTHONPATH=" + str(os.environ.get("PYTHONPATH")))
    return "\n".join(lines)


# --------- API ---------

@app.get("/healthz")
def healthz():
    return {"ok": True}


@app.post("/run", response_model=RunResponse)
async def run_job(req: RunRequest):
    if req.mode == "append":
        if not req.base_url:
            raise HTTPException(422, "base_url is required for mode='append'")
    elif req.mode == "search":
        if not (req.start_url and req.search and req.search.input_selector):
            raise HTTPException(422, "start_url and search.input_selector are required for mode='search'")
    else:
        raise HTTPException(422, "Unsupported mode")

    run_id = str(uuid.uuid4())
    new_run(run_id, payload=req.dict())
    update_run(run_id, status="running")
    logger.info(f"Run {run_id} started (mode={req.mode})")

    async def _do():
        try:
            if req.mode == "append":
                output_path, total, ok, err = await scrape_append_to_csv(
                    base_url=str(req.base_url),
                    input_list=req.input_list,
                    selectors=[s.dict() for s in req.selectors],
                    headless=req.headless,
                    delay_ms_min=req.delay_ms_min,
                    delay_ms_max=req.delay_ms_max,
                    timeout_ms=req.timeout_ms,
                    run_id=run_id,
                )
            else:
                s = req.search  # type: ignore[assignment]
                output_path, total, ok, err = await scrape_search_to_csv(
                    start_url=str(req.start_url),
                    input_list=req.input_list,
                    selectors=[sel.dict() for sel in req.selectors],
                    input_selector=s.input_selector,                   # type: ignore[arg-type]
                    submit_selector=s.submit_selector,                 # type: ignore[arg-type]
                    results_selector=s.results_selector,               # type: ignore[arg-type]
                    detail_ready_selector=s.detail_ready_selector,     # type: ignore[arg-type]
                    back_to_search_selector=s.back_to_search_selector, # type: ignore[arg-type]
                    headless=req.headless,
                    delay_ms_min=req.delay_ms_min,
                    delay_ms_max=req.delay_ms_max,
                    timeout_ms=req.timeout_ms,
                    run_id=run_id,
                )

            if not os.path.exists(output_path):
                raise RuntimeError("CSV not found after run")

            update_run(
                run_id,
                status="done",
                finished_at=datetime.utcnow().isoformat(),
                stats={"total": total, "ok": ok, "err": err},
                output_path=output_path,
            )
            logger.info(f"Run {run_id} done: {ok}/{total} ok")

        except Exception as e:
            tb = traceback.format_exc()
            err_msg = f"{type(e).__name__}: {e}".strip()
            env_lines = [
                f"sys.executable={sys.executable}",
                f"sys.prefix={sys.prefix}",
                "PYTHONHOME=" + str(os.environ.get("PYTHONHOME")),
                "PYTHONPATH=" + str(os.environ.get("PYTHONPATH")),
            ]
            error_blob = err_msg + "\n" + "\n".join(env_lines) + "\n" + (tb[:3000] + ("..." if len(tb) > 3000 else ""))
            logger.exception(f"Run {run_id} error: {err_msg}")
            update_run(run_id, status="error", finished_at=datetime.utcnow().isoformat(), error=error_blob)

    asyncio.create_task(_do())
    return RunResponse(run_id=run_id, status="running")


@app.get("/runs/{run_id}")
def runs_status(run_id: str):
    info = get_run(run_id)
    if not info:
        raise HTTPException(404, "run not found")
    return info


@app.get("/runs/{run_id}/download")
def runs_download(run_id: str):
    info = get_run(run_id)
    if not info:
        raise HTTPException(404, "run not found")
    if info["status"] != "done" or not info["output_path"]:
        raise HTTPException(409, "run not finished")
    return FileResponse(path=info["output_path"], media_type="text/csv", filename=f"{run_id}.csv")

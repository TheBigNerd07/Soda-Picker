from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Annotated
from urllib.parse import urlencode

import uvicorn
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .catalog import SodaCatalog
from .config import Settings, parse_bool
from .database import Database
from .models import RecommendationResult, format_clock_time
from .recommendation import RecommendationEngine

settings = Settings.from_env()
logging.basicConfig(
    level=getattr(logging, settings.log_level, logging.INFO),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)

catalog = SodaCatalog(settings.csv_path)
database = Database(settings.database_path)
engine = RecommendationEngine()


def _format_number(value: float | int | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:g}"


def _yes_no(value: bool) -> str:
    return "Yes" if value else "No"


templates = Jinja2Templates(directory="templates")
templates.env.filters["number"] = _format_number
templates.env.filters["yes_no"] = _yes_no


@asynccontextmanager
async def lifespan(app: FastAPI):
    database.initialize()
    catalog.refresh(force=True)
    app.state.settings = settings
    app.state.catalog = catalog
    app.state.database = database
    yield


app = FastAPI(title="Soda Picker", version="1.0.0", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")


def _build_dashboard_context(
    request: Request,
    *,
    local_now,
    recommendation: RecommendationResult | None = None,
    flash_message: str | None = None,
    chaos_mode: bool | None = None,
) -> dict[str, object]:
    daily_total = database.get_today_caffeine_total(local_now)
    history = database.get_today_entries(local_now)
    loaded_sodas = catalog.list_sodas()

    return {
        "request": request,
        "settings": settings,
        "local_now": local_now,
        "local_now_iso": local_now.isoformat(),
        "local_now_label": format_clock_time(local_now),
        "daily_total": daily_total,
        "history": history,
        "loaded_sodas": loaded_sodas,
        "catalog_warnings": catalog.warnings,
        "recommendation": recommendation,
        "flash_message": flash_message,
        "chaos_mode": settings.chaos_mode_default if chaos_mode is None else chaos_mode,
    }


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request) -> HTMLResponse:
    local_now = settings.local_now()
    flash_message = None

    if request.query_params.get("logged") == "1":
        soda_name = request.query_params.get("name", "your soda")
        flash_message = f"Logged {soda_name}."
    elif request.query_params.get("flash"):
        flash_message = request.query_params.get("flash")

    context = _build_dashboard_context(request, local_now=local_now, flash_message=flash_message)
    return templates.TemplateResponse("index.html", context)


@app.post("/pick", response_class=HTMLResponse)
async def pick_soda(
    request: Request,
    chaos_mode: Annotated[str | None, Form()] = None,
) -> HTMLResponse:
    local_now = settings.local_now()
    chaos_enabled = parse_bool(chaos_mode, settings.chaos_mode_default)
    recommendation = engine.recommend(
        settings=settings,
        sodas=catalog.list_sodas(),
        daily_caffeine_total=database.get_today_caffeine_total(local_now),
        recent_consumed_ids=database.get_recent_consumed_ids(limit=3),
        local_now=local_now,
        chaos_mode=chaos_enabled,
    )
    context = _build_dashboard_context(
        request,
        local_now=local_now,
        recommendation=recommendation,
        chaos_mode=chaos_enabled,
    )
    return templates.TemplateResponse("index.html", context)


@app.post("/log-consumption")
async def log_consumption(
    soda_id: Annotated[str, Form()],
    reason: Annotated[str, Form()] = "",
    chaos_mode: Annotated[str | None, Form()] = None,
):
    soda = catalog.get_by_id(soda_id)
    if soda is None:
        query = urlencode({"flash": "That soda is no longer available in the catalog."})
        return RedirectResponse(url=f"/?{query}", status_code=303)

    chaos_enabled = parse_bool(chaos_mode, settings.chaos_mode_default)
    local_now = settings.local_now()
    database.log_consumption(soda, local_now, reason=reason.strip(), chaos_mode=chaos_enabled)
    query = urlencode({"logged": "1", "name": soda.name})
    return RedirectResponse(url=f"/?{query}", status_code=303)


@app.get("/catalog", response_class=HTMLResponse)
async def catalog_page(request: Request) -> HTMLResponse:
    local_now = settings.local_now()
    return templates.TemplateResponse(
        "catalog.html",
        {
            "request": request,
            "settings": settings,
            "local_now": local_now,
            "sodas": catalog.list_sodas(),
            "warnings": catalog.warnings,
        },
    )


@app.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request) -> HTMLResponse:
    local_now = settings.local_now()
    return templates.TemplateResponse(
        "settings.html",
        {
            "request": request,
            "settings": settings,
            "local_now": local_now,
            "settings_items": settings.display_items(),
        },
    )


@app.get("/healthz", response_class=JSONResponse)
async def healthz() -> JSONResponse:
    local_now = settings.local_now()
    payload = {
        "status": "ok",
        "local_time": local_now.isoformat(),
        "timezone": settings.timezone_name,
        "sodas_loaded": len(catalog.list_sodas()),
        "csv_path": settings.csv_path,
        "database_path": settings.database_path,
    }
    return JSONResponse(payload)


if __name__ == "__main__":
    uvicorn.run(
        app,
        host=settings.app_host,
        port=settings.app_port,
        proxy_headers=True,
        forwarded_allow_ips="*",
    )

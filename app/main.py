from __future__ import annotations

import logging
import shutil
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlparse

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.trustedhost import TrustedHostMiddleware

from .catalog import SodaCatalog
from .config import Settings, parse_bool
from .database import Database
from .models import (
    BackupFile,
    CatalogItem,
    PREFERENCE_NEUTRAL,
    SodaState,
    format_calendar_date,
    format_clock_time,
)
from .recommendation import RecommendationEngine
from .security import (
    AUTH_COOKIE_NAME,
    AccessControlMiddleware,
    BasicAuthMiddleware,
    RateLimitMiddleware,
    credentials_valid,
    issue_auth_token,
    read_auth_token,
)
from .settings_store import SettingsStore

base_settings = Settings.from_env()
logging.basicConfig(
    level=getattr(logging, base_settings.log_level, logging.INFO),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
LOGGER = logging.getLogger(__name__)

catalog = SodaCatalog(base_settings.csv_path)
database = Database(base_settings.database_path)
settings_store = SettingsStore(base_settings, database)
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
templates.env.filters["calendar_date"] = format_calendar_date


@asynccontextmanager
async def lifespan(app: FastAPI):
    database.initialize()
    catalog.refresh(force=True)
    app.state.catalog = catalog
    app.state.database = database
    app.state.settings_store = settings_store
    yield


app = FastAPI(title="Soda Picker", version="2.0.0", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")

if base_settings.trusted_hosts_list:
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=base_settings.trusted_hosts_list)

if base_settings.rate_limit_requests > 0 and base_settings.rate_limit_window_seconds > 0:
    app.add_middleware(
        RateLimitMiddleware,
        max_requests=base_settings.rate_limit_requests,
        window_seconds=base_settings.rate_limit_window_seconds,
        exempt_paths={"/healthz"},
    )

if base_settings.access_control_enabled:
    app.add_middleware(
        AccessControlMiddleware,
        mode=base_settings.access_control_mode,
        secret=base_settings.access_control_secret,
        exempt_paths={"/healthz", "/login", "/logout"},
    )

if base_settings.basic_auth_enabled:
    app.add_middleware(
        BasicAuthMiddleware,
        username=base_settings.basic_auth_username,
        password=base_settings.basic_auth_password,
        exempt_paths={"/healthz"},
    )


def _current_settings() -> Settings:
    return settings_store.current()


def _redirect_with_flash(path: str, message: str) -> RedirectResponse:
    query = urlencode({"flash": message})
    return RedirectResponse(url=f"{path}?{query}", status_code=303)


def _parse_flash(request: Request) -> str | None:
    return request.query_params.get("flash")


def _safe_next_path(raw_value: str | None) -> str:
    if not raw_value:
        return "/"
    parsed = urlparse(raw_value)
    if parsed.scheme or parsed.netloc:
        return "/"
    if not raw_value.startswith("/") or raw_value.startswith("//"):
        return "/"
    return raw_value


def _parse_local_datetime(raw_value: str | None, settings: Settings, fallback: datetime | None = None) -> datetime:
    if not raw_value:
        return fallback or settings.local_now()
    parsed = datetime.fromisoformat(raw_value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=settings.timezone)
    return parsed.astimezone(settings.timezone)


def _parse_optional_date(raw_value: str | None) -> date | None:
    if not raw_value:
        return None
    return date.fromisoformat(raw_value)


def _parse_optional_rating(raw_value: str | None) -> int | None:
    if raw_value is None:
        return None
    cleaned = raw_value.strip()
    if not cleaned:
        return None
    rating = int(cleaned)
    if rating < 1 or rating > 5:
        raise ValueError("Rating must be between 1 and 5.")
    return rating


def _parse_priority(raw_value: str | None) -> int:
    cleaned = (raw_value or "").strip()
    if not cleaned:
        return 3
    priority = int(cleaned)
    if priority < 1 or priority > 5:
        raise ValueError("Priority must be between 1 and 5.")
    return priority


def _parse_wishlist_status(raw_value: str | None) -> str:
    cleaned = (raw_value or "").strip().lower()
    if cleaned in {"active", "found", "archived"}:
        return cleaned
    return "active"


def _build_catalog_items(local_now: datetime) -> list[CatalogItem]:
    sodas = catalog.list_sodas()
    state_map = database.get_soda_state_map()
    items: list[CatalogItem] = []
    for soda in sodas:
        state = state_map.get(soda.id, SodaState(soda_id=soda.id, preference=PREFERENCE_NEUTRAL))
        items.append(CatalogItem(soda=soda, state=state))
    return items


def _merged_recent_soda_ids(limit: int) -> list[str]:
    ordered: list[str] = []
    for source in (
        database.get_recent_consumed_catalog_ids(limit=limit),
        database.get_recent_recommended_ids(limit=limit),
    ):
        for soda_id in source:
            if soda_id and soda_id not in ordered:
                ordered.append(soda_id)
                if len(ordered) >= limit:
                    return ordered
    return ordered


def _build_backup_list(backup_dir: str, *, limit: int = 12, local_timezone=None) -> list[BackupFile]:
    backup_root = Path(backup_dir)
    if not backup_root.exists():
        return []

    local_timezone = local_timezone or base_settings.timezone
    entries: list[BackupFile] = []
    for path in sorted(backup_root.glob("*"), key=lambda current: current.stat().st_mtime, reverse=True):
        if not path.is_file():
            continue
        stat = path.stat()
        entries.append(
            BackupFile(
                name=path.name,
                path=str(path),
                created_at=datetime.fromtimestamp(stat.st_mtime, tz=local_timezone),
                size_bytes=stat.st_size,
            )
        )
    return entries[:limit]


def _backup_catalog_file(settings: Settings) -> Path | None:
    source = Path(settings.csv_path)
    if not source.exists():
        return None
    backup_root = Path(settings.backup_dir)
    backup_root.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(settings.timezone).strftime("%Y%m%d-%H%M%S")
    destination = backup_root / f"catalog-{timestamp}.csv"
    shutil.copy2(source, destination)
    return destination


def _authenticated_username(request: Request, settings: Settings) -> str | None:
    cached = request.scope.get("soda_picker_user")
    if isinstance(cached, str) and cached:
        return cached
    token = request.cookies.get(AUTH_COOKIE_NAME)
    username = read_auth_token(token, settings.access_control_secret)
    if username:
        request.scope["soda_picker_user"] = username
    return username


def _build_auth_context(request: Request, *, settings: Settings) -> dict[str, Any]:
    username = _authenticated_username(request, settings)
    current_path = request.url.path
    if request.url.query:
        current_path = f"{current_path}?{request.url.query}"
    can_write = (not settings.access_control_enabled) or username is not None
    write_locked = settings.access_control_mode == "writes" and not can_write
    return {
        "auth_username": username,
        "is_authenticated": username is not None,
        "can_write": can_write,
        "write_locked": write_locked,
        "access_control_enabled": settings.access_control_enabled,
        "access_control_mode": settings.access_control_mode,
        "access_control_mode_label": settings.access_control_mode_label,
        "access_login_url": f"/login?{urlencode({'next': current_path})}",
    }


def _build_common_context(
    request: Request,
    *,
    settings: Settings,
    local_now: datetime,
    flash_message: str | None = None,
) -> dict[str, Any]:
    rules = settings.effective_rules(local_now)
    catalog_items = _build_catalog_items(local_now)
    today_entries = database.get_today_entries(local_now)
    daily_total = database.get_today_caffeine_total(local_now)
    passport_entries = database.list_passport_entries(limit=8)
    passport_summary = database.get_passport_summary()
    wishlist_entries = database.list_wishlist_entries(limit=8, include_archived=False)
    wishlist_summary = database.get_wishlist_summary()
    context = {
        "request": request,
        "settings": settings,
        "rules": rules,
        "local_now": local_now,
        "local_now_iso": local_now.isoformat(),
        "local_now_label": format_clock_time(local_now),
        "flash_message": flash_message,
        "catalog_items": catalog_items,
        "catalog_diagnostics": catalog.diagnostics,
        "today_entries": today_entries,
        "daily_total": daily_total,
        "recent_recommendations": database.get_recent_recommendations(limit=8),
        "backup_files": _build_backup_list(settings.backup_dir, local_timezone=settings.timezone),
        "override_keys": settings_store.override_keys(),
        "passport_summary": passport_summary,
        "recent_passport_entries": passport_entries,
        "wishlist_summary": wishlist_summary,
        "recent_wishlist_entries": wishlist_entries,
    }
    context.update(_build_auth_context(request, settings=settings))
    return context


def _dashboard_context(
    request: Request,
    *,
    settings: Settings,
    local_now: datetime,
    flash_message: str | None = None,
    recommendation=None,
    recommendation_id: int | None = None,
    chaos_mode: bool | None = None,
) -> dict[str, Any]:
    context = _build_common_context(
        request,
        settings=settings,
        local_now=local_now,
        flash_message=flash_message,
    )
    context.update(
        {
            "recommendation": recommendation,
            "recommendation_id": recommendation_id,
            "chaos_mode": settings.chaos_mode_default if chaos_mode is None else chaos_mode,
            "available_count": sum(1 for item in context["catalog_items"] if item.state.is_available),
            "favorite_count": sum(1 for item in context["catalog_items"] if item.state.preference == "favorite"),
            "manual_entry_time": local_now.strftime("%Y-%m-%dT%H:%M"),
            "passport_entry_date": local_now.date().isoformat(),
        }
    )
    return context


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request) -> HTMLResponse:
    settings = _current_settings()
    local_now = settings.local_now()
    context = _dashboard_context(
        request,
        settings=settings,
        local_now=local_now,
        flash_message=_parse_flash(request),
    )
    return templates.TemplateResponse("index.html", context)


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request) -> HTMLResponse:
    settings = _current_settings()
    if not settings.access_control_enabled:
        return _redirect_with_flash("/", "Access control is not enabled.")

    if _authenticated_username(request, settings):
        next_path = _safe_next_path(request.query_params.get("next"))
        return RedirectResponse(url=next_path, status_code=303)

    context = {
        "request": request,
        "settings": settings,
        "flash_message": _parse_flash(request),
        "login_error": None,
        "next_path": _safe_next_path(request.query_params.get("next")),
        "write_locked": False,
        "can_write": True,
        "access_control_enabled": settings.access_control_enabled,
        "access_control_mode": settings.access_control_mode,
        "access_control_mode_label": settings.access_control_mode_label,
        "auth_username": None,
        "is_authenticated": False,
        "access_login_url": "/login",
    }
    return templates.TemplateResponse("login.html", context)


@app.post("/login", response_class=HTMLResponse)
async def login_submit(request: Request) -> Response:
    settings = _current_settings()
    if not settings.access_control_enabled:
        return _redirect_with_flash("/", "Access control is not enabled.")

    form = await request.form()
    next_path = _safe_next_path(str(form.get("next", "/")))
    username = str(form.get("username", "")).strip()
    password = str(form.get("password", ""))
    if not credentials_valid(
        username,
        password,
        expected_username=settings.access_control_username,
        expected_password=settings.access_control_password,
    ):
        context = {
            "request": request,
            "settings": settings,
            "flash_message": None,
            "login_error": "That username or password did not match.",
            "next_path": next_path,
            "write_locked": False,
            "can_write": True,
            "access_control_enabled": settings.access_control_enabled,
            "access_control_mode": settings.access_control_mode,
            "access_control_mode_label": settings.access_control_mode_label,
            "auth_username": None,
            "is_authenticated": False,
            "access_login_url": "/login",
        }
        return templates.TemplateResponse("login.html", context, status_code=401)

    token = issue_auth_token(
        settings.access_control_username,
        settings.access_control_secret,
        lifetime_seconds=settings.access_control_session_days * 86400,
    )
    response = RedirectResponse(url=next_path, status_code=303)
    response.set_cookie(
        AUTH_COOKIE_NAME,
        token,
        max_age=settings.access_control_session_days * 86400,
        httponly=True,
        samesite="lax",
    )
    return response


@app.post("/logout")
async def logout() -> RedirectResponse:
    response = _redirect_with_flash("/login", "Signed out.")
    response.delete_cookie(AUTH_COOKIE_NAME)
    return response


@app.post("/pick", response_class=HTMLResponse)
async def pick_soda(request: Request) -> HTMLResponse:
    form = await request.form()
    settings = _current_settings()
    local_now = settings.local_now()
    rules = settings.effective_rules(local_now)
    chaos_mode = parse_bool(form.get("chaos_mode"), settings.chaos_mode_default)
    catalog_items = _build_catalog_items(local_now)
    today_entries = database.get_today_entries(local_now)
    daily_total = database.get_today_caffeine_total(local_now)

    recommendation = engine.recommend(
        rules=rules,
        catalog_items=catalog_items,
        daily_caffeine_total=daily_total,
        recent_soda_ids=_merged_recent_soda_ids(rules.duplicate_lookback),
        today_entries=today_entries,
        local_now=local_now,
        chaos_mode=chaos_mode,
    )
    recommendation_id = database.log_recommendation(recommendation, local_now)
    context = _dashboard_context(
        request,
        settings=settings,
        local_now=local_now,
        recommendation=recommendation,
        recommendation_id=recommendation_id,
        chaos_mode=chaos_mode,
    )
    return templates.TemplateResponse("index.html", context)


@app.post("/log-consumption")
async def log_consumption(request: Request) -> RedirectResponse:
    form = await request.form()
    settings = _current_settings()
    soda_id = str(form.get("soda_id", "")).strip()
    soda = catalog.get_by_id(soda_id)
    if soda is None:
        return _redirect_with_flash("/", "That soda is no longer available in the catalog.")

    recommendation_id = form.get("recommendation_id")
    recommendation_value = int(recommendation_id) if recommendation_id else None
    local_now = settings.local_now()
    database.log_catalog_consumption(
        soda,
        local_now,
        reason=str(form.get("reason", "")).strip(),
        chaos_mode=parse_bool(form.get("chaos_mode"), settings.chaos_mode_default),
        notes=str(form.get("notes", "")).strip(),
        recommendation_id=recommendation_value,
    )
    return _redirect_with_flash("/", f"Logged {soda.display_name}.")


@app.post("/manual-entry")
async def log_manual_entry(request: Request) -> RedirectResponse:
    form = await request.form()
    settings = _current_settings()
    local_now = _parse_local_datetime(str(form.get("consumed_at_local", "")), settings, settings.local_now())
    name = str(form.get("name", "")).strip()
    if not name:
        return _redirect_with_flash("/activity", "Manual entry needs a name.")
    caffeine_mg = float(str(form.get("caffeine_mg", "0")).strip() or 0)
    database.log_manual_entry(
        name=name,
        caffeine_mg=caffeine_mg,
        local_now=local_now,
        notes=str(form.get("notes", "")).strip(),
    )
    return _redirect_with_flash("/activity", f"Logged manual caffeine entry for {name}.")


@app.get("/activity", response_class=HTMLResponse)
async def activity_page(request: Request) -> HTMLResponse:
    settings = _current_settings()
    local_now = settings.local_now()
    context = _build_common_context(
        request,
        settings=settings,
        local_now=local_now,
        flash_message=_parse_flash(request),
    )
    context.update(
        {
            "all_entries": database.get_recent_entries(limit=60),
            "manual_entry_time": local_now.strftime("%Y-%m-%dT%H:%M"),
        }
    )
    return templates.TemplateResponse("activity.html", context)


@app.get("/wishlist", response_class=HTMLResponse)
async def wishlist_page(request: Request) -> HTMLResponse:
    settings = _current_settings()
    local_now = settings.local_now()
    context = _build_common_context(
        request,
        settings=settings,
        local_now=local_now,
        flash_message=_parse_flash(request),
    )
    context["wishlist_entries"] = database.list_wishlist_entries(limit=300)
    return templates.TemplateResponse("wishlist.html", context)


@app.post("/wishlist/add")
async def add_wishlist_entry(request: Request) -> RedirectResponse:
    form = await request.form()
    soda_name = str(form.get("soda_name", "")).strip()
    brand = str(form.get("brand", "")).strip()
    if not soda_name:
        return _redirect_with_flash("/wishlist", "Wishlist entries need a soda name.")

    existing = database.find_active_wishlist_entry(soda_name=soda_name, brand=brand)
    if existing is not None:
        return _redirect_with_flash("/wishlist", f"{existing.display_name} is already on the active wishlist.")

    try:
        priority = _parse_priority(str(form.get("priority", "3")))
    except ValueError as exc:
        return _redirect_with_flash("/wishlist", str(exc))

    database.add_wishlist_entry(
        soda_name=soda_name,
        brand=brand,
        country=str(form.get("country", "")).strip(),
        category=str(form.get("category", "")).strip(),
        source_type="manual",
        source_ref="",
        priority=priority,
        status=_parse_wishlist_status(str(form.get("status", "active"))),
        notes=str(form.get("notes", "")).strip(),
    )
    return _redirect_with_flash("/wishlist", f"Added {soda_name} to your wishlist.")


@app.post("/wishlist/from-catalog")
async def add_wishlist_from_catalog(request: Request) -> RedirectResponse:
    form = await request.form()
    soda_id = str(form.get("soda_id", "")).strip()
    soda = catalog.get_by_id(soda_id)
    if soda is None:
        return _redirect_with_flash("/catalog", "That soda is no longer in the catalog.")

    existing = database.find_active_wishlist_entry(soda_name=soda.name, brand=soda.brand)
    if existing is not None:
        return _redirect_with_flash("/catalog", f"{existing.display_name} is already on the active wishlist.")

    database.add_wishlist_entry(
        soda_name=soda.name,
        brand=soda.brand,
        country="",
        category=soda.category,
        source_type="catalog",
        source_ref=soda.id,
        priority=3,
        status="active",
        notes="Added from the catalog page.",
    )
    return _redirect_with_flash("/catalog", f"Added {soda.display_name} to your wishlist.")


@app.post("/wishlist/from-passport")
async def add_wishlist_from_passport(request: Request) -> RedirectResponse:
    form = await request.form()
    entry_id = int(str(form.get("entry_id", "0")).strip() or 0)
    passport_entry = database.get_passport_entry(entry_id)
    if passport_entry is None:
        return _redirect_with_flash("/passport", "That passport entry no longer exists.")

    existing = database.find_active_wishlist_entry(
        soda_name=passport_entry.soda_name,
        brand=passport_entry.brand,
    )
    if existing is not None:
        return _redirect_with_flash("/passport", f"{existing.display_name} is already on the active wishlist.")

    database.add_wishlist_entry(
        soda_name=passport_entry.soda_name,
        brand=passport_entry.brand,
        country=passport_entry.country,
        category=passport_entry.category,
        source_type="passport",
        source_ref=str(passport_entry.id),
        priority=4 if passport_entry.would_try_again else 3,
        status="active",
        notes="Added from the soda passport.",
    )
    return _redirect_with_flash("/passport", f"Added {passport_entry.display_name} to your wishlist.")


@app.post("/wishlist/{entry_id}/update")
async def update_wishlist_entry(request: Request, entry_id: int) -> RedirectResponse:
    form = await request.form()
    soda_name = str(form.get("soda_name", "")).strip()
    if not soda_name:
        return _redirect_with_flash("/wishlist", "Wishlist entries need a soda name.")

    try:
        priority = _parse_priority(str(form.get("priority", "3")))
    except ValueError as exc:
        return _redirect_with_flash("/wishlist", str(exc))

    success = database.update_wishlist_entry(
        entry_id,
        soda_name=soda_name,
        brand=str(form.get("brand", "")).strip(),
        country=str(form.get("country", "")).strip(),
        category=str(form.get("category", "")).strip(),
        priority=priority,
        status=_parse_wishlist_status(str(form.get("status", "active"))),
        notes=str(form.get("notes", "")).strip(),
    )
    if not success:
        return _redirect_with_flash("/wishlist", "That wishlist entry no longer exists.")
    return _redirect_with_flash("/wishlist", "Updated the wishlist entry.")


@app.post("/wishlist/{entry_id}/delete")
async def delete_wishlist_entry(entry_id: int) -> RedirectResponse:
    if not database.delete_wishlist_entry(entry_id):
        return _redirect_with_flash("/wishlist", "That wishlist entry no longer exists.")
    return _redirect_with_flash("/wishlist", "Deleted the wishlist entry.")


@app.get("/passport", response_class=HTMLResponse)
async def passport_page(request: Request) -> HTMLResponse:
    settings = _current_settings()
    local_now = settings.local_now()
    context = _build_common_context(
        request,
        settings=settings,
        local_now=local_now,
        flash_message=_parse_flash(request),
    )
    context.update(
        {
            "passport_entries": database.list_passport_entries(limit=300),
            "passport_entry_date": local_now.date().isoformat(),
        }
    )
    return templates.TemplateResponse("passport.html", context)


@app.post("/passport/add")
async def add_passport_entry(request: Request) -> RedirectResponse:
    form = await request.form()
    settings = _current_settings()
    soda_name = str(form.get("soda_name", "")).strip()
    if not soda_name:
        return _redirect_with_flash("/passport", "Passport entry needs a soda name.")

    tried_on = _parse_optional_date(str(form.get("tried_on", "")).strip() or None) or settings.local_now().date()
    try:
        rating = _parse_optional_rating(str(form.get("rating", "")))
    except ValueError as exc:
        return _redirect_with_flash("/passport", str(exc))

    database.add_passport_entry(
        soda_name=soda_name,
        brand=str(form.get("brand", "")).strip(),
        country=str(form.get("country", "")).strip(),
        region=str(form.get("region", "")).strip(),
        city=str(form.get("city", "")).strip(),
        category=str(form.get("category", "")).strip(),
        tried_on=tried_on,
        where_tried=str(form.get("where_tried", "")).strip(),
        rating=rating,
        would_try_again=parse_bool(form.get("would_try_again"), False),
        notes=str(form.get("notes", "")).strip(),
    )
    return _redirect_with_flash("/passport", f"Added {soda_name} to your soda passport.")


@app.post("/passport/{entry_id}/update")
async def update_passport_entry(request: Request, entry_id: int) -> RedirectResponse:
    form = await request.form()
    soda_name = str(form.get("soda_name", "")).strip()
    if not soda_name:
        return _redirect_with_flash("/passport", "Passport entry needs a soda name.")

    tried_on = _parse_optional_date(str(form.get("tried_on", "")).strip() or None)
    if tried_on is None:
        return _redirect_with_flash("/passport", "Passport entry needs a tried date.")

    try:
        rating = _parse_optional_rating(str(form.get("rating", "")))
    except ValueError as exc:
        return _redirect_with_flash("/passport", str(exc))

    success = database.update_passport_entry(
        entry_id,
        soda_name=soda_name,
        brand=str(form.get("brand", "")).strip(),
        country=str(form.get("country", "")).strip(),
        region=str(form.get("region", "")).strip(),
        city=str(form.get("city", "")).strip(),
        category=str(form.get("category", "")).strip(),
        tried_on=tried_on,
        where_tried=str(form.get("where_tried", "")).strip(),
        rating=rating,
        would_try_again=parse_bool(form.get("would_try_again"), False),
        notes=str(form.get("notes", "")).strip(),
    )
    if not success:
        return _redirect_with_flash("/passport", "That passport entry no longer exists.")
    return _redirect_with_flash("/passport", "Updated the soda passport entry.")


@app.post("/passport/{entry_id}/delete")
async def delete_passport_entry(entry_id: int) -> RedirectResponse:
    if not database.delete_passport_entry(entry_id):
        return _redirect_with_flash("/passport", "That passport entry no longer exists.")
    return _redirect_with_flash("/passport", "Deleted the soda passport entry.")


@app.post("/activity/{entry_id}/update")
async def update_entry(request: Request, entry_id: int) -> RedirectResponse:
    form = await request.form()
    settings = _current_settings()
    local_now = _parse_local_datetime(str(form.get("consumed_at_local", "")), settings, settings.local_now())
    success = database.update_entry(
        entry_id,
        soda_name=str(form.get("soda_name", "")).strip(),
        brand=str(form.get("brand", "")).strip(),
        caffeine_mg=float(str(form.get("caffeine_mg", "0")).strip() or 0),
        local_now=local_now,
        reason=str(form.get("reason", "")).strip(),
        notes=str(form.get("notes", "")).strip(),
    )
    if not success:
        return _redirect_with_flash("/activity", "That log entry no longer exists.")
    return _redirect_with_flash("/activity", "Updated the log entry.")


@app.post("/activity/{entry_id}/delete")
async def delete_entry(entry_id: int) -> RedirectResponse:
    if not database.delete_entry(entry_id):
        return _redirect_with_flash("/activity", "That log entry no longer exists.")
    return _redirect_with_flash("/activity", "Deleted the log entry.")


@app.get("/catalog", response_class=HTMLResponse)
async def catalog_page(request: Request) -> HTMLResponse:
    settings = _current_settings()
    local_now = settings.local_now()
    context = _build_common_context(
        request,
        settings=settings,
        local_now=local_now,
        flash_message=_parse_flash(request),
    )
    return templates.TemplateResponse("catalog.html", context)


@app.post("/catalog/state")
async def save_catalog_state(request: Request) -> RedirectResponse:
    form = await request.form()
    soda_id = str(form.get("soda_id", "")).strip()
    if not soda_id:
        return _redirect_with_flash("/catalog", "Missing soda ID.")

    database.save_soda_state(
        soda_id=soda_id,
        is_available=parse_bool(form.get("is_available"), False),
        preference=str(form.get("preference", "neutral")).strip() or "neutral",
        temp_ban_until=_parse_optional_date(str(form.get("temp_ban_until", "")).strip() or None),
    )
    return _redirect_with_flash("/catalog", "Saved catalog controls.")


@app.post("/catalog/import", response_class=HTMLResponse)
async def import_catalog(request: Request) -> HTMLResponse:
    settings = _current_settings()
    local_now = settings.local_now()
    form = await request.form()
    upload = form.get("upload")
    if upload is None or not hasattr(upload, "read"):
        context = _build_common_context(
            request,
            settings=settings,
            local_now=local_now,
            flash_message="Choose a CSV file first.",
        )
        context["import_feedback"] = {
            "status": "error",
            "title": "Import failed",
            "details": ["No upload was received."],
        }
        return templates.TemplateResponse("catalog.html", context)

    raw_bytes = await upload.read()
    try:
        raw_text = raw_bytes.decode("utf-8")
    except UnicodeDecodeError:
        context = _build_common_context(
            request,
            settings=settings,
            local_now=local_now,
            flash_message="CSV upload must be UTF-8 encoded.",
        )
        context["import_feedback"] = {
            "status": "error",
            "title": "Import failed",
            "details": ["The uploaded file could not be decoded as UTF-8."],
        }
        return templates.TemplateResponse("catalog.html", context)

    preview_sodas, diagnostics = catalog.preview_upload(raw_text)
    fatal_warning = any(
        warning.startswith("Missing required") or "missing a header row" in warning
        for warning in diagnostics.warnings
    )
    if fatal_warning:
        context = _build_common_context(
            request,
            settings=settings,
            local_now=local_now,
            flash_message="CSV validation failed.",
        )
        context["import_feedback"] = {
            "status": "error",
            "title": "Import failed",
            "details": list(diagnostics.warnings),
        }
        return templates.TemplateResponse("catalog.html", context)

    backup_path = _backup_catalog_file(settings)
    _, updated_diagnostics = catalog.replace_with_text(raw_text)
    detail_lines = [
        f"Loaded {updated_diagnostics.loaded_rows} enabled sodas.",
        f"Skipped {updated_diagnostics.disabled_rows} disabled rows and {updated_diagnostics.invalid_rows} invalid rows.",
    ]
    if backup_path is not None:
        detail_lines.append(f"Previous CSV backed up to {backup_path.name}.")
    detail_lines.extend(updated_diagnostics.warnings)
    context = _build_common_context(
        request,
        settings=settings,
        local_now=local_now,
        flash_message="Catalog imported.",
    )
    context["import_feedback"] = {
        "status": "success",
        "title": "Catalog imported",
        "details": detail_lines,
    }
    return templates.TemplateResponse("catalog.html", context)


@app.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request) -> HTMLResponse:
    settings = _current_settings()
    local_now = settings.local_now()
    context = _build_common_context(
        request,
        settings=settings,
        local_now=local_now,
        flash_message=_parse_flash(request),
    )
    context["settings_items"] = settings.display_items(settings_store.override_keys())
    return templates.TemplateResponse("settings.html", context)


@app.post("/settings/save")
async def save_settings(request: Request) -> RedirectResponse:
    form = await request.form()
    try:
        settings_store.save({key: str(value) for key, value in form.items()})
    except ValueError as exc:
        LOGGER.warning("Invalid settings override: %s", exc)
        return _redirect_with_flash("/settings", f"Could not save settings: {exc}")
    return _redirect_with_flash("/settings", "Saved runtime settings.")


@app.post("/settings/reset")
async def reset_settings() -> RedirectResponse:
    settings_store.reset()
    return _redirect_with_flash("/settings", "Cleared saved runtime overrides.")


@app.post("/backups/create")
async def create_backup() -> RedirectResponse:
    settings = _current_settings()
    backup_path = database.create_database_backup(settings.backup_dir)
    catalog_backup = _backup_catalog_file(settings)
    message = f"Created database backup {backup_path.name}."
    if catalog_backup is not None:
        message += f" Catalog backup {catalog_backup.name} created too."
    return _redirect_with_flash("/settings", message)


@app.get("/exports/consumption.csv")
async def export_consumption_csv() -> Response:
    payload = database.export_consumption_csv()
    return Response(
        content=payload,
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="soda-picker-consumption.csv"'},
    )


@app.get("/exports/recommendations.csv")
async def export_recommendations_csv() -> Response:
    payload = database.export_recommendation_csv()
    return Response(
        content=payload,
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="soda-picker-recommendations.csv"'},
    )


@app.get("/exports/passport.csv")
async def export_passport_csv() -> Response:
    payload = database.export_passport_csv()
    return Response(
        content=payload,
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="soda-picker-passport.csv"'},
    )


@app.get("/exports/wishlist.csv")
async def export_wishlist_csv() -> Response:
    payload = database.export_wishlist_csv()
    return Response(
        content=payload,
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="soda-picker-wishlist.csv"'},
    )


@app.get("/exports/catalog.csv")
async def export_catalog_csv() -> FileResponse:
    path = Path(base_settings.csv_path)
    return FileResponse(path, media_type="text/csv", filename=path.name)


@app.get("/exports/database.sqlite")
async def export_database_backup() -> FileResponse:
    settings = _current_settings()
    backup_path = database.create_database_backup(settings.backup_dir)
    return FileResponse(
        backup_path,
        media_type="application/octet-stream",
        filename=backup_path.name,
    )


@app.get("/exports/reminder.ics")
async def export_reminder_calendar() -> Response:
    settings = _current_settings()
    local_now = settings.local_now()
    rules = settings.effective_rules(local_now)
    start_at = datetime.combine(local_now.date(), rules.reminder_time, tzinfo=settings.timezone)
    end_at = start_at + timedelta(minutes=15)
    dtstart = start_at.strftime("%Y%m%dT%H%M%S")
    dtend = end_at.strftime("%Y%m%dT%H%M%S")
    payload = "\r\n".join(
        [
            "BEGIN:VCALENDAR",
            "VERSION:2.0",
            "PRODID:-//Soda Picker//EN",
            "BEGIN:VEVENT",
            f"UID:soda-picker-reminder@{settings.timezone_name}",
            f"DTSTAMP:{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}",
            f"DTSTART;TZID={settings.timezone_name}:{dtstart}",
            f"DTEND;TZID={settings.timezone_name}:{dtend}",
            "RRULE:FREQ=DAILY",
            "SUMMARY:Soda window is open",
            "DESCRIPTION:Local Soda Picker reminder.",
            "END:VEVENT",
            "END:VCALENDAR",
            "",
        ]
    )
    return Response(
        content=payload,
        media_type="text/calendar",
        headers={"Content-Disposition": 'attachment; filename="soda-picker-reminder.ics"'},
    )


@app.get("/healthz", response_class=JSONResponse)
async def healthz() -> JSONResponse:
    settings = _current_settings()
    local_now = settings.local_now()
    payload = {
        "status": "ok",
        "local_time": local_now.isoformat(),
        "timezone": settings.timezone_name,
        "sodas_loaded": len(catalog.list_sodas()),
        "csv_path": settings.csv_path,
        "database_path": settings.database_path,
        "override_count": len(settings_store.override_keys()),
        "access_control_mode": settings.access_control_mode,
    }
    return JSONResponse(payload)


if __name__ == "__main__":
    uvicorn.run(
        app,
        host=base_settings.app_host,
        port=base_settings.app_port,
        proxy_headers=True,
        forwarded_allow_ips="*",
    )

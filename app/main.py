from __future__ import annotations

import logging
import shutil
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlparse

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.trustedhost import TrustedHostMiddleware

from .catalog import DEFAULT_ESTIMATED_CAFFEINE_MG, SodaCatalog
from .config import Settings, parse_bool
from .database import Database
from .models import (
    BackupFile,
    CatalogItem,
    PREFERENCE_NEUTRAL,
    RECOMMENDATION_FEEDBACK_OPTIONS,
    Soda,
    PassportSummary,
    SodaState,
    UserAccount,
    WishlistSummary,
    format_calendar_date,
    format_clock_time,
)
from .pick_styles import build_pick_style_groups, resolve_pick_style_option
from .recommendation import RecommendationEngine
from .security import (
    AUTH_COOKIE_NAME,
    AccessControlMiddleware,
    BasicAuthMiddleware,
    RateLimitMiddleware,
    issue_auth_token,
    read_auth_token,
)
from .settings_store import SettingsStore
from .training import (
    build_training_menu,
    build_training_mode_options,
    build_training_strength_options,
    profile_from_storage,
    serialize_training_form,
)

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
RECOMMENDATION_FEEDBACK_LABEL_MAP = dict(RECOMMENDATION_FEEDBACK_OPTIONS)


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


def _asset_version(path: str) -> str:
    asset_path = Path("static") / path.lstrip("/")
    try:
        return str(asset_path.stat().st_mtime_ns)
    except OSError:
        return "0"


def _app_build_stamp() -> str:
    tracked_paths = (
        Path("app/main.py"),
        Path("templates/base.html"),
        Path("static/style.css"),
        Path("static/app.js"),
        Path("static/manifest.webmanifest"),
    )
    latest_mtime = 0.0
    for path in tracked_paths:
        try:
            latest_mtime = max(latest_mtime, path.stat().st_mtime)
        except OSError:
            continue
    if latest_mtime <= 0:
        return "unknown"
    return datetime.fromtimestamp(latest_mtime, tz=timezone.utc).strftime("%Y%m%d%H%M")


templates.env.globals["asset_version"] = _asset_version


@asynccontextmanager
async def lifespan(app: FastAPI):
    database.initialize(base_settings)
    catalog.refresh(force=True)
    app.state.catalog = catalog
    app.state.database = database
    app.state.settings_store = settings_store
    yield


app = FastAPI(title="Soda Picker", version="2.0.0", lifespan=lifespan)
templates.env.globals["app_version_label"] = f"v{app.version} ({_app_build_stamp()})"
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
        exempt_paths={"/healthz", "/login", "/logout", "/manifest.webmanifest", "/service-worker.js"},
        identity_validator=database.user_exists,
    )

if base_settings.basic_auth_enabled:
    app.add_middleware(
        BasicAuthMiddleware,
        username=base_settings.basic_auth_username,
        password=base_settings.basic_auth_password,
        exempt_paths={"/healthz"},
    )


def _current_settings(user: UserAccount | None = None) -> Settings:
    if user is None:
        return base_settings
    return settings_store.current(user.id)


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


def _parse_recommendation_feedback(raw_value: str | None) -> str:
    cleaned = (raw_value or "").strip().lower()
    if cleaned not in RECOMMENDATION_FEEDBACK_LABEL_MAP:
        raise ValueError("Choose a valid recommendation feedback option.")
    return cleaned


def _find_catalog_match(*, soda_name: str, brand: str = "") -> Soda | None:
    normalized_name = soda_name.strip().casefold()
    normalized_brand = brand.strip().casefold()
    normalized_display = f"{brand.strip()} {soda_name.strip()}".strip().casefold()
    for soda in catalog.list_sodas():
        if soda.name.strip().casefold() == normalized_name and soda.brand.strip().casefold() == normalized_brand:
            return soda
        if soda.display_name.strip().casefold() == normalized_display:
            return soda
    return None


def _current_user(request: Request) -> UserAccount | None:
    cached = request.scope.get("soda_picker_user_account")
    if isinstance(cached, UserAccount):
        return cached

    if not base_settings.access_control_enabled:
        user = database.get_local_user()
        request.scope["soda_picker_user_account"] = user
        request.scope["soda_picker_user"] = user.username
        return user

    username = _authenticated_username(request, base_settings)
    if username is None:
        return None

    user = database.get_user_by_username(username)
    if user is None:
        return None

    request.scope["soda_picker_user_account"] = user
    return user


def _settings_for_request(request: Request) -> tuple[Settings, UserAccount | None]:
    user = _current_user(request)
    return _current_settings(user), user


def _build_catalog_items(local_now: datetime, *, user_id: int | None) -> list[CatalogItem]:
    sodas = catalog.list_sodas()
    state_map = database.get_soda_state_map(user_id) if user_id is not None else {}
    items: list[CatalogItem] = []
    for soda in sodas:
        state = state_map.get(soda.id, SodaState(soda_id=soda.id, preference=PREFERENCE_NEUTRAL))
        items.append(CatalogItem(soda=soda, state=state))
    return items


def _merged_recent_soda_ids(user_id: int | None, limit: int) -> list[str]:
    if user_id is None:
        return []

    ordered: list[str] = []
    for source in (
        database.get_recent_consumed_catalog_ids(user_id, limit=limit),
        database.get_recent_recommended_ids(user_id, limit=limit),
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


def _build_auth_context(request: Request, *, settings: Settings, user: UserAccount | None) -> dict[str, Any]:
    current_path = request.url.path
    if request.url.query:
        current_path = f"{current_path}?{request.url.query}"
    can_write = (not base_settings.access_control_enabled) or user is not None
    write_locked = base_settings.access_control_mode == "writes" and not can_write
    return {
        "auth_username": user.username if user is not None else None,
        "current_user": user,
        "is_authenticated": user is not None,
        "is_admin": user.is_admin if user is not None else False,
        "can_write": can_write,
        "write_locked": write_locked,
        "access_control_enabled": base_settings.access_control_enabled,
        "access_control_mode": base_settings.access_control_mode,
        "access_control_mode_label": base_settings.access_control_mode_label,
        "access_login_url": f"/login?{urlencode({'next': current_path})}",
    }


def _build_common_context(
    request: Request,
    *,
    settings: Settings,
    local_now: datetime,
    flash_message: str | None = None,
) -> dict[str, Any]:
    user = _current_user(request)
    user_id = user.id if user is not None else None
    rules = settings.effective_rules(local_now)
    catalog_items = _build_catalog_items(local_now, user_id=user_id)
    if user_id is None:
        today_entries = []
        daily_total = 0.0
        recent_recommendations = []
        backup_files: list[BackupFile] = []
        override_keys: set[str] = set()
        passport_entries = []
        passport_summary: PassportSummary | None = None
        wishlist_entries = []
        wishlist_summary: WishlistSummary | None = None
    else:
        today_entries = database.get_today_entries(user_id, local_now)
        daily_total = database.get_today_caffeine_total(user_id, local_now)
        recent_recommendations = database.get_recent_recommendations(user_id, limit=8)
        backup_files = (
            _build_backup_list(settings.backup_dir, local_timezone=settings.timezone)
            if user.is_admin
            else []
        )
        override_keys = settings_store.override_keys(user_id)
        passport_entries = database.list_passport_entries(user_id, limit=8)
        passport_summary = database.get_passport_summary(user_id)
        wishlist_entries = database.list_wishlist_entries(user_id, limit=8, include_archived=False)
        wishlist_summary = database.get_wishlist_summary(user_id)

    if passport_summary is None:
        passport_summary = PassportSummary()
        wishlist_summary = WishlistSummary()

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
        "recent_recommendations": recent_recommendations,
        "recommendation_feedback_options": RECOMMENDATION_FEEDBACK_OPTIONS,
        "backup_files": backup_files,
        "override_keys": override_keys,
        "passport_summary": passport_summary,
        "recent_passport_entries": passport_entries,
        "wishlist_summary": wishlist_summary,
        "recent_wishlist_entries": wishlist_entries,
    }
    context.update(_build_auth_context(request, settings=settings, user=user))
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
    pick_style_value: str | None = None,
) -> dict[str, Any]:
    context = _build_common_context(
        request,
        settings=settings,
        local_now=local_now,
        flash_message=flash_message,
    )
    pick_style_groups = build_pick_style_groups(context["catalog_items"])
    selected_pick_style = resolve_pick_style_option(pick_style_value, context["catalog_items"])
    context.update(
        {
            "recommendation": recommendation,
            "recommendation_id": recommendation_id,
            "chaos_mode": settings.chaos_mode_default if chaos_mode is None else chaos_mode,
            "pick_style_groups": pick_style_groups,
            "pick_style_value": selected_pick_style.value,
            "pick_style_label": selected_pick_style.label,
            "pick_style_description": selected_pick_style.description,
            "available_count": sum(1 for item in context["catalog_items"] if item.state.is_available),
            "favorite_count": sum(1 for item in context["catalog_items"] if item.state.preference == "favorite"),
            "manual_entry_time": local_now.strftime("%Y-%m-%dT%H:%M"),
            "passport_entry_date": local_now.date().isoformat(),
        }
    )
    return context


def _build_training_context(*, user_id: int, catalog_items: list[CatalogItem]) -> dict[str, Any]:
    passport_entries = database.list_passport_entries(user_id, limit=400)
    training_profile = profile_from_storage(database.get_taste_training(user_id))
    training_menu = build_training_menu(
        passport_entries=passport_entries,
        catalog_items=catalog_items,
        profile=training_profile,
    )
    return {
        "training_profile": training_profile,
        "training_menu": training_menu,
        "training_mode_options": build_training_mode_options(),
        "training_strength_options": build_training_strength_options(),
    }


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request) -> HTMLResponse:
    settings, _ = _settings_for_request(request)
    local_now = settings.local_now()
    context = _dashboard_context(
        request,
        settings=settings,
        local_now=local_now,
        flash_message=_parse_flash(request),
    )
    return templates.TemplateResponse(request, "index.html", context)


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request) -> HTMLResponse:
    if not base_settings.access_control_enabled:
        return _redirect_with_flash("/", "Access control is not enabled.")

    if _current_user(request) is not None:
        next_path = _safe_next_path(request.query_params.get("next"))
        return RedirectResponse(url=next_path, status_code=303)

    context = {
        "request": request,
        "settings": base_settings,
        "flash_message": _parse_flash(request),
        "login_error": None,
        "next_path": _safe_next_path(request.query_params.get("next")),
        "write_locked": False,
        "can_write": True,
        "access_control_enabled": base_settings.access_control_enabled,
        "access_control_mode": base_settings.access_control_mode,
        "access_control_mode_label": base_settings.access_control_mode_label,
        "auth_username": None,
        "current_user": None,
        "is_authenticated": False,
        "is_admin": False,
        "access_login_url": "/login",
    }
    return templates.TemplateResponse(request, "login.html", context)


@app.post("/login", response_class=HTMLResponse)
async def login_submit(request: Request) -> Response:
    if not base_settings.access_control_enabled:
        return _redirect_with_flash("/", "Access control is not enabled.")

    form = await request.form()
    next_path = _safe_next_path(str(form.get("next", "/")))
    username = str(form.get("username", "")).strip()
    password = str(form.get("password", ""))
    user = database.authenticate_user(username, password)
    if user is None:
        context = {
            "request": request,
            "settings": base_settings,
            "flash_message": None,
            "login_error": "That username or password did not match.",
            "next_path": next_path,
            "write_locked": False,
            "can_write": True,
            "access_control_enabled": base_settings.access_control_enabled,
            "access_control_mode": base_settings.access_control_mode,
            "access_control_mode_label": base_settings.access_control_mode_label,
            "auth_username": None,
            "current_user": None,
            "is_authenticated": False,
            "is_admin": False,
            "access_login_url": "/login",
        }
        return templates.TemplateResponse(request, "login.html", context, status_code=401)

    token = issue_auth_token(
        user.username,
        base_settings.access_control_secret,
        lifetime_seconds=base_settings.access_control_session_days * 86400,
    )
    response = RedirectResponse(url=next_path, status_code=303)
    response.set_cookie(
        AUTH_COOKIE_NAME,
        token,
        max_age=base_settings.access_control_session_days * 86400,
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
    settings, user = _settings_for_request(request)
    if user is None:
        return templates.TemplateResponse(
            request,
            "login.html",
            {
                "request": request,
                "settings": base_settings,
                "flash_message": None,
                "login_error": "Sign in to generate recommendations.",
                "next_path": "/",
                "write_locked": False,
                "can_write": True,
                "access_control_enabled": base_settings.access_control_enabled,
                "access_control_mode": base_settings.access_control_mode,
                "access_control_mode_label": base_settings.access_control_mode_label,
                "auth_username": None,
                "current_user": None,
                "is_authenticated": False,
                "is_admin": False,
                "access_login_url": "/login",
            },
            status_code=401,
        )

    form = await request.form()
    local_now = settings.local_now()
    rules = settings.effective_rules(local_now)
    chaos_mode = parse_bool(form.get("chaos_mode"), settings.chaos_mode_default)
    catalog_items = _build_catalog_items(local_now, user_id=user.id)
    pick_style = resolve_pick_style_option(str(form.get("pick_style", "")).strip(), catalog_items)
    today_entries = database.get_today_entries(user.id, local_now)
    daily_total = database.get_today_caffeine_total(user.id, local_now)
    training_profile = profile_from_storage(database.get_taste_training(user.id))

    recommendation = engine.recommend(
        rules=rules,
        catalog_items=catalog_items,
        daily_caffeine_total=daily_total,
        recent_soda_ids=_merged_recent_soda_ids(user.id, rules.duplicate_lookback),
        today_entries=today_entries,
        local_now=local_now,
        chaos_mode=chaos_mode,
        pick_style=pick_style,
        training_profile=training_profile if training_profile.uses_training else None,
    )
    recommendation_id = database.log_recommendation(user.id, recommendation, local_now)
    context = _dashboard_context(
        request,
        settings=settings,
        local_now=local_now,
        recommendation=recommendation,
        recommendation_id=recommendation_id,
        chaos_mode=chaos_mode,
        pick_style_value=pick_style.value,
    )
    return templates.TemplateResponse(request, "index.html", context)


@app.post("/log-consumption")
async def log_consumption(request: Request) -> RedirectResponse:
    settings, user = _settings_for_request(request)
    if user is None:
        return _redirect_with_flash("/login", "Sign in to log drinks.")

    form = await request.form()
    soda_id = str(form.get("soda_id", "")).strip()
    soda = catalog.get_by_id(soda_id)
    if soda is None:
        return _redirect_with_flash("/", "That soda is no longer available in the catalog.")

    recommendation_id = form.get("recommendation_id")
    recommendation_value = int(recommendation_id) if recommendation_id else None
    local_now = settings.local_now()
    database.log_catalog_consumption(
        user.id,
        soda,
        local_now,
        reason=str(form.get("reason", "")).strip(),
        chaos_mode=parse_bool(form.get("chaos_mode"), settings.chaos_mode_default),
        notes=str(form.get("notes", "")).strip(),
        recommendation_id=recommendation_value,
    )
    return _redirect_with_flash("/", f"Logged {soda.display_name}.")


@app.post("/recommendations/{recommendation_id}/feedback")
async def save_recommendation_feedback(request: Request, recommendation_id: int) -> RedirectResponse:
    _, user = _settings_for_request(request)
    if user is None:
        return _redirect_with_flash("/login", "Sign in to save recommendation feedback.")

    form = await request.form()
    return_to = _safe_next_path(str(form.get("return_to", "/activity")))
    try:
        feedback = _parse_recommendation_feedback(str(form.get("feedback", "")))
    except ValueError as exc:
        return _redirect_with_flash(return_to, str(exc))

    if not database.set_recommendation_feedback(user.id, recommendation_id, feedback):
        return _redirect_with_flash(return_to, "That recommendation no longer exists.")
    return _redirect_with_flash(return_to, f"Saved feedback: {RECOMMENDATION_FEEDBACK_LABEL_MAP[feedback]}.")


@app.post("/manual-entry")
async def log_manual_entry(request: Request) -> RedirectResponse:
    settings, user = _settings_for_request(request)
    if user is None:
        return _redirect_with_flash("/login", "Sign in to log caffeine.")

    form = await request.form()
    local_now = _parse_local_datetime(str(form.get("consumed_at_local", "")), settings, settings.local_now())
    name = str(form.get("name", "")).strip()
    if not name:
        return _redirect_with_flash("/activity", "Manual entry needs a name.")
    caffeine_mg = float(str(form.get("caffeine_mg", "0")).strip() or 0)
    database.log_manual_entry(
        user.id,
        name=name,
        caffeine_mg=caffeine_mg,
        local_now=local_now,
        notes=str(form.get("notes", "")).strip(),
    )
    return _redirect_with_flash("/activity", f"Logged manual caffeine entry for {name}.")


@app.post("/manual-soda-entry")
async def log_manual_soda_entry(request: Request) -> RedirectResponse:
    settings, user = _settings_for_request(request)
    if user is None:
        return _redirect_with_flash("/login", "Sign in to log sodas.")

    form = await request.form()
    local_now = _parse_local_datetime(str(form.get("consumed_at_local", "")), settings, settings.local_now())
    soda_name = str(form.get("soda_name", "")).strip()
    if not soda_name:
        return _redirect_with_flash("/activity", "Manual soda entry needs a soda name.")

    brand = str(form.get("brand", "")).strip()
    contains_caffeine = parse_bool(form.get("contains_caffeine"), False)
    caffeine_raw = str(form.get("caffeine_mg", "")).strip()
    caffeine_value = float(caffeine_raw) if caffeine_raw else None
    database.log_manual_soda_entry(
        user.id,
        soda_name=soda_name,
        brand=brand,
        local_now=local_now,
        contains_caffeine=contains_caffeine,
        caffeine_mg=caffeine_value,
        notes=str(form.get("notes", "")).strip(),
    )
    message = f"Logged manual soda entry for {soda_name}."
    if contains_caffeine and caffeine_value is None:
        message += f" Used the default {DEFAULT_ESTIMATED_CAFFEINE_MG:g} mg estimate."
    return _redirect_with_flash("/activity", message)


@app.get("/activity", response_class=HTMLResponse)
async def activity_page(request: Request) -> HTMLResponse:
    settings, user = _settings_for_request(request)
    local_now = settings.local_now()
    context = _build_common_context(
        request,
        settings=settings,
        local_now=local_now,
        flash_message=_parse_flash(request),
    )
    context.update(
        {
            "all_entries": database.get_recent_entries(user.id, limit=60) if user is not None else [],
            "manual_entry_time": local_now.strftime("%Y-%m-%dT%H:%M"),
        }
    )
    return templates.TemplateResponse(request, "activity.html", context)


@app.get("/wishlist", response_class=HTMLResponse)
async def wishlist_page(request: Request) -> HTMLResponse:
    settings, user = _settings_for_request(request)
    local_now = settings.local_now()
    context = _build_common_context(
        request,
        settings=settings,
        local_now=local_now,
        flash_message=_parse_flash(request),
    )
    context["wishlist_entries"] = database.list_wishlist_entries(user.id, limit=300) if user is not None else []
    return templates.TemplateResponse(request, "wishlist.html", context)


@app.post("/wishlist/add")
async def add_wishlist_entry(request: Request) -> RedirectResponse:
    _, user = _settings_for_request(request)
    if user is None:
        return _redirect_with_flash("/login", "Sign in to save wishlist entries.")

    form = await request.form()
    soda_name = str(form.get("soda_name", "")).strip()
    brand = str(form.get("brand", "")).strip()
    if not soda_name:
        return _redirect_with_flash("/wishlist", "Wishlist entries need a soda name.")

    existing = database.find_active_wishlist_entry(user.id, soda_name=soda_name, brand=brand)
    if existing is not None:
        return _redirect_with_flash("/wishlist", f"{existing.display_name} is already on the active wishlist.")

    try:
        priority = _parse_priority(str(form.get("priority", "3")))
    except ValueError as exc:
        return _redirect_with_flash("/wishlist", str(exc))

    database.add_wishlist_entry(
        user.id,
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
    _, user = _settings_for_request(request)
    if user is None:
        return _redirect_with_flash("/login", "Sign in to save wishlist entries.")

    form = await request.form()
    soda_id = str(form.get("soda_id", "")).strip()
    soda = catalog.get_by_id(soda_id)
    if soda is None:
        return _redirect_with_flash("/catalog", "That soda is no longer in the catalog.")

    existing = database.find_active_wishlist_entry(user.id, soda_name=soda.name, brand=soda.brand)
    if existing is not None:
        return _redirect_with_flash("/catalog", f"{existing.display_name} is already on the active wishlist.")

    database.add_wishlist_entry(
        user.id,
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
    _, user = _settings_for_request(request)
    if user is None:
        return _redirect_with_flash("/login", "Sign in to save wishlist entries.")

    form = await request.form()
    entry_id = int(str(form.get("entry_id", "0")).strip() or 0)
    passport_entry = database.get_passport_entry(user.id, entry_id)
    if passport_entry is None:
        return _redirect_with_flash("/passport", "That passport entry no longer exists.")

    existing = database.find_active_wishlist_entry(
        user.id,
        soda_name=passport_entry.soda_name,
        brand=passport_entry.brand,
    )
    if existing is not None:
        return _redirect_with_flash("/passport", f"{existing.display_name} is already on the active wishlist.")

    database.add_wishlist_entry(
        user.id,
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
    _, user = _settings_for_request(request)
    if user is None:
        return _redirect_with_flash("/login", "Sign in to update wishlist entries.")

    form = await request.form()
    soda_name = str(form.get("soda_name", "")).strip()
    if not soda_name:
        return _redirect_with_flash("/wishlist", "Wishlist entries need a soda name.")

    try:
        priority = _parse_priority(str(form.get("priority", "3")))
    except ValueError as exc:
        return _redirect_with_flash("/wishlist", str(exc))

    success = database.update_wishlist_entry(
        user.id,
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
async def delete_wishlist_entry(request: Request, entry_id: int) -> RedirectResponse:
    _, user = _settings_for_request(request)
    if user is None:
        return _redirect_with_flash("/login", "Sign in to delete wishlist entries.")

    if not database.delete_wishlist_entry(user.id, entry_id):
        return _redirect_with_flash("/wishlist", "That wishlist entry no longer exists.")
    return _redirect_with_flash("/wishlist", "Deleted the wishlist entry.")


@app.get("/passport", response_class=HTMLResponse)
async def passport_page(request: Request) -> HTMLResponse:
    settings, user = _settings_for_request(request)
    local_now = settings.local_now()
    context = _build_common_context(
        request,
        settings=settings,
        local_now=local_now,
        flash_message=_parse_flash(request),
    )
    context.update(
        {
            "passport_entries": database.list_passport_entries(user.id, limit=300) if user is not None else [],
            "passport_duplicate_groups": database.list_passport_duplicate_groups(user.id, limit=12) if user is not None else [],
            "passport_insights": database.get_passport_insights(user.id, limit=5) if user is not None else None,
            "passport_entry_date": local_now.date().isoformat(),
        }
    )
    return templates.TemplateResponse(request, "passport.html", context)


@app.post("/passport/add")
async def add_passport_entry(request: Request) -> RedirectResponse:
    settings, user = _settings_for_request(request)
    if user is None:
        return _redirect_with_flash("/login", "Sign in to save passport entries.")

    form = await request.form()
    soda_name = str(form.get("soda_name", "")).strip()
    if not soda_name:
        return _redirect_with_flash("/passport", "Passport entry needs a soda name.")

    tried_on = _parse_optional_date(str(form.get("tried_on", "")).strip() or None) or settings.local_now().date()
    try:
        rating = _parse_optional_rating(str(form.get("rating", "")))
    except ValueError as exc:
        return _redirect_with_flash("/passport", str(exc))

    database.add_passport_entry(
        user.id,
        soda_name=soda_name,
        brand=str(form.get("brand", "")).strip(),
        country=str(form.get("country", "")).strip(),
        region=str(form.get("region", "")).strip(),
        city=str(form.get("city", "")).strip(),
        category=str(form.get("category", "")).strip(),
        tried_on=tried_on,
        where_tried=str(form.get("where_tried", "")).strip(),
        contains_caffeine=parse_bool(form.get("contains_caffeine"), False),
        rating=rating,
        would_try_again=parse_bool(form.get("would_try_again"), False),
        notes=str(form.get("notes", "")).strip(),
    )
    return _redirect_with_flash("/passport", f"Added {soda_name} to your soda passport.")


@app.post("/passport/{entry_id}/update")
async def update_passport_entry(request: Request, entry_id: int) -> RedirectResponse:
    _, user = _settings_for_request(request)
    if user is None:
        return _redirect_with_flash("/login", "Sign in to update passport entries.")

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
        user.id,
        entry_id,
        soda_name=soda_name,
        brand=str(form.get("brand", "")).strip(),
        country=str(form.get("country", "")).strip(),
        region=str(form.get("region", "")).strip(),
        city=str(form.get("city", "")).strip(),
        category=str(form.get("category", "")).strip(),
        tried_on=tried_on,
        where_tried=str(form.get("where_tried", "")).strip(),
        contains_caffeine=parse_bool(form.get("contains_caffeine"), False),
        rating=rating,
        would_try_again=parse_bool(form.get("would_try_again"), False),
        notes=str(form.get("notes", "")).strip(),
    )
    if not success:
        return _redirect_with_flash("/passport", "That passport entry no longer exists.")
    return _redirect_with_flash("/passport", "Updated the soda passport entry.")


@app.post("/passport/{entry_id}/delete")
async def delete_passport_entry(request: Request, entry_id: int) -> RedirectResponse:
    _, user = _settings_for_request(request)
    if user is None:
        return _redirect_with_flash("/login", "Sign in to delete passport entries.")

    if not database.delete_passport_entry(user.id, entry_id):
        return _redirect_with_flash("/passport", "That passport entry no longer exists.")
    return _redirect_with_flash("/passport", "Deleted the soda passport entry.")


@app.post("/passport/merge-duplicates")
async def merge_passport_duplicates(request: Request) -> RedirectResponse:
    _, user = _settings_for_request(request)
    if user is None:
        return _redirect_with_flash("/login", "Sign in to merge passport entries.")

    form = await request.form()
    raw_ids = str(form.get("entry_ids", "")).strip()
    try:
        entry_ids = [int(part) for part in raw_ids.split(",") if part.strip()]
    except ValueError:
        return _redirect_with_flash("/passport", "Could not read the duplicate entry list.")

    merged = database.merge_passport_entries(user.id, entry_ids)
    if merged is None:
        return _redirect_with_flash("/passport", "Those passport entries could not be merged.")
    return _redirect_with_flash("/passport", f"Merged duplicate entries into {merged.display_name}.")


@app.post("/passport/{entry_id}/own")
async def own_passport_soda(request: Request, entry_id: int) -> RedirectResponse:
    settings, user = _settings_for_request(request)
    if user is None:
        return _redirect_with_flash("/login", "Sign in to mark passport sodas as owned.")

    passport_entry = database.get_passport_entry(user.id, entry_id)
    if passport_entry is None:
        return _redirect_with_flash("/passport", "That passport entry no longer exists.")

    soda = _find_catalog_match(soda_name=passport_entry.soda_name, brand=passport_entry.brand)
    backup_path: Path | None = None
    if soda is None:
        if not user.is_admin:
            return _redirect_with_flash(
                "/passport",
                "That soda is not in the shared catalog yet. An admin has to add it before it can become owned inventory.",
            )
        backup_path = _backup_catalog_file(settings)
        try:
            soda = catalog.add_soda(
                name=passport_entry.soda_name,
                brand=passport_entry.brand,
                category=passport_entry.category,
                contains_caffeine=passport_entry.contains_caffeine,
                is_diet=False,
                tags=(),
                priority=2 if passport_entry.would_try_again else 1,
            )
        except ValueError as exc:
            return _redirect_with_flash("/passport", f"Could not add soda: {exc}")

    current_state = database.get_soda_state_map(user.id).get(soda.id, SodaState(soda_id=soda.id, preference=PREFERENCE_NEUTRAL))
    database.save_soda_state(
        user.id,
        soda_id=soda.id,
        is_available=True,
        preference=current_state.preference,
        temp_ban_until=current_state.temp_ban_until,
    )

    message = f"Marked {soda.display_name} as owned in your catalog inventory."
    if backup_path is not None:
        message += f" Added it to the shared catalog too. Backup saved as {backup_path.name}."
    return _redirect_with_flash("/passport", message)


@app.post("/activity/{entry_id}/update")
async def update_entry(request: Request, entry_id: int) -> RedirectResponse:
    settings, user = _settings_for_request(request)
    if user is None:
        return _redirect_with_flash("/login", "Sign in to edit activity.")

    form = await request.form()
    local_now = _parse_local_datetime(str(form.get("consumed_at_local", "")), settings, settings.local_now())
    success = database.update_entry(
        user.id,
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
async def delete_entry(request: Request, entry_id: int) -> RedirectResponse:
    _, user = _settings_for_request(request)
    if user is None:
        return _redirect_with_flash("/login", "Sign in to edit activity.")

    if not database.delete_entry(user.id, entry_id):
        return _redirect_with_flash("/activity", "That log entry no longer exists.")
    return _redirect_with_flash("/activity", "Deleted the log entry.")


@app.post("/activity/{entry_id}/to-passport")
async def add_activity_entry_to_passport(request: Request, entry_id: int) -> RedirectResponse:
    _, user = _settings_for_request(request)
    if user is None:
        return _redirect_with_flash("/login", "Sign in to save passport entries.")

    entry = database.get_entry(user.id, entry_id)
    if entry is None or entry.entry_type != "manual_soda":
        return _redirect_with_flash("/activity", "That manual soda entry no longer exists.")

    tried_on = entry.consumed_at_local.date()
    existing = database.find_passport_entry(
        user.id,
        soda_name=entry.soda_name,
        brand=entry.brand,
        tried_on=tried_on,
    )
    if existing is not None:
        return _redirect_with_flash("/activity", f"{existing.display_name} is already in your passport for that day.")

    database.add_passport_entry(
        user.id,
        soda_name=entry.soda_name,
        brand=entry.brand,
        country="",
        region="",
        city="",
        category="",
        tried_on=tried_on,
        where_tried="",
        contains_caffeine=entry.caffeine_mg > 0,
        rating=None,
        would_try_again=False,
        notes=entry.notes,
    )
    return _redirect_with_flash("/activity", f"Added {entry.display_name} to your passport.")


@app.post("/activity/{entry_id}/to-catalog")
async def add_activity_entry_to_catalog(request: Request, entry_id: int) -> RedirectResponse:
    settings, user = _settings_for_request(request)
    if user is None:
        return _redirect_with_flash("/login", "Sign in as an admin to add shared catalog sodas.")
    if not user.is_admin:
        return _redirect_with_flash("/activity", "Only admins can add shared sodas to the catalog CSV.")

    entry = database.get_entry(user.id, entry_id)
    if entry is None or entry.entry_type != "manual_soda":
        return _redirect_with_flash("/activity", "That manual soda entry no longer exists.")

    existing = _find_catalog_match(soda_name=entry.soda_name, brand=entry.brand)
    if existing is not None:
        return _redirect_with_flash("/activity", f"{existing.display_name} is already in the shared catalog.")

    backup_path = _backup_catalog_file(settings)
    try:
        soda = catalog.add_soda(
            name=entry.soda_name,
            brand=entry.brand,
            category="",
            contains_caffeine=entry.caffeine_mg > 0,
            is_diet=False,
            tags=(),
        )
    except ValueError as exc:
        return _redirect_with_flash("/activity", f"Could not add soda: {exc}")

    message = f"Added {soda.display_name} to the shared catalog."
    if backup_path is not None:
        message += f" Backup saved as {backup_path.name}."
    return _redirect_with_flash("/activity", message)


@app.get("/catalog", response_class=HTMLResponse)
async def catalog_page(request: Request) -> HTMLResponse:
    settings, _ = _settings_for_request(request)
    local_now = settings.local_now()
    context = _build_common_context(
        request,
        settings=settings,
        local_now=local_now,
        flash_message=_parse_flash(request),
    )
    return templates.TemplateResponse(request, "catalog.html", context)


@app.post("/catalog/state")
async def save_catalog_state(request: Request) -> RedirectResponse:
    _, user = _settings_for_request(request)
    if user is None:
        return _redirect_with_flash("/login", "Sign in to save catalog controls.")

    form = await request.form()
    soda_id = str(form.get("soda_id", "")).strip()
    if not soda_id:
        return _redirect_with_flash("/catalog", "Missing soda ID.")

    database.save_soda_state(
        user.id,
        soda_id=soda_id,
        is_available=parse_bool(form.get("is_available"), False),
        preference=str(form.get("preference", "neutral")).strip() or "neutral",
        temp_ban_until=_parse_optional_date(str(form.get("temp_ban_until", "")).strip() or None),
    )
    return _redirect_with_flash("/catalog", "Saved catalog controls.")


@app.post("/catalog/add")
async def add_catalog_soda(request: Request) -> RedirectResponse:
    settings, user = _settings_for_request(request)
    if user is None:
        return _redirect_with_flash("/login", "Sign in as an admin to add shared catalog sodas.")
    if not user.is_admin:
        return _redirect_with_flash("/catalog", "Only admins can add shared sodas to the catalog CSV.")

    form = await request.form()
    backup_path = _backup_catalog_file(settings)
    try:
        soda = catalog.add_soda(
            name=str(form.get("name", "")).strip(),
            brand=str(form.get("brand", "")).strip(),
            category=str(form.get("category", "")).strip(),
            contains_caffeine=parse_bool(form.get("contains_caffeine"), False),
            is_diet=parse_bool(form.get("is_diet"), False),
            tags=tuple(part.strip() for part in str(form.get("tags", "")).replace(",", "|").split("|") if part.strip()),
        )
    except ValueError as exc:
        return _redirect_with_flash("/catalog", f"Could not add soda: {exc}")

    message = f"Added {soda.display_name} to the shared catalog CSV."
    if backup_path is not None:
        message += f" Backup saved as {backup_path.name}."
    return _redirect_with_flash("/catalog", message)


@app.post("/catalog/import", response_class=HTMLResponse)
async def import_catalog(request: Request) -> HTMLResponse:
    settings, user = _settings_for_request(request)
    if user is None:
        return templates.TemplateResponse(
            request,
            "login.html",
            {
                "request": request,
                "settings": base_settings,
                "flash_message": None,
                "login_error": "Sign in as an admin to import a catalog.",
                "next_path": "/catalog",
                "write_locked": False,
                "can_write": True,
                "access_control_enabled": base_settings.access_control_enabled,
                "access_control_mode": base_settings.access_control_mode,
                "access_control_mode_label": base_settings.access_control_mode_label,
                "auth_username": None,
                "current_user": None,
                "is_authenticated": False,
                "is_admin": False,
                "access_login_url": "/login",
            },
            status_code=401,
        )
    if not user.is_admin:
        return templates.TemplateResponse(
            request,
            "catalog.html",
            {
                **_build_common_context(
                    request,
                    settings=settings,
                    local_now=settings.local_now(),
                    flash_message="Catalog import is reserved for admins.",
                ),
                "import_feedback": {
                    "status": "error",
                    "title": "Admin access required",
                    "details": ["Only admins can replace the shared catalog CSV."],
                },
            },
            status_code=403,
        )

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
        return templates.TemplateResponse(request, "catalog.html", context)

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
        return templates.TemplateResponse(request, "catalog.html", context)

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
        return templates.TemplateResponse(request, "catalog.html", context)

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
    return templates.TemplateResponse(request, "catalog.html", context)


@app.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request) -> HTMLResponse:
    settings, user = _settings_for_request(request)
    local_now = settings.local_now()
    context = _build_common_context(
        request,
        settings=settings,
        local_now=local_now,
        flash_message=_parse_flash(request),
    )
    override_keys = settings_store.override_keys(user.id) if user is not None else set()
    context["settings_items"] = settings.display_items(override_keys)
    context["managed_users"] = database.list_users() if user is not None and user.is_admin else []
    context["managed_user_count"] = database.count_users()
    context["managed_admin_count"] = database.count_admin_users()
    if user is not None:
        context.update(_build_training_context(user_id=user.id, catalog_items=context["catalog_items"]))
    return templates.TemplateResponse(request, "settings.html", context)


@app.post("/settings/save")
async def save_settings(request: Request) -> RedirectResponse:
    _, user = _settings_for_request(request)
    if user is None:
        return _redirect_with_flash("/login", "Sign in to save settings.")

    form = await request.form()
    try:
        settings_store.save(user.id, {key: str(value) for key, value in form.items()})
    except ValueError as exc:
        LOGGER.warning("Invalid settings override: %s", exc)
        return _redirect_with_flash("/settings", f"Could not save settings: {exc}")
    return _redirect_with_flash("/settings", "Saved runtime settings.")


@app.post("/settings/training/save")
async def save_training_settings(request: Request) -> RedirectResponse:
    settings, user = _settings_for_request(request)
    if user is None:
        return _redirect_with_flash("/login", "Sign in to save taste training.")

    form = await request.form()
    local_now = settings.local_now()
    catalog_items = _build_catalog_items(local_now, user_id=user.id)
    current_profile = profile_from_storage(database.get_taste_training(user.id))
    training_menu = build_training_menu(
        passport_entries=database.list_passport_entries(user.id, limit=400),
        catalog_items=catalog_items,
        profile=current_profile,
    )
    payload = serialize_training_form(
        boost_category_keys=form.getlist("boost_categories"),
        avoid_category_keys=form.getlist("avoid_categories"),
        boost_mood_values=form.getlist("boost_moods"),
        avoid_mood_values=form.getlist("avoid_moods"),
        mode=str(form.get("mode", "")),
        strength=str(form.get("strength", "")),
        allowed_category_keys={option.key for option in training_menu.category_options},
        allowed_mood_values={option.key for option in training_menu.mood_options},
    )
    database.set_taste_training(user.id, payload)
    return _redirect_with_flash("/settings", "Saved passport-based taste training.")


@app.post("/settings/training/reset")
async def reset_training_settings(request: Request) -> RedirectResponse:
    _, user = _settings_for_request(request)
    if user is None:
        return _redirect_with_flash("/login", "Sign in to clear taste training.")

    database.clear_taste_training(user.id)
    return _redirect_with_flash("/settings", "Cleared passport-based taste training.")


@app.post("/settings/reset")
async def reset_settings(request: Request) -> RedirectResponse:
    _, user = _settings_for_request(request)
    if user is None:
        return _redirect_with_flash("/login", "Sign in to reset settings.")

    settings_store.reset(user.id)
    return _redirect_with_flash("/settings", "Cleared saved runtime overrides.")


@app.post("/backups/create")
async def create_backup(request: Request) -> RedirectResponse:
    settings, user = _settings_for_request(request)
    if user is None:
        return _redirect_with_flash("/login", "Sign in as an admin to create backups.")
    if not user.is_admin:
        return _redirect_with_flash("/settings", "Only admins can create full backups.")

    backup_path = database.create_database_backup(settings.backup_dir)
    catalog_backup = _backup_catalog_file(settings)
    message = f"Created database backup {backup_path.name}."
    if catalog_backup is not None:
        message += f" Catalog backup {catalog_backup.name} created too."
    return _redirect_with_flash("/settings", message)


@app.post("/admin/users/create")
async def create_user_account(request: Request) -> RedirectResponse:
    _, user = _settings_for_request(request)
    if user is None:
        return _redirect_with_flash("/login", "Sign in as an admin to create accounts.")
    if not user.is_admin:
        return _redirect_with_flash("/settings", "Only admins can manage user accounts.")

    form = await request.form()
    try:
        created = database.create_user(
            username=str(form.get("username", "")).strip(),
            password=str(form.get("password", "")),
            is_admin=parse_bool(form.get("is_admin"), False),
        )
    except ValueError as exc:
        return _redirect_with_flash("/settings", str(exc))
    return _redirect_with_flash("/settings", f"Created account {created.username}.")


@app.post("/admin/users/{user_id}/update")
async def update_user_account(request: Request, user_id: int) -> RedirectResponse:
    _, user = _settings_for_request(request)
    if user is None:
        return _redirect_with_flash("/login", "Sign in as an admin to manage accounts.")
    if not user.is_admin:
        return _redirect_with_flash("/settings", "Only admins can manage user accounts.")

    form = await request.form()
    password = str(form.get("password", ""))
    try:
        updated = database.update_user(
            user_id,
            password=password if password.strip() else None,
            is_admin=parse_bool(form.get("is_admin"), False),
        )
    except ValueError as exc:
        return _redirect_with_flash("/settings", str(exc))
    if updated is None:
        return _redirect_with_flash("/settings", "That user account no longer exists.")
    return _redirect_with_flash("/settings", f"Updated account {updated.username}.")


@app.post("/admin/users/{user_id}/delete")
async def delete_user_account(request: Request, user_id: int) -> RedirectResponse:
    _, user = _settings_for_request(request)
    if user is None:
        return _redirect_with_flash("/login", "Sign in as an admin to manage accounts.")
    if not user.is_admin:
        return _redirect_with_flash("/settings", "Only admins can manage user accounts.")

    try:
        deleted = database.delete_user(user_id, acting_user_id=user.id)
    except ValueError as exc:
        return _redirect_with_flash("/settings", str(exc))
    if not deleted:
        return _redirect_with_flash("/settings", "That user account no longer exists.")
    return _redirect_with_flash("/settings", "Deleted the user account.")


@app.get("/exports/consumption.csv")
async def export_consumption_csv(request: Request) -> Response:
    _, user = _settings_for_request(request)
    if user is None:
        return Response("Authentication required.", status_code=401)

    payload = database.export_consumption_csv(user.id)
    return Response(
        content=payload,
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="soda-picker-consumption.csv"'},
    )


@app.get("/exports/recommendations.csv")
async def export_recommendations_csv(request: Request) -> Response:
    _, user = _settings_for_request(request)
    if user is None:
        return Response("Authentication required.", status_code=401)

    payload = database.export_recommendation_csv(user.id)
    return Response(
        content=payload,
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="soda-picker-recommendations.csv"'},
    )


@app.get("/exports/passport.csv")
async def export_passport_csv(request: Request) -> Response:
    _, user = _settings_for_request(request)
    if user is None:
        return Response("Authentication required.", status_code=401)

    payload = database.export_passport_csv(user.id)
    return Response(
        content=payload,
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="soda-picker-passport.csv"'},
    )


@app.get("/exports/wishlist.csv")
async def export_wishlist_csv(request: Request) -> Response:
    _, user = _settings_for_request(request)
    if user is None:
        return Response("Authentication required.", status_code=401)

    payload = database.export_wishlist_csv(user.id)
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
async def export_database_backup(request: Request) -> Response:
    settings, user = _settings_for_request(request)
    if user is None:
        return Response("Authentication required.", status_code=401)
    if not user.is_admin:
        return Response("Admin access required.", status_code=403)

    backup_path = database.create_database_backup(settings.backup_dir)
    return FileResponse(
        backup_path,
        media_type="application/octet-stream",
        filename=backup_path.name,
    )


@app.get("/exports/reminder.ics")
async def export_reminder_calendar(request: Request) -> Response:
    settings, user = _settings_for_request(request)
    if user is None:
        return Response("Authentication required.", status_code=401)

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


@app.get("/manifest.webmanifest")
async def web_app_manifest() -> Response:
    return FileResponse(
        Path("static/manifest.webmanifest"),
        media_type="application/manifest+json",
    )


@app.get("/service-worker.js")
async def service_worker() -> Response:
    return FileResponse(
        Path("static/service-worker.js"),
        media_type="application/javascript",
        headers={"Service-Worker-Allowed": "/"},
    )


@app.get("/healthz", response_class=JSONResponse)
async def healthz() -> JSONResponse:
    local_now = base_settings.local_now()
    payload = {
        "status": "ok",
        "local_time": local_now.isoformat(),
        "timezone": base_settings.timezone_name,
        "sodas_loaded": len(catalog.list_sodas()),
        "csv_path": base_settings.csv_path,
        "database_path": base_settings.database_path,
        "user_count": database.count_users(),
        "admin_user_count": database.count_admin_users(),
        "access_control_mode": base_settings.access_control_mode,
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

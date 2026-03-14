# Soda Picker

Soda Picker is a local-first FastAPI app that reads a soda catalog from CSV, recommends a soda using inventory, preference, time-of-day, weekend, and caffeine-budget rules, and stores local state in SQLite. It is intentionally lightweight so it runs well on a MacBook in Docker and later on a Raspberry Pi behind Cloudflare Tunnel or another reverse proxy.

## Architecture

- Backend: FastAPI with Jinja templates and static CSS/JS.
- Catalog source: CSV from the mounted `./data` directory.
- Local state: SQLite for consumption history, recommendation history, per-soda local state, and saved runtime overrides.
- Runtime: single-container Docker deployment based on `python:3.12-slim`, which is multi-arch for `amd64` Macs and `arm64` Raspberry Pi hosts.

Why this fits Raspberry Pi + Docker:

- No frontend build chain and only a few Python dependencies.
- SQLite keeps persistence simple and efficient on local storage.
- The container image is small and straightforward.
- The app exposes one HTTP port, includes `/healthz`, and supports optional basic auth, trusted hosts, and rate limiting via environment variables.

## Feature summary

The app now includes:

- CSV import from the UI with validation feedback and automatic CSV backups.
- Inventory flags so only in-stock sodas get recommended.
- Favorites, dislikes, and temporary bans per soda.
- Stronger duplicate avoidance using recent recommendation and consumption history.
- Weekday and weekend timing rules.
- A bedtime-aware caffeine squeeze window.
- Manual caffeine entries for coffee, tea, or anything else.
- Editable and deletable history entries.
- Recommendation history with “logged or skipped” tracking.
- A Soda Passport page for world sodas you have already tried, with origin notes, ratings, and export.
- A Wishlist page for sodas you want to find again, plus quick-add actions from Catalog and Passport.
- Export endpoints for consumption history, recommendation history, and the current catalog.
- Database backup creation and backup file listing.
- Runtime rule overrides saved in SQLite from the settings UI.
- Optional browser reminder support plus a calendar reminder export.
- Optional in-app login access control with multiple user accounts, per-user state, admin-managed accounts, basic auth, trusted-host filtering, and in-memory rate limiting.

## Project layout

```text
app/
templates/
static/
data/
tests/
Dockerfile
docker-compose.yml
.env.example
README.md
```

## Quick start on macOS

1. Create your local environment file:

   ```bash
   cp .env.example .env
   ```

2. Start the app:

   ```bash
   docker compose up --build
   ```

3. Open the app:

   ```text
   http://localhost:8000
   ```

4. The sample catalog is already mounted at `./data/sample_sodas.csv`. To use your own file, place it in `./data/` and change:

   ```env
   CSV_PATH=/data/my-sodas.csv
   ```

## Local tests

The tests cover CSV diagnostics, recommendation rules, weekend timing behavior, duplicate/inventory handling, and the expanded SQLite state layer.

```bash
python3 -m unittest discover -s tests
```

## Docker behavior

- `./data` is bind-mounted as `/data`.
- SQLite lives at `DATABASE_PATH`.
- Backups are written to `BACKUP_DIR`.
- The container restarts with `unless-stopped`.
- Health checks hit `GET /healthz`.
- Uvicorn is started with proxy header support enabled for future reverse-proxy use.

## Raspberry Pi deployment

This project targets Raspberry Pi OS 64-bit (`arm64`) so the official multi-arch Python image works cleanly.

1. Install Docker Engine and the Docker Compose plugin on PiOne.
2. Copy the project directory to the Pi.
3. Create `.env`:

   ```bash
   cp .env.example .env
   ```

4. Adjust `.env` as needed. A typical Pi configuration looks like this:

   ```env
   TZ=America/Los_Angeles
   APP_PORT=8000
   CSV_PATH=/data/sample_sodas.csv
   DATABASE_PATH=/data/soda_picker.db
   BACKUP_DIR=/data/backups
   ```

5. Make sure the mounted data directory is writable by the container user:

   ```bash
   sudo chown -R 1000:1000 data
   ```

6. Start the app:

   ```bash
   docker compose up -d --build
   ```

7. Check health:

   ```bash
   docker compose ps
   curl http://localhost:8000/healthz
   ```

8. Open it from another device on your network:

   ```text
   http://PI_IP_ADDRESS:8000
   ```

## Catalog format

Supported CSV columns:

- `name` (required)
- `brand`
- `caffeine_mg`
- `sugar_g`
- `category`
- `is_diet`
- `is_caffeine_free`
- `tags`
- `priority`
- `enabled`

Behavior:

- Missing optional columns are allowed and reported as diagnostics.
- Disabled rows are ignored.
- Invalid rows are skipped and logged.
- Duplicate soda names are detected and reported.

## Runtime pages

- `/` dashboard: recommendation flow, quick manual entries, quick passport entry, reminder controls, and a summary of today’s log, recent recommendations, passport entries, and wishlist items.
- `/catalog`: CSV import, catalog diagnostics, stock controls, favorites, dislikes, and temporary bans.
- `/activity`: full editable caffeine log plus recommendation history exports.
- `/passport`: a long-term soda memory page for sodas you have tried from anywhere, with country/city notes, ratings, and CSV export.
- `/wishlist`: a separate list of sodas you want to track down, restock, or revisit later.
- `/settings`: per-user runtime rule overrides, environment-backed value display, exports, backup controls, and admin user management.
- `/healthz`: container health endpoint.

## Security and proxy notes

- `ACCESS_CONTROL_MODE=off|writes|all`: `writes` keeps the dashboard and catalog readable but requires login for personal pages, picks, edits, admin pages, and exports; `all` requires login for the whole app.
- `ACCESS_CONTROL_SECRET`: required when `ACCESS_CONTROL_MODE` is enabled.
- `ACCESS_CONTROL_USERNAME` and `ACCESS_CONTROL_PASSWORD`: optional bootstrap credentials for the first admin account. If the named account does not exist yet, Soda Picker creates it on startup.
- `ACCESS_CONTROL_SESSION_DAYS`: cookie lifetime for the in-app login.
- `BASIC_AUTH_USERNAME` and `BASIC_AUTH_PASSWORD`: if both are set, the app requires HTTP basic auth on all HTTP endpoints except `/healthz`.
- Do not enable both `ACCESS_CONTROL_*` and `BASIC_AUTH_*` at the same time.
- `TRUSTED_HOSTS`: optional comma-separated allowlist for Host header validation. Leave blank to disable.
- `RATE_LIMIT_REQUESTS` and `RATE_LIMIT_WINDOW_SECONDS`: simple in-memory rate limiting.
- TLS is still expected to terminate at Cloudflare Tunnel or another reverse proxy.
- Once the first admin can sign in, additional accounts and admin role changes happen from the Settings page and are stored in SQLite.

## Reminder behavior

- `REMINDER_ENABLED=true` turns on reminder support in the UI.
- Browser reminders work when the dashboard is open and the browser grants notification permission.
- `/exports/reminder.ics` exports a recurring calendar reminder, which is the better option if you want reminders outside the browser tab.

## Backup strategy

- Use the “Create on-disk backup snapshot” button on the settings page to copy the database and current catalog into `BACKUP_DIR`.
- Use the export links for ad hoc downloads.
- Because `./data` is the full local state of the app, backing up that directory is the simplest disaster-recovery plan.

## What to customize

Replace `./data/sample_sodas.csv` with your real catalog and review these environment variables:

- `TZ`
- `NO_SODA_BEFORE`
- `WEEKEND_NO_SODA_BEFORE`
- `DAILY_CAFFEINE_LIMIT_MG`
- `WEEKEND_DAILY_CAFFEINE_LIMIT_MG`
- `CAFFEINE_CUTOFF_HOUR`
- `WEEKEND_CAFFEINE_CUTOFF_HOUR`
- `BEDTIME_HOUR`
- `WEEKEND_BEDTIME_HOUR`
- `LATEST_CAFFEINE_HOURS_BEFORE_BED`
- `DUPLICATE_LOOKBACK`
- `CSV_PATH`
- `DATABASE_PATH`
- `BACKUP_DIR`
- `CHAOS_MODE_DEFAULT`
- `REMINDER_ENABLED`
- `REMINDER_TIME`
- `ACCESS_CONTROL_MODE`
- `ACCESS_CONTROL_USERNAME`
- `ACCESS_CONTROL_PASSWORD`
- `ACCESS_CONTROL_SECRET`
- `ACCESS_CONTROL_SESSION_DAYS`
- `BASIC_AUTH_USERNAME`
- `BASIC_AUTH_PASSWORD`
- `TRUSTED_HOSTS`
- `RATE_LIMIT_REQUESTS`
- `RATE_LIMIT_WINDOW_SECONDS`

Assumption: PiOne runs a 64-bit `arm64` Raspberry Pi OS or equivalent Docker host.

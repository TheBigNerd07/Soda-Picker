# Soda Picker

Soda Picker is a local-first FastAPI web app that reads a soda catalog from CSV, recommends a soda using time-of-day and caffeine rules, and tracks today's caffeine intake in SQLite. It is intentionally lightweight so the same project works well on a MacBook in Docker and later on a Raspberry Pi behind a reverse proxy or Cloudflare Tunnel.

## Architecture

- Backend: FastAPI with Jinja templates and static CSS/JS.
- Catalog: mounted CSV file under `./data`, reloaded automatically when the file changes.
- State: SQLite database stored in the same mounted `./data` directory.
- Runtime: a single multi-arch Docker container based on `python:3.12-slim`, suitable for `amd64` Macs and `arm64` Raspberry Pi deployments.

Why this fits Raspberry Pi + Docker:

- The app uses only a few Python dependencies and no frontend build pipeline.
- SQLite keeps persistence simple and fast on local storage.
- The Docker image stays small and portable.
- The app exposes a single HTTP port and includes a `/healthz` endpoint for container health checks and future reverse-proxy use.

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

1. Copy the example environment file:

   ```bash
   cp .env.example .env
   ```

2. Optionally edit `.env` to change the timezone, caffeine limit, or mounted CSV path.

3. Start the app:

   ```bash
   docker compose up --build
   ```

4. Open the app in your browser:

   ```text
   http://localhost:8000
   ```

   If you changed `APP_PORT`, use that port instead.

5. The default CSV is already included at `./data/sample_sodas.csv`. To use your own catalog, drop a CSV file into `./data/` and update `CSV_PATH` in `.env`, for example:

   ```env
   CSV_PATH=/data/my-sodas.csv
   ```

## Running tests locally

The test suite covers CSV parsing, the pre-10:30 rule, general recommendation behavior, and caffeine-limit behavior.

```bash
python3 -m unittest discover -s tests
```

## Docker behavior

- `./data` is bind-mounted into the container as `/data`.
- The SQLite file is created at `DATABASE_PATH`.
- The container restarts with `unless-stopped`.
- Health checks hit `GET /healthz`.
- The app trusts proxy headers so it can sit behind Cloudflare Tunnel or another reverse proxy later.

## Raspberry Pi deployment

This project is aimed at Raspberry Pi OS 64-bit (`arm64`), which is the cleanest match for the official multi-arch Python image used here.

1. Install Docker Engine and the Docker Compose plugin on the Pi.
2. Copy the project directory to the Pi.
3. Create `.env`:

   ```bash
   cp .env.example .env
   ```

4. Adjust `.env` for the Pi if needed. Typical changes:

   ```env
   TZ=America/Los_Angeles
   APP_PORT=8000
   CSV_PATH=/data/sample_sodas.csv
   DATABASE_PATH=/data/soda_picker.db
   ```

5. Make sure the mounted data directory is writable. If the container cannot create the SQLite file, fix the directory owner on the Pi:

   ```bash
   sudo chown -R 1000:1000 data
   ```

6. Start the app in the background:

   ```bash
   docker compose up -d --build
   ```

7. Check health:

   ```bash
   docker compose ps
   curl http://localhost:8000/healthz
   ```

## CSV format

Supported columns:

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

- Missing optional columns are handled gracefully.
- Disabled rows are ignored.
- Invalid rows are skipped and logged.
- The app keeps running even if some rows are malformed.

## App behavior summary

- Before `NO_SODA_BEFORE`, the app refuses to recommend any soda.
- After that time, recommendations come from the enabled CSV catalog.
- Caffeine intake is tracked per local day using the configured timezone.
- Once the daily caffeine limit is reached, caffeinated sodas are blocked.
- After the cutoff hour, caffeinated sodas are heavily penalized.
- At night, the engine strongly prefers caffeine-free sodas.
- Treat mode adds extra randomness without bypassing the hard time and caffeine safety rules.

## Future reverse proxy / Cloudflare notes

- The app serves plain HTTP on one port and expects the proxy to terminate TLS.
- Uvicorn is started with proxy header support enabled.
- No authentication is built in yet, but the app surface is small and easy to put behind Cloudflare Access, basic auth, or a home-lab reverse proxy later.

## What to customize

- Replace `./data/sample_sodas.csv` with your real soda catalog.
- Edit `.env` to set:
  - `TZ`
  - `NO_SODA_BEFORE`
  - `DAILY_CAFFEINE_LIMIT_MG`
  - `CAFFEINE_CUTOFF_HOUR`
  - `CSV_PATH`
  - `DATABASE_PATH`
  - `CHAOS_MODE_DEFAULT`
- Assumption: PiOne will run a 64-bit Raspberry Pi OS or another `arm64` Docker host.

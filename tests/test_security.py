from __future__ import annotations

import unittest

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse, RedirectResponse
from fastapi.testclient import TestClient

from app.security import AUTH_COOKIE_NAME, AccessControlMiddleware, issue_auth_token


def build_app(mode: str) -> FastAPI:
    app = FastAPI()
    app.add_middleware(
        AccessControlMiddleware,
        mode=mode,
        secret="test-secret",
        exempt_paths={"/healthz", "/login", "/logout"},
    )

    @app.get("/")
    async def dashboard() -> PlainTextResponse:
        return PlainTextResponse("dashboard")

    @app.get("/activity")
    async def activity() -> PlainTextResponse:
        return PlainTextResponse("activity")

    @app.post("/save")
    async def save() -> PlainTextResponse:
        return PlainTextResponse("saved")

    @app.post("/login")
    async def login() -> RedirectResponse:
        response = RedirectResponse(url="/", status_code=303)
        response.set_cookie(
            AUTH_COOKIE_NAME,
            issue_auth_token("tester", "test-secret", lifetime_seconds=3600),
            httponly=True,
        )
        return response

    @app.get("/healthz")
    async def healthz() -> PlainTextResponse:
        return PlainTextResponse("ok")

    return app


class AccessControlMiddlewareTests(unittest.TestCase):
    def test_writes_mode_allows_public_read_but_redirects_mutations(self) -> None:
        client = TestClient(build_app("writes"))

        self.assertEqual(client.get("/").status_code, 200)

        response = client.post("/save", headers={"referer": "http://testserver/"}, follow_redirects=False)
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], "/login?next=/")

    def test_writes_mode_still_protects_activity_and_exports_style_pages(self) -> None:
        client = TestClient(build_app("writes"))

        response = client.get("/activity", follow_redirects=False)
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], "/login?next=/activity")

    def test_all_mode_requires_login_until_cookie_is_set(self) -> None:
        client = TestClient(build_app("all"))

        blocked = client.get("/", follow_redirects=False)
        self.assertEqual(blocked.status_code, 303)
        self.assertEqual(blocked.headers["location"], "/login?next=/")

        login = client.post("/login", follow_redirects=False)
        self.assertEqual(login.status_code, 303)
        self.assertEqual(login.headers["location"], "/")

        allowed_dashboard = client.get("/")
        self.assertEqual(allowed_dashboard.status_code, 200)
        self.assertEqual(allowed_dashboard.text, "dashboard")

        allowed_post = client.post("/save")
        self.assertEqual(allowed_post.status_code, 200)
        self.assertEqual(allowed_post.text, "saved")


if __name__ == "__main__":
    unittest.main()

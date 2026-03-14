from __future__ import annotations

import base64
import hashlib
import hmac
import os
import time
from collections import defaultdict, deque
from collections.abc import Callable
from http.cookies import SimpleCookie
from typing import Deque
from urllib.parse import quote, urlparse

from starlette.responses import PlainTextResponse, RedirectResponse
from starlette.types import ASGIApp, Receive, Scope, Send

AUTH_COOKIE_NAME = "soda_picker_auth"
SAFE_METHODS = {"GET", "HEAD", "OPTIONS"}


def credentials_valid(
    incoming_username: str,
    incoming_password: str,
    *,
    expected_username: str,
    expected_password: str,
) -> bool:
    return hmac.compare_digest(incoming_username, expected_username) and hmac.compare_digest(
        incoming_password,
        expected_password,
    )


def hash_password(password: str, *, iterations: int = 240_000) -> str:
    salt = os.urandom(16)
    derived = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return f"pbkdf2_sha256${iterations}${base64.urlsafe_b64encode(salt).decode('ascii')}${base64.urlsafe_b64encode(derived).decode('ascii')}"


def password_matches(password: str, password_hash: str) -> bool:
    if not password_hash:
        return False

    try:
        algorithm, iterations_text, salt_text, digest_text = password_hash.split("$", maxsplit=3)
        if algorithm != "pbkdf2_sha256":
            return False
        iterations = int(iterations_text)
        salt = base64.urlsafe_b64decode(salt_text.encode("ascii"))
        expected_digest = base64.urlsafe_b64decode(digest_text.encode("ascii"))
    except Exception:
        return False

    actual_digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return hmac.compare_digest(actual_digest, expected_digest)


def issue_auth_token(username: str, secret: str, *, lifetime_seconds: int) -> str:
    expires_at = int(time.time()) + max(lifetime_seconds, 60)
    payload = f"{username}\n{expires_at}"
    signature = hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), "sha256").hexdigest()
    raw_token = f"{payload}\n{signature}".encode("utf-8")
    return base64.urlsafe_b64encode(raw_token).decode("ascii")


def read_auth_token(token: str | None, secret: str) -> str | None:
    if not token or not secret:
        return None

    try:
        raw_value = base64.urlsafe_b64decode(token.encode("ascii")).decode("utf-8")
        username, expires_text, signature = raw_value.split("\n", maxsplit=2)
        expires_at = int(expires_text)
    except Exception:
        return None

    payload = f"{username}\n{expires_at}"
    expected_signature = hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), "sha256").hexdigest()
    if not hmac.compare_digest(signature, expected_signature):
        return None
    if expires_at < int(time.time()):
        return None
    return username


def read_auth_cookie(scope: Scope, cookie_name: str = AUTH_COOKIE_NAME) -> str | None:
    headers = dict(scope.get("headers", []))
    raw_cookie = headers.get(b"cookie")
    if raw_cookie is None:
        return None

    jar = SimpleCookie()
    try:
        jar.load(raw_cookie.decode("latin1"))
    except Exception:
        return None

    morsel = jar.get(cookie_name)
    if morsel is None:
        return None
    return morsel.value


def authenticated_username(scope: Scope, *, secret: str, cookie_name: str = AUTH_COOKIE_NAME) -> str | None:
    token = read_auth_cookie(scope, cookie_name=cookie_name)
    return read_auth_token(token, secret)


class BasicAuthMiddleware:
    def __init__(
        self,
        app: ASGIApp,
        *,
        username: str,
        password: str,
        exempt_paths: set[str] | None = None,
    ) -> None:
        self.app = app
        self.username = username
        self.password = password
        self.exempt_paths = exempt_paths or set()

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")
        if path in self.exempt_paths:
            await self.app(scope, receive, send)
            return

        headers = dict(scope.get("headers", []))
        raw_header = headers.get(b"authorization")
        if raw_header is None:
            response = PlainTextResponse(
                "Authentication required.",
                status_code=401,
                headers={"WWW-Authenticate": "Basic"},
            )
            await response(scope, receive, send)
            return

        try:
            scheme, encoded = raw_header.decode("utf-8").split(" ", maxsplit=1)
            decoded = base64.b64decode(encoded).decode("utf-8")
            incoming_username, incoming_password = decoded.split(":", maxsplit=1)
        except Exception:
            response = PlainTextResponse("Invalid authentication header.", status_code=401)
            await response(scope, receive, send)
            return

        if scheme.lower() != "basic":
            response = PlainTextResponse("Unsupported authentication scheme.", status_code=401)
            await response(scope, receive, send)
            return

        valid = credentials_valid(
            incoming_username,
            incoming_password,
            expected_username=self.username,
            expected_password=self.password,
        )
        if not valid:
            response = PlainTextResponse(
                "Invalid credentials.",
                status_code=401,
                headers={"WWW-Authenticate": "Basic"},
            )
            await response(scope, receive, send)
            return

        await self.app(scope, receive, send)


class RateLimitMiddleware:
    def __init__(
        self,
        app: ASGIApp,
        *,
        max_requests: int,
        window_seconds: int,
        exempt_paths: set[str] | None = None,
        key_func: Callable[[Scope], str] | None = None,
    ) -> None:
        self.app = app
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.exempt_paths = exempt_paths or set()
        self.key_func = key_func or self._default_key
        self.events: dict[str, Deque[float]] = defaultdict(deque)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or self.max_requests <= 0 or self.window_seconds <= 0:
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")
        if path in self.exempt_paths or path.startswith("/static/"):
            await self.app(scope, receive, send)
            return

        key = self.key_func(scope)
        now = time.monotonic()
        cutoff = now - self.window_seconds
        bucket = self.events[key]

        while bucket and bucket[0] < cutoff:
            bucket.popleft()

        if len(bucket) >= self.max_requests:
            response = PlainTextResponse("Too many requests.", status_code=429)
            await response(scope, receive, send)
            return

        bucket.append(now)
        await self.app(scope, receive, send)

    @staticmethod
    def _default_key(scope: Scope) -> str:
        client = scope.get("client")
        if client:
            return client[0]
        return "unknown"


class AccessControlMiddleware:
    def __init__(
        self,
        app: ASGIApp,
        *,
        mode: str,
        secret: str,
        login_path: str = "/login",
        cookie_name: str = AUTH_COOKIE_NAME,
        exempt_paths: set[str] | None = None,
        always_protected_prefixes: tuple[str, ...] = ("/activity", "/passport", "/wishlist", "/settings", "/exports/"),
        identity_validator: Callable[[str], bool] | None = None,
    ) -> None:
        self.app = app
        self.mode = mode
        self.secret = secret
        self.login_path = login_path
        self.cookie_name = cookie_name
        self.exempt_paths = exempt_paths or set()
        self.always_protected_prefixes = always_protected_prefixes
        self.identity_validator = identity_validator

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or self.mode == "off":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")
        if path.startswith("/static/") or path in self.exempt_paths:
            await self.app(scope, receive, send)
            return

        username = authenticated_username(scope, secret=self.secret, cookie_name=self.cookie_name)
        if username is not None and self.identity_validator is not None and not self.identity_validator(username):
            username = None
        if username is not None:
            scope["soda_picker_user"] = username
            await self.app(scope, receive, send)
            return

        method = scope.get("method", "GET").upper()
        is_protected_prefix = any(path.startswith(prefix) for prefix in self.always_protected_prefixes)
        should_require_login = self.mode == "all" or is_protected_prefix or method not in SAFE_METHODS
        if not should_require_login:
            await self.app(scope, receive, send)
            return

        target = self._next_target(scope, path=path, method=method)
        location = f"{self.login_path}?next={quote(target, safe='/?=&')}"
        response = RedirectResponse(location, status_code=303)
        await response(scope, receive, send)

    @staticmethod
    def _next_target(scope: Scope, *, path: str, method: str) -> str:
        headers = dict(scope.get("headers", []))
        if method not in SAFE_METHODS:
            referer = headers.get(b"referer")
            if referer:
                try:
                    parsed = urlparse(referer.decode("utf-8"))
                    if parsed.path and not parsed.scheme.startswith("javascript"):
                        target = parsed.path
                        if parsed.query:
                            target = f"{target}?{parsed.query}"
                        return target
                except Exception:
                    pass
            return "/"

        query_string = scope.get("query_string", b"").decode("utf-8")
        if query_string:
            return f"{path}?{query_string}"
        return path

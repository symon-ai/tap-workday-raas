# -*- coding: utf-8 -*-
"""Workday OAuth 2.0 token acquisition and caching (RaaS Bearer on HTTP requests)."""

from __future__ import annotations

import base64
import json
import re
import threading
import time
from typing import Any, Dict, Optional, Tuple

import requests


class WorkdayOAuthError(Exception):
    """Token endpoint returned an error."""

    def __init__(self, message: str, status_code: Optional[int] = None, response_body: Optional[str] = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_body = response_body


class WorkdayRefreshTokenInvalidError(WorkdayOAuthError):
    """Refresh token rejected by Workday (expired, revoked, or invalid). Update config with a new token."""


_OAUTH_ERROR_RESPONSE_BODY_MAX_LEN = 2000


def workday_oauth_error_details(exc: WorkdayOAuthError) -> Dict[str, Any]:
    """Preserve token-endpoint diagnostics for error_info while keeping a stable user-facing message."""
    details: Dict[str, Any] = {
        "oauthErrorType": type(exc).__name__,
        "oauthErrorMessage": str(exc),
    }
    if exc.status_code is not None:
        details["statusCode"] = exc.status_code
    if exc.response_body:
        body = exc.response_body
        if len(body) > _OAUTH_ERROR_RESPONSE_BODY_MAX_LEN:
            details["responseBody"] = body[:_OAUTH_ERROR_RESPONSE_BODY_MAX_LEN]
            details["responseBodyTruncated"] = True
        else:
            details["responseBody"] = body
    return details


def _oauth_token_error_code(body: Optional[str]) -> Optional[str]:
    raw = (body or "").strip()
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            err = (parsed.get("error") or "").strip().lower()
            return err or None
    except (json.JSONDecodeError, TypeError, ValueError):
        pass
    return None


def _oauth_token_error_is(body: Optional[str], error: str) -> bool:
    expected = error.lower()
    code = _oauth_token_error_code(body)
    if code:
        return code == expected
    return expected in (body or "").lower()


def _oauth_refresh_token_rejected(status_code: int, body: Optional[str]) -> bool:
    if status_code not in (400, 401):
        return False
    return _oauth_token_error_is(body, "invalid_grant")


_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)


def _resolve_workday_oauth_client_id(client_id: str) -> str:
    """Workday often displays the API client ID as Base64(UUID); the token endpoint expects the UUID string."""
    s = (client_id or "").strip()
    if _UUID_RE.match(s):
        return s
    try:
        pad = "=" * ((4 - len(s) % 4) % 4)
        decoded = base64.b64decode(s + pad).decode("utf-8").strip()
        if _UUID_RE.match(decoded):
            return decoded
    except Exception:
        pass
    return s


def _basic_auth_header(client_id: str, client_secret: str) -> str:
    raw = f"{client_id}:{client_secret}".encode("utf-8")
    return "Basic " + base64.b64encode(raw).decode("ascii")


def raas_config_uses_oauth(config: Dict[str, Any]) -> bool:
    explicit = (config.get("auth_type") or "").strip().lower()
    if explicit == "oauth":
        return True
    if explicit == "basic":
        return False
    has_oauth = bool(config.get("client_id") and config.get("client_secret") and config.get("token_url"))
    has_basic = bool(config.get("username") and config.get("password"))
    if has_oauth and not has_basic:
        return True
    if has_basic and not has_oauth:
        return False
    if has_oauth and has_basic:
        raise ValueError(
            "Specify auth_type 'oauth' or 'basic' when both username/password and OAuth fields are present."
        )
    raise ValueError(
        "Authentication required: username/password (basic) or client_id, client_secret, token_url (oauth)."
    )


def _token_client_id(client_id_raw: str, client_id_format: str) -> str:
    fmt = (client_id_format or "auto").strip().lower()
    if fmt == "raw":
        return (client_id_raw or "").strip()
    if fmt not in ("auto", "uuid"):
        raise ValueError("oauth_client_id_format must be 'auto', 'raw', or 'uuid'")
    return _resolve_workday_oauth_client_id(client_id_raw)


def _config_str(config: Dict[str, Any], key: str, default: str = "") -> str:
    value = config.get(key)
    if not value:
        value = default
    return str(value).strip().lower()


def _parse_oauth_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """Validate OAuth settings and return normalized values for the token provider."""
    for key in ("client_id", "client_secret", "token_url"):
        if not config.get(key):
            raise ValueError(f"OAuth auth requires {key} in config")

    grant_type = _config_str(config, "oauth_grant_type", "client_credentials")
    if grant_type not in ("refresh_token", "client_credentials"):
        raise ValueError("oauth_grant_type must be refresh_token or client_credentials")
    if grant_type == "refresh_token" and not config.get("refresh_token"):
        raise ValueError("refresh_token is required when oauth_grant_type is refresh_token")
    if grant_type == "client_credentials" and config.get("refresh_token"):
        raise ValueError(
            "OAuth config includes refresh_token but oauth_grant_type is client_credentials. "
            "For Workday API clients using Authorization Code (refresh tokens), set oauth_grant_type to "
            "refresh_token. For client_credentials-only clients, remove refresh_token from config."
        )

    client_id_format = _config_str(config, "oauth_client_id_format", "auto")
    if client_id_format not in ("auto", "raw", "uuid"):
        raise ValueError("oauth_client_id_format must be 'auto', 'raw', or 'uuid'")

    token_client_auth = _config_str(config, "oauth_token_client_auth", "basic")
    if token_client_auth not in ("basic", "post_body"):
        raise ValueError("oauth_token_client_auth must be 'basic' or 'post_body'")

    leeway, min_cache = _oauth_access_token_cache_settings(config)
    return {
        "grant_type": grant_type,
        "token_client_auth": token_client_auth,
        "client_id_format": client_id_format,
        "leeway": leeway,
        "min_cache": min_cache,
    }


def _oauth_access_token_cache_settings(config: Dict[str, Any]) -> Tuple[int, int]:
    if config.get("oauth_access_token_refresh_leeway_seconds") is not None:
        leeway = int(config["oauth_access_token_refresh_leeway_seconds"])
        if leeway < 0 or leeway > 86400:
            raise ValueError("oauth_access_token_refresh_leeway_seconds must be between 0 and 86400 inclusive")
    else:
        leeway = 60
    if config.get("oauth_access_token_min_cache_seconds") is not None:
        min_cache = int(config["oauth_access_token_min_cache_seconds"])
        if min_cache < 0 or min_cache > 86400:
            raise ValueError("oauth_access_token_min_cache_seconds must be between 0 and 86400 inclusive")
    else:
        min_cache = 60
    return leeway, min_cache


def _seed_access_token_from_config(
    provider: "WorkdayOAuthTokenProvider",
    config: Dict[str, Any],
    leeway: int,
    min_cache: int,
) -> None:
    """Use a platform-supplied access token until cache TTL expires."""
    access = config.get("access_token")
    if not access or not str(access).strip():
        return
    provider._access_token = str(access).strip()
    expires_in = int(config.get("oauth_access_token_expires_in", 3600))
    ttl = _oauth_access_token_cache_ttl_seconds(expires_in, leeway, min_cache)
    provider._expires_at = time.time() + float(ttl)


def _oauth_access_token_cache_ttl_seconds(
    expires_in: int,
    refresh_leeway_seconds: int,
    min_cache_seconds: int,
) -> int:
    ttl = expires_in - refresh_leeway_seconds
    if ttl < min_cache_seconds:
        ttl = min_cache_seconds
    return ttl


class WorkdayOAuthTokenProvider:
    """Fetches and refreshes OAuth access tokens (Workday API Client for Integrations pattern)."""

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        token_url: str,
        grant_type: str = "client_credentials",
        refresh_token: Optional[str] = None,
        scope: Optional[str] = None,
        verify: bool = True,
        session: Optional[requests.Session] = None,
        token_client_auth: str = "basic",
        client_id_format: str = "auto",
        access_token_refresh_leeway_seconds: int = 60,
        access_token_min_cache_seconds: int = 60,
    ):
        self._client_id_raw = (client_id or "").strip()
        self._client_id = _token_client_id(self._client_id_raw, client_id_format)
        self._client_secret = client_secret
        self._token_url = token_url
        self._grant_type = grant_type.strip().lower()
        self._refresh_token = refresh_token
        self._scope = scope
        self._verify = verify
        self._token_client_auth = token_client_auth.strip().lower()
        self._client_id_format = (client_id_format or "auto").strip().lower()
        self._token_client_id_for_auth: Optional[str] = None
        self._http = session or requests.Session()
        self._lock = threading.Lock()
        self._access_token: Optional[str] = None
        self._expires_at: float = 0.0
        self._session_ref: Optional[requests.Session] = None
        self._access_token_refresh_leeway_seconds = int(access_token_refresh_leeway_seconds)
        self._access_token_min_cache_seconds = int(access_token_min_cache_seconds)

    @classmethod
    def from_config(cls, config: Dict[str, Any], verify: bool = True) -> WorkdayOAuthTokenProvider:
        oauth = _parse_oauth_config(config)
        provider = cls(
            client_id=config["client_id"],
            client_secret=config["client_secret"],
            token_url=config["token_url"],
            grant_type=oauth["grant_type"],
            refresh_token=config.get("refresh_token"),
            scope=config.get("oauth_scope"),
            verify=verify,
            token_client_auth=oauth["token_client_auth"],
            client_id_format=oauth["client_id_format"],
            access_token_refresh_leeway_seconds=oauth["leeway"],
            access_token_min_cache_seconds=oauth["min_cache"],
        )
        _seed_access_token_from_config(provider, config, oauth["leeway"], oauth["min_cache"])
        return provider

    def apply_to_session(self, session: requests.Session) -> None:
        self._session_ref = session
        session.headers["Authorization"] = "Bearer " + self.get_access_token()

    def get_access_token(self) -> str:
        with self._lock:
            now = time.time()
            if self._access_token and now < self._expires_at:
                return self._access_token
            self._fetch_token_locked()
            assert self._access_token is not None
            return self._access_token

    def force_refresh(self) -> str:
        with self._lock:
            self._access_token = None
            self._expires_at = 0.0
            self._fetch_token_locked()
            assert self._access_token is not None
            return self._access_token

    def _post_token(self, data: Dict[str, str], client_id_for_auth: str) -> requests.Response:
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        post_data = dict(data)
        if self._token_client_auth == "post_body":
            post_data["client_id"] = client_id_for_auth
            post_data["client_secret"] = self._client_secret
        else:
            headers["Authorization"] = _basic_auth_header(client_id_for_auth, self._client_secret)
        return self._http.post(
            self._token_url,
            data=post_data,
            headers=headers,
            verify=self._verify,
            timeout=120,
        )

    def _token_request_body(self) -> Dict[str, str]:
        data: Dict[str, str] = {"grant_type": self._grant_type}
        if self._grant_type == "refresh_token":
            data["refresh_token"] = self._refresh_token or ""
        if self._scope:
            data["scope"] = self._scope
        return data

    def _client_ids_for_token_request(self) -> list:
        if self._token_client_id_for_auth:
            return [self._token_client_id_for_auth]
        ids = [self._client_id]
        if (
            self._grant_type == "refresh_token"
            and self._client_id_format == "auto"
            and self._client_id_raw
            and self._client_id_raw != self._client_id
        ):
            ids.append(self._client_id_raw)
        return ids

    def _post_token_trying_client_ids(
        self, data: Dict[str, str], client_ids: list
    ) -> Tuple[Optional[requests.Response], Optional[str]]:
        resp = None
        try:
            for i, client_id in enumerate(client_ids):
                resp = self._post_token(data, client_id)
                if resp.status_code < 400:
                    return resp, client_id
                if (
                    resp.status_code in (400, 401)
                    and i < len(client_ids) - 1
                    and _oauth_token_error_is(resp.text, "invalid_client")
                ):
                    continue
                break
        except requests.exceptions.RequestException as e:
            raise WorkdayOAuthError(f"Token request failed: {e}") from e
        return resp, None

    def _raise_token_endpoint_error(self, resp: requests.Response) -> None:
        body_snip = (resp.text or "").strip()
        msg = f"Token endpoint error: HTTP {resp.status_code}"
        if body_snip:
            msg = f"{msg} — {body_snip[:1000]}"
        if self._grant_type == "refresh_token" and _oauth_refresh_token_rejected(
            resp.status_code, resp.text
        ):
            msg = (
                "Please update your Workday OAuth credentials: the refresh token is no longer valid "
                "(expired, revoked, or rotated). Obtain a new refresh token in Workday and update your "
                f"configuration. Details: {msg}"
            )
            raise WorkdayRefreshTokenInvalidError(
                msg,
                status_code=resp.status_code,
                response_body=resp.text,
            )
        raise WorkdayOAuthError(
            msg,
            status_code=resp.status_code,
            response_body=resp.text,
        )

    def _save_access_token(self, resp: requests.Response, winning_client_id: Optional[str]) -> None:
        try:
            body = resp.json()
        except json.JSONDecodeError as e:
            raise WorkdayOAuthError(f"Token response was not JSON: {resp.text[:500]}") from e
        access = body.get("access_token")
        if not access:
            raise WorkdayOAuthError(f"Token response missing access_token: {body}")
        self._access_token = access
        if winning_client_id is not None:
            self._token_client_id_for_auth = winning_client_id
        expires_in = int(body.get("expires_in", 3600))
        ttl = _oauth_access_token_cache_ttl_seconds(
            expires_in,
            self._access_token_refresh_leeway_seconds,
            self._access_token_min_cache_seconds,
        )
        self._expires_at = time.time() + float(ttl)
        if self._session_ref is not None:
            self._session_ref.headers["Authorization"] = "Bearer " + self._access_token

    def _fetch_token_locked(self) -> None:
        data = self._token_request_body()
        client_ids = self._client_ids_for_token_request()

        resp, winning_client_id = self._post_token_trying_client_ids(data, client_ids)
        if resp is None:
            raise WorkdayOAuthError("Token request returned no response")
        if resp.status_code >= 400:
            self._raise_token_endpoint_error(resp)

        self._save_access_token(resp, winning_client_id)


def validate_raas_tap_config(config: Dict[str, Any]) -> None:
    """Ensure reports plus either basic or OAuth credentials."""
    if not config.get("reports"):
        raise ValueError("Missing required config key: reports")
    if raas_config_uses_oauth(config):
        _parse_oauth_config(config)
    else:
        if not config.get("username") or not config.get("password"):
            raise ValueError("Basic auth requires username and password in config")

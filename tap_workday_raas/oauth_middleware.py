# -*- coding: utf-8 -*-
"""Workday OAuth 2.0 token acquisition and caching (RaaS Bearer on HTTP requests)."""

from __future__ import annotations

import base64
import json
import re
import threading
import time
from typing import Any, Dict, Optional

import requests


class WorkdayOAuthError(Exception):
    """Token endpoint returned an error."""

    def __init__(self, message: str, status_code: Optional[int] = None, response_body: Optional[str] = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_body = response_body


_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)


def _resolve_workday_oauth_client_id(client_id: str) -> str:
    """Workday often displays the API client ID as Base64(UUID); the token endpoint expects the UUID string in Basic auth."""
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
    """Resolve client_id for the token endpoint (auto = Base64→UUID when applicable)."""
    fmt = (client_id_format or "auto").strip().lower()
    if fmt == "raw":
        return (client_id_raw or "").strip()
    if fmt not in ("auto", "uuid"):
        raise ValueError("oauth_client_id_format must be 'auto', 'raw', or 'uuid'")
    return _resolve_workday_oauth_client_id(client_id_raw)


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
        self._http = session or requests.Session()
        self._lock = threading.Lock()
        self._access_token: Optional[str] = None
        self._expires_at: float = 0.0
        self._session_ref: Optional[requests.Session] = None

    @classmethod
    def from_config(cls, config: Dict[str, Any], verify: bool = True) -> WorkdayOAuthTokenProvider:
        grant = (config.get("oauth_grant_type") or "client_credentials").strip().lower()
        if grant not in ("refresh_token", "client_credentials"):
            raise ValueError("oauth_grant_type must be 'refresh_token' or 'client_credentials'")
        if grant == "refresh_token" and not config.get("refresh_token"):
            raise ValueError("refresh_token is required when oauth_grant_type is refresh_token")
        auth_mode = (config.get("oauth_token_client_auth") or "basic").strip().lower()
        if auth_mode not in ("basic", "post_body"):
            raise ValueError("oauth_token_client_auth must be 'basic' or 'post_body'")
        id_fmt = (config.get("oauth_client_id_format") or "auto").strip().lower()
        if id_fmt not in ("auto", "raw", "uuid"):
            raise ValueError("oauth_client_id_format must be 'auto', 'raw', or 'uuid'")
        return cls(
            client_id=config["client_id"],
            client_secret=config["client_secret"],
            token_url=config["token_url"],
            grant_type=grant,
            refresh_token=config.get("refresh_token"),
            scope=config.get("oauth_scope"),
            verify=verify,
            token_client_auth=auth_mode,
            client_id_format=id_fmt,
        )

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
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
        }
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

    def _fetch_token_locked(self) -> None:
        data: Dict[str, str] = {"grant_type": self._grant_type}
        if self._grant_type == "refresh_token":
            data["refresh_token"] = self._refresh_token or ""
        if self._scope:
            data["scope"] = self._scope

        client_ids_to_try = [self._client_id]
        if (
            self._grant_type == "refresh_token"
            and self._client_id_format == "auto"
            and self._client_id_raw != self._client_id
        ):
            client_ids_to_try.append(self._client_id_raw)

        resp = None
        try:
            for i, cid in enumerate(client_ids_to_try):
                resp = self._post_token(data, cid)
                if resp.status_code < 400:
                    break
                if (
                    resp.status_code == 401
                    and i == 0
                    and len(client_ids_to_try) > 1
                    and "invalid_client" in (resp.text or "").lower()
                ):
                    continue
                break
        except requests.exceptions.RequestException as e:
            raise WorkdayOAuthError("Token request failed: {}".format(e)) from e

        if resp is None:
            raise WorkdayOAuthError("Token request returned no response")
        if resp.status_code >= 400:
            body_snip = (resp.text or "").strip()
            msg = "Token endpoint error: HTTP {}".format(resp.status_code)
            if body_snip:
                msg = "{} — {}".format(msg, body_snip[:1000])
            raise WorkdayOAuthError(
                msg,
                status_code=resp.status_code,
                response_body=resp.text,
            )
        try:
            body = resp.json()
        except json.JSONDecodeError as e:
            raise WorkdayOAuthError("Token response was not JSON: {}".format(resp.text[:500])) from e
        access = body.get("access_token")
        if not access:
            raise WorkdayOAuthError("Token response missing access_token: {}".format(body))
        self._access_token = access
        skew = int(body.get("expires_in", 3600)) - 60
        if skew < 60:
            skew = 60
        self._expires_at = time.time() + float(skew)
        if self._session_ref is not None:
            self._session_ref.headers["Authorization"] = "Bearer " + self._access_token


def validate_raas_tap_config(config: Dict[str, Any]) -> None:
    """Ensure reports plus either basic or OAuth credentials."""
    if not config.get("reports"):
        raise ValueError("Missing required config key: reports")
    uses_oauth = raas_config_uses_oauth(config)
    if uses_oauth:
        for key in ("client_id", "client_secret", "token_url"):
            if not config.get(key):
                raise ValueError("OAuth auth requires {} in config".format(key))
        grant = (config.get("oauth_grant_type") or "client_credentials").strip().lower()
        if grant not in ("refresh_token", "client_credentials"):
            raise ValueError("oauth_grant_type must be refresh_token or client_credentials")
        if grant == "refresh_token" and not config.get("refresh_token"):
            raise ValueError("refresh_token is required when oauth_grant_type is refresh_token")
        if grant == "client_credentials" and config.get("refresh_token"):
            raise ValueError(
                "OAuth config includes refresh_token but oauth_grant_type is client_credentials. "
                "For Workday API clients using Authorization Code (refresh tokens), set oauth_grant_type to "
                "refresh_token. For client_credentials-only clients, remove refresh_token from config."
            )
        id_fmt = (config.get("oauth_client_id_format") or "auto").strip().lower()
        if id_fmt not in ("auto", "raw", "uuid"):
            raise ValueError("oauth_client_id_format must be 'auto', 'raw', or 'uuid'")
        auth_mode = (config.get("oauth_token_client_auth") or "basic").strip().lower()
        if auth_mode not in ("basic", "post_body"):
            raise ValueError("oauth_token_client_auth must be 'basic' or 'post_body'")
    else:
        if not config.get("username") or not config.get("password"):
            raise ValueError("Basic auth requires username and password in config")

"""Prove stream_report holds one report GET for the full response stream (OAuth)."""

import importlib.util
import sys
import types
import unittest
from pathlib import Path
from unittest import mock

_ROOT = Path(__file__).resolve().parents[2]


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


# Minimal package namespace so client.py imports do not load tap __init__ / discover.
sys.modules.setdefault("tap_workday_raas", types.ModuleType("tap_workday_raas"))
_load_module("tap_workday_raas.symon_exception", _ROOT / "tap_workday_raas" / "symon_exception.py")
_oauth_mod = _load_module("tap_workday_raas.oauth_middleware", _ROOT / "tap_workday_raas" / "oauth_middleware.py")
_client_mod = _load_module("tap_workday_raas.client", _ROOT / "tap_workday_raas" / "client.py")

stream_report = _client_mod.stream_report
_wrap_oauth_error = _client_mod._wrap_oauth_error
WorkdayOAuthError = _oauth_mod.WorkdayOAuthError
WorkdayRefreshTokenInvalidError = _oauth_mod.WorkdayRefreshTokenInvalidError
SymonException = sys.modules["tap_workday_raas.symon_exception"].SymonException


def _minimal_report_json_bytes():
    return b'{"Report_Entry": [{"row_id": "1"}, {"row_id": "2"}, {"row_id": "3"}]}'


class _StreamingReportResponse:
    def __init__(self, body: bytes, status_code: int = 200):
        self._body = body
        self.status_code = status_code

    def iter_content(self, chunk_size=512):
        mid = max(1, len(self._body) // 2)
        yield self._body[:mid]
        yield self._body[mid:]

    def raise_for_status(self):
        return None

    def close(self):
        return None


class _GetContext:
    def __init__(self, response):
        self._response = response

    def __enter__(self):
        return self._response

    def __exit__(self, *args):
        return False


class TestWrapOAuthError(unittest.TestCase):
    def test_refresh_token_invalid_uses_standard_user_message(self):
        friendly = (
            "Please update your Workday OAuth credentials: the refresh token is no longer valid "
            "(expired, revoked, or rotated). Obtain a new refresh token in Workday and update your "
            "configuration. Details: Token endpoint error: HTTP 400 — "
            '{"error":"invalid_grant"}'
        )
        exc = WorkdayRefreshTokenInvalidError(
            friendly,
            status_code=400,
            response_body='{"error":"invalid_grant"}',
        )
        wrapped = _wrap_oauth_error(exc)
        self.assertIsInstance(wrapped, SymonException)
        self.assertEqual(
            str(wrapped),
            "The Workday OAuth token request failed. Check the token configuration and expiration.",
        )

    def test_other_oauth_400_uses_standard_user_message(self):
        msg = 'Token endpoint error: HTTP 400 — {"error":"invalid_client"}'
        exc = WorkdayOAuthError(msg, status_code=400, response_body='{"error":"invalid_client"}')
        wrapped = _wrap_oauth_error(exc)
        self.assertEqual(
            str(wrapped),
            "The Workday OAuth token request failed. Check the token configuration and expiration.",
        )


class TestStreamReportOAuth(unittest.TestCase):
    @mock.patch.object(_client_mod, "_session_for_config")
    def test_single_report_get_for_entire_stream_despite_token_cache_expiry(self, mock_session_for_config):
        """
        Long syncs use one streaming GET. Expired access-token cache must not open a second
        report request mid-stream (that would restart the download from Workday).
        """
        body = _minimal_report_json_bytes()
        report_get_count = {"n": 0}

        def counting_get(*_args, **_kwargs):
            report_get_count["n"] += 1
            return _GetContext(_StreamingReportResponse(body))

        session = mock.Mock()
        session.get.side_effect = counting_get
        session.headers = {}

        provider = mock.Mock()
        provider.get_access_token = mock.Mock(return_value="access-token-1")
        provider.force_refresh = mock.Mock()
        provider._expires_at = 0.0

        def apply_to_session(sess):
            sess.headers["Authorization"] = "Bearer " + provider.get_access_token()

        provider.apply_to_session = apply_to_session

        config = {
            "auth_type": "oauth",
            "client_id": "id",
            "client_secret": "sec",
            "token_url": "https://example/token",
            "refresh_token": "rt",
            "oauth_grant_type": "refresh_token",
        }

        def fake_session_for_config(_cfg):
            provider.apply_to_session(session)
            return session, provider

        mock_session_for_config.side_effect = fake_session_for_config

        records = list(stream_report("https://example/report?format=csv", config))

        self.assertEqual(len(records), 3)
        self.assertEqual(report_get_count["n"], 1)
        self.assertEqual(session.get.call_count, 1)
        # Token is resolved once before the stream; not again while rows are read.
        self.assertEqual(provider.get_access_token.call_count, 1)
        provider.force_refresh.assert_not_called()

    @mock.patch.object(_client_mod, "_session_for_config")
    def test_401_on_report_retries_once_with_force_refresh(self, mock_session_for_config):
        """Initial 401 on the report URL refreshes token and retries a single new GET."""
        body = _minimal_report_json_bytes()
        unauthorized = _StreamingReportResponse(b"", status_code=401)
        ok = _StreamingReportResponse(body)

        session = mock.Mock()
        session.get.side_effect = [_GetContext(unauthorized), _GetContext(ok)]
        session.headers = {}

        provider = mock.Mock()
        provider.get_access_token = mock.Mock(side_effect=["stale-token", "fresh-token"])
        provider.force_refresh = mock.Mock()

        def apply_to_session(sess):
            sess.headers["Authorization"] = "Bearer " + provider.get_access_token()

        provider.apply_to_session = apply_to_session

        config = {
            "auth_type": "oauth",
            "client_id": "id",
            "client_secret": "sec",
            "token_url": "https://example/token",
            "refresh_token": "rt",
            "oauth_grant_type": "refresh_token",
        }

        def fake_session_for_config(_cfg):
            provider.apply_to_session(session)
            return session, provider

        mock_session_for_config.side_effect = fake_session_for_config

        records = list(stream_report("https://example/report", config))

        self.assertEqual(len(records), 3)
        self.assertEqual(session.get.call_count, 2)
        provider.force_refresh.assert_called_once()
        self.assertEqual(provider.get_access_token.call_count, 1)


if __name__ == "__main__":
    unittest.main()
